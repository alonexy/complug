package nats

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alonexy/complug/components/queue"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOptionalHeartbeatUsesSDKDefaultsWhenOmitted(t *testing.T) {
	cfg := defaultConfig[[]byte]()
	sess := &session{cfg: cfg.Config}
	options := natsgo.GetDefaultOptions()
	for _, option := range sess.connectionOptions() {
		if err := option(&options); err != nil {
			t.Fatal(err)
		}
	}
	if options.PingInterval != natsgo.DefaultPingInterval {
		t.Fatalf("default ping interval = %s", options.PingInterval)
	}
	cons := &consumer[[]byte]{cfg: typedConfig[[]byte]{Config: cfg.Config}}
	if opts := cons.pullOptions(); len(opts) != 1 {
		t.Fatalf("omitted heartbeat must leave SDK pull defaults unchanged: %v", opts)
	}
}

func TestOptionalHeartbeatReachesSDKOptions(t *testing.T) {
	cfg := defaultConfig[[]byte]()
	WithPingInterval(3 * time.Second)(&cfg)
	WithPullHeartbeat(time.Second)(&cfg)
	sess := &session{cfg: cfg.Config}
	options := natsgo.GetDefaultOptions()
	for _, option := range sess.connectionOptions() {
		if err := option(&options); err != nil {
			t.Fatal(err)
		}
	}
	if options.PingInterval != 3*time.Second {
		t.Fatalf("ping interval = %s, want 3s", options.PingInterval)
	}
	cons := &consumer[[]byte]{cfg: typedConfig[[]byte]{Config: cfg.Config}}
	opts := cons.pullOptions()
	if len(opts) != 2 || opts[1] != jetstream.PullHeartbeat(time.Second) {
		t.Fatalf("pull heartbeat was not forwarded: %v", opts)
	}
}

func TestInvalidHeartbeatFailsBeforeConnecting(t *testing.T) {
	for _, option := range []Option{
		WithPingInterval(-time.Second),
		WithPullHeartbeat(-time.Second),
		WithPullHeartbeat(499 * time.Millisecond),
		WithPullHeartbeat(16 * time.Second),
		WithSetupTimeout(-time.Second),
	} {
		_, err := NewNATSConsumer[[]byte](WithURL("nats://unused:4222"),
			WithSubject("events"), WithStream("EVENTS"), WithDurable("consumer"), option)
		if err == nil || (!strings.Contains(err.Error(), "heartbeat") && !strings.Contains(err.Error(), "must not be negative")) {
			t.Fatalf("invalid timing did not fail before connecting: %v", err)
		}
	}
}

func TestOptionalSetupTimeoutPreservesDialTimeoutDefault(t *testing.T) {
	for _, test := range []struct {
		setup time.Duration
		want  time.Duration
	}{
		{setup: 0, want: 3 * time.Second},
		{setup: time.Second, want: time.Second},
	} {
		cfg := typedConfig[[]byte]{Config: Config{WarmUp: true, DialTimeout: 3 * time.Second, SetupTimeout: test.setup}}
		err := warmUpIfEnabled(cfg, func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			remaining := time.Until(deadline)
			if !ok || remaining > test.want || remaining < test.want-100*time.Millisecond {
				t.Fatalf("warm up deadline remaining = %s, want %s", remaining, test.want)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestJetStreamOptionalHeartbeatPublishesAndConsumes(t *testing.T) {
	for _, heartbeat := range []time.Duration{500 * time.Millisecond, 15 * time.Second} {
		t.Run(heartbeat.String(), func(t *testing.T) {
			stream, subject, suffix := uniqueStreamSubject("heartbeat")
			deleteLocalStream(t, stream)
			provider, err := NewNATSProvider[string](
				WithURL(localIntegrationURL), WithStream(stream), WithSubject(subject),
				WithSubjects(subject), WithDurable("heartbeat-"+suffix), WithAutoCreateStream(true),
				WithPingInterval(time.Second), WithPullHeartbeat(heartbeat), WithSetupTimeout(5*time.Second),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer provider.Producer().Close()
			defer provider.Consumer().Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := provider.Producer().Send(ctx, queue.Message[string]{Value: "heartbeat-configured"}); err != nil {
				t.Fatal(err)
			}
			message, err := provider.Consumer().Receive(ctx)
			if err != nil || message.Value != "heartbeat-configured" {
				t.Fatalf("receive = %q, error = %v", message.Value, err)
			}
			if err := provider.Consumer().Commit(ctx, message); err != nil {
				t.Fatal(err)
			}
		})
	}
}
