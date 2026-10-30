package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alonexy/complug/components/queue"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestReconnectDelayUsesExponentialBackoffAndConfiguredLimit(t *testing.T) {
	cfg := defaultConfig[string]()
	WithReconnectBackoff(100 * time.Millisecond)(&cfg)
	WithReconnectMaxBackoff(500 * time.Millisecond)(&cfg)
	WithReconnectForever(false)(&cfg)
	WithMaxRetries(0)(&cfg)
	options := natsgo.GetDefaultOptions()
	for _, option := range (&session{cfg: cfg.Config}).connectionOptions() {
		if err := option(&options); err != nil {
			t.Fatal(err)
		}
	}
	if options.MaxReconnect != 0 {
		t.Fatalf("connection repair changed MaxRetries=0: %d", options.MaxReconnect)
	}
	if options.CustomReconnectDelayCB == nil {
		t.Fatal("ReconnectMaxBackoff requires an exponential reconnect callback")
	}
	for _, test := range []struct {
		attempt int
		ceiling time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 500 * time.Millisecond},
		{int(^uint(0) >> 1), 500 * time.Millisecond},
	} {
		for sample := 0; sample < 64; sample++ {
			delay := options.CustomReconnectDelayCB(test.attempt)
			if delay < test.ceiling*4/5 || delay > test.ceiling {
				t.Fatalf("SDK attempt %d delay=%s, want 80%%-100%% of %s", test.attempt, delay, test.ceiling)
			}
		}
	}
}

func TestReconnectDelayPreservesExplicitZeroTiming(t *testing.T) {
	for _, setting := range []Option{WithReconnectBackoff(0), WithReconnectMaxBackoff(0)} {
		cfg := defaultConfig[string]()
		setting(&cfg)
		options := natsgo.GetDefaultOptions()
		for _, option := range (&session{cfg: cfg.Config}).connectionOptions() {
			if err := option(&options); err != nil {
				t.Fatal(err)
			}
		}
		if options.CustomReconnectDelayCB != nil || options.ReconnectJitter != natsgo.DefaultReconnectJitter {
			t.Fatal("explicit zero timing must preserve SDK wait and jitter instead of introducing an immediate retry loop")
		}
	}
}

type recoveryErrorPublisher struct {
	err                   error
	syncCalls, asyncCalls int
}

func (p *recoveryErrorPublisher) PublishMsg(context.Context, *natsgo.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.syncCalls++
	return nil, p.err
}

func (p *recoveryErrorPublisher) PublishMsgAsync(*natsgo.Msg, ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	p.asyncCalls++
	return nil, p.err
}

func TestPublishFailureCallsPublisherOnceWithoutReplayingMessage(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%t", async), func(t *testing.T) {
			cfg := defaultConfig[string]()
			WithReconnectForever(false)(&cfg)
			WithMaxRetries(0)(&cfg)
			WithAsyncPublish(async)(&cfg)
			ackCalls := 0
			WithAsyncPublishAckHandler(func(jetstream.PubAckFuture) { ackCalls++ })(&cfg)
			typed, err := toTypedConfig[string](cfg)
			if err != nil {
				t.Fatal(err)
			}
			failure := errors.New("synthetic publisher failure")
			publisher := &recoveryErrorPublisher{err: failure}
			producer := &producer[string]{cfg: typed}
			if err := producer.publishJetStream(context.Background(), publisher, &natsgo.Msg{Data: []byte("original")}); !errors.Is(err, failure) {
				t.Fatalf("publish failure changed: %v", err)
			}
			wantSync, wantAsync := 1, 0
			if async {
				wantSync, wantAsync = 0, 1
			}
			if publisher.syncCalls != wantSync || publisher.asyncCalls != wantAsync || ackCalls != 0 {
				t.Fatalf("failed publish was replayed or acknowledged: sync=%d async=%d acks=%d", publisher.syncCalls, publisher.asyncCalls, ackCalls)
			}
		})
	}
}

func newRecoveryProducer(t *testing.T, prefix string) (*producer[string], string) {
	t.Helper()
	stream, subject, _ := uniqueStreamSubject(prefix)
	deleteLocalStream(t, stream)
	endpoint, err := NewNATSProducer[string](
		WithURL(localIntegrationURL), WithStream(stream), WithSubject(subject), WithSubjects(subject),
		WithAutoCreateStream(true), WithWarmUp(true), WithReconnectForever(false), WithMaxRetries(0),
		WithCodec[string](queue.StringCodec{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	return endpoint.(*producer[string]), stream
}

func closedRecoveryConnection(t *testing.T, p *producer[string]) (*natsgo.Conn, jetstream.JetStream) {
	t.Helper()
	p.session.mu.Lock()
	nc, js := p.session.nc, p.session.js
	p.session.mu.Unlock()
	if nc == nil || js == nil || js.Conn() != nc {
		t.Fatal("producer warmup did not cache its connection and JetStream context")
	}
	nc.Close()
	if !nc.IsClosed() {
		t.Fatal("old connection did not close")
	}
	return nc, js
}

func assertRecoveryCacheAndMessages(t *testing.T, ctx context.Context, p *producer[string], oldNC *natsgo.Conn, oldJS jetstream.JetStream, stream string, want map[string]int) {
	t.Helper()
	p.session.mu.Lock()
	nc, js := p.session.nc, p.session.js
	p.session.mu.Unlock()
	if nc == nil || nc == oldNC || nc.IsClosed() || js == nil || js == oldJS || js.Conn() != nc {
		t.Fatal("producer retained or restored the old connection/JetStream context")
	}
	stored, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stored.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	for _, n := range want {
		count += n
	}
	if info.State.Msgs != uint64(count) {
		t.Fatalf("stream message count=%d, want %d explicit sends", info.State.Msgs, count)
	}
	got := make(map[string]int)
	for sequence := info.State.FirstSeq; sequence <= info.State.LastSeq; sequence++ {
		message, err := stored.GetMsg(ctx, sequence)
		if err != nil {
			t.Fatal(err)
		}
		got[string(message.Data)]++
	}
	if len(got) != len(want) {
		t.Fatalf("stream values=%v, want %v", got, want)
	}
	for value, n := range want {
		if got[value] != n {
			t.Fatalf("stream value %q count=%d, want %d; all=%v", value, got[value], n, got)
		}
	}
}

func TestJetStreamProducerReplacesClosedConnectionForExplicitNewSend(t *testing.T) {
	p, stream := newRecoveryProducer(t, "producer_recovery")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Send(ctx, queue.Message[string]{Value: "before"}); err != nil {
		t.Fatal(err)
	}
	oldNC, oldJS := closedRecoveryConnection(t, p)
	if err := p.Send(ctx, queue.Message[string]{Value: "explicit-new-message"}); err != nil {
		t.Fatalf("same producer cannot send a new message after its old connection closed: %v", err)
	}
	assertRecoveryCacheAndMessages(t, ctx, p, oldNC, oldJS, stream, map[string]int{"before": 1, "explicit-new-message": 1})
}

func TestJetStreamProducerConcurrentNewSendsReplaceClosedCache(t *testing.T) {
	p, stream := newRecoveryProducer(t, "concurrent_producer_recovery")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Send(ctx, queue.Message[string]{Value: "before"}); err != nil {
		t.Fatal(err)
	}
	oldNC, oldJS := closedRecoveryConnection(t, p)
	const concurrent = 16
	start := make(chan struct{})
	results := make(chan error, concurrent)
	var group sync.WaitGroup
	want := map[string]int{"before": 1, "after-concurrent": 1}
	for i := 0; i < concurrent; i++ {
		value := fmt.Sprintf("explicit-new-%d", i)
		want[value] = 1
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			results <- p.Send(ctx, queue.Message[string]{Value: value})
		}()
	}
	close(start)
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent explicit send after connection close: %v", err)
		}
	}
	if err := p.Send(ctx, queue.Message[string]{Value: "after-concurrent"}); err != nil {
		t.Fatalf("concurrent initialization restored a stale cached context: %v", err)
	}
	assertRecoveryCacheAndMessages(t, ctx, p, oldNC, oldJS, stream, want)
}
