package nats

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alonexy/complug/components/queue"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const localIntegrationURL = "nats://127.0.0.1:4222"

type integrationPayload struct {
	ID string `json:"id"`
}

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	if cfg.Mode != JetStream {
		t.Fatalf("expected default Mode=JetStream, got %v", cfg.Mode)
	}
	if cfg.URL != DefaultURL {
		t.Fatalf("expected default URL=%s, got %s", DefaultURL, cfg.URL)
	}
	if cfg.MaxChanSize != 64 {
		t.Fatalf("expected default MaxChanSize=64, got %d", cfg.MaxChanSize)
	}
	if cfg.PullMaxMessages != 1 {
		t.Fatalf("expected default PullMaxMessages=1, got %d", cfg.PullMaxMessages)
	}
	if cfg.AckWait != 30*time.Second {
		t.Fatalf("expected default AckWait=30s, got %s", cfg.AckWait)
	}
	if cfg.MaxDeliver != 5 {
		t.Fatalf("expected default MaxDeliver=5, got %d", cfg.MaxDeliver)
	}
	if cfg.ReconnectBackoff != time.Second {
		t.Fatalf("expected default ReconnectBackoff=1s, got %s", cfg.ReconnectBackoff)
	}
	if cfg.ReconnectMaxBackoff != 30*time.Second {
		t.Fatalf("expected default ReconnectMaxBackoff=30s, got %s", cfg.ReconnectMaxBackoff)
	}
	if cfg.AsyncPublishAckHandler == nil {
		t.Fatalf("expected default AsyncPublishAckHandler to be set")
	}
}

func TestWithOptionsApplies(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	opt := WithOptions(
		WithMode(Core),
		WithURL("nats://demo:4222"),
		WithSubject("events.created"),
		WithSubjects("events.*", "audit.*"),
		WithQueueGroup("workers"),
		WithStream("EVENTS"),
		WithDurable("events-worker"),
		WithFilterSubject("events.created"),
		WithConsumerMode(PushConsumer),
		WithDeliverSubject("deliver.events.created"),
		WithDeliverGroup("push-workers"),
		WithReplayPolicy(ReplayOriginal),
		WithAutoCreateStream(true),
		WithStreamMaxAge(time.Hour),
		WithStreamMaxBytes(1024),
		WithDuplicateWindow(time.Minute),
		WithAckWait(2*time.Minute),
		WithMaxDeliver(10),
		WithPullMaxMessages(5),
		WithDoubleAck(true),
		WithMaxChanSize(9),
		WithDialTimeout(2*time.Second),
		WithReconnectBackoff(3*time.Second),
		WithReconnectMaxBackoff(4*time.Second),
		WithReconnectForever(false),
		WithMaxRetries(6),
		WithMsgIDFromKey(true),
		WithAsyncPublish(true),
		WithAsyncPublishAckHandler(func(jetstream.PubAckFuture) {}),
	)
	opt(&cfg)

	if cfg.Mode != Core || cfg.URL != "nats://demo:4222" {
		t.Fatalf("unexpected mode/url: %v/%s", cfg.Mode, cfg.URL)
	}
	if cfg.Subject != "events.created" || len(cfg.Subjects) != 2 || cfg.Subjects[0] != "events.*" {
		t.Fatalf("unexpected subjects: %s/%#v", cfg.Subject, cfg.Subjects)
	}
	if cfg.QueueGroup != "workers" || cfg.Stream != "EVENTS" || cfg.Durable != "events-worker" {
		t.Fatalf("unexpected queue/stream/durable: %s/%s/%s", cfg.QueueGroup, cfg.Stream, cfg.Durable)
	}
	if cfg.FilterSubject != "events.created" || !cfg.AutoCreateStream {
		t.Fatalf("unexpected filter/auto-create: %s/%v", cfg.FilterSubject, cfg.AutoCreateStream)
	}
	if cfg.ConsumerMode != PushConsumer || cfg.DeliverSubject != "deliver.events.created" || cfg.DeliverGroup != "push-workers" {
		t.Fatalf("unexpected push consumer config: %v/%s/%s", cfg.ConsumerMode, cfg.DeliverSubject, cfg.DeliverGroup)
	}
	if cfg.ReplayPolicy != ReplayOriginal {
		t.Fatalf("unexpected replay policy: %v", cfg.ReplayPolicy)
	}
	if cfg.StreamMaxAge != time.Hour || cfg.StreamMaxBytes != 1024 || cfg.DuplicateWindow != time.Minute {
		t.Fatalf("unexpected stream limits: %s/%d/%s", cfg.StreamMaxAge, cfg.StreamMaxBytes, cfg.DuplicateWindow)
	}
	if cfg.AckWait != 2*time.Minute || cfg.MaxDeliver != 10 || cfg.PullMaxMessages != 5 || !cfg.DoubleAck {
		t.Fatalf("unexpected consumer ack config")
	}
	if cfg.MaxChanSize != 9 || cfg.DialTimeout != 2*time.Second {
		t.Fatalf("unexpected common config: %d/%s", cfg.MaxChanSize, cfg.DialTimeout)
	}
	if cfg.ReconnectBackoff != 3*time.Second || cfg.ReconnectMaxBackoff != 4*time.Second {
		t.Fatalf("unexpected reconnect backoff: %s/%s", cfg.ReconnectBackoff, cfg.ReconnectMaxBackoff)
	}
	if cfg.ReconnectForever || cfg.MaxRetries != 6 || !cfg.MsgIDFromKey {
		t.Fatalf("unexpected retry/msg-id config")
	}
	if !cfg.AsyncPublish || cfg.AsyncPublishAckHandler == nil {
		t.Fatalf("unexpected async publish config")
	}
}

func TestWithSubjectsCopies(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	input := []string{"events.*", "audit.*"}
	WithSubjects(input...)(&cfg)
	input[0] = "changed"
	if cfg.Subjects[0] != "events.*" {
		t.Fatalf("subjects slice should be copied, got %#v", cfg.Subjects)
	}
}

func TestAuthOptionsAreSemanticAndExclusive(t *testing.T) {
	userCfg := defaultConfig[struct{}]()
	WithAuthUserInfo("user", "pass")(&userCfg)
	if userCfg.AuthUser != "user" || userCfg.AuthPassword != "pass" {
		t.Fatalf("unexpected user auth config")
	}

	tokenCfg := defaultConfig[struct{}]()
	WithAuthToken("token")(&tokenCfg)
	if tokenCfg.AuthToken != "token" {
		t.Fatalf("unexpected token auth config")
	}

	cfg := defaultConfig[struct{}]()
	WithAuthUserInfo("user", "pass")(&cfg)
	WithAuthToken("token")(&cfg)
	if err := validateConfig(cfg); !errors.Is(err, ErrAuthConflict) {
		t.Fatalf("expected ErrAuthConflict, got %v", err)
	}
}

func TestValidateAuthRejectsEmptyValues(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	WithAuthUserInfo("", "pass")(&cfg)
	if err := validateConfig(cfg); !errors.Is(err, ErrInvalidAuth) {
		t.Fatalf("expected ErrInvalidAuth for empty user, got %v", err)
	}

	cfg = defaultConfig[struct{}]()
	WithAuthToken("")(&cfg)
	if err := validateConfig(cfg); !errors.Is(err, ErrInvalidAuth) {
		t.Fatalf("expected ErrInvalidAuth for empty token, got %v", err)
	}
}

func TestValidateModeSpecificRequiredFields(t *testing.T) {
	coreCfg := defaultConfig[struct{}]()
	WithMode(Core)(&coreCfg)
	if err := validateConfig(coreCfg); !errors.Is(err, ErrSubjectRequired) {
		t.Fatalf("expected ErrSubjectRequired for core mode, got %v", err)
	}

	jsCfg := defaultConfig[struct{}]()
	WithMode(JetStream)(&jsCfg)
	WithSubject("events.created")(&jsCfg)
	if err := validateConfig(jsCfg); !errors.Is(err, ErrStreamRequired) {
		t.Fatalf("expected ErrStreamRequired for jetstream mode, got %v", err)
	}
}

func TestToTypedConfigMismatch(t *testing.T) {
	cfg := defaultConfig[string]()
	cfg.Encoder = queue.JSONCodec[int]{}
	_, err := toTypedConfig[string](cfg)
	if err == nil {
		t.Fatalf("expected encoder type mismatch error")
	}
}

func TestCoreCommitIsNoop(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	WithMode(Core)(&cfg)
	typedCfg, err := toTypedConfig[struct{}](cfg)
	if err != nil {
		t.Fatalf("toTypedConfig error: %v", err)
	}
	consumer := &consumer[struct{}]{cfg: typedCfg}
	if err := consumer.Commit(context.Background(), queue.Message[struct{}]{}); err != nil {
		t.Fatalf("core commit should be no-op, got %v", err)
	}
}

func TestJetStreamPushCommitRejectsInvalidRawMessage(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	WithMode(JetStream)(&cfg)
	WithConsumerMode(PushConsumer)(&cfg)
	typedCfg, err := toTypedConfig[struct{}](cfg)
	if err != nil {
		t.Fatalf("toTypedConfig error: %v", err)
	}
	consumer := &consumer[struct{}]{cfg: typedCfg}
	err = consumer.Commit(context.Background(), queue.Message[struct{}]{Raw: "invalid"})
	if !errors.Is(err, queue.ErrInvalidMessage) {
		t.Fatalf("expected ErrInvalidMessage, got %v", err)
	}
}

func TestJetStreamCommitRejectsInvalidRawMessage(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	WithMode(JetStream)(&cfg)
	typedCfg, err := toTypedConfig[struct{}](cfg)
	if err != nil {
		t.Fatalf("toTypedConfig error: %v", err)
	}
	consumer := &consumer[struct{}]{cfg: typedCfg}
	err = consumer.Commit(context.Background(), queue.Message[struct{}]{Raw: "invalid"})
	if !errors.Is(err, queue.ErrInvalidMessage) {
		t.Fatalf("expected ErrInvalidMessage, got %v", err)
	}
}

func TestPublishJetStreamUsesSyncByDefault(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	typedCfg, err := toTypedConfig[struct{}](cfg)
	if err != nil {
		t.Fatalf("toTypedConfig error: %v", err)
	}
	publisher := &fakePublisher{}
	producer := &producer[struct{}]{cfg: typedCfg}

	if err := producer.publishJetStream(context.Background(), publisher, &natsgo.Msg{}); err != nil {
		t.Fatalf("publishJetStream error: %v", err)
	}

	if publisher.syncCalls != 1 || publisher.asyncCalls != 0 {
		t.Fatalf("expected sync publish only, got sync=%d async=%d", publisher.syncCalls, publisher.asyncCalls)
	}
}

func TestPublishJetStreamUsesAsyncWhenConfigured(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	WithAsyncPublish(true)(&cfg)
	handlerCalls := 0
	WithAsyncPublishAckHandler(func(future jetstream.PubAckFuture) {
		handlerCalls++
		if future.Msg() == nil {
			t.Fatalf("expected future message")
		}
	})(&cfg)
	typedCfg, err := toTypedConfig[struct{}](cfg)
	if err != nil {
		t.Fatalf("toTypedConfig error: %v", err)
	}
	publisher := &fakePublisher{}
	producer := &producer[struct{}]{cfg: typedCfg}

	if err := producer.publishJetStream(context.Background(), publisher, &natsgo.Msg{}); err != nil {
		t.Fatalf("publishJetStream error: %v", err)
	}

	if publisher.syncCalls != 0 || publisher.asyncCalls != 1 {
		t.Fatalf("expected async publish only, got sync=%d async=%d", publisher.syncCalls, publisher.asyncCalls)
	}
	if handlerCalls != 1 {
		t.Fatalf("expected ack handler to be called once, got %d", handlerCalls)
	}
}

func TestDefaultAsyncPublishAckHandlerDrainsResult(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	future := fakePubAckFuture{msg: &natsgo.Msg{}}

	cfg.AsyncPublishAckHandler(future)
}

func TestNewNATSProviderWarmsUpByDefault(t *testing.T) {
	_, err := NewNATSProvider[integrationPayload](
		WithURL("nats://127.0.0.1:1"),
		WithSubject("events.created"),
		WithStream("EVENTS"),
		WithDurable("events-worker"),
		WithDialTimeout(10*time.Millisecond),
		WithCodec[integrationPayload](queue.JSONCodec[integrationPayload]{}),
	)
	if err == nil {
		t.Fatalf("expected default warm up to return connection error")
	}
}

func TestWarmUpCanBeCalledExplicitly(t *testing.T) {
	provider, err := NewNATSProvider[integrationPayload](
		WithURL("nats://127.0.0.1:1"),
		WithSubject("events.created"),
		WithStream("EVENTS"),
		WithDurable("events-worker"),
		WithDialTimeout(10*time.Millisecond),
		WithWarmUp(false),
		WithCodec[integrationPayload](queue.JSONCodec[integrationPayload]{}),
	)
	if err != nil {
		t.Fatalf("provider should not connect when warm up is disabled: %v", err)
	}

	warm, ok := provider.(interface {
		WarmUp(context.Context) error
	})
	if !ok {
		t.Fatalf("provider should expose WarmUp")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := warm.WarmUp(ctx); err == nil {
		t.Fatalf("expected explicit warm up to return connection error")
	}
}

func TestNewNATSProducerWarmsUpByDefault(t *testing.T) {
	_, err := NewNATSProducer[integrationPayload](
		WithURL("nats://127.0.0.1:1"),
		WithSubject("events.created"),
		WithStream("EVENTS"),
		WithDurable("events-worker"),
		WithDialTimeout(10*time.Millisecond),
		WithCodec[integrationPayload](queue.JSONCodec[integrationPayload]{}),
	)
	if err == nil {
		t.Fatalf("expected default producer warm up to return connection error")
	}
}

func TestNATSProducerWarmUpRejectsClosedProducer(t *testing.T) {
	producer, err := NewNATSProducer[integrationPayload](
		WithURL("nats://127.0.0.1:1"),
		WithSubject("events.created"),
		WithStream("EVENTS"),
		WithDurable("events-worker"),
		WithWarmUp(false),
		WithCodec[integrationPayload](queue.JSONCodec[integrationPayload]{}),
	)
	if err != nil {
		t.Fatalf("producer should not connect when warm up is disabled: %v", err)
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("close producer: %v", err)
	}

	warm, ok := producer.(interface {
		WarmUp(context.Context) error
	})
	if !ok {
		t.Fatalf("producer should expose WarmUp")
	}
	if err := warm.WarmUp(context.Background()); !errors.Is(err, queue.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestJetStreamPushConsumerWithoutDeliverGroupFanOut(t *testing.T) {
	stream, subject, suffix := uniqueStreamSubject("fanout")
	deleteLocalStream(t, stream)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	consumerA, err := NewNATSProvider[integrationPayload](
		localPushOptions(stream, subject, "durableA"+suffix, "deliver."+subject+".a", "")...,
	)
	if err != nil {
		t.Fatalf("consumer A provider error: %v", err)
	}
	defer consumerA.Consumer().Close()
	defer consumerA.Producer().Close()

	consumerB, err := NewNATSProvider[integrationPayload](
		localPushOptions(stream, subject, "durableB"+suffix, "deliver."+subject+".b", "")...,
	)
	if err != nil {
		t.Fatalf("consumer B provider error: %v", err)
	}
	defer consumerB.Consumer().Close()
	defer consumerB.Producer().Close()

	received := make(chan receivedIntegrationMessage, 8)
	errs := make(chan error, 2)
	go collectIntegrationMessages(ctx, 0, consumerA.Consumer(), received, errs)
	go collectIntegrationMessages(ctx, 1, consumerB.Consumer(), received, errs)

	time.Sleep(300 * time.Millisecond)
	publishIntegrationMessages(t, ctx, consumerA.Producer(), 2)

	counts := map[int]int{}
	for counts[0] < 2 || counts[1] < 2 {
		select {
		case msg := <-received:
			counts[msg.consumerID]++
		case err := <-errs:
			t.Fatalf("receive error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for fan-out messages; counts=%v", counts)
		}
	}
}

func TestJetStreamPushConsumerWithDeliverGroupLoadBalances(t *testing.T) {
	stream, subject, suffix := uniqueStreamSubject("group")
	deleteLocalStream(t, stream)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := localPushOptions(stream, subject, "durable"+suffix, "deliver."+subject, "workers"+suffix)
	consumerA, err := NewNATSProvider[integrationPayload](opts...)
	if err != nil {
		t.Fatalf("consumer A provider error: %v", err)
	}
	defer consumerA.Consumer().Close()
	defer consumerA.Producer().Close()

	consumerB, err := NewNATSProvider[integrationPayload](opts...)
	if err != nil {
		t.Fatalf("consumer B provider error: %v", err)
	}
	defer consumerB.Consumer().Close()
	defer consumerB.Producer().Close()

	received := make(chan receivedIntegrationMessage, 16)
	errs := make(chan error, 2)
	go collectIntegrationMessages(ctx, 0, consumerA.Consumer(), received, errs)
	go collectIntegrationMessages(ctx, 1, consumerB.Consumer(), received, errs)

	time.Sleep(300 * time.Millisecond)
	publishIntegrationMessages(t, ctx, consumerA.Producer(), 6)

	seen := map[string]int{}
	for len(seen) < 6 {
		select {
		case msg := <-received:
			seen[msg.messageID]++
			if seen[msg.messageID] > 1 {
				t.Fatalf("message %s was delivered more than once with deliver group; seen=%v", msg.messageID, seen)
			}
		case err := <-errs:
			t.Fatalf("receive error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for grouped messages; seen=%v", seen)
		}
	}
}

func TestJetStreamProducerSendElapsed(t *testing.T) {
	tests := []struct {
		name         string
		asyncPublish bool
	}{
		{name: "sync", asyncPublish: false},
		{name: "async", asyncPublish: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream, subject, suffix := uniqueStreamSubject("send_elapsed_" + tt.name)
			deleteLocalStream(t, stream)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			provider, err := NewNATSProvider[integrationPayload](
				WithMode(JetStream),
				WithURL(localIntegrationURL),
				WithStream(stream),
				WithSubjects(subject),
				WithSubject(subject),
				WithDurable("send-elapsed-"+suffix),
				WithFilterSubject(subject),
				WithAutoCreateStream(true),
				WithMsgIDFromKey(true),
				WithAsyncPublish(tt.asyncPublish),
				WithCodec[integrationPayload](queue.JSONCodec[integrationPayload]{}),
			)
			if err != nil {
				t.Fatalf("provider error: %v", err)
			}
			defer provider.Consumer().Close()
			defer provider.Producer().Close()

			const count = 10
			var total time.Duration
			var warmTotal time.Duration
			var min time.Duration
			var max time.Duration
			for i := 0; i < count; i++ {
				id := fmt.Sprintf("send-elapsed-%s-%d", tt.name, i)
				start := time.Now()
				if err := provider.Producer().Send(ctx, queue.Message[integrationPayload]{
					Key:   id,
					Value: integrationPayload{ID: id},
				}); err != nil {
					t.Fatalf("send %s error: %v", id, err)
				}
				elapsed := time.Since(start)
				total += elapsed
				if i == 0 || elapsed < min {
					min = elapsed
				}
				if elapsed > max {
					max = elapsed
				}
				if i > 0 {
					warmTotal += elapsed
				}
				t.Logf("Producer.Send async=%v %s elapsed=%s", tt.asyncPublish, id, elapsed)
			}
			t.Logf(
				"Producer.Send async=%v count=%d total=%s average=%s warm_average=%s min=%s max=%s",
				tt.asyncPublish,
				count,
				total,
				total/count,
				warmTotal/(count-1),
				min,
				max,
			)
		})
	}
}

type receivedIntegrationMessage struct {
	consumerID int
	messageID  string
}

func collectIntegrationMessages(ctx context.Context, consumerID int, consumer queue.Consumer[integrationPayload], out chan<- receivedIntegrationMessage, errs chan<- error) {
	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			if ctx.Err() == nil {
				errs <- err
			}
			return
		}
		if err := consumer.Commit(ctx, msg); err != nil {
			errs <- err
			return
		}
		select {
		case out <- receivedIntegrationMessage{consumerID: consumerID, messageID: msg.Value.ID}:
		case <-ctx.Done():
			return
		}
	}
}

func publishIntegrationMessages(t *testing.T, ctx context.Context, producer queue.Producer[integrationPayload], count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("msg-%d", i)
		if err := producer.Send(ctx, queue.Message[integrationPayload]{
			Key:   id,
			Value: integrationPayload{ID: id},
		}); err != nil {
			t.Fatalf("publish %s error: %v", id, err)
		}
	}
}

func localPushOptions(stream, subject, durable, deliverSubject, deliverGroup string) []Option {
	opts := []Option{
		WithMode(JetStream),
		WithConsumerMode(PushConsumer),
		WithURL(localIntegrationURL),
		WithStream(stream),
		WithSubjects(subject),
		WithSubject(subject),
		WithDurable(durable),
		WithFilterSubject(subject),
		WithDeliverSubject(deliverSubject),
		WithReplayPolicy(ReplayInstant),
		WithAutoCreateStream(true),
		WithMsgIDFromKey(true),
		WithCodec[integrationPayload](queue.JSONCodec[integrationPayload]{}),
	}
	if deliverGroup != "" {
		opts = append(opts, WithDeliverGroup(deliverGroup))
	}
	return opts
}

func uniqueStreamSubject(prefix string) (string, string, string) {
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	return "NATS_PUSH_" + prefix + "_" + suffix, "test.push." + prefix + "." + suffix, suffix
}

func deleteLocalStream(t *testing.T, stream string) {
	t.Helper()
	nc, err := natsgo.Connect(localIntegrationURL, natsgo.Timeout(time.Second))
	if err != nil {
		t.Fatalf("connect local nats %s: %v", localIntegrationURL, err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("create local jetstream context: %v", err)
	}
	_ = js.DeleteStream(stream)
	t.Cleanup(func() {
		_ = js.DeleteStream(stream)
	})
}

type fakePublisher struct {
	syncCalls  int
	asyncCalls int
}

func (f *fakePublisher) PublishMsg(context.Context, *natsgo.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	f.syncCalls++
	return &jetstream.PubAck{}, nil
}

func (f *fakePublisher) PublishMsgAsync(msg *natsgo.Msg, _ ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	f.asyncCalls++
	return fakePubAckFuture{msg: msg}, nil
}

type fakePubAckFuture struct {
	msg *natsgo.Msg
}

func (f fakePubAckFuture) Ok() <-chan *jetstream.PubAck {
	ch := make(chan *jetstream.PubAck, 1)
	ch <- &jetstream.PubAck{}
	close(ch)
	return ch
}

func (f fakePubAckFuture) Err() <-chan error {
	ch := make(chan error)
	close(ch)
	return ch
}

func (f fakePubAckFuture) Msg() *natsgo.Msg {
	return f.msg
}
