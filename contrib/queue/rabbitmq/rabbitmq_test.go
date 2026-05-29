package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alonexy/complug/components/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	if cfg.ExchangeType != "direct" {
		t.Fatalf("expected default ExchangeType=direct, got %s", cfg.ExchangeType)
	}
	if !cfg.Durable {
		t.Fatalf("expected default Durable=true")
	}
	if cfg.PrefetchCount != 20 {
		t.Fatalf("expected default PrefetchCount=20, got %d", cfg.PrefetchCount)
	}
	if cfg.ReconnectBackoff != time.Second {
		t.Fatalf("expected default ReconnectBackoff=1s, got %s", cfg.ReconnectBackoff)
	}
	if cfg.ReconnectMaxBackoff != 30*time.Second {
		t.Fatalf("expected default ReconnectMaxBackoff=30s, got %s", cfg.ReconnectMaxBackoff)
	}
	if cfg.DecodeErrorStrategy != DecodeErrorReturn {
		t.Fatalf("expected default DecodeErrorStrategy=DecodeErrorReturn, got %v", cfg.DecodeErrorStrategy)
	}
	if !cfg.WarmUp {
		t.Fatalf("expected default WarmUp=true")
	}
}

func TestWithOptionsApplies(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	opt := WithOptions(
		WithURL("amqp://guest:guest@localhost:5672/"),
		WithExchange("ex", "topic"),
		WithRoutingKey("rk"),
		WithQueueName("q"),
		WithConsumerTag("tag"),
		WithAutoAck(true),
		WithDurable(false),
		WithAutoDelete(true),
		WithExclusive(true),
		WithNoWait(true),
		WithPrefetchCount(11),
		WithMaxChanSize(12),
		WithDialTimeout(2*time.Second),
		WithReconnectBackoff(3*time.Second),
		WithReconnectMaxBackoff(4*time.Second),
		WithReconnectForever(false),
		WithMaxRetries(5),
		WithDecodeErrorStrategy(DecodeErrorNackDrop),
		WithWarmUp(false),
	)
	opt(&cfg)

	if cfg.URL == "" || cfg.ExchangeName != "ex" || cfg.ExchangeType != "topic" {
		t.Fatalf("unexpected URL/exchange: %s/%s/%s", cfg.URL, cfg.ExchangeName, cfg.ExchangeType)
	}
	if cfg.RoutingKey != "rk" || cfg.QueueName != "q" || cfg.ConsumerTag != "tag" {
		t.Fatalf("unexpected routing/queue/tag: %s/%s/%s", cfg.RoutingKey, cfg.QueueName, cfg.ConsumerTag)
	}
	if !cfg.AutoAck {
		t.Fatalf("expected AutoAck=true")
	}
	if cfg.Durable || !cfg.AutoDelete || !cfg.Exclusive || !cfg.NoWait {
		t.Fatalf("unexpected durable/autoDelete/exclusive/noWait: %v/%v/%v/%v", cfg.Durable, cfg.AutoDelete, cfg.Exclusive, cfg.NoWait)
	}
	if cfg.PrefetchCount != 11 || cfg.MaxChanSize != 12 {
		t.Fatalf("unexpected prefetch/maxChanSize: %d/%d", cfg.PrefetchCount, cfg.MaxChanSize)
	}
	if cfg.DialTimeout != 2*time.Second {
		t.Fatalf("unexpected dial timeout: %s", cfg.DialTimeout)
	}
	if cfg.ReconnectBackoff != 3*time.Second || cfg.ReconnectMaxBackoff != 4*time.Second {
		t.Fatalf("unexpected reconnect backoff: %s/%s", cfg.ReconnectBackoff, cfg.ReconnectMaxBackoff)
	}
	if cfg.ReconnectForever || cfg.MaxRetries != 5 {
		t.Fatalf("unexpected retry config: %v/%d", cfg.ReconnectForever, cfg.MaxRetries)
	}
	if cfg.DecodeErrorStrategy != DecodeErrorNackDrop {
		t.Fatalf("unexpected decode error strategy: %v", cfg.DecodeErrorStrategy)
	}
	if cfg.WarmUp {
		t.Fatalf("expected WarmUp=false")
	}
}

func TestWithArgsApplies(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	exArgs := amqp.Table{"alternate-exchange": "fallback"}
	qArgs := amqp.Table{"x-message-ttl": int32(1000)}
	bArgs := amqp.Table{"x-match": "all"}
	WithExchangeArgs(exArgs)(&cfg)
	WithQueueArgs(qArgs)(&cfg)
	WithBindingArgs(bArgs)(&cfg)
	if cfg.ExchangeArgs["alternate-exchange"] != "fallback" {
		t.Fatalf("unexpected exchange args: %#v", cfg.ExchangeArgs)
	}
	if cfg.QueueArgs["x-message-ttl"] != int32(1000) {
		t.Fatalf("unexpected queue args: %#v", cfg.QueueArgs)
	}
	if cfg.BindingArgs["x-match"] != "all" {
		t.Fatalf("unexpected binding args: %#v", cfg.BindingArgs)
	}
}

func TestToTypedConfigMismatch(t *testing.T) {
	cfg := defaultConfig[string]()
	cfg.Decoder = queue.JSONCodec[int]{}
	_, err := toTypedConfig[string](cfg)
	if err == nil {
		t.Fatalf("expected decoder type mismatch error")
	}
}

func TestNewRabbitProviderWarmsUpByDefault(t *testing.T) {
	_, err := NewRabbitProvider[struct{}](
		WithURL("amqp://guest:guest@127.0.0.1:1/"),
		WithDialTimeout(10*time.Millisecond),
	)
	if err == nil {
		t.Fatalf("expected default warm up to return connection error")
	}
}

func TestRabbitWarmUpCanBeCalledExplicitly(t *testing.T) {
	provider, err := NewRabbitProvider[struct{}](
		WithURL("amqp://guest:guest@127.0.0.1:1/"),
		WithDialTimeout(10*time.Millisecond),
		WithWarmUp(false),
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

func TestNewRabbitProducerWarmsUpByDefault(t *testing.T) {
	_, err := NewRabbitProducer[struct{}](
		WithURL("amqp://guest:guest@127.0.0.1:1/"),
		WithDialTimeout(10*time.Millisecond),
	)
	if err == nil {
		t.Fatalf("expected default producer warm up to return connection error")
	}
}

func TestRabbitProducerWarmUpRejectsClosedProducer(t *testing.T) {
	producer, err := NewRabbitProducer[struct{}](
		WithURL("amqp://guest:guest@127.0.0.1:1/"),
		WithWarmUp(false),
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
