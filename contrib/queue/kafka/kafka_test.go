package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alonexy/complug/components/queue"
	kafkago "github.com/segmentio/kafka-go"
)

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	if cfg.TopicPartitions != 1 {
		t.Fatalf("expected default TopicPartitions=1, got %d", cfg.TopicPartitions)
	}
	if cfg.TopicReplication != 1 {
		t.Fatalf("expected default TopicReplication=1, got %d", cfg.TopicReplication)
	}
	if cfg.ReconnectBackoff != time.Second {
		t.Fatalf("expected default ReconnectBackoff=1s, got %s", cfg.ReconnectBackoff)
	}
	if cfg.ReconnectMaxBackoff != 30*time.Second {
		t.Fatalf("expected default ReconnectMaxBackoff=30s, got %s", cfg.ReconnectMaxBackoff)
	}
	if cfg.Balancer == nil {
		t.Fatalf("expected default Balancer to be set")
	}
	if _, ok := cfg.Balancer.(*kafkago.LeastBytes); !ok {
		t.Fatalf("expected default Balancer=*kafka.LeastBytes, got %T", cfg.Balancer)
	}
}

func TestWithOptionsApplies(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	opt := WithOptions(
		WithBrokers("b1", "b2"),
		WithTopic("demo"),
		WithGroupID("g1"),
		WithConsumePartition(3),
		WithMaxChanSize(9),
		WithQueueCapacity(10),
		WithMinBytes(1),
		WithMaxBytes(2),
		WithMaxWait(3*time.Second),
		WithReadBatchTimeout(4*time.Second),
		WithGroupBalancers(&kafkago.RangeGroupBalancer{}),
		WithStartOffset(5),
		WithIsolationLevel(kafkago.ReadCommitted),
		WithEnableAutoCommit(true),
		WithAutoCommitInterval(2*time.Second),
		WithReadTimeout(6*time.Second),
		WithWriteTimeout(7*time.Second),
		WithBatchSize(8),
		WithBatchBytes(9),
		WithBatchTimeout(10*time.Second),
		WithAsync(true),
		WithMaxAttempts(11),
		WithReconnectBackoff(2*time.Second),
		WithReconnectMaxBackoff(3*time.Second),
		WithReconnectForever(false),
		WithMaxRetries(7),
		WithAutoCreateTopic(true),
		WithCreateTopicPartitions(4),
		WithTopicReplication(2),
		WithTopicConfig("cleanup.policy", "delete"),
		WithApplyTopicConfigOnExists(true),
	)
	opt(&cfg)

	if len(cfg.Brokers) != 2 || cfg.Brokers[0] != "b1" {
		t.Fatalf("unexpected brokers: %#v", cfg.Brokers)
	}
	if cfg.Topic != "demo" || cfg.GroupID != "g1" {
		t.Fatalf("unexpected topic/group: %s/%s", cfg.Topic, cfg.GroupID)
	}
	if cfg.Partition != 3 {
		t.Fatalf("unexpected partition: %d", cfg.Partition)
	}
	if cfg.MaxChanSize != 9 || cfg.QueueCapacity != 10 {
		t.Fatalf("unexpected sizes: %d/%d", cfg.MaxChanSize, cfg.QueueCapacity)
	}
	if cfg.MinBytes != 1 || cfg.MaxBytes != 2 {
		t.Fatalf("unexpected min/max bytes: %d/%d", cfg.MinBytes, cfg.MaxBytes)
	}
	if cfg.MaxWait != 3*time.Second || cfg.ReadBatchTimeout != 4*time.Second {
		t.Fatalf("unexpected wait/timeout: %s/%s", cfg.MaxWait, cfg.ReadBatchTimeout)
	}
	if len(cfg.GroupBalancers) != 1 {
		t.Fatalf("unexpected group balancers: %v", cfg.GroupBalancers)
	}
	if cfg.StartOffset != 5 || cfg.IsolationLevel != kafkago.ReadCommitted {
		t.Fatalf("unexpected start/isolation: %d/%v", cfg.StartOffset, cfg.IsolationLevel)
	}
	if !cfg.EnableAutoCommit || cfg.AutoCommitInterval != 2*time.Second {
		t.Fatalf("unexpected auto commit config: %v/%s", cfg.EnableAutoCommit, cfg.AutoCommitInterval)
	}
	if cfg.ReadTimeout != 6*time.Second || cfg.WriteTimeout != 7*time.Second {
		t.Fatalf("unexpected read/write timeout: %s/%s", cfg.ReadTimeout, cfg.WriteTimeout)
	}
	if cfg.BatchSize != 8 || cfg.BatchBytes != 9 || cfg.BatchTimeout != 10*time.Second {
		t.Fatalf("unexpected batch config: %d/%d/%s", cfg.BatchSize, cfg.BatchBytes, cfg.BatchTimeout)
	}
	if !cfg.Async || cfg.MaxAttempts != 11 {
		t.Fatalf("unexpected async/maxAttempts: %v/%d", cfg.Async, cfg.MaxAttempts)
	}
	if cfg.ReconnectBackoff != 2*time.Second || cfg.ReconnectMaxBackoff != 3*time.Second {
		t.Fatalf("unexpected reconnect backoff: %s/%s", cfg.ReconnectBackoff, cfg.ReconnectMaxBackoff)
	}
	if cfg.ReconnectForever || cfg.MaxRetries != 7 {
		t.Fatalf("unexpected retry config: %v/%d", cfg.ReconnectForever, cfg.MaxRetries)
	}
	if !cfg.AutoCreateTopic || cfg.TopicPartitions != 4 || cfg.TopicReplication != 2 {
		t.Fatalf("unexpected topic create: %v/%d/%d", cfg.AutoCreateTopic, cfg.TopicPartitions, cfg.TopicReplication)
	}
	if cfg.TopicConfigs["cleanup.policy"] != "delete" {
		t.Fatalf("unexpected topic configs: %#v", cfg.TopicConfigs)
	}
	if !cfg.ApplyTopicConfigOnExists {
		t.Fatalf("expected ApplyTopicConfigOnExists=true")
	}
}

func TestWithBrokersCopies(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	input := []string{"a", "b"}
	WithBrokers(input...)(&cfg)
	input[0] = "c"
	if cfg.Brokers[0] != "a" {
		t.Fatalf("brokers slice should be copied, got %#v", cfg.Brokers)
	}
}

func TestWithGroupBalancersCopies(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	orig := []kafkago.GroupBalancer{&kafkago.RangeGroupBalancer{}}
	WithGroupBalancers(orig...)(&cfg)
	orig[0] = &kafkago.RoundRobinGroupBalancer{}
	if _, ok := cfg.GroupBalancers[0].(*kafkago.RangeGroupBalancer); !ok {
		t.Fatalf("group balancers should be copied")
	}
}

func TestWithCreateTopicPartitionsIgnoreNonPositive(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	WithCreateTopicPartitions(0)(&cfg)
	if cfg.TopicPartitions != 1 {
		t.Fatalf("expected TopicPartitions to remain default=1, got %d", cfg.TopicPartitions)
	}
}

func TestTopicConfigsMerge(t *testing.T) {
	cfg := defaultConfig[struct{}]()
	WithTopicConfig("a", "1")(&cfg)
	WithTopicConfigs(map[string]string{"b": "2"})(&cfg)
	if cfg.TopicConfigs["a"] != "1" || cfg.TopicConfigs["b"] != "2" {
		t.Fatalf("unexpected topic configs: %#v", cfg.TopicConfigs)
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

func TestDefaultRetryClassifier(t *testing.T) {
	if DefaultRetryClassifier(kafkago.UnknownTopicOrPartition) {
		t.Fatalf("expected non-retryable for UnknownTopicOrPartition")
	}
	if !DefaultRetryClassifier(errors.New("network")) {
		t.Fatalf("expected retryable for generic error")
	}
	if DefaultRetryClassifier(context.Canceled) != true {
		t.Fatalf("context.Canceled should be retryable by default")
	}
}
