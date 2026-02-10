package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alonexy/complug/components/queue"
	"github.com/segmentio/kafka-go"
)

// Config holds Kafka-specific configuration.
type Config struct {
	Brokers                  []string
	Topic                    string
	GroupID                  string
	ClientID                 string
	Partition                int
	MaxChanSize              int
	QueueCapacity            int
	MinBytes                 int
	MaxBytes                 int
	MaxWait                  time.Duration
	ReadBatchTimeout         time.Duration
	GroupBalancers           []kafka.GroupBalancer
	StartOffset              int64
	IsolationLevel           kafka.IsolationLevel
	EnableAutoCommit         bool
	AutoCommitInterval       time.Duration
	ReadTimeout              time.Duration
	WriteTimeout             time.Duration
	BatchSize                int
	BatchBytes               int64
	BatchTimeout             time.Duration
	Async                    bool
	MaxAttempts              int
	ReconnectBackoff         time.Duration
	ReconnectMaxBackoff      time.Duration
	ReconnectForever         bool
	MaxRetries               int
	RetryClassifier          RetryClassifier
	AutoCreateTopic          bool
	TopicPartitions          int
	TopicReplication         int
	TopicConfigs             map[string]string
	ApplyTopicConfigOnExists bool
	Balancer                 kafka.Balancer
	Transport                *kafka.Transport
	Logger                   func(format string, args ...any)
}

// Balancer exposes the kafka-go balancer interface for external implementations.
type Balancer = kafka.Balancer

// Message aliases kafka-go Message for custom balancers.
type Message = kafka.Message

// Partition aliases kafka-go Partition for custom balancers.
type Partition = kafka.Partition

// RetryClassifier decides whether an error is retryable.
type RetryClassifier func(err error) bool

// DefaultRetryClassifier returns false for common non-retriable errors.
func DefaultRetryClassifier(err error) bool {
	var kerr kafka.Error
	if errors.As(err, &kerr) {
		switch kerr {
		case kafka.UnknownTopicOrPartition,
			kafka.InvalidTopic,
			kafka.TopicAuthorizationFailed,
			kafka.GroupAuthorizationFailed,
			kafka.ClusterAuthorizationFailed,
			kafka.TransactionalIDAuthorizationFailed,
			kafka.SASLAuthenticationFailed,
			kafka.BrokerAuthorizationFailed,
			kafka.InvalidConfiguration,
			kafka.SecurityDisabled:
			return false
		}
	}
	return true
}

type runtimeConfig struct {
	Config
	Encoder any
	Decoder any
}

type typedConfig[T any] struct {
	Config
	Encoder queue.Encoder[T]
	Decoder queue.Decoder[T]
}

// Option applies changes to Config.
type Option func(*runtimeConfig)

// WithOptions 批量追加多个配置选项。
func WithOptions(opts ...Option) Option {
	return func(cfg *runtimeConfig) {
		for _, opt := range opts {
			if opt != nil {
				opt(cfg)
			}
		}
	}
}

// WithBrokers 设置 Kafka broker 地址列表。
func WithBrokers(brokers ...string) Option {
	return func(cfg *runtimeConfig) {
		cfg.Brokers = append([]string{}, brokers...)
	}
}

// WithTopic 设置 Kafka Topic。
func WithTopic(topic string) Option {
	return func(cfg *runtimeConfig) {
		cfg.Topic = topic
	}
}

// WithGroupID 设置消费组 ID（有 GroupID 时忽略 Partition）。
func WithGroupID(groupID string) Option {
	return func(cfg *runtimeConfig) {
		cfg.GroupID = groupID
	}
}

// WithClientID 设置客户端 ID（用于 Transport/Reader Dialer）。
func WithClientID(clientID string) Option {
	return func(cfg *runtimeConfig) {
		cfg.ClientID = clientID
	}
}

// WithConsumePartition 设置消费分区（未设置 GroupID 时生效）。
func WithConsumePartition(partition int) Option {
	return func(cfg *runtimeConfig) {
		cfg.Partition = partition
	}
}

// WithMaxChanSize 设置 Channel 消费缓冲大小。
func WithMaxChanSize(size int) Option {
	return func(cfg *runtimeConfig) {
		if size > 0 {
			cfg.MaxChanSize = size
		}
	}
}

// WithQueueCapacity 设置 Reader 内部队列容量。
func WithQueueCapacity(capacity int) Option {
	return func(cfg *runtimeConfig) {
		if capacity > 0 {
			cfg.QueueCapacity = capacity
		}
	}
}

// WithMinBytes 设置 Reader 读取的最小批次字节数。
func WithMinBytes(bytes int) Option {
	return func(cfg *runtimeConfig) {
		if bytes > 0 {
			cfg.MinBytes = bytes
		}
	}
}

// WithMaxBytes 设置 Reader 读取的最大批次字节数。
func WithMaxBytes(bytes int) Option {
	return func(cfg *runtimeConfig) {
		if bytes > 0 {
			cfg.MaxBytes = bytes
		}
	}
}

// WithMaxWait 设置 Reader 最大等待时间。
func WithMaxWait(maxWait time.Duration) Option {
	return func(cfg *runtimeConfig) {
		if maxWait > 0 {
			cfg.MaxWait = maxWait
		}
	}
}

// WithReadBatchTimeout 设置 Reader 批次读取超时时间。
func WithReadBatchTimeout(timeout time.Duration) Option {
	return func(cfg *runtimeConfig) {
		if timeout > 0 {
			cfg.ReadBatchTimeout = timeout
		}
	}
}

// WithGroupBalancers 设置消费组的平衡策略列表。
func WithGroupBalancers(balancers ...kafka.GroupBalancer) Option {
	return func(cfg *runtimeConfig) {
		if len(balancers) > 0 {
			cfg.GroupBalancers = append([]kafka.GroupBalancer{}, balancers...)
		}
	}
}

// WithStartOffset 设置读取起始偏移量。
func WithStartOffset(offset int64) Option {
	return func(cfg *runtimeConfig) {
		cfg.StartOffset = offset
	}
}

// WithIsolationLevel 设置隔离级别（ReadUncommitted/ReadCommitted）。
func WithIsolationLevel(level kafka.IsolationLevel) Option {
	return func(cfg *runtimeConfig) {
		cfg.IsolationLevel = level
	}
}

// WithEnableAutoCommit 设置是否启用自动提交（仅对消费组生效）。
func WithEnableAutoCommit(enabled bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.EnableAutoCommit = enabled
	}
}

// WithAutoCommitInterval 设置自动提交间隔（EnableAutoCommit=true 时生效）。
func WithAutoCommitInterval(interval time.Duration) Option {
	return func(cfg *runtimeConfig) {
		if interval > 0 {
			cfg.AutoCommitInterval = interval
		}
	}
}

// WithReadTimeout 设置读取超时。
func WithReadTimeout(timeout time.Duration) Option {
	return func(cfg *runtimeConfig) {
		cfg.ReadTimeout = timeout
	}
}

// WithWriteTimeout 设置写入超时。
func WithWriteTimeout(timeout time.Duration) Option {
	return func(cfg *runtimeConfig) {
		cfg.WriteTimeout = timeout
	}
}

// WithBatchSize 设置 writer 批量消息条数。
func WithBatchSize(size int) Option {
	return func(cfg *runtimeConfig) {
		if size > 0 {
			cfg.BatchSize = size
		}
	}
}

// WithBatchBytes 设置 writer 批量字节大小。
func WithBatchBytes(bytes int64) Option {
	return func(cfg *runtimeConfig) {
		if bytes > 0 {
			cfg.BatchBytes = bytes
		}
	}
}

// WithBatchTimeout 设置 writer 批量超时时间。
func WithBatchTimeout(timeout time.Duration) Option {
	return func(cfg *runtimeConfig) {
		if timeout > 0 {
			cfg.BatchTimeout = timeout
		}
	}
}

// WithAsync 设置异步写入。
func WithAsync(async bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.Async = async
	}
}

// WithMaxAttempts 设置 writer 最大写入尝试次数。
func WithMaxAttempts(attempts int) Option {
	return func(cfg *runtimeConfig) {
		if attempts > 0 {
			cfg.MaxAttempts = attempts
		}
	}
}

// WithReconnectBackoff 设置重连退避初始间隔。
func WithReconnectBackoff(backoff time.Duration) Option {
	return func(cfg *runtimeConfig) {
		cfg.ReconnectBackoff = backoff
	}
}

// WithReconnectMaxBackoff 设置重连退避最大间隔。
func WithReconnectMaxBackoff(backoff time.Duration) Option {
	return func(cfg *runtimeConfig) {
		cfg.ReconnectMaxBackoff = backoff
	}
}

// WithReconnectForever 设置是否无限重连。
func WithReconnectForever(enabled bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.ReconnectForever = enabled
	}
}

// WithMaxRetries 设置最大重试次数（当 ReconnectForever=false 时生效）。
func WithMaxRetries(retries int) Option {
	return func(cfg *runtimeConfig) {
		if retries >= 0 {
			cfg.MaxRetries = retries
		}
	}
}

// WithRetryClassifier 设置是否可重试的判断函数。
func WithRetryClassifier(classifier RetryClassifier) Option {
	return func(cfg *runtimeConfig) {
		cfg.RetryClassifier = classifier
	}
}

// WithDefaultRetryClassifier 使用内置的可重试判断。
func WithDefaultRetryClassifier() Option {
	return WithRetryClassifier(DefaultRetryClassifier)
}

// WithAutoCreateTopic 设置是否自动创建 Topic。
func WithAutoCreateTopic(enabled bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.AutoCreateTopic = enabled
	}
}

// WithCreateTopicPartitions 设置自动创建 Topic 的分区数。
func WithCreateTopicPartitions(partitions int) Option {
	return func(cfg *runtimeConfig) {
		if partitions > 0 {
			cfg.TopicPartitions = partitions
		}
	}
}

// WithTopicReplication 设置自动创建 Topic 的副本数。
func WithTopicReplication(replication int) Option {
	return func(cfg *runtimeConfig) {
		if replication > 0 {
			cfg.TopicReplication = replication
		}
	}
}

// WithTopicConfig 设置 Topic 的配置项。
func WithTopicConfig(key, value string) Option {
	return func(cfg *runtimeConfig) {
		if key == "" {
			return
		}
		if cfg.TopicConfigs == nil {
			cfg.TopicConfigs = make(map[string]string)
		}
		cfg.TopicConfigs[key] = value
	}
}

// WithTopicConfigs 批量设置 Topic 配置项。
func WithTopicConfigs(configs map[string]string) Option {
	return func(cfg *runtimeConfig) {
		if len(configs) == 0 {
			return
		}
		if cfg.TopicConfigs == nil {
			cfg.TopicConfigs = make(map[string]string, len(configs))
		}
		for k, v := range configs {
			if k == "" {
				continue
			}
			cfg.TopicConfigs[k] = v
		}
	}
}

// WithApplyTopicConfigOnExists 设置当 Topic 已存在时是否应用配置更新。
func WithApplyTopicConfigOnExists(enabled bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.ApplyTopicConfigOnExists = enabled
	}
}

// WithRetentionMs 设置 Topic 保留时间（毫秒）。
func WithRetentionMs(ms int64) Option {
	return WithTopicConfig("retention.ms", fmt.Sprintf("%d", ms))
}

// WithRetentionBytes 设置 Topic 保留大小（字节）。
func WithRetentionBytes(bytes int64) Option {
	return WithTopicConfig("retention.bytes", fmt.Sprintf("%d", bytes))
}

// WithSegmentBytes 设置 segment 文件大小（字节）。
func WithSegmentBytes(bytes int64) Option {
	return WithTopicConfig("segment.bytes", fmt.Sprintf("%d", bytes))
}

// WithCleanupPolicy 设置清理策略（delete/compact）。
func WithCleanupPolicy(policy string) Option {
	return WithTopicConfig("cleanup.policy", policy)
}

// WithBalancer 设置分区器（自定义 balancer）。
func WithBalancer(balancer Balancer) Option {
	return func(cfg *runtimeConfig) {
		if balancer != nil {
			cfg.Balancer = balancer
		}
	}
}

// WithTransport 设置自定义 Transport（连接层）。
func WithTransport(transport *kafka.Transport) Option {
	return func(cfg *runtimeConfig) {
		cfg.Transport = transport
	}
}

// WithHashBalancer 使用 Hash 分区（基于 Key）。
func WithHashBalancer() Option {
	return WithBalancer(&kafka.Hash{})
}

// WithRoundRobinBalancer 使用轮询分区。
func WithRoundRobinBalancer() Option {
	return WithBalancer(&kafka.RoundRobin{})
}

// WithLeastBytesBalancer 使用最少字节分区策略。
func WithLeastBytesBalancer() Option {
	return WithBalancer(&kafka.LeastBytes{})
}

// WithEncoder 设置自定义编码器。
func WithEncoder[T any](enc queue.Encoder[T]) Option {
	return func(cfg *runtimeConfig) {
		cfg.Encoder = enc
	}
}

// WithDecoder 设置自定义解码器。
func WithDecoder[T any](dec queue.Decoder[T]) Option {
	return func(cfg *runtimeConfig) {
		cfg.Decoder = dec
	}
}

// WithCodec 同时设置编码器与解码器。
func WithCodec[T any](codec queue.Codec[T]) Option {
	return func(cfg *runtimeConfig) {
		cfg.Encoder = codec
		cfg.Decoder = codec
	}
}

// WithQueueConfig 复用通用队列配置。
func WithQueueConfig(base queue.Config) Option {
	return func(cfg *runtimeConfig) {
		if base.MaxChanSize > 0 {
			cfg.MaxChanSize = base.MaxChanSize
		}
		if base.ReadTimeout != 0 {
			cfg.ReadTimeout = base.ReadTimeout
		}
		if base.WriteTimeout != 0 {
			cfg.WriteTimeout = base.WriteTimeout
		}
	}
}

// WithLogger 设置日志函数。
func WithLogger(logger func(format string, args ...any)) Option {
	return func(cfg *runtimeConfig) {
		cfg.Logger = logger
	}
}

func defaultConfig[T any]() runtimeConfig {
	return runtimeConfig{
		Config: Config{
			Partition:                0,
			MaxChanSize:              64,
			QueueCapacity:            0,
			MinBytes:                 0,
			MaxBytes:                 0,
			MaxWait:                  0,
			ReadBatchTimeout:         0,
			StartOffset:              0,
			IsolationLevel:           0,
			EnableAutoCommit:         false,
			AutoCommitInterval:       0,
			ReadTimeout:              10 * time.Second,
			WriteTimeout:             10 * time.Second,
			BatchSize:                0,
			BatchBytes:               0,
			BatchTimeout:             0,
			Async:                    false,
			MaxAttempts:              0,
			ReconnectBackoff:         time.Second,
			ReconnectMaxBackoff:      30 * time.Second,
			ReconnectForever:         true,
			MaxRetries:               0,
			RetryClassifier:          nil,
			AutoCreateTopic:          false,
			TopicPartitions:          1,
			TopicReplication:         1,
			TopicConfigs:             nil,
			ApplyTopicConfigOnExists: false,
			Balancer:                 &kafka.LeastBytes{},
			Logger:                   func(string, ...any) {},
		},
		Encoder: queue.JSONCodec[T]{},
		Decoder: queue.JSONCodec[T]{},
	}
}

// NewKafkaProvider creates a Kafka provider implementing queue.Provider.
func NewKafkaProvider[T any](opts ...Option) (queue.Provider[T], error) {
	cfg := defaultConfig[T]()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	typedCfg, err := toTypedConfig[T](cfg)
	if err != nil {
		return nil, err
	}
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: brokers not configured")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka: topic not configured")
	}
	if typedCfg.Encoder == nil {
		return nil, queue.ErrNoEncoder
	}
	if typedCfg.Decoder == nil {
		return nil, queue.ErrNoDecoder
	}
	if cfg.AutoCreateTopic {
		if err := ensureTopic(cfg); err != nil {
			return nil, err
		}
	}
	prod := &producer[T]{cfg: typedCfg}
	cons := &consumer[T]{cfg: typedCfg}
	return &provider[T]{producer: prod, consumer: cons}, nil
}

type provider[T any] struct {
	producer *producer[T]
	consumer *consumer[T]
}

func (p *provider[T]) Producer() queue.Producer[T] { return p.producer }
func (p *provider[T]) Consumer() queue.Consumer[T] { return p.consumer }

// NewKafkaProducer creates a Kafka producer only.
func NewKafkaProducer[T any](opts ...Option) (queue.Producer[T], error) {
	provider, err := NewKafkaProvider[T](opts...)
	if err != nil {
		return nil, err
	}
	return provider.Producer(), nil
}

// NewKafkaConsumer creates a Kafka consumer only.
func NewKafkaConsumer[T any](opts ...Option) (queue.Consumer[T], error) {
	provider, err := NewKafkaProvider[T](opts...)
	if err != nil {
		return nil, err
	}
	return provider.Consumer(), nil
}

type producer[T any] struct {
	cfg    typedConfig[T]
	mu     sync.Mutex
	writer *kafka.Writer
	closed atomic.Bool
}

func (p *producer[T]) Send(ctx context.Context, msg queue.Message[T]) error {
	if p.closed.Load() {
		return queue.ErrClosed
	}
	payload, err := p.cfg.Encoder.Encode(ctx, msg.Value)
	if err != nil {
		return err
	}
	kmsg := kafka.Message{
		Key:   []byte(msg.Key),
		Value: payload,
		Time:  msg.Timestamp,
	}
	if len(msg.Headers) > 0 {
		kmsg.Headers = make([]kafka.Header, 0, len(msg.Headers))
		for key, value := range msg.Headers {
			kmsg.Headers = append(kmsg.Headers, kafka.Header{Key: key, Value: []byte(value)})
		}
	}
	if kmsg.Time.IsZero() {
		kmsg.Time = time.Now()
	}

	attempt := 0
	for {
		writer := p.ensureWriter()
		if err := writer.WriteMessages(ctx, kmsg); err == nil {
			return nil
		} else {
			p.cfg.Logger("kafka producer error: %v", err)
			p.resetWriter()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if p.cfg.RetryClassifier != nil && !p.cfg.RetryClassifier(err) {
				return err
			}
			if !p.cfg.ReconnectForever && attempt >= p.cfg.MaxRetries {
				return err
			}
			p.sleepBackoff(attempt)
			attempt++
		}
	}
}

func (p *producer[T]) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writer != nil {
		err := p.writer.Close()
		p.writer = nil
		return err
	}
	return nil
}

func (p *producer[T]) ensureWriter() *kafka.Writer {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writer == nil {
		if p.cfg.Transport == nil {
			p.cfg.Transport = &kafka.Transport{
				ClientID: p.cfg.ClientID,
			}
		}
		p.writer = &kafka.Writer{
			Addr:         kafka.TCP(p.cfg.Brokers...),
			Topic:        p.cfg.Topic,
			Balancer:     p.cfg.Balancer,
			RequiredAcks: kafka.RequireAll,
			ReadTimeout:  p.cfg.ReadTimeout,
			WriteTimeout: p.cfg.WriteTimeout,
			Transport:    p.cfg.Transport,
		}
		if p.cfg.BatchSize > 0 {
			p.writer.BatchSize = p.cfg.BatchSize
		}
		if p.cfg.BatchBytes > 0 {
			p.writer.BatchBytes = p.cfg.BatchBytes
		}
		if p.cfg.BatchTimeout > 0 {
			p.writer.BatchTimeout = p.cfg.BatchTimeout
		}
		if p.cfg.MaxAttempts > 0 {
			p.writer.MaxAttempts = p.cfg.MaxAttempts
		}
		p.writer.Async = p.cfg.Async
	}
	return p.writer
}

func (p *producer[T]) resetWriter() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writer != nil {
		_ = p.writer.Close()
		p.writer = nil
	}
}

func (p *producer[T]) sleepBackoff(attempt int) {
	backoff := backoffDuration(p.cfg.ReconnectBackoff, p.cfg.ReconnectMaxBackoff, attempt)
	time.Sleep(backoff)
}

type consumer[T any] struct {
	cfg    typedConfig[T]
	mu     sync.Mutex
	reader *kafka.Reader
	closed atomic.Bool
}

func (c *consumer[T]) Receive(ctx context.Context) (queue.Message[T], error) {
	if c.closed.Load() {
		return queue.Message[T]{}, queue.ErrClosed
	}
	attempt := 0
	for {
		reader := c.ensureReader()
		kmsg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return queue.Message[T]{}, ctx.Err()
			}
			c.cfg.Logger("kafka consumer error: %v", err)
			c.resetReader()
			if c.cfg.RetryClassifier != nil && !c.cfg.RetryClassifier(err) {
				return queue.Message[T]{}, err
			}
			if !c.cfg.ReconnectForever && attempt >= c.cfg.MaxRetries {
				return queue.Message[T]{}, err
			}
			c.sleepBackoff(attempt)
			attempt++
			continue
		}
		attempt = 0
		value, err := c.cfg.Decoder.Decode(ctx, kmsg.Value)
		if err != nil {
			return queue.Message[T]{}, err
		}
		msg := queue.Message[T]{
			Key:       string(kmsg.Key),
			Value:     value,
			Timestamp: kmsg.Time,
			Headers:   headersToMap(kmsg.Headers),
			Meta: map[string]string{
				"partition": fmt.Sprintf("%d", kmsg.Partition),
				"offset":    fmt.Sprintf("%d", kmsg.Offset),
			},
			Raw: kmsg,
		}
		return msg, nil
	}
}

func (c *consumer[T]) Commit(ctx context.Context, msg queue.Message[T]) error {
	if c.closed.Load() {
		return queue.ErrClosed
	}
	if c.cfg.GroupID == "" {
		return nil
	}
	if c.cfg.EnableAutoCommit {
		return nil
	}
	kmsg, ok := msg.Raw.(kafka.Message)
	if !ok {
		return queue.ErrInvalidMessage
	}
	reader := c.ensureReader()
	return reader.CommitMessages(ctx, kmsg)
}

func (c *consumer[T]) Channel(ctx context.Context) (<-chan queue.Message[T], <-chan error) {
	messages := make(chan queue.Message[T], c.channelSize())
	errs := make(chan error, 1)
	go func() {
		defer close(messages)
		defer close(errs)
		for {
			msg, err := c.Receive(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				errs <- err
				continue
			}
			select {
			case messages <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	return messages, errs
}

func (c *consumer[T]) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reader != nil {
		err := c.reader.Close()
		c.reader = nil
		return err
	}
	return nil
}

func (c *consumer[T]) ensureReader() *kafka.Reader {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reader == nil {
		config := kafka.ReaderConfig{
			Brokers:        c.cfg.Brokers,
			Topic:          c.cfg.Topic,
			Dialer:         dialerFromTransport(c.cfg.Transport, c.cfg.ClientID),
			MaxWait:        c.cfg.ReadTimeout,
			CommitInterval: 0,
		}
		if c.cfg.EnableAutoCommit {
			if c.cfg.AutoCommitInterval > 0 {
				config.CommitInterval = c.cfg.AutoCommitInterval
			} else {
				config.CommitInterval = time.Second
			}
		}
		if c.cfg.GroupID != "" {
			config.GroupID = c.cfg.GroupID
		} else {
			config.Partition = c.cfg.Partition
		}
		if c.cfg.QueueCapacity > 0 {
			config.QueueCapacity = c.cfg.QueueCapacity
		}
		if c.cfg.MinBytes > 0 {
			config.MinBytes = c.cfg.MinBytes
		}
		if c.cfg.MaxBytes > 0 {
			config.MaxBytes = c.cfg.MaxBytes
		}
		if c.cfg.MaxWait > 0 {
			config.MaxWait = c.cfg.MaxWait
		}
		if c.cfg.ReadBatchTimeout > 0 {
			config.ReadBatchTimeout = c.cfg.ReadBatchTimeout
		}
		if len(c.cfg.GroupBalancers) > 0 {
			config.GroupBalancers = append([]kafka.GroupBalancer{}, c.cfg.GroupBalancers...)
		}
		if c.cfg.StartOffset != 0 {
			config.StartOffset = c.cfg.StartOffset
		}
		if c.cfg.IsolationLevel != 0 {
			config.IsolationLevel = c.cfg.IsolationLevel
		}
		c.reader = kafka.NewReader(config)
	}
	return c.reader
}

func (c *consumer[T]) resetReader() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reader != nil {
		_ = c.reader.Close()
		c.reader = nil
	}
}

func (c *consumer[T]) sleepBackoff(attempt int) {
	backoff := backoffDuration(c.cfg.ReconnectBackoff, c.cfg.ReconnectMaxBackoff, attempt)
	time.Sleep(backoff)
}

func (c *consumer[T]) channelSize() int {
	if c.cfg.MaxChanSize > 0 {
		return c.cfg.MaxChanSize
	}
	return 1
}

func headersToMap(headers []kafka.Header) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string]string, len(headers))
	for _, header := range headers {
		out[header.Key] = string(header.Value)
	}
	return out
}

func backoffDuration(base, max time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if max <= 0 {
		max = 30 * time.Second
	}
	d := base
	for i := 0; i < attempt; i++ {
		d *= 2
		if d > max {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}

func ensureTopic(cfg runtimeConfig) error {
	transport := cfg.Transport
	if transport == nil {
		transport = &kafka.Transport{ClientID: cfg.ClientID}
	}
	if len(cfg.Brokers) == 0 {
		return errors.New("kafka: brokers not configured")
	}
	conn, err := (&kafka.Dialer{ClientID: transport.ClientID, Timeout: transport.DialTimeout}).DialContext(
		context.Background(),
		"tcp",
		cfg.Brokers[0],
	)
	if err != nil {
		return err
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return err
	}
	defer controllerConn.Close()
	topicConfig := kafka.TopicConfig{
		Topic:             cfg.Topic,
		NumPartitions:     cfg.TopicPartitions,
		ReplicationFactor: cfg.TopicReplication,
	}
	if len(cfg.TopicConfigs) > 0 {
		topicConfig.ConfigEntries = make([]kafka.ConfigEntry, 0, len(cfg.TopicConfigs))
		for k, v := range cfg.TopicConfigs {
			if k == "" {
				continue
			}
			topicConfig.ConfigEntries = append(topicConfig.ConfigEntries, kafka.ConfigEntry{
				ConfigName:  k,
				ConfigValue: v,
			})
		}
	}
	if err := controllerConn.CreateTopics(topicConfig); err != nil {
		if errors.Is(err, kafka.TopicAlreadyExists) {
			if cfg.ApplyTopicConfigOnExists && len(cfg.TopicConfigs) > 0 {
				return applyTopicConfigs(cfg)
			}
			return nil
		}
		return err
	}
	return nil
}

func applyTopicConfigs(cfg runtimeConfig) error {
	if len(cfg.TopicConfigs) == 0 {
		return nil
	}
	client := &kafka.Client{
		Addr: kafka.TCP(cfg.Brokers...),
	}
	req := &kafka.IncrementalAlterConfigsRequest{
		Resources: []kafka.IncrementalAlterConfigsRequestResource{
			{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: cfg.Topic,
				Configs:      make([]kafka.IncrementalAlterConfigsRequestConfig, 0, len(cfg.TopicConfigs)),
			},
		},
	}
	for key, value := range cfg.TopicConfigs {
		if key == "" {
			continue
		}
		req.Resources[0].Configs = append(req.Resources[0].Configs, kafka.IncrementalAlterConfigsRequestConfig{
			Name:            key,
			Value:           value,
			ConfigOperation: kafka.ConfigOperationSet,
		})
	}
	_, err := client.IncrementalAlterConfigs(context.Background(), req)
	return err
}

func dialerFromTransport(transport *kafka.Transport, clientID string) *kafka.Dialer {
	if transport == nil && clientID == "" {
		return nil
	}
	if transport == nil {
		transport = &kafka.Transport{
			ClientID: clientID,
		}
	}
	dialer := &kafka.Dialer{
		ClientID:      transport.ClientID,
		Timeout:       transport.DialTimeout,
		TLS:           transport.TLS,
		SASLMechanism: transport.SASL,
	}
	if dialer.ClientID == "" {
		dialer.ClientID = clientID
	}
	if transport.Dial != nil {
		dialer.DialFunc = transport.Dial
	}
	return dialer
}

func toTypedConfig[T any](cfg runtimeConfig) (typedConfig[T], error) {
	enc, ok := cfg.Encoder.(queue.Encoder[T])
	if !ok && cfg.Encoder != nil {
		return typedConfig[T]{}, errors.New("kafka: encoder type mismatch")
	}
	dec, ok := cfg.Decoder.(queue.Decoder[T])
	if !ok && cfg.Decoder != nil {
		return typedConfig[T]{}, errors.New("kafka: decoder type mismatch")
	}
	return typedConfig[T]{
		Config:  cfg.Config,
		Encoder: enc,
		Decoder: dec,
	}, nil
}
