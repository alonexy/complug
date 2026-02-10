package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alonexy/complug/components/queue"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Config holds RabbitMQ-specific configuration.
type Config struct {
	URL                 string
	ExchangeName        string
	ExchangeType        string
	RoutingKey          string
	QueueName           string
	ConsumerTag         string
	AutoAck             bool
	Durable             bool
	AutoDelete          bool
	Exclusive           bool
	NoWait              bool
	PrefetchCount       int
	MaxChanSize         int
	DialTimeout         time.Duration
	ReconnectBackoff    time.Duration
	ReconnectMaxBackoff time.Duration
	ReconnectForever    bool
	MaxRetries          int
	RetryClassifier     RetryClassifier
	DecodeErrorStrategy DecodeErrorStrategy
	ExchangeArgs        amqp.Table
	QueueArgs           amqp.Table
	BindingArgs         amqp.Table
	Logger              func(format string, args ...any)
}

// RetryClassifier decides whether an error is retryable.
type RetryClassifier func(err error) bool

// DecodeErrorStrategy controls how decode errors are handled.
type DecodeErrorStrategy int

const (
	DecodeErrorReturn DecodeErrorStrategy = iota
	DecodeErrorAck
	DecodeErrorNackRequeue
	DecodeErrorNackDrop
)

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

// WithURL 设置 RabbitMQ 连接地址。
func WithURL(url string) Option {
	return func(cfg *runtimeConfig) {
		cfg.URL = url
	}
}

// WithExchange 设置交换机名称与类型。
func WithExchange(name, exchangeType string) Option {
	return func(cfg *runtimeConfig) {
		cfg.ExchangeName = name
		cfg.ExchangeType = exchangeType
	}
}

// WithRoutingKey 设置路由键。
func WithRoutingKey(key string) Option {
	return func(cfg *runtimeConfig) {
		cfg.RoutingKey = key
	}
}

// WithQueueName 设置队列名称。
func WithQueueName(name string) Option {
	return func(cfg *runtimeConfig) {
		cfg.QueueName = name
	}
}

// WithConsumerTag 设置消费者标签。
func WithConsumerTag(tag string) Option {
	return func(cfg *runtimeConfig) {
		cfg.ConsumerTag = tag
	}
}

// WithAutoAck 设置是否自动确认消息。
func WithAutoAck(autoAck bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.AutoAck = autoAck
	}
}

// WithDurable 设置是否持久化。
func WithDurable(durable bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.Durable = durable
	}
}

// WithAutoDelete 设置是否自动删除。
func WithAutoDelete(autoDelete bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.AutoDelete = autoDelete
	}
}

// WithExclusive 设置是否独占队列。
func WithExclusive(exclusive bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.Exclusive = exclusive
	}
}

// WithNoWait 设置是否无需等待。
func WithNoWait(noWait bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.NoWait = noWait
	}
}

// WithExchangeArgs 设置交换机声明参数。
func WithExchangeArgs(args amqp.Table) Option {
	return func(cfg *runtimeConfig) {
		cfg.ExchangeArgs = args
	}
}

// WithQueueArgs 设置队列声明参数。
func WithQueueArgs(args amqp.Table) Option {
	return func(cfg *runtimeConfig) {
		cfg.QueueArgs = args
	}
}

// WithBindingArgs 设置绑定参数。
func WithBindingArgs(args amqp.Table) Option {
	return func(cfg *runtimeConfig) {
		cfg.BindingArgs = args
	}
}

// WithPrefetchCount 设置预取数量。
func WithPrefetchCount(count int) Option {
	return func(cfg *runtimeConfig) {
		cfg.PrefetchCount = count
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

// WithDialTimeout 设置连接超时。
func WithDialTimeout(timeout time.Duration) Option {
	return func(cfg *runtimeConfig) {
		cfg.DialTimeout = timeout
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

// WithRetryClassifier 设置是否可重试的判断函数。
func WithRetryClassifier(classifier RetryClassifier) Option {
	return func(cfg *runtimeConfig) {
		cfg.RetryClassifier = classifier
	}
}

// WithDecodeErrorStrategy 设置解码失败处理策略。
func WithDecodeErrorStrategy(strategy DecodeErrorStrategy) Option {
	return func(cfg *runtimeConfig) {
		cfg.DecodeErrorStrategy = strategy
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
		if base.DialTimeout != 0 {
			cfg.DialTimeout = base.DialTimeout
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
			ExchangeType:        "direct",
			Durable:             true,
			PrefetchCount:       20,
			MaxChanSize:         64,
			DialTimeout:         10 * time.Second,
			ReconnectBackoff:    time.Second,
			ReconnectMaxBackoff: 30 * time.Second,
			ReconnectForever:    true,
			MaxRetries:          0,
			RetryClassifier:     nil,
			DecodeErrorStrategy: DecodeErrorReturn,
			Logger:              func(string, ...any) {},
		},
		Encoder: queue.JSONCodec[T]{},
		Decoder: queue.JSONCodec[T]{},
	}
}

// NewRabbitProvider creates a RabbitMQ provider implementing queue.Provider.
func NewRabbitProvider[T any](opts ...Option) (queue.Provider[T], error) {
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
	if cfg.URL == "" {
		return nil, errors.New("rabbitmq: url not configured")
	}
	if typedCfg.Encoder == nil {
		return nil, queue.ErrNoEncoder
	}
	if typedCfg.Decoder == nil {
		return nil, queue.ErrNoDecoder
	}
	sess := &session[T]{cfg: typedCfg}
	prod := &producer[T]{cfg: typedCfg, session: sess}
	cons := &consumer[T]{cfg: typedCfg, session: sess}
	return &provider[T]{producer: prod, consumer: cons}, nil
}

type provider[T any] struct {
	producer *producer[T]
	consumer *consumer[T]
}

func (p *provider[T]) Producer() queue.Producer[T] { return p.producer }
func (p *provider[T]) Consumer() queue.Consumer[T] { return p.consumer }

// NewRabbitProducer creates a RabbitMQ producer only.
func NewRabbitProducer[T any](opts ...Option) (queue.Producer[T], error) {
	provider, err := NewRabbitProvider[T](opts...)
	if err != nil {
		return nil, err
	}
	return provider.Producer(), nil
}

// NewRabbitConsumer creates a RabbitMQ consumer only.
func NewRabbitConsumer[T any](opts ...Option) (queue.Consumer[T], error) {
	provider, err := NewRabbitProvider[T](opts...)
	if err != nil {
		return nil, err
	}
	return provider.Consumer(), nil
}

type session[T any] struct {
	cfg  typedConfig[T]
	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

func (s *session[T]) ensureChannel() (*amqp.Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil || s.conn.IsClosed() {
		conn, err := amqp.DialConfig(s.cfg.URL, amqp.Config{
			Dial: amqp.DefaultDial(s.cfg.DialTimeout),
		})
		if err != nil {
			return nil, err
		}
		s.conn = conn
		s.ch = nil
	}
	if s.ch == nil {
		ch, err := s.conn.Channel()
		if err != nil {
			return nil, err
		}
		if s.cfg.PrefetchCount > 0 {
			if err := ch.Qos(s.cfg.PrefetchCount, 0, false); err != nil {
				_ = ch.Close()
				return nil, err
			}
		}
		if s.cfg.ExchangeName != "" {
			if err := ch.ExchangeDeclare(
				s.cfg.ExchangeName,
				s.cfg.ExchangeType,
				s.cfg.Durable,
				s.cfg.AutoDelete,
				false,
				s.cfg.NoWait,
				s.cfg.ExchangeArgs,
			); err != nil {
				_ = ch.Close()
				return nil, err
			}
		}
		if s.cfg.QueueName != "" {
			queueDeclare, err := ch.QueueDeclare(
				s.cfg.QueueName,
				s.cfg.Durable,
				s.cfg.AutoDelete,
				s.cfg.Exclusive,
				s.cfg.NoWait,
				s.cfg.QueueArgs,
			)
			if err != nil {
				_ = ch.Close()
				return nil, err
			}
			if s.cfg.ExchangeName != "" {
				if err := ch.QueueBind(
					queueDeclare.Name,
					s.cfg.RoutingKey,
					s.cfg.ExchangeName,
					s.cfg.NoWait,
					s.cfg.BindingArgs,
				); err != nil {
					_ = ch.Close()
					return nil, err
				}
			}
		}
		s.ch = ch
	}
	return s.ch, nil
}

func (s *session[T]) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch != nil {
		_ = s.ch.Close()
		s.ch = nil
	}
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
}

type producer[T any] struct {
	cfg     typedConfig[T]
	session *session[T]
	closed  atomic.Bool
}

func (p *producer[T]) Send(ctx context.Context, msg queue.Message[T]) error {
	if p.closed.Load() {
		return queue.ErrClosed
	}
	payload, err := p.cfg.Encoder.Encode(ctx, msg.Value)
	if err != nil {
		return err
	}
	publishing := amqp.Publishing{
		Timestamp: msg.Timestamp,
		Body:      payload,
		Headers:   amqp.Table{},
	}
	for key, value := range msg.Headers {
		publishing.Headers[key] = value
	}
	if publishing.Timestamp.IsZero() {
		publishing.Timestamp = time.Now()
	}
	attempt := 0
	for {
		ch, err := p.session.ensureChannel()
		if err != nil {
			p.cfg.Logger("rabbitmq producer error: %v", err)
			p.session.reset()
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
			continue
		}
		if err := ch.PublishWithContext(
			ctx,
			p.cfg.ExchangeName,
			p.cfg.RoutingKey,
			false,
			false,
			publishing,
		); err != nil {
			p.cfg.Logger("rabbitmq producer error: %v", err)
			p.session.reset()
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
			continue
		}
		return nil
	}
}

func (p *producer[T]) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.session.reset()
	return nil
}

func (p *producer[T]) sleepBackoff(attempt int) {
	backoff := backoffDuration(p.cfg.ReconnectBackoff, p.cfg.ReconnectMaxBackoff, attempt)
	time.Sleep(backoff)
}

type consumer[T any] struct {
	cfg        typedConfig[T]
	session    *session[T]
	deliveries <-chan amqp.Delivery
	mu         sync.Mutex
	closed     atomic.Bool
}

func (c *consumer[T]) Receive(ctx context.Context) (queue.Message[T], error) {
	if c.closed.Load() {
		return queue.Message[T]{}, queue.ErrClosed
	}
	attempt := 0
	for {
		deliveries, err := c.ensureDeliveries()
		if err != nil {
			if ctx.Err() != nil {
				return queue.Message[T]{}, ctx.Err()
			}
			c.cfg.Logger("rabbitmq consumer error: %v", err)
			c.session.reset()
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
		select {
		case <-ctx.Done():
			return queue.Message[T]{}, ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				c.resetDeliveries()
				continue
			}
			attempt = 0
			value, err := c.cfg.Decoder.Decode(ctx, delivery.Body)
			if err != nil {
				if c.handleDecodeError(delivery, err) {
					continue
				}
				return queue.Message[T]{}, err
			}
			msg := queue.Message[T]{
				Key:       delivery.RoutingKey,
				Value:     value,
				Timestamp: delivery.Timestamp,
				Headers:   headersToMap(delivery.Headers),
				Meta: map[string]string{
					"exchange":   delivery.Exchange,
					"routingKey": delivery.RoutingKey,
				},
				Raw: delivery,
			}
			return msg, nil
		}
	}
}

func (c *consumer[T]) Commit(ctx context.Context, msg queue.Message[T]) error {
	if c.closed.Load() {
		return queue.ErrClosed
	}
	if c.cfg.AutoAck {
		return nil
	}
	delivery, ok := msg.Raw.(amqp.Delivery)
	if !ok {
		return queue.ErrInvalidMessage
	}
	return delivery.Ack(false)
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
	c.session.reset()
	return nil
}

func (c *consumer[T]) ensureDeliveries() (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deliveries != nil {
		return c.deliveries, nil
	}
	ch, err := c.session.ensureChannel()
	if err != nil {
		return nil, err
	}
	if c.cfg.QueueName == "" {
		return nil, errors.New("rabbitmq: queue name not configured for consumer")
	}
	deliveries, err := ch.Consume(
		c.cfg.QueueName,
		c.cfg.ConsumerTag,
		c.cfg.AutoAck,
		c.cfg.Exclusive,
		false,
		c.cfg.NoWait,
		nil,
	)
	if err != nil {
		return nil, err
	}
	c.deliveries = deliveries
	return deliveries, nil
}

func (c *consumer[T]) resetDeliveries() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deliveries = nil
}

func (c *consumer[T]) handleDecodeError(delivery amqp.Delivery, err error) bool {
	if c.cfg.AutoAck {
		c.cfg.Logger("rabbitmq decode error with autoAck: %v", err)
		return true
	}
	switch c.cfg.DecodeErrorStrategy {
	case DecodeErrorAck:
		if ackErr := delivery.Ack(false); ackErr != nil {
			c.cfg.Logger("rabbitmq decode ack error: %v", ackErr)
		}
		return true
	case DecodeErrorNackRequeue:
		if nackErr := delivery.Nack(false, true); nackErr != nil {
			c.cfg.Logger("rabbitmq decode nack error: %v", nackErr)
		}
		return true
	case DecodeErrorNackDrop:
		if nackErr := delivery.Nack(false, false); nackErr != nil {
			c.cfg.Logger("rabbitmq decode nack error: %v", nackErr)
		}
		return true
	default:
		return false
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

func headersToMap(headers amqp.Table) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	out := make(map[string]string, len(headers))
	for key, value := range headers {
		out[key] = fmt.Sprint(value)
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

func toTypedConfig[T any](cfg runtimeConfig) (typedConfig[T], error) {
	enc, ok := cfg.Encoder.(queue.Encoder[T])
	if !ok && cfg.Encoder != nil {
		return typedConfig[T]{}, errors.New("rabbitmq: encoder type mismatch")
	}
	dec, ok := cfg.Decoder.(queue.Decoder[T])
	if !ok && cfg.Decoder != nil {
		return typedConfig[T]{}, errors.New("rabbitmq: decoder type mismatch")
	}
	return typedConfig[T]{
		Config:  cfg.Config,
		Encoder: enc,
		Decoder: dec,
	}, nil
}
