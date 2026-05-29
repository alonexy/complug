package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alonexy/complug/components/queue"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	DefaultURL = natsgo.DefaultURL
	msgIDKey   = "Nats-Msg-Id"
)

var (
	ErrAuthConflict    = errors.New("nats: auth user info and token are mutually exclusive")
	ErrInvalidAuth     = errors.New("nats: invalid auth configuration")
	ErrSubjectRequired = errors.New("nats: subject not configured")
	ErrStreamRequired  = errors.New("nats: stream not configured")
	ErrDurableRequired = errors.New("nats: durable not configured")
)

// Mode selects Core NATS or JetStream semantics.
type Mode int

const (
	Core Mode = iota
	JetStream
)

// ConsumerMode selects JetStream pull or push delivery.
type ConsumerMode int

const (
	PullConsumer ConsumerMode = iota
	PushConsumer
)

// ReplayPolicy controls JetStream push consumer replay speed.
type ReplayPolicy int

const (
	ReplayInstant ReplayPolicy = iota
	ReplayOriginal
)

// Config holds NATS-specific configuration.
type Config struct {
	Mode                   Mode
	ConsumerMode           ConsumerMode
	URL                    string
	Subject                string
	Subjects               []string
	QueueGroup             string
	Stream                 string
	Durable                string
	FilterSubject          string
	DeliverSubject         string
	DeliverGroup           string
	ReplayPolicy           ReplayPolicy
	AutoCreateStream       bool
	Storage                jetstream.StorageType
	Retention              jetstream.RetentionPolicy
	Discard                jetstream.DiscardPolicy
	Replicas               int
	StreamMaxAge           time.Duration
	StreamMaxBytes         int64
	DuplicateWindow        time.Duration
	AckWait                time.Duration
	MaxDeliver             int
	PullMaxMessages        int
	DoubleAck              bool
	MaxChanSize            int
	DialTimeout            time.Duration
	ReconnectBackoff       time.Duration
	ReconnectMaxBackoff    time.Duration
	ReconnectForever       bool
	MaxRetries             int
	RetryClassifier        RetryClassifier
	MsgIDFromKey           bool
	AsyncPublish           bool
	AsyncPublishAckHandler func(jetstream.PubAckFuture)
	WarmUp                 bool
	AuthUser               string
	AuthPassword           string
	AuthToken              string
	Logger                 func(format string, args ...any)
}

// RetryClassifier decides whether an error is retryable.
type RetryClassifier func(err error) bool

type runtimeConfig struct {
	Config
	Encoder         any
	Decoder         any
	authUserInfoSet bool
	authTokenSet    bool
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

// WithMode 设置 Core NATS 或 JetStream 模式。
func WithMode(mode Mode) Option {
	return func(cfg *runtimeConfig) {
		cfg.Mode = mode
	}
}

// WithURL 设置 NATS 连接地址。
func WithURL(url string) Option {
	return func(cfg *runtimeConfig) {
		cfg.URL = url
	}
}

// WithSubject 设置发布和消费使用的 subject。
func WithSubject(subject string) Option {
	return func(cfg *runtimeConfig) {
		cfg.Subject = subject
	}
}

// WithSubjects 设置自动创建 JetStream stream 时捕获的 subjects。
func WithSubjects(subjects ...string) Option {
	return func(cfg *runtimeConfig) {
		cfg.Subjects = append([]string{}, subjects...)
	}
}

// WithQueueGroup 设置 Core NATS queue subscribe 的队列组。
func WithQueueGroup(group string) Option {
	return func(cfg *runtimeConfig) {
		cfg.QueueGroup = group
	}
}

// WithConsumerMode 设置 JetStream pull 或 push consumer 模式。
func WithConsumerMode(mode ConsumerMode) Option {
	return func(cfg *runtimeConfig) {
		cfg.ConsumerMode = mode
	}
}

// WithStream 设置 JetStream stream 名称。
func WithStream(stream string) Option {
	return func(cfg *runtimeConfig) {
		cfg.Stream = stream
	}
}

// WithDurable 设置 JetStream durable consumer 名称。
func WithDurable(durable string) Option {
	return func(cfg *runtimeConfig) {
		cfg.Durable = durable
	}
}

// WithFilterSubject 设置 JetStream consumer 过滤 subject。
func WithFilterSubject(subject string) Option {
	return func(cfg *runtimeConfig) {
		cfg.FilterSubject = subject
	}
}

// WithDeliverSubject 设置 JetStream push consumer 的投递 subject。
func WithDeliverSubject(subject string) Option {
	return func(cfg *runtimeConfig) {
		cfg.DeliverSubject = subject
	}
}

// WithDeliverGroup 设置 JetStream push consumer 的 queue group。
func WithDeliverGroup(group string) Option {
	return func(cfg *runtimeConfig) {
		cfg.DeliverGroup = group
	}
}

// WithReplayPolicy 设置 JetStream push consumer replay 速度。
func WithReplayPolicy(policy ReplayPolicy) Option {
	return func(cfg *runtimeConfig) {
		cfg.ReplayPolicy = policy
	}
}

// WithAutoCreateStream 设置是否自动创建或更新 JetStream stream。
func WithAutoCreateStream(enabled bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.AutoCreateStream = enabled
	}
}

// WithStorage 设置 JetStream stream 存储类型。
func WithStorage(storage jetstream.StorageType) Option {
	return func(cfg *runtimeConfig) {
		cfg.Storage = storage
	}
}

// WithRetention 设置 JetStream stream 保留策略。
func WithRetention(retention jetstream.RetentionPolicy) Option {
	return func(cfg *runtimeConfig) {
		cfg.Retention = retention
	}
}

// WithDiscard 设置 JetStream stream 丢弃策略。
func WithDiscard(discard jetstream.DiscardPolicy) Option {
	return func(cfg *runtimeConfig) {
		cfg.Discard = discard
	}
}

// WithReplicas 设置 JetStream stream 副本数。
func WithReplicas(replicas int) Option {
	return func(cfg *runtimeConfig) {
		if replicas > 0 {
			cfg.Replicas = replicas
		}
	}
}

// WithStreamMaxAge 设置 JetStream stream 消息最大保留时间。
func WithStreamMaxAge(maxAge time.Duration) Option {
	return func(cfg *runtimeConfig) {
		cfg.StreamMaxAge = maxAge
	}
}

// WithStreamMaxBytes 设置 JetStream stream 最大保留字节数。
func WithStreamMaxBytes(maxBytes int64) Option {
	return func(cfg *runtimeConfig) {
		cfg.StreamMaxBytes = maxBytes
	}
}

// WithDuplicateWindow 设置 JetStream 去重窗口。
func WithDuplicateWindow(window time.Duration) Option {
	return func(cfg *runtimeConfig) {
		cfg.DuplicateWindow = window
	}
}

// WithAckWait 设置 JetStream consumer ack 等待时间。
func WithAckWait(wait time.Duration) Option {
	return func(cfg *runtimeConfig) {
		if wait > 0 {
			cfg.AckWait = wait
		}
	}
}

// WithMaxDeliver 设置 JetStream consumer 最大投递次数。
func WithMaxDeliver(maxDeliver int) Option {
	return func(cfg *runtimeConfig) {
		if maxDeliver > 0 {
			cfg.MaxDeliver = maxDeliver
		}
	}
}

// WithPullMaxMessages 设置 JetStream pull consumer 每次拉取的最大消息数。
func WithPullMaxMessages(maxMessages int) Option {
	return func(cfg *runtimeConfig) {
		if maxMessages > 0 {
			cfg.PullMaxMessages = maxMessages
		}
	}
}

// WithDoubleAck 设置 Commit 是否等待 JetStream 服务端确认 ack。
func WithDoubleAck(enabled bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.DoubleAck = enabled
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

// WithReconnectForever 设置是否无限重连。
func WithReconnectForever(enabled bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.ReconnectForever = enabled
	}
}

// WithMaxRetries 设置最大重连次数（当 ReconnectForever=false 时生效）。
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

// WithMsgIDFromKey 设置是否将 Message.Key 写入 Nats-Msg-Id 以启用 JetStream 去重。
func WithMsgIDFromKey(enabled bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.MsgIDFromKey = enabled
	}
}

// WithAsyncPublish 设置 JetStream Producer 是否使用 PublishMsgAsync。
func WithAsyncPublish(enabled bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.AsyncPublish = enabled
	}
}

// WithAsyncPublishAckHandler 设置 JetStream async publish ack future 处理函数。
func WithAsyncPublishAckHandler(handler func(jetstream.PubAckFuture)) Option {
	return func(cfg *runtimeConfig) {
		cfg.AsyncPublishAckHandler = handler
	}
}

// WithWarmUp sets whether provider construction establishes the NATS connection eagerly.
func WithWarmUp(enabled bool) Option {
	return func(cfg *runtimeConfig) {
		cfg.WarmUp = enabled
	}
}

// WithAuthUserInfo 设置用户名密码认证。
func WithAuthUserInfo(user, password string) Option {
	return func(cfg *runtimeConfig) {
		cfg.AuthUser = user
		cfg.AuthPassword = password
		cfg.authUserInfoSet = true
	}
}

// WithAuthToken 设置 Token 认证。
func WithAuthToken(token string) Option {
	return func(cfg *runtimeConfig) {
		cfg.AuthToken = token
		cfg.authTokenSet = true
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
		if base.ReadTimeout != 0 {
			cfg.AckWait = base.ReadTimeout
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
	cfg := runtimeConfig{
		Config: Config{
			Mode:                JetStream,
			ConsumerMode:        PullConsumer,
			URL:                 DefaultURL,
			ReplayPolicy:        ReplayInstant,
			Storage:             jetstream.FileStorage,
			Retention:           jetstream.LimitsPolicy,
			Discard:             jetstream.DiscardOld,
			Replicas:            1,
			AckWait:             30 * time.Second,
			MaxDeliver:          5,
			PullMaxMessages:     1,
			MaxChanSize:         64,
			DialTimeout:         10 * time.Second,
			ReconnectBackoff:    time.Second,
			ReconnectMaxBackoff: 30 * time.Second,
			ReconnectForever:    true,
			WarmUp:              true,
			Logger:              func(string, ...any) {},
		},
		Encoder: queue.JSONCodec[T]{},
		Decoder: queue.JSONCodec[T]{},
	}
	cfg.AsyncPublishAckHandler = cfg.defaultAsyncPublishAckHandler
	return cfg
}

func (cfg runtimeConfig) defaultAsyncPublishAckHandler(future jetstream.PubAckFuture) {
	go func() {
		select {
		case ack := <-future.Ok():
			if ack != nil {
				cfg.Logger("nats async publish ack: stream=%s seq=%d", ack.Stream, ack.Sequence)
			}
		case err := <-future.Err():
			if err != nil {
				cfg.Logger("nats async publish error: %v", err)
			}
		}
	}()
}

// NewNATSProvider creates a NATS provider implementing queue.Provider.
func NewNATSProvider[T any](opts ...Option) (queue.Provider[T], error) {
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
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	sess := &session{cfg: typedCfg.Config}
	prod := &producer[T]{cfg: typedCfg, session: sess}
	cons := &consumer[T]{cfg: typedCfg, session: sess}
	provider := &provider[T]{producer: prod, consumer: cons}
	if typedCfg.WarmUp {
		ctx, cancel := context.WithTimeout(context.Background(), typedCfg.DialTimeout)
		defer cancel()
		if err := provider.WarmUp(ctx); err != nil {
			return nil, err
		}
	}
	return provider, nil
}

type provider[T any] struct {
	producer *producer[T]
	consumer *consumer[T]
}

func (p *provider[T]) Producer() queue.Producer[T] { return p.producer }
func (p *provider[T]) Consumer() queue.Consumer[T] { return p.consumer }

// WarmUp eagerly establishes the shared NATS connection and JetStream context.
func (p *provider[T]) WarmUp(ctx context.Context) error {
	return p.producer.WarmUp(ctx)
}

// NewNATSProducer creates a NATS producer only.
func NewNATSProducer[T any](opts ...Option) (queue.Producer[T], error) {
	provider, err := NewNATSProvider[T](opts...)
	if err != nil {
		return nil, err
	}
	return provider.Producer(), nil
}

// NewNATSConsumer creates a NATS consumer only.
func NewNATSConsumer[T any](opts ...Option) (queue.Consumer[T], error) {
	provider, err := NewNATSProvider[T](opts...)
	if err != nil {
		return nil, err
	}
	return provider.Consumer(), nil
}

func toTypedConfig[T any](cfg runtimeConfig) (typedConfig[T], error) {
	enc, ok := cfg.Encoder.(queue.Encoder[T])
	if !ok {
		return typedConfig[T]{}, fmt.Errorf("nats: encoder type mismatch")
	}
	dec, ok := cfg.Decoder.(queue.Decoder[T])
	if !ok {
		return typedConfig[T]{}, fmt.Errorf("nats: decoder type mismatch")
	}
	return typedConfig[T]{
		Config:  cfg.Config,
		Encoder: enc,
		Decoder: dec,
	}, nil
}

func validateConfig(cfg runtimeConfig) error {
	if cfg.URL == "" {
		return errors.New("nats: url not configured")
	}
	if cfg.authTokenSet && cfg.authUserInfoSet {
		return ErrAuthConflict
	}
	if cfg.authTokenSet && cfg.AuthToken == "" {
		return ErrInvalidAuth
	}
	if cfg.authUserInfoSet && (cfg.AuthUser == "" || cfg.AuthPassword == "") {
		return ErrInvalidAuth
	}
	return validateModeConfig(cfg.Config)
}

func validateModeConfig(cfg Config) error {
	if cfg.Subject == "" {
		return ErrSubjectRequired
	}
	if cfg.Mode == Core {
		return nil
	}
	if cfg.Stream == "" {
		return ErrStreamRequired
	}
	if cfg.Durable == "" {
		return ErrDurableRequired
	}
	return nil
}

type session struct {
	cfg Config
	mu  sync.Mutex
	nc  *natsgo.Conn
	js  jetstream.JetStream
}

func (s *session) ensureConn() (*natsgo.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nc != nil && !s.nc.IsClosed() {
		return s.nc, nil
	}
	opts := []natsgo.Option{
		natsgo.Timeout(s.cfg.DialTimeout),
		natsgo.ReconnectWait(s.cfg.ReconnectBackoff),
		natsgo.MaxReconnects(maxReconnects(s.cfg)),
	}
	if s.cfg.AuthToken != "" {
		opts = append(opts, natsgo.Token(s.cfg.AuthToken))
	}
	if s.cfg.AuthUser != "" {
		opts = append(opts, natsgo.UserInfo(s.cfg.AuthUser, s.cfg.AuthPassword))
	}
	nc, err := natsgo.Connect(s.cfg.URL, opts...)
	if err != nil {
		return nil, err
	}
	s.nc = nc
	s.js = nil
	return nc, nil
}

func (s *session) ensureJetStream(ctx context.Context) (jetstream.JetStream, error) {
	s.mu.Lock()
	js := s.js
	s.mu.Unlock()
	if js != nil {
		return js, nil
	}
	nc, err := s.ensureConn()
	if err != nil {
		return nil, err
	}
	js, err = jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	if s.cfg.AutoCreateStream {
		if _, err := js.CreateOrUpdateStream(ctx, s.streamConfig()); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	s.js = js
	s.mu.Unlock()
	return js, nil
}

func (s *session) streamConfig() jetstream.StreamConfig {
	subjects := append([]string{}, s.cfg.Subjects...)
	if len(subjects) == 0 {
		subjects = []string{s.cfg.Subject}
	}
	return jetstream.StreamConfig{
		Name:       s.cfg.Stream,
		Subjects:   subjects,
		Storage:    s.cfg.Storage,
		Retention:  s.cfg.Retention,
		Discard:    s.cfg.Discard,
		Replicas:   s.cfg.Replicas,
		MaxAge:     s.cfg.StreamMaxAge,
		MaxBytes:   s.cfg.StreamMaxBytes,
		Duplicates: s.cfg.DuplicateWindow,
	}
}

func (s *session) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nc != nil {
		s.nc.Close()
	}
	s.nc = nil
	s.js = nil
}

func maxReconnects(cfg Config) int {
	if cfg.ReconnectForever {
		return -1
	}
	return cfg.MaxRetries
}

type producer[T any] struct {
	cfg     typedConfig[T]
	session *session
	closed  atomic.Bool
}

// WarmUp eagerly establishes the producer's NATS connection and JetStream context.
func (p *producer[T]) WarmUp(ctx context.Context) error {
	if p.closed.Load() {
		return queue.ErrClosed
	}
	if p.cfg.Mode == Core {
		_, err := p.session.ensureConn()
		return err
	}
	_, err := p.session.ensureJetStream(ctx)
	return err
}

func (p *producer[T]) Send(ctx context.Context, msg queue.Message[T]) error {
	if p.closed.Load() {
		return queue.ErrClosed
	}
	payload, err := p.cfg.Encoder.Encode(ctx, msg.Value)
	if err != nil {
		return err
	}
	natsMsg := &natsgo.Msg{
		Subject: p.cfg.Subject,
		Data:    payload,
		Header:  headersFromMap(msg.Headers),
	}
	if p.cfg.MsgIDFromKey && msg.Key != "" && natsMsg.Header.Get(msgIDKey) == "" {
		natsMsg.Header.Set(msgIDKey, msg.Key)
	}
	if p.cfg.Mode == Core {
		nc, err := p.session.ensureConn()
		if err != nil {
			return err
		}
		return nc.PublishMsg(natsMsg)
	}
	js, err := p.session.ensureJetStream(ctx)
	if err != nil {
		return err
	}
	return p.publishJetStream(ctx, js, natsMsg)
}

type jetStreamPublisher interface {
	PublishMsg(context.Context, *natsgo.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	PublishMsgAsync(*natsgo.Msg, ...jetstream.PublishOpt) (jetstream.PubAckFuture, error)
}

func (p *producer[T]) publishJetStream(ctx context.Context, publisher jetStreamPublisher, msg *natsgo.Msg) error {
	if !p.cfg.AsyncPublish {
		_, err := publisher.PublishMsg(ctx, msg)
		return err
	}
	future, err := publisher.PublishMsgAsync(msg)
	if err != nil {
		return err
	}
	p.cfg.AsyncPublishAckHandler(future)
	return nil
}

func (p *producer[T]) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.session.reset()
	return nil
}

type consumer[T any] struct {
	cfg      typedConfig[T]
	session  *session
	sub      *natsgo.Subscription
	messages jetstream.MessagesContext
	mu       sync.Mutex
	closed   atomic.Bool
}

func (c *consumer[T]) Receive(ctx context.Context) (queue.Message[T], error) {
	if c.closed.Load() {
		return queue.Message[T]{}, queue.ErrClosed
	}
	if c.cfg.Mode == Core {
		return c.receiveCore(ctx)
	}
	if c.cfg.ConsumerMode == PushConsumer {
		return c.receiveJetStreamPush(ctx)
	}
	return c.receiveJetStream(ctx)
}

func (c *consumer[T]) receiveCore(ctx context.Context) (queue.Message[T], error) {
	sub, err := c.ensureSubscription()
	if err != nil {
		return queue.Message[T]{}, err
	}
	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return queue.Message[T]{}, err
	}
	return c.decodeNATSMessage(ctx, msg)
}

func (c *consumer[T]) receiveJetStream(ctx context.Context) (queue.Message[T], error) {
	messages, err := c.ensureMessages(ctx)
	if err != nil {
		return queue.Message[T]{}, err
	}
	msg, err := messages.Next()
	if err != nil {
		return queue.Message[T]{}, err
	}
	value, err := c.cfg.Decoder.Decode(ctx, msg.Data())
	if err != nil {
		return queue.Message[T]{}, err
	}
	meta := map[string]string{
		"subject": msg.Subject(),
	}
	if metadata, err := msg.Metadata(); err == nil {
		meta["stream"] = metadata.Stream
		meta["consumer"] = metadata.Consumer
		meta["streamSeq"] = strconv.FormatUint(metadata.Sequence.Stream, 10)
		meta["consumerSeq"] = strconv.FormatUint(metadata.Sequence.Consumer, 10)
		meta["numDelivered"] = strconv.FormatUint(uint64(metadata.NumDelivered), 10)
	}
	return queue.Message[T]{
		Key:       msg.Headers().Get(msgIDKey),
		Value:     value,
		Timestamp: time.Now(),
		Headers:   headersToMap(msg.Headers()),
		Meta:      meta,
		Raw:       msg,
	}, nil
}

func (c *consumer[T]) receiveJetStreamPush(ctx context.Context) (queue.Message[T], error) {
	sub, err := c.ensurePushSubscription(ctx)
	if err != nil {
		return queue.Message[T]{}, err
	}
	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return queue.Message[T]{}, err
	}
	return c.decodeJetStreamNATSMessage(ctx, msg)
}

func (c *consumer[T]) Commit(ctx context.Context, msg queue.Message[T]) error {
	if c.closed.Load() {
		return queue.ErrClosed
	}
	if c.cfg.Mode == Core {
		return nil
	}
	if c.cfg.ConsumerMode == PushConsumer {
		raw, ok := msg.Raw.(*natsgo.Msg)
		if !ok {
			return queue.ErrInvalidMessage
		}
		if c.cfg.DoubleAck {
			return raw.AckSync(natsgo.Context(ctx))
		}
		return raw.Ack()
	}
	raw, ok := msg.Raw.(jetstream.Msg)
	if !ok {
		return queue.ErrInvalidMessage
	}
	if c.cfg.DoubleAck {
		return raw.DoubleAck(ctx)
	}
	return raw.Ack()
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
			case <-ctx.Done():
				return
			case messages <- msg:
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
	if c.sub != nil {
		_ = c.sub.Unsubscribe()
		c.sub = nil
	}
	if c.messages != nil {
		c.messages.Stop()
		c.messages = nil
	}
	c.mu.Unlock()
	c.session.reset()
	return nil
}

func (c *consumer[T]) ensureSubscription() (*natsgo.Subscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sub != nil {
		return c.sub, nil
	}
	nc, err := c.session.ensureConn()
	if err != nil {
		return nil, err
	}
	if c.cfg.QueueGroup != "" {
		c.sub, err = nc.QueueSubscribeSync(c.cfg.Subject, c.cfg.QueueGroup)
	} else {
		c.sub, err = nc.SubscribeSync(c.cfg.Subject)
	}
	return c.sub, err
}

func (c *consumer[T]) ensurePushSubscription(ctx context.Context) (*natsgo.Subscription, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sub != nil {
		return c.sub, nil
	}
	if c.cfg.AutoCreateStream {
		if _, err := c.session.ensureJetStream(ctx); err != nil {
			return nil, err
		}
	}
	nc, err := c.session.ensureConn()
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	opts := c.pushSubscribeOptions()
	if c.cfg.DeliverGroup != "" {
		c.sub, err = js.QueueSubscribeSync(c.filterSubject(), c.cfg.DeliverGroup, opts...)
	} else {
		c.sub, err = js.SubscribeSync(c.filterSubject(), opts...)
	}
	return c.sub, err
}

func (c *consumer[T]) ensureMessages(ctx context.Context) (jetstream.MessagesContext, error) {
	c.mu.Lock()
	messages := c.messages
	c.mu.Unlock()
	if messages != nil {
		return messages, nil
	}
	js, err := c.session.ensureJetStream(ctx)
	if err != nil {
		return nil, err
	}
	consumer, err := js.CreateOrUpdateConsumer(ctx, c.cfg.Stream, jetstream.ConsumerConfig{
		Durable:       c.cfg.Durable,
		FilterSubject: c.filterSubject(),
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       c.cfg.AckWait,
		MaxDeliver:    c.cfg.MaxDeliver,
	})
	if err != nil {
		return nil, err
	}
	messages, err = consumer.Messages(jetstream.PullMaxMessages(c.cfg.PullMaxMessages))
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.messages = messages
	c.mu.Unlock()
	return messages, nil
}

func (c *consumer[T]) pushSubscribeOptions() []natsgo.SubOpt {
	opts := []natsgo.SubOpt{
		natsgo.BindStream(c.cfg.Stream),
		natsgo.Durable(c.cfg.Durable),
		natsgo.ManualAck(),
		natsgo.AckExplicit(),
		natsgo.AckWait(c.cfg.AckWait),
		natsgo.MaxDeliver(c.cfg.MaxDeliver),
	}
	if c.cfg.DeliverSubject != "" {
		opts = append(opts, natsgo.DeliverSubject(c.cfg.DeliverSubject))
	}
	if c.cfg.ReplayPolicy == ReplayOriginal {
		opts = append(opts, natsgo.ReplayOriginal())
	} else {
		opts = append(opts, natsgo.ReplayInstant())
	}
	return opts
}

func (c *consumer[T]) filterSubject() string {
	if c.cfg.FilterSubject != "" {
		return c.cfg.FilterSubject
	}
	return c.cfg.Subject
}

func (c *consumer[T]) decodeNATSMessage(ctx context.Context, raw *natsgo.Msg) (queue.Message[T], error) {
	value, err := c.cfg.Decoder.Decode(ctx, raw.Data)
	if err != nil {
		return queue.Message[T]{}, err
	}
	return queue.Message[T]{
		Key:       raw.Header.Get(msgIDKey),
		Value:     value,
		Timestamp: time.Now(),
		Headers:   headersToMap(raw.Header),
		Meta: map[string]string{
			"subject": raw.Subject,
			"reply":   raw.Reply,
		},
		Raw: raw,
	}, nil
}

func (c *consumer[T]) decodeJetStreamNATSMessage(ctx context.Context, raw *natsgo.Msg) (queue.Message[T], error) {
	msg, err := c.decodeNATSMessage(ctx, raw)
	if err != nil {
		return queue.Message[T]{}, err
	}
	if msg.Meta == nil {
		msg.Meta = map[string]string{}
	}
	if metadata, err := raw.Metadata(); err == nil {
		msg.Meta["stream"] = metadata.Stream
		msg.Meta["consumer"] = metadata.Consumer
		msg.Meta["streamSeq"] = strconv.FormatUint(metadata.Sequence.Stream, 10)
		msg.Meta["consumerSeq"] = strconv.FormatUint(metadata.Sequence.Consumer, 10)
		msg.Meta["numDelivered"] = strconv.FormatUint(uint64(metadata.NumDelivered), 10)
	}
	return msg, nil
}

func (c *consumer[T]) channelSize() int {
	if c.cfg.MaxChanSize > 0 {
		return c.cfg.MaxChanSize
	}
	return 1
}

func headersFromMap(headers map[string]string) natsgo.Header {
	out := natsgo.Header{}
	for key, value := range headers {
		out.Set(key, value)
	}
	return out
}

func headersToMap(headers natsgo.Header) map[string]string {
	out := make(map[string]string, len(headers))
	for key, values := range headers {
		if len(values) > 0 {
			out[key] = values[0]
		}
	}
	return out
}
