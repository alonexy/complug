package queue

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrClosed         = errors.New("queue: closed")
	ErrNoEncoder      = errors.New("queue: encoder not configured")
	ErrNoDecoder      = errors.New("queue: decoder not configured")
	ErrInvalidMessage = errors.New("queue: invalid message")
)

// Message carries a typed payload and optional metadata.
type Message[T any] struct {
	Key       string
	Value     T
	Headers   map[string]string
	Timestamp time.Time
	Meta      map[string]string
	Raw       any
}

// Encoder turns a typed value into bytes for transport.
type Encoder[T any] interface {
	Encode(ctx context.Context, value T) ([]byte, error)
}

// Decoder turns transport bytes into a typed value.
type Decoder[T any] interface {
	Decode(ctx context.Context, data []byte) (T, error)
}

// Codec provides both encoder and decoder.
type Codec[T any] interface {
	Encoder[T]
	Decoder[T]
}

// Config stores common queue configuration.
type Config struct {
	MaxChanSize  int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Option applies changes to Config.
type Option func(*Config)

// NewConfig builds a Config with options applied.
func NewConfig(opts ...Option) Config {
	cfg := Config{
		MaxChanSize: 1,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

// WithMaxChanSize sets the buffered channel size for streaming consumers.
func WithMaxChanSize(size int) Option {
	return func(cfg *Config) {
		if size > 0 {
			cfg.MaxChanSize = size
		}
	}
}

// WithDialTimeout sets the dial timeout.
func WithDialTimeout(timeout time.Duration) Option {
	return func(cfg *Config) {
		cfg.DialTimeout = timeout
	}
}

// WithReadTimeout sets the read timeout.
func WithReadTimeout(timeout time.Duration) Option {
	return func(cfg *Config) {
		cfg.ReadTimeout = timeout
	}
}

// WithWriteTimeout sets the write timeout.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(cfg *Config) {
		cfg.WriteTimeout = timeout
	}
}

// JSONCodec is a default codec using JSON encoding.
type JSONCodec[T any] struct{}

func (JSONCodec[T]) Encode(_ context.Context, value T) ([]byte, error) {
	return json.Marshal(value)
}

func (JSONCodec[T]) Decode(_ context.Context, data []byte) (T, error) {
	var v T
	return v, json.Unmarshal(data, &v)
}
