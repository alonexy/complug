package bridge

import (
	"context"
	"time"

	"github.com/alonexy/complug/components/queue"
)

// Config controls bridge behavior.
type Config struct {
	RetryBackoff time.Duration
}

// Option applies changes to Config.
type Option func(*Config)

// WithRetryBackoff sets the retry backoff for transient failures.
func WithRetryBackoff(backoff time.Duration) Option {
	return func(cfg *Config) {
		cfg.RetryBackoff = backoff
	}
}

func defaultConfig() Config {
	return Config{
		RetryBackoff: time.Second,
	}
}

// Run consumes messages from the source and publishes them to the destination.
func Run[T any](ctx context.Context, source queue.Consumer[T], dest queue.Producer[T], opts ...Option) error {
	cfg := defaultConfig()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	for {
		msg, err := source.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			time.Sleep(cfg.RetryBackoff)
			continue
		}
		if err := dest.Send(ctx, msg); err != nil {
			time.Sleep(cfg.RetryBackoff)
			continue
		}
		_ = source.Commit(ctx, msg)
	}
}
