package bridge

import (
	"context"
	"log"
	"time"

	"github.com/alonexy/complug/components/queue"
)

// Config controls bridge behavior.
type Config struct {
	RetryBackoff   time.Duration
	CommitStrategy CommitStrategy
	LogLevel       LogLevel
}

// Option applies changes to Config.
type Option func(*Config)

// LogLevel defines log verbosity.
type LogLevel int

const (
	LogError LogLevel = iota
	LogInfo
	LogDebug
)

// CommitStrategy defines when to commit source messages.
type CommitStrategy int

const (
	CommitAlways CommitStrategy = iota
	CommitOnSuccess
)

// WithRetryBackoff sets the retry backoff for transient failures.
func WithRetryBackoff(backoff time.Duration) Option {
	return func(cfg *Config) {
		cfg.RetryBackoff = backoff
	}
}

// WithCommitStrategy sets the commit strategy.
func WithCommitStrategy(strategy CommitStrategy) Option {
	return func(cfg *Config) {
		cfg.CommitStrategy = strategy
	}
}

// WithLogLevel sets the minimum log level.
func WithLogLevel(level LogLevel) Option {
	return func(cfg *Config) {
		cfg.LogLevel = level
	}
}

func defaultConfig() Config {
	return Config{
		RetryBackoff:   time.Second,
		CommitStrategy: CommitOnSuccess,
		LogLevel:       LogInfo,
	}
}

func (c Config) log(level LogLevel, format string, args ...any) {
	if level <= c.LogLevel {
		log.Printf("[bridge][%s] "+format, append([]any{levelString(level)}, args...)...)
	}
}

func levelString(level LogLevel) string {
	switch level {
	case LogError:
		return "ERROR"
	case LogDebug:
		return "DEBUG"
	default:
		return "INFO"
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
			cfg.log(LogError, "bridge receive error: %v", err)
			time.Sleep(cfg.RetryBackoff)
			continue
		}
		cfg.log(LogDebug, "bridge received message key=%s value=%v", msg.Key, msg.Value)
		if err := dest.Send(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			cfg.log(LogError, "bridge send error: %v", err)
			time.Sleep(cfg.RetryBackoff)
			continue
		}
		cfg.log(LogDebug, "bridge sent message key=%s", msg.Key)
		if cfg.CommitStrategy == CommitOnSuccess || cfg.CommitStrategy == CommitAlways {
			for {
				if err := source.Commit(ctx, msg); err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					cfg.log(LogError, "bridge commit error: %v", err)
					time.Sleep(cfg.RetryBackoff)
					continue
				}
				cfg.log(LogDebug, "bridge committed message key=%s", msg.Key)
				break
			}
		}
	}
}
