package queue

import "context"

// Provider exposes producer and consumer for a queue implementation.
type Provider[T any] interface {
	Producer() Producer[T]
	Consumer() Consumer[T]
}

// Producer sends messages to a queue.
type Producer[T any] interface {
	Send(ctx context.Context, msg Message[T]) error
	Close() error
}

// Consumer receives messages from a queue.
type Consumer[T any] interface {
	Receive(ctx context.Context) (Message[T], error)
	Commit(ctx context.Context, msg Message[T]) error
	Close() error
}

// ChannelConsumer exposes a buffered channel stream of messages.
type ChannelConsumer[T any] interface {
	Consumer[T]
	Channel(ctx context.Context) (<-chan Message[T], <-chan error)
}
