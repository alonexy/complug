package bridge

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alonexy/complug/components/queue"
)

type fakeConsumer[T any] struct {
	mu        sync.Mutex
	messages  []queue.Message[T]
	recvErr   error
	commitErr error
	commits   int
}

func (c *fakeConsumer[T]) Receive(ctx context.Context) (queue.Message[T], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.recvErr != nil {
		return queue.Message[T]{}, c.recvErr
	}
	if len(c.messages) == 0 {
		<-ctx.Done()
		return queue.Message[T]{}, ctx.Err()
	}
	msg := c.messages[0]
	c.messages = c.messages[1:]
	return msg, nil
}

func (c *fakeConsumer[T]) Commit(ctx context.Context, _ queue.Message[T]) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commits++
	return c.commitErr
}

func (c *fakeConsumer[T]) Close() error { return nil }

type fakeProducer[T any] struct {
	mu      sync.Mutex
	sent    []queue.Message[T]
	sendErr error
}

func (p *fakeProducer[T]) Send(ctx context.Context, msg queue.Message[T]) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sendErr != nil {
		return p.sendErr
	}
	p.sent = append(p.sent, msg)
	return nil
}

func (p *fakeProducer[T]) Close() error { return nil }

func TestRunHappyPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cons := &fakeConsumer[string]{messages: []queue.Message[string]{
		{Value: "a"},
	}}
	prod := &fakeProducer[string]{}

	done := make(chan error, 1)
	go func() {
		done <- Run[string](ctx, cons, prod, WithRetryBackoff(5*time.Millisecond))
	}()

	// Wait for commit to happen.
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	if len(prod.sent) != 1 {
		t.Fatalf("expected 1 sent message, got %d", len(prod.sent))
	}
	if cons.commits != 1 {
		t.Fatalf("expected 1 commit, got %d", cons.commits)
	}
}

func TestRunRetriesOnReceiveError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cons := &fakeConsumer[string]{recvErr: errors.New("temporary")}
	prod := &fakeProducer[string]{}

	done := make(chan error, 1)
	go func() {
		done <- Run[string](ctx, cons, prod, WithRetryBackoff(5*time.Millisecond))
	}()

	time.Sleep(15 * time.Millisecond)
	cancel()
	<-done
}

func TestRunRetriesOnSendError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cons := &fakeConsumer[string]{messages: []queue.Message[string]{{Value: "a"}}}
	prod := &fakeProducer[string]{sendErr: errors.New("temporary")}

	done := make(chan error, 1)
	go func() {
		done <- Run[string](ctx, cons, prod, WithRetryBackoff(5*time.Millisecond))
	}()

	time.Sleep(15 * time.Millisecond)
	cancel()
	<-done
}
