package nats

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/alonexy/complug/components/queue"
)

func TestJetStreamReceiveDoesNotPrefetchWithoutDemand(t *testing.T) {
	stream, subject, suffix := uniqueStreamSubject("demand")
	deleteLocalStream(t, stream)
	opts := []Option{
		WithURL(localIntegrationURL), WithStream(stream), WithSubject(subject),
		WithDurable("demand-" + suffix), WithAutoCreateStream(true),
		WithPullMaxMessages(1), WithMaxDeliver(1), WithAckWait(100 * time.Millisecond),
		WithCodec[string](queue.JSONCodec[string]{}),
	}
	provider, err := NewNATSProvider[string](opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Producer().Close()
	defer provider.Consumer().Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, value := range []string{"A", "B"} {
		if err := provider.Producer().Send(ctx, queue.Message[string]{Value: value}); err != nil {
			t.Fatal(err)
		}
	}
	msg, err := provider.Consumer().Receive(ctx)
	if err != nil || msg.Value != "A" {
		t.Fatalf("first Receive = %q, %v", msg.Value, err)
	}
	if err := provider.Consumer().Commit(ctx, msg); err != nil {
		t.Fatal(err)
	}
	// No Receive requests B. It must remain available to the next consumer even
	// with MaxDeliver=1, rather than being held by an unsolicited background pull.
	time.Sleep(150 * time.Millisecond)
	if err := provider.Consumer().Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewNATSConsumer[string](opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	nextCtx, nextCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer nextCancel()
	msg, err = reopened.Receive(nextCtx)
	if err != nil || msg.Value != "B" {
		t.Fatalf("unrequested message after reopening = %q, %v; want B", msg.Value, err)
	}
}

func TestJetStreamReceiveCancellationWhileAnotherCallInitializes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	receiver, err := NewNATSConsumer[string](
		WithURL("nats://"+listener.Addr().String()), WithStream("INIT"), WithSubject("init"),
		WithDurable("init"), WithWarmUp(false), WithDialTimeout(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	first := make(chan error, 1)
	go func() { _, err := receiver.Receive(context.Background()); first <- err }()
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(3 * time.Second):
		t.Fatal("first Receive did not start connecting")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { _, err := receiver.Receive(ctx); second <- err }()
	select {
	case err := <-second:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waiting Receive = %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cancellation blocked behind another Receive's initialization")
	}
}

func TestJetStreamReceiveContextCancellation(t *testing.T) {
	for _, warmUp := range []bool{true, false} {
		name := "warm"
		if !warmUp {
			name = "lazy"
		}
		t.Run(name, func(t *testing.T) {
			stream, subject, suffix := uniqueStreamSubject("cancel")
			deleteLocalStream(t, stream)
			receiver, err := NewNATSConsumer[string](
				WithURL(localIntegrationURL), WithStream(stream), WithSubject(subject),
				WithDurable("cancel-"+suffix), WithAutoCreateStream(true), WithWarmUp(warmUp),
				WithCodec[string](queue.JSONCodec[string]{}),
				WithPullMaxMessages(4), WithMaxDeliver(1),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer receiver.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, err := receiver.Receive(ctx)
				result <- err
			}()
			waitForPullReceive(t, receiver.(*consumer[string]), result)
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Receive = %v, want context.Canceled", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("context cancellation did not unblock Receive")
			}

			producer, err := NewNATSProducer[string](
				WithURL(localIntegrationURL), WithStream(stream), WithSubject(subject),
				WithCodec[string](queue.JSONCodec[string]{}),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer producer.Close()
			nextCtx, nextCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer nextCancel()
			for _, value := range []string{"first", "second", "third"} {
				if err := producer.Send(nextCtx, queue.Message[string]{Value: value}); err != nil {
					t.Fatal(err)
				}
			}
			// A context canceled before Receive must not consume an available message.
			if _, err := receiver.Receive(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("Receive with canceled context = %v", err)
			}
			for _, want := range []string{"first", "second", "third"} {
				msg, err := receiver.Receive(nextCtx)
				if err != nil || msg.Value != want {
					t.Fatalf("Receive after cancellation = %q, %v; want %q", msg.Value, err, want)
				}
				if err := receiver.Commit(nextCtx, msg); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestJetStreamReceiveDeadline(t *testing.T) {
	provider := newCancellationProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := provider.Consumer().Receive(ctx)
		result <- err
	}()
	waitForPullReceive(t, provider.Consumer().(*consumer[string]), result)
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Receive = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("deadline did not unblock Receive")
	}
}

func TestJetStreamCancelDoesNotInterruptOtherReceiver(t *testing.T) {
	provider := newCancellationProvider(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := provider.Consumer().Receive(ctx)
		result <- err
	}()
	waitForPullReceive(t, provider.Consumer().(*consumer[string]), result)
	otherCtx, otherCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer otherCancel()
	other := make(chan error, 1)
	go func() {
		msg, err := provider.Consumer().Receive(otherCtx)
		if err == nil {
			if msg.Value != "other" {
				err = errors.New("other receiver got wrong message")
			} else {
				err = provider.Consumer().Commit(otherCtx, msg)
			}
		}
		other <- err
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled receiver = %v", err)
		}
	case <-otherCtx.Done():
		t.Fatal("canceled receiver did not return")
	}
	if err := provider.Producer().Send(otherCtx, queue.Message[string]{Value: "other"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-other:
		if err != nil {
			t.Fatalf("unrelated receiver failed: %v", err)
		}
	case <-otherCtx.Done():
		t.Fatal("unrelated receiver did not receive the message")
	}
}

func TestJetStreamChannelCancellation(t *testing.T) {
	provider := newCancellationProvider(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	receiver := provider.Consumer().(*consumer[string])
	messages, errs := receiver.Channel(ctx)
	waitForPullReceive(t, receiver, errs)
	cancel()
	select {
	case _, ok := <-messages:
		if ok {
			t.Fatal("unexpected message on empty stream")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Channel did not close after cancellation")
	}
	select {
	case err, ok := <-errs:
		if ok {
			t.Fatalf("Channel reported cancellation as an error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("error channel did not close after cancellation")
	}
}

func TestJetStreamCancelAndCloseConcurrent(t *testing.T) {
	for i := 0; i < 20; i++ {
		provider := newCancellationProvider(t)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := provider.Consumer().Receive(ctx)
			result <- err
		}()
		receiver := provider.Consumer().(*consumer[string])
		waitForPullReceive(t, receiver, result)
		closed := make(chan error, 1)
		go func() { closed <- receiver.Close() }()
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) && !errors.Is(err, queue.ErrClosed) {
				t.Fatalf("Receive during cancel/close = %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Receive blocked during cancel/close")
		}
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Close blocked during cancellation")
		}
		select {
		case <-receiver.pull.stopped:
		default:
			t.Fatal("Close returned before the pull receiver stopped")
		}
	}
}

func TestJetStreamChannelCancellationWithFullErrorBuffer(t *testing.T) {
	decodeErr := errors.New("test decode failure")
	decoded := make(chan struct{}, 2)
	provider := newCancellationProvider(t, WithDecoder[string](cancellationTestDecoder{decoded, decodeErr}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	messages, errs := provider.Consumer().(queue.ChannelConsumer[string]).Channel(ctx)
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer sendCancel()
	for i := 0; i < 2; i++ {
		if err := provider.Producer().Send(sendCtx, queue.Message[string]{Value: "bad"}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-decoded:
		case <-sendCtx.Done():
			t.Fatal("Channel did not fill the error buffer")
		}
	}
	cancel()
	// Do not drain errs until Channel exits: the pending error send must be cancellable.
	select {
	case _, ok := <-messages:
		if ok {
			t.Fatal("unexpected decoded message")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Channel cancellation blocked on the full error buffer")
	}
	if err, ok := <-errs; !ok || !errors.Is(err, decodeErr) {
		t.Fatalf("buffered error = %v, open=%v", err, ok)
	}
	if err, ok := <-errs; ok {
		t.Fatalf("error channel still open: %v", err)
	}
}

type cancellationTestDecoder struct {
	decoded chan<- struct{}
	err     error
}

func (d cancellationTestDecoder) Decode(context.Context, []byte) (string, error) {
	d.decoded <- struct{}{}
	return "", d.err
}

func newCancellationProvider(t *testing.T, extra ...Option) queue.Provider[string] {
	t.Helper()
	stream, subject, suffix := uniqueStreamSubject("context")
	deleteLocalStream(t, stream)
	opts := []Option{
		WithURL(localIntegrationURL), WithStream(stream), WithSubject(subject),
		WithDurable("context-" + suffix), WithAutoCreateStream(true),
		WithCodec[string](queue.JSONCodec[string]{}),
	}
	provider, err := NewNATSProvider[string](append(opts, extra...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Producer().Close() })
	t.Cleanup(func() { _ = provider.Consumer().Close() })
	return provider
}

func waitForPullReceive(t *testing.T, receiver *consumer[string], result <-chan error) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		receiver.mu.Lock()
		ready := receiver.pull != nil
		receiver.mu.Unlock()
		if ready {
			break
		}
		select {
		case err := <-result:
			t.Fatalf("Receive returned before cancellation: %v", err)
		case <-deadline:
			t.Fatal("Receive did not initialize")
		case <-ticker.C:
		}
	}
	select {
	case err := <-result:
		t.Fatalf("idle Receive returned before cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}
