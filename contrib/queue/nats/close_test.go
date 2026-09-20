package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alonexy/complug/components/queue"
)

func TestJetStreamCloseUnblocksReceive(t *testing.T) {
	for _, warmUp := range []bool{true, false} {
		name := "warm"
		if !warmUp {
			name = "lazy"
		}
		t.Run(name, func(t *testing.T) {
			stream, subject, suffix := uniqueStreamSubject("close")
			deleteLocalStream(t, stream)
			receiver, err := NewNATSConsumer[string](
				WithURL(localIntegrationURL), WithStream(stream), WithSubject(subject),
				WithDurable("close-"+suffix), WithAutoCreateStream(true), WithWarmUp(warmUp),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer receiver.Close()
			result := make(chan error, 1)
			go func() {
				_, err := receiver.Receive(context.Background())
				result <- err
			}()
			// 等到真实迭代器完成初始化，确保验证的是进行中的 Receive。
			internal := receiver.(*consumer[string])
			deadline := time.After(3 * time.Second)
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				internal.mu.Lock()
				ready := internal.messages != nil
				internal.mu.Unlock()
				if ready {
					break
				}
				select {
				case err := <-result:
					t.Fatalf("Receive returned before Close: %v", err)
				case <-deadline:
					t.Fatal("Receive did not initialize")
				case <-ticker.C:
				}
			}
			select {
			case err := <-result:
				t.Fatalf("idle Receive returned before Close: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			closed := make(chan error, 1)
			go func() { closed <- receiver.Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Close blocked")
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("closed Receive returned no error")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Close did not unblock Receive")
			}
			if err := receiver.Close(); err != nil {
				t.Fatalf("repeated Close: %v", err)
			}
			if _, err := receiver.Receive(context.Background()); !errors.Is(err, queue.ErrClosed) {
				t.Fatalf("Receive after Close = %v, want queue.ErrClosed", err)
			}
		})
	}
}
