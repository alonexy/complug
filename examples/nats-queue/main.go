package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alonexy/complug/components/queue"
	natsq "github.com/alonexy/complug/contrib/queue/nats"
)

type Payload struct {
	ID   string `json:"id"`
	Body string `json:"body"`
}

const localNATSURL = "nats://127.0.0.1:4222"

func main() {
	loadDotEnv(".env")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	provider, err := natsq.NewNATSProvider[Payload](providerOptions()...)
	if err != nil {
		log.Fatalf("nats provider error: %v", err)
	}
	defer provider.Producer().Close()
	defer provider.Consumer().Close()

	received := make(chan queue.Message[Payload], 1)
	errs := make(chan error, 1)
	go func() {
		msg, err := provider.Consumer().Receive(ctx)
		if err != nil {
			errs <- err
			return
		}
		received <- msg
	}()

	// Core NATS has no retention, so give the subscription a moment to attach.
	time.Sleep(200 * time.Millisecond)

	msg := queue.Message[Payload]{
		Key:       fmt.Sprintf("demo-%d", time.Now().UnixNano()),
		Timestamp: time.Now(),
		Headers: map[string]string{
			"source": "examples/nats-queue",
		},
		Value: Payload{
			ID:   "demo",
			Body: "hello from nats example",
		},
	}
	if err := provider.Producer().Send(ctx, msg); err != nil {
		log.Fatalf("nats publish error: %v", err)
	}
	log.Printf("published key=%s", msg.Key)

	select {
	case msg := <-received:
		log.Printf("received key=%s value=%+v meta=%v", msg.Key, msg.Value, msg.Meta)
		if err := provider.Consumer().Commit(ctx, msg); err != nil {
			log.Fatalf("nats commit error: %v", err)
		}
		log.Printf("committed key=%s", msg.Key)
	case err := <-errs:
		log.Fatalf("nats receive error: %v", err)
	case <-time.After(10 * time.Second):
		log.Fatalf("timed out waiting for nats message")
	case <-ctx.Done():
		log.Fatalf("context canceled: %v", ctx.Err())
	}
}

func providerOptions() []natsq.Option {
	mode := strings.ToLower(getenv("NATS_MODE", "jetstream"))
	opts := []natsq.Option{
		natsq.WithURL(getenv("NATS_URL", localNATSURL)),
		natsq.WithSubject(getenv("NATS_SUBJECT", "events.demo")),
		natsq.WithCodec[Payload](queue.JSONCodec[Payload]{}),
		natsq.WithLogger(log.Printf),
	}

	if mode == "core" {
		opts = append(opts,
			natsq.WithMode(natsq.Core),
			natsq.WithQueueGroup(getenv("NATS_QUEUE_GROUP", "demo-workers")),
		)
		return opts
	}

	opts = append(opts,
		natsq.WithMode(natsq.JetStream),
		natsq.WithStream(getenv("NATS_STREAM", "EVENTS")),
		natsq.WithSubjects(splitCSV(getenv("NATS_STREAM_SUBJECTS", "events.*"))...),
		natsq.WithDurable(getenv("NATS_DURABLE", "events-demo-worker")),
		natsq.WithFilterSubject(getenv("NATS_FILTER_SUBJECT", getenv("NATS_SUBJECT", "events.demo"))),
		natsq.WithAutoCreateStream(true),
		natsq.WithMsgIDFromKey(true),
	)
	if strings.ToLower(getenv("NATS_CONSUMER_MODE", "pull")) == "push" {
		opts = append(opts,
			natsq.WithConsumerMode(natsq.PushConsumer),
			natsq.WithDeliverSubject(getenv("NATS_DELIVER_SUBJECT", "deliver.events.demo")),
			natsq.WithDeliverGroup(getenv("NATS_DELIVER_GROUP", "events-demo-push-workers")),
			natsq.WithReplayPolicy(replayPolicyFromEnv()),
		)
	}
	if truthy(os.Getenv("NATS_ASYNC_PUBLISH")) {
		opts = append(opts,
			natsq.WithAsyncPublish(true),
			natsq.WithAsyncPublishAckHandler(logAsyncPublishResult),
		)
	}
	return opts
}

func replayPolicyFromEnv() natsq.ReplayPolicy {
	if strings.ToLower(getenv("NATS_REPLAY_POLICY", "instant")) == "original" {
		return natsq.ReplayOriginal
	}
	return natsq.ReplayInstant
}

func logAsyncPublishResult(future jetstream.PubAckFuture) {
	select {
	case ack := <-future.Ok():
		log.Printf("async publish ack stream=%s seq=%d", ack.Stream, ack.Sequence)
	case err := <-future.Err():
		log.Printf("async publish error: %v", err)
	}
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	var out []string
	start := 0
	for i := 0; i <= len(value); i++ {
		if i == len(value) || value[i] == ',' {
			part := strings.TrimSpace(value[start:i])
			if part != "" {
				out = append(out, part)
			}
			start = i + 1
		}
	}
	return out
}

func truthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func loadDotEnv(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	lines := bytes.Split(data, []byte{'\n'})
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		parts := bytes.SplitN(line, []byte{'='}, 2)
		if len(parts) != 2 {
			continue
		}
		key := string(bytes.TrimSpace(parts[0]))
		if key == "" {
			continue
		}
		value := string(bytes.TrimSpace(parts[1]))
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, value)
		}
	}
}
