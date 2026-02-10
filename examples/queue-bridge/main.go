package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alonexy/complug/components/queue"
	"github.com/alonexy/complug/contrib/queue/bridge"
	"github.com/alonexy/complug/contrib/queue/kafka"
	"github.com/alonexy/complug/contrib/queue/rabbitmq"
)

type Payload struct {
	ID   string `json:"id"`
	Body string `json:"body"`
}

func main() {
	loadDotEnv(".env")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	kafkaBrokers := getenv("KAFKA_BROKERS", "localhost:9092")
	kafkaTopic := getenv("KAFKA_TOPIC", "source-topic")
	kafkaGroupID := getenv("KAFKA_GROUP_ID", "demo-group")

	rabbitURL := getenv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
	rabbitExchange := getenv("RABBIT_EXCHANGE", "events-ex")
	rabbitExchangeType := getenv("RABBIT_EXCHANGE_TYPE", "direct")
	//rabbitQueue := getenv("RABBIT_QUEUE", "events-queue")

	kProvider, err := kafka.NewKafkaProvider[Payload](
		kafka.WithQueueConfig(queue.NewConfig(
			queue.WithMaxChanSize(128),
		)),
		kafka.WithBrokers(splitCSV(kafkaBrokers)...),
		kafka.WithTopic(kafkaTopic),
		kafka.WithGroupID(kafkaGroupID),
		kafka.WithAutoCreateTopic(true),
		kafka.WithHashBalancer(),
		kafka.WithCreateTopicPartitions(5),
		kafka.WithCodec[Payload](queue.JSONCodec[Payload]{}),
		kafka.WithLogger(log.Printf),
	)
	if err != nil {
		log.Fatalf("kafka provider error: %v", err)
	}
	defer kProvider.Producer().Close()
	defer kProvider.Consumer().Close()

	rProvider, err := rabbitmq.NewRabbitProvider[Payload](
		rabbitmq.WithQueueConfig(queue.NewConfig(
			queue.WithMaxChanSize(128),
		)),
		rabbitmq.WithURL(rabbitURL),
		rabbitmq.WithExchange(rabbitExchange, rabbitExchangeType),
		rabbitmq.WithExchangeArgs(amqp091.Table{"x-message-ttl": "30000"}),
		rabbitmq.WithCodec[Payload](queue.JSONCodec[Payload]{}),
		rabbitmq.WithLogger(log.Printf),
	)
	if err != nil {
		log.Fatalf("rabbit provider error: %v", err)
	}
	defer rProvider.Producer().Close()
	defer rProvider.Consumer().Close()

	log.Printf("bridge running: kafka %s/%s -> rabbitmq %s", kafkaBrokers, kafkaTopic, rabbitExchange)
	go produceEvery(ctx, kProvider.Producer())
	if err := waitForSeed(ctx, kProvider.Consumer()); err != nil {
		log.Printf("seed wait error: %v", err)
		return
	}
	if err := bridge.Run(
		ctx,
		kProvider.Consumer(),
		rProvider.Producer(),
		bridge.WithRetryBackoff(2*time.Second),
		bridge.WithLogLevel(bridge.LogDebug),
	); err != nil {
		log.Printf("bridge exit: %v", err)
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
			part := value[start:i]
			if part != "" {
				out = append(out, part)
			}
			start = i + 1
		}
	}
	return out
}

func produceEvery(ctx context.Context, producer queue.Producer[Payload]) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	counter := 0
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			counter++
			mid := fmt.Sprintf("demo-%d", counter)
			msg := queue.Message[Payload]{
				Key:       mid,
				Timestamp: now,
				Value: Payload{
					ID:   mid,
					Body: "hello from demo",
				},
			}
			if err := producer.Send(ctx, msg); err != nil {
				log.Printf("kafka produce error: %v", err)
			}
		}
	}
}

func waitForSeed(ctx context.Context, consumer queue.Consumer[Payload]) error {
	seedCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	msg, err := consumer.Receive(seedCtx)
	if err != nil {
		return err
	}
	return consumer.Commit(ctx, msg)
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
