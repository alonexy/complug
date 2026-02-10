# Queue 使用说明

本目录说明 `components/queue` 的通用接口，以及 `contrib/queue/kafka` / `contrib/queue/rabbitmq` 的适配实现与常用配置。

## 1. 命名对照

旧名称已移除，请使用以下新名称：

| 旧名称 | 新名称 | 说明 |
| --- | --- | --- |
| `WithPartition` | `WithConsumePartition` | 指定消费者分区（无 `GroupID` 时生效） |
| `WithTopicPartitions` | `WithCreateTopicPartitions` | 自动创建 Topic 的分区数 |

## 2. 通用接口

核心接口在 `components/queue`：
- `Provider[T]` 提供 `Producer[T]` 与 `Consumer[T]`
- `Producer[T]` 负责发送消息
- `Consumer[T]` 负责接收与提交消息

通用配置：
```go
base := queue.NewConfig(
    queue.WithMaxChanSize(128),
    queue.WithReadTimeout(5*time.Second),
)
```

## 3. Kafka 使用

### 3.1 创建 Provider
```go
kProvider, err := kafka.NewKafkaProvider[Payload](
kafka.WithQueueConfig(base),
kafka.WithBrokers("localhost:9092"),
kafka.WithTopic("demo-topic"),
kafka.WithGroupID("demo-group"),
kafka.WithConsumePartition(0),
kafka.WithHashBalancer(),
kafka.WithCodec[Payload](queue.JSONCodec[Payload]{}),
)
```

### 3.2 只创建 Producer / Consumer
```go
producer, _ := kafka.NewKafkaProducer[Payload](/* options... */)
consumer, _ := kafka.NewKafkaConsumer[Payload](/* options... */)
```

### 3.3 分区策略
- `WithHashBalancer()`：按 Key 分区
- `WithRoundRobinBalancer()`：轮询分区
- `WithLeastBytesBalancer()`：最少字节优先
- 自定义分区器：实现 `kafka.Balancer`

### 3.4 重连与重试
```go
kafka.WithReconnectForever(true),      // 默认 true
kafka.WithMaxRetries(5),               // 当 ReconnectForever=false 才生效
kafka.WithDefaultRetryClassifier(),
kafka.WithRetryClassifier(func(err error) bool {
    return !strings.Contains(err.Error(), "ACL")
}),
```

### 3.5 Reader 调优
```go
kafka.WithQueueCapacity(200),
kafka.WithMinBytes(1),
kafka.WithMaxBytes(10<<20),
kafka.WithMaxWait(2*time.Second),
kafka.WithReadBatchTimeout(5*time.Second),
kafka.WithGroupBalancers(kafka.RangeGroupBalancer, kafka.RoundRobinGroupBalancer),
kafka.WithStartOffset(kafka.FirstOffset),
kafka.WithIsolationLevel(kafka.ReadCommitted),
kafka.WithEnableAutoCommit(true),
kafka.WithAutoCommitInterval(2*time.Second),
```

### 3.6 Writer 调优
```go
kafka.WithBatchSize(100),
kafka.WithBatchBytes(1048576),
kafka.WithBatchTimeout(1*time.Second),
kafka.WithAsync(false),
kafka.WithMaxAttempts(10),
```

### 3.7 Transport / Topic 配置
```go
kafka.WithTransport(&kafka.Transport{
    ClientID: "demo-client",
    // TLS / SASL / Dial 等配置...
}),
kafka.WithAutoCreateTopic(true),
kafka.WithCreateTopicPartitions(3),
kafka.WithTopicReplication(1),
kafka.WithRetentionMs(604800000),       // 7 天
kafka.WithRetentionBytes(1073741824),   // 1 GB
kafka.WithSegmentBytes(1073741824),     // 1 GB segment
kafka.WithCleanupPolicy("delete"),      // delete / compact
kafka.WithApplyTopicConfigOnExists(true),
```

## 4. RabbitMQ 使用

### 4.1 创建 Provider
```go
rProvider, err := rabbitmq.NewRabbitProvider[Payload](
    rabbitmq.WithQueueConfig(base),
    rabbitmq.WithURL("amqp://guest:guest@localhost:5672/"),
    rabbitmq.WithExchange("events-ex", "topic"),
rabbitmq.WithQueueName("events-queue"),
rabbitmq.WithAutoAck(false),
rabbitmq.WithRoutingKey("events.*"),
rabbitmq.WithCodec[Payload](queue.JSONCodec[Payload]{}),
)
```

### 4.2 只创建 Producer / Consumer
```go
producer, _ := rabbitmq.NewRabbitProducer[Payload](/* options... */)
consumer, _ := rabbitmq.NewRabbitConsumer[Payload](/* options... */)
```

### 4.3 交换机 / 队列参数
```go
rabbitmq.WithExchangeArgs(amqp.Table{
    "alternate-exchange": "fallback-ex",
}),
rabbitmq.WithQueueArgs(amqp.Table{
    "x-message-ttl": int32(60000),
    "x-dead-letter-exchange": "dlx-ex",
}),
rabbitmq.WithBindingArgs(amqp.Table{
    "x-match": "all",
}),
```

### 4.4 解码失败处理
```go
rabbitmq.WithDecodeErrorStrategy(rabbitmq.DecodeErrorNackDrop)
```

可选策略：
- `DecodeErrorReturn`
- `DecodeErrorAck`
- `DecodeErrorNackRequeue`
- `DecodeErrorNackDrop`

### 4.5 重连与重试
```go
rabbitmq.WithReconnectForever(true),
rabbitmq.WithMaxRetries(5),
rabbitmq.WithRetryClassifier(func(err error) bool {
    return !strings.Contains(err.Error(), "ACCESS_REFUSED")
}),
```

## 5. Bridge 使用

```go
bridge.Run(ctx, source, dest,
    bridge.WithRetryBackoff(1*time.Second),
    bridge.WithCommitStrategy(bridge.CommitOnSuccess),
    bridge.WithLogLevel(bridge.LogInfo),
)
```

提交策略：
- `CommitAlways`：无论发送成功与否都提交（可能丢消息）
- `CommitOnSuccess`：发送成功后提交（至少一次语义）

## 6. Demo

运行 `examples/queue-bridge` 可看到 Kafka -> RabbitMQ 桥接与定时生产示例：
```bash
cd examples/queue-bridge
go run .
```
