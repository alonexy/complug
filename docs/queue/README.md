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

### 2.1 内置 Codec

`components/queue` 内置以下编码器：
- `JSONCodec[T]`
- `BytesCodec`（`[]byte`）
- `StringCodec`（`string`）

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

### 3.8 Protobuf Codec

```go
import "github.com/alonexy/complug/contrib/queue/protobuf"

type MyMessage = pb.MyMessage

provider, _ := kafka.NewKafkaProvider[*MyMessage](
    kafka.WithBrokers("localhost:9092"),
    kafka.WithTopic("demo-topic"),
    kafka.WithCodec[*MyMessage](protobuf.Codec[*MyMessage]{}),
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

说明：
- `WithEnableAutoCommit(true)` 在当前适配层中表示“启用 `CommitInterval` 异步 flush”。
- `Consumer.Receive()` 仍然使用 `FetchMessage()`，不会切换到 `kafka-go` 的 `ReadMessage()`。
- `Consumer.Commit()` 仍需由业务侧或 `bridge.CommitOnSuccess` 显式调用，以保持“处理成功后提交”的至少一次语义。

### 3.6 Writer 调优

默认 writer 配置：
- `BatchSize=10`
- `BatchTimeout=3*time.Millisecond`
- `Compression=kafka.Lz4`
- `RequiredAcks=kafka.RequireAll`

```go
kafka.WithBatchSize(100),
kafka.WithBatchBytes(1048576),
kafka.WithBatchTimeout(1*time.Second),
kafka.WithCompression(kafka.Snappy),
kafka.WithRequiredAcks(kafka.RequireOne),
kafka.WithAsync(false),
kafka.WithMaxAttempts(10),
```

### 3.7 Transport / Topic 配置
未显式传入 `WithTransport` 时，Kafka Producer 默认会创建以下 Transport：
- `ClientID` 使用 `WithClientID` 的值
- `IdleTimeout=30*time.Minute`
- `MetadataTTL=10*time.Minute`
- `MetadataTopics=[]string{topic}`（topic 来自 `WithTopic`）

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

## 5. NATS 使用

`contrib/queue/nats` 同时支持 Core NATS 与 JetStream：
- Core NATS：适合实时 pub/sub、低延迟通知；`Commit` 是 no-op，不提供持久化与重投递。
- JetStream：适合队列、Bridge、任务消费；`Commit` 对应 JetStream ack，默认至少一次语义。

### 5.1 Core NATS

```go
provider, err := nats.NewNATSProvider[Payload](
    nats.WithMode(nats.Core),
    nats.WithURL("nats://localhost:4222"),
    nats.WithAuthUserInfo("user", "pass"),
    // 或 nats.WithAuthToken("token"),
    nats.WithSubject("events.created"),
    nats.WithQueueGroup("workers"),
    nats.WithCodec[Payload](queue.JSONCodec[Payload]{}),
)
```

Core 模式下 `WithQueueGroup` 会使用 NATS queue subscribe，让多个消费者在同一个队列组内负载均衡。由于 Core NATS 没有 broker 级 ack，`Consumer.Commit` 只返回 nil。

### 5.2 JetStream

```go
provider, err := nats.NewNATSProvider[Payload](
    nats.WithMode(nats.JetStream),
    nats.WithURL("nats://localhost:4222"),
    nats.WithAuthToken("token"),
    nats.WithStream("EVENTS"),
    nats.WithSubjects("events.*"),
    nats.WithSubject("events.created"),
    nats.WithDurable("events-worker"),
    nats.WithFilterSubject("events.created"),
    nats.WithAutoCreateStream(true),
    nats.WithMsgIDFromKey(true),
    nats.WithCodec[Payload](queue.JSONCodec[Payload]{}),
)
```

默认 JetStream consumer 使用 Pull 模式，适合 `Receive(ctx)` 主动拉取并处理后 `Commit` 的队列语义。需要更低延迟时可以切换到 Push 模式：

```go
provider, err := nats.NewNATSProvider[Payload](
    nats.WithMode(nats.JetStream),
    nats.WithConsumerMode(nats.PushConsumer),
    nats.WithURL("nats://localhost:4222"),
    nats.WithStream("EVENTS"),
    nats.WithSubjects("events.*"),
    nats.WithSubject("events.created"),
    nats.WithDurable("events-worker"),
    nats.WithFilterSubject("events.created"),
    nats.WithDeliverSubject("deliver.events.created"),
    nats.WithDeliverGroup("events-push-workers"),
    nats.WithReplayPolicy(nats.ReplayInstant),
    nats.WithAutoCreateStream(true),
    nats.WithCodec[Payload](queue.JSONCodec[Payload]{}),
)
```

Push 模式使用 JetStream push subscription，服务端主动投递到 `DeliverSubject`。`WithDeliverGroup` 用于 push consumer 的 queue group；它和 Core NATS 的 `WithQueueGroup` 是两个不同概念。

JetStream 模式默认：
- `Storage=FileStorage`
- `Retention=LimitsPolicy`
- `Discard=DiscardOld`
- `Replicas=1`
- `AckWait=30*time.Second`
- `MaxDeliver=5`
- `PullMaxMessages=1`
- `ConsumerMode=PullConsumer`
- `ReplayPolicy=ReplayInstant`

常用调优：

```go
nats.WithStreamMaxAge(7*24*time.Hour),
nats.WithStreamMaxBytes(1<<30),
nats.WithDuplicateWindow(2*time.Minute),
nats.WithAckWait(60*time.Second),
nats.WithMaxDeliver(10),
nats.WithDoubleAck(true),
nats.WithAsyncPublish(true),
nats.WithAsyncPublishAckHandler(func(future jetstream.PubAckFuture) {
    select {
    case ack := <-future.Ok():
        log.Printf("nats async publish ack: stream=%s seq=%d", ack.Stream, ack.Sequence)
    case err := <-future.Err():
        log.Printf("nats async publish error: %v", err)
    }
}),
```

`WithMsgIDFromKey(true)` 会把 `queue.Message.Key` 写入 `Nats-Msg-Id` header，用于 JetStream 去重窗口内的重复发布检测。`WithDoubleAck(true)` 会在 `Commit` 时等待服务端确认 ack，可靠性更强但吞吐更低。

`WithAsyncPublish(true)` 会让 JetStream Producer 使用 `PublishMsgAsync`。此时 `Producer.Send` 只返回 async publish 请求创建阶段的错误，不等待服务端 ack。适配器默认会在后台等待 future 并打印 ack/error 日志；如果业务需要自定义指标、告警或失败处理，可以用 `WithAsyncPublishAckHandler` 覆盖默认 handler。

### 5.3 认证

当前 NATS 适配器只实现两种认证方式：
- `WithAuthUserInfo(user, password)`
- `WithAuthToken(token)`

两者互斥；同时配置会返回错误。不配置认证时使用匿名连接。

### 5.4 本地示例

启动带 JetStream 的 NATS：

```bash
docker run --rm -p 4222:4222 nats:latest -js
```

运行 JetStream 示例：

```bash
cd examples/nats-queue
go run .
```

运行 Core NATS 示例：

```bash
cd examples/nats-queue
NATS_MODE=core go run .
```

运行 JetStream Push 示例：

```bash
cd examples/nats-queue
NATS_CONSUMER_MODE=push go run .
```

可用环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `NATS_MODE` | `jetstream` | `jetstream` 或 `core` |
| `NATS_CONSUMER_MODE` | `pull` | JetStream 使用 `pull` 或 `push` |
| `NATS_URL` | `nats://127.0.0.1:4222` | NATS server 地址 |
| `NATS_SUBJECT` | `events.demo` | 发布与消费 subject |
| `NATS_QUEUE_GROUP` | `demo-workers` | Core 模式 queue group |
| `NATS_STREAM` | `EVENTS` | JetStream stream 名称 |
| `NATS_STREAM_SUBJECTS` | `events.*` | JetStream stream 捕获 subjects |
| `NATS_DURABLE` | `events-demo-worker` | JetStream durable consumer |
| `NATS_FILTER_SUBJECT` | `NATS_SUBJECT` | JetStream consumer 过滤 subject |
| `NATS_DELIVER_SUBJECT` | `deliver.events.demo` | JetStream Push deliver subject |
| `NATS_DELIVER_GROUP` | `events-demo-push-workers` | JetStream Push deliver group |
| `NATS_REPLAY_POLICY` | `instant` | JetStream Push replay 策略：`instant` 或 `original` |
| `NATS_ASYNC_PUBLISH` | 空 | 设置为 `true` / `1` 时使用 JetStream async publish |

## 6. Bridge 使用

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

## 7. Demo

运行 `examples/queue-bridge` 可看到 Kafka -> RabbitMQ 桥接与定时生产示例：
```bash
cd examples/queue-bridge
go run .
```
