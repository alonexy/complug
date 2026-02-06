module github.com/alonexy/complug/examples/queue-bridge

go 1.23.2

require (
	github.com/alonexy/complug/components/queue v0.0.0
	github.com/alonexy/complug/contrib/queue/bridge v0.0.0
	github.com/alonexy/complug/contrib/queue/kafka v0.0.0
	github.com/alonexy/complug/contrib/queue/rabbitmq v0.0.0
)

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/rabbitmq/amqp091-go v1.10.0 // indirect
	github.com/segmentio/kafka-go v0.4.47 // indirect
)

replace github.com/alonexy/complug/components/queue => ../../components/queue

replace github.com/alonexy/complug/contrib/queue/bridge => ../../contrib/queue/bridge

replace github.com/alonexy/complug/contrib/queue/kafka => ../../contrib/queue/kafka

replace github.com/alonexy/complug/contrib/queue/rabbitmq => ../../contrib/queue/rabbitmq
