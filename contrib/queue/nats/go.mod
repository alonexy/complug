module github.com/alonexy/complug/contrib/queue/nats

go 1.23.2

require (
	github.com/alonexy/complug/components/queue v1.0.3
	github.com/nats-io/nats.go v1.44.0
)

require (
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/nats-io/nkeys v0.4.11 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/sys v0.32.0 // indirect
)

replace github.com/alonexy/complug/components/queue => ../../../components/queue
