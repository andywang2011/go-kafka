# go-kafka

Thin, testable Kafka client for Go services, built on [IBM Sarama](https://github.com/IBM/sarama).

## Install

```bash
go get github.com/andywang2011/go-kafka@v0.1.0
```

## Quick start

```go
cfg := kafka.Config{Brokers: []string{"127.0.0.1:9092"}}
pc := kafka.ProducerConfigDefault()

p, err := kafka.NewAsyncProducer(ctx, cfg, pc)
if err != nil {
	// handle
}
defer p.Close()

_, _, err = p.SendMessage(ctx, &sarama.ProducerMessage{
	Topic: "orders",
	Key:   sarama.StringEncoder("user-1"),
	Value: sarama.ByteEncoder([]byte(`{"id":1}`)),
})
```

## Examples

```bash
export KAFKA_BROKERS=127.0.0.1:9092
cd go-kafka/examples/producer && go run .
cd go-kafka/examples/consumer && go run .
```

## License

MIT — see [LICENSE](LICENSE).
