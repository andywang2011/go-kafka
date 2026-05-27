# go-kafka

Thin, testable Kafka client for Go services, built on [IBM Sarama](https://github.com/IBM/sarama).

```bash
go get github.com/andywang2011/go-kafka@v0.1.0
```

## Design

- No viper, no global singletons, no `log.Fatal` inside the library.
- Constructors return interfaces (`Producer`, `GroupConsumer`) for mocking.
- Connection config (`Config`) is separate from producer/consumer tuning.
- Consumers: `NewConsumer` + `Start(ctx)` + `Close()` from `main` or a dedicated worker process.

## Quick start — producer

```go
cfg := kafka.Config{Brokers: []string{"127.0.0.1:9092"}}
pc := kafka.ProducerConfigDefault()

p, err := kafka.NewAsyncProducer(cfg, pc) // high-throughput, SendMessage still waits for ack
if err != nil { /* handle */ }
defer p.Close()

_, _, err = p.SendMessage(&sarama.ProducerMessage{
    Topic: "orders",
    Key:   sarama.StringEncoder("user-1"),
    Value: sarama.ByteEncoder([]byte(`{"id":1}`)),
})
```

Sync producer: `kafka.NewSyncProducer(cfg, pc)` or deprecated `kafka.NewProducer(cfg)` (uses defaults).

## Quick start — consumer

```go
cc := kafka.ConsumerConfig{
    GroupID:     "my-service",
    Topics:      []string{"orders"},
    Concurrency: 4,
    MaxRetries:  3,
    DLQTopic:    "orders-dlq", // optional
}

consumer, err := kafka.NewConsumer(cfg, cc, handler)
if err != nil { /* handle */ }

ctx, cancel := context.WithCancel(context.Background())
done := make(chan struct{})
go func() {
    defer close(done)
    _ = consumer.Start(ctx)
}()

// on SIGTERM: cancel(); <-done; consumer.Close()
```

## Async vs sync producer

| | `NewSyncProducer` | `NewAsyncProducer` |
|---|-------------------|---------------------|
| Per-call API | Blocks until broker ack | Blocks until broker ack (wrapper) |
| Multi-goroutine throughput | Lower | Higher (pipelined in-flight) |
| Shutdown | `Close()` required | `Close()` required |

Async does **not** auto-spawn goroutines when QPS rises. Throughput comes from your app (e.g. many HTTP handlers) sharing one producer.

Do **not** set `ProducerMessage.Metadata` on messages passed to `NewAsyncProducer` — the wrapper reserves it for in-flight tracking (`ErrMetadataSet`).

## Configuration

### `Config` (connection)

| Field | Description |
|-------|-------------|
| `Brokers` | Required broker addresses |
| `KafkaVersion` | Protocol version (e.g. `3.6.0`); default `V4_1_0_0` |
| `SASLEnable` / `SASLUser` / `SASLPassword` | SASL plaintext |
| `TLSEnable` + cert fields | Mutual TLS when cert/key/ca are all set |

### `ProducerConfig`

| Field | Default |
|-------|---------|
| `RequiredAcks` | `WaitForAll` |
| `Compression` | `none` (`gzip`, `snappy`, `lz4`, `zstd`) |
| `ChannelBufferSize` | `1024` |
| `Idempotent` | `false` |

### `ConsumerConfig`

| Field | Default |
|-------|---------|
| `Concurrency` | `1` |
| `MaxRetries` / `RetryDelay` | `0` / `1s` |
| `ConsumeRetryDelay` | `2s` (Consume loop retry) |
| `InitialOffset` | `newest` (`oldest`) |
| `RebalanceStrategy` | `range` (`sticky`, `roundrobin`) |
| `WorkerShutdownTimeout` | `10s` (wait for workers after claim ends) |
| `DLQTopic` | empty (disabled) |

`DLQTopic` requires a non-nil DLQ `Producer` in `NewConsumerWithDLQ`.

Optional `ConsumerHooks`: `OnHandlerError`, `OnPoison`, `OnDLQFailed`, `OnGroupError`, `OnWorkerShutdownTimeout`.

## DLQ

After `MaxRetries`, the message is marked consumed. If `DLQTopic` and a DLQ `Producer` are configured (`NewConsumerWithDLQ`), a JSON `DLQEnvelope` is sent (best-effort).

## Examples

```bash
export KAFKA_BROKERS=127.0.0.1:9092
cd go-kafka/examples/producer && go run .
cd go-kafka/examples/consumer && go run .
```

## Limitations

- SASL: plaintext only
- No transactional producer API in this wrapper
- DLQ delivery is best-effort; monitor `OnDLQFailed`
- Tag releases (`v0.1.0`) match `go get ...@v0.1.0`

## License

MIT — see [LICENSE](LICENSE).
