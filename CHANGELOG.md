# Changelog

## Unreleased

### Added

- `Config.KafkaVersion` applied to all clients via `applyConnection`
- `ConsumerConfig.WorkerShutdownTimeout` (default 10s)
- `ConsumerHooks.OnGroupError`, `OnWorkerShutdownTimeout`
- `ErrMetadataSet` when `AsyncProducer` receives a message with pre-set `Metadata`
- DLQ validation: `DLQTopic` requires non-nil producer in `NewConsumerWithDLQ`
- `goleak` in `TestMain` for goroutine leak regression

### Fixed

- `ConsumeClaim` respects session cancel on blocked `msgCh` send and cancels worker context on claim end
- Bounded worker wait on claim shutdown instead of skipping `wg.Wait` on cancel
- `AsyncProducer.Close` waits for in-flight `SendMessage` before closing Sarama producer

### Changed

- Godoc unified to English; `deliverResult` documents duplicate ack handling

## v0.1.0

### Added

- `Producer` interface with `NewSyncProducer` and `NewAsyncProducer`
- `ProducerConfig` for acks, compression, buffering, idempotent mode
- `ConsumerConfig` with concurrency, retries, DLQ, rebalance strategy, initial offset
- `ConsumerHooks` for handler errors, poison messages, DLQ failures
- Structured DLQ payload (`DLQEnvelope`)
- Bootstrap retry when creating consumer groups
- English README, examples, and package documentation

### Breaking

- `NewProducer(cfg)` deprecated; use `NewSyncProducer(cfg, ProducerConfigDefault())`
- `NewAsyncProducer` and `NewSyncProducer` require `ProducerConfig` as second argument
- `ConsumerConfig` fields moved to `consumer_config.go`; validate via `Validate()`

### Changed

- Single `applyConnection` for SASL/TLS across producers and consumers
- TLS enabled only when full cert material is present
