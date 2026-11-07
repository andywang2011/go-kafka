# Changelog

## [0.1.0] - 2026-11-07

### Added

- `Producer` interface with `NewSyncProducer` and `NewAsyncProducer`
- `ProducerConfig` for acks, compression, buffering, idempotent mode
- `ConsumerConfig` with concurrency, retries, DLQ, rebalance strategy, initial offset
- `ConsumerHooks` for handler errors, poison messages, DLQ failures
- Structured DLQ payload (`DLQEnvelope`)
- Bootstrap retry when creating consumer groups
- English README, examples, and package documentation

### Changed

- Single `applyConnection` for SASL/TLS across producers and consumers
- TLS enabled only when full cert material is present

### Breaking

- `NewProducer(cfg)` deprecated; use `NewSyncProducer(cfg, ProducerConfigDefault())`
- `NewAsyncProducer` and `NewSyncProducer` require `ProducerConfig` as second argument
