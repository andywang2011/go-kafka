# Changelog

## [Unreleased]

## [0.2.0] - 2026-11-07

### Breaking

- See repository diff for API changes in this release (secure defaults, renamed packages where applicable, context-aware constructors).

### Security

- Hardened defaults: TLS, timeouts, body limits, PII redaction where logging applies.

### Fixed

- Bug fixes from the v0.2.0 hardening pass.

### Changed

- CI uses go-version-file; added race, staticcheck, govulncheck, golangci-lint (gosec).



### Changed

- **Breaking:** `Producer.SendMessage` now takes a `context.Context` first.
- **Breaking:** `NewSyncProducer`, `NewAsyncProducer`, `NewProducer`, `NewConsumer`, and `NewConsumerWithDLQ` take a `context.Context` first; bootstrap retries, backoff, and consume retry delay stop on cancellation.
- A failed DLQ publish no longer commits the offset; the claim ends with an error so the message is redelivered.
- `pumpGroupErrors` is tracked with a `WaitGroup` and `Start` waits for it; `Start` returns when the group is closed.
- TLS is enabled with `TLSEnable` and a CA only; cert and key must be set together.
- `Close` is idempotent via `sync.Once` (sync producer wrapper and consumer).

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
