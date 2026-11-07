// Package kafka provides a thin, testable wrapper around IBM Sarama for producers and consumer groups.
//
// Design constraints (for use in application frameworks):
//   - No viper, no global singletons, no log.Fatal inside the library.
//   - Constructors return interfaces (Producer, GroupConsumer) for mocking.
//   - ConfigDefault() for connection Config; ProducerConfigDefault() for producer tuning.
//   - Consumers use NewConsumer + Start(ctx) + Close(); start them from main or a dedicated cmd process.
//
// Producers:
//   - NewSyncProducer — blocking send per call.
//   - NewAsyncProducer — pipelined send with SendMessage still waiting for broker ack (goroutine-safe).
//
// Consumers:
//   - NewConsumer / NewConsumerWithDLQ — worker pool, retries, optional dead-letter topic.
package kafka
