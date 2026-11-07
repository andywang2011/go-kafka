package kafka

import "errors"

var (
	// ErrProducerClosed is returned when SendMessage is called after Close or during shutdown.
	ErrProducerClosed = errors.New("kafka: producer closed")

	// ErrInvalidConfig indicates validation failed on Config, ProducerConfig, or ConsumerConfig.
	ErrInvalidConfig = errors.New("kafka: invalid config")

	// ErrHandlerNil is returned when NewConsumer is called with a nil handler.
	ErrHandlerNil = errors.New("kafka: handler is nil")

	// ErrMetadataSet is returned when AsyncProducer.SendMessage is called with non-nil ProducerMessage.Metadata.
	ErrMetadataSet = errors.New("kafka: ProducerMessage.Metadata is reserved by AsyncProducer")
)
