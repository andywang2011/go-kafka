package kafka

import (
	"context"

	"github.com/IBM/sarama"
)

// Producer is the recommended producer abstraction (sync or async wrapper).
//
// SendMessage honours ctx before the message is handed to Sarama; the async
// implementation also aborts while waiting for the broker ack.
type Producer interface {
	SendMessage(ctx context.Context, msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
	Close() error
}

// SyncProducer is the raw Sarama sync producer type alias for advanced use and mocks.
type SyncProducer = sarama.SyncProducer
