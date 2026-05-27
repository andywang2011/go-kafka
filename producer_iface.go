package kafka

import "github.com/IBM/sarama"

// Producer is the recommended producer abstraction (sync or async wrapper).
type Producer interface {
	SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
	Close() error
}

// SyncProducer is the raw Sarama sync producer type alias for advanced use and mocks.
type SyncProducer = sarama.SyncProducer
