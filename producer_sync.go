package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
)

// NewSyncProducer creates a synchronous Kafka producer.
func NewSyncProducer(cfg Config, pc ProducerConfig) (Producer, error) {
	saramaCfg, err := buildSyncSaramaConfig(cfg, pc)
	if err != nil {
		return nil, err
	}
	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("kafka sync producer: %w", err)
	}
	return producer, nil
}

// NewProducer is deprecated; use NewSyncProducer with ProducerConfigDefault.
func NewProducer(cfg Config) (Producer, error) {
	return NewSyncProducer(cfg, ProducerConfigDefault())
}
