package kafka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

// DLQEnvelope is the JSON payload written to a dead-letter topic.
type DLQEnvelope struct {
	// OriginalTopic is the source topic of the failed message.
	OriginalTopic string `json:"original_topic"`
	// OriginalPartition is the source partition.
	OriginalPartition int32 `json:"original_partition"`
	// OriginalOffset is the source offset.
	OriginalOffset int64 `json:"original_offset"`
	// OriginalKey is the source message key.
	OriginalKey []byte `json:"original_key,omitempty"`
	// OriginalValue is the source message value.
	OriginalValue []byte `json:"original_value,omitempty"`
	// Error is the handler error string.
	Error string `json:"error"`
	// FailedAt is the Unix timestamp when the handler failed.
	FailedAt int64 `json:"failed_at_unix"`
}

func buildDLQMessage(dlqTopic string, msg *sarama.ConsumerMessage, handlerErr error) (*sarama.ProducerMessage, error) {
	env := DLQEnvelope{
		OriginalTopic:     msg.Topic,
		OriginalPartition: msg.Partition,
		OriginalOffset:    msg.Offset,
		OriginalKey:       msg.Key,
		OriginalValue:     msg.Value,
		Error:             handlerErr.Error(),
		FailedAt:          time.Now().Unix(),
	}
	body, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("kafka dlq marshal: %w", err)
	}
	return &sarama.ProducerMessage{
		Topic: dlqTopic,
		Key:   sarama.ByteEncoder(msg.Key),
		Value: sarama.ByteEncoder(body),
	}, nil
}

func sendDLQ(producer Producer, dlqTopic string, msg *sarama.ConsumerMessage, handlerErr error) error {
	if producer == nil || dlqTopic == "" {
		return nil
	}
	pm, err := buildDLQMessage(dlqTopic, msg, handlerErr)
	if err != nil {
		return err
	}
	_, _, err = producer.SendMessage(pm)
	return err
}
