package kafka

import (
	"context"
	"errors"
	"testing"
)

func TestNewConsumerWithDLQ_DLQTopicRequiresProducer(t *testing.T) {
	_, err := NewConsumerWithDLQ(context.Background(),
		Config{Brokers: []string{"127.0.0.1:9092"}},
		ConsumerConfig{
			GroupID:  "g",
			Topics:   []string{"t"},
			DLQTopic: "dlq",
		},
		fakeHandler{},
		nil,
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}
