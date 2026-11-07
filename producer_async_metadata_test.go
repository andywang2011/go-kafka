package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

func TestAsyncProducer_SendMessage_RejectsMetadata(t *testing.T) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	mp := mocks.NewAsyncProducer(t, cfg)
	ap := newAsyncProducer(mp)
	defer func() { _ = ap.Close() }()

	_, _, err := ap.SendMessage(context.Background(), &sarama.ProducerMessage{
		Topic:    "t",
		Value:    sarama.ByteEncoder("v"),
		Metadata: "user-data",
	})
	if !errors.Is(err, ErrMetadataSet) {
		t.Fatalf("expected ErrMetadataSet, got %v", err)
	}
}
