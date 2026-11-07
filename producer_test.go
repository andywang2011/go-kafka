package kafka

import "context"

import "testing"

func TestNewSyncProducer_EmptyBrokers(t *testing.T) {
	_, err := NewSyncProducer(context.Background(), Config{}, ProducerConfigDefault())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewSyncProducer_InvalidBrokers(t *testing.T) {
	_, err := NewSyncProducer(context.Background(), Config{Brokers: []string{"127.0.0.1:19999"}}, ProducerConfigDefault())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewProducer_deprecatedAlias(t *testing.T) {
	_, err := NewProducer(context.Background(), Config{})
	if err == nil {
		t.Fatal("expected error")
	}
}
