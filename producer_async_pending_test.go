package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

// TestAsyncProducer_CloseUnblocksStalledSend ensures Close wakes callers blocked waiting for broker ack.
func TestAsyncProducer_CloseUnblocksStalledSend(t *testing.T) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.ChannelBufferSize = 16

	mp := mocks.NewAsyncProducer(t, cfg)
	mp.ExpectInputWithMessageCheckerFunctionAndSucceed(func(*sarama.ProducerMessage) error {
		return nil
	})

	ap := newAsyncProducer(mp)

	var wg sync.WaitGroup
	wg.Add(1)
	var sendErr error
	go func() {
		defer wg.Done()
		_, _, sendErr = ap.SendMessage(context.Background(), &sarama.ProducerMessage{Topic: "t", Value: sarama.ByteEncoder("v")})
	}()

	// Message is in mock pipeline but success is not published until Close races; wait then close producer.
	time.Sleep(50 * time.Millisecond)
	_ = ap.Close()
	wg.Wait()

	if sendErr != nil && !errors.Is(sendErr, ErrProducerClosed) {
		t.Fatalf("expected nil or ErrProducerClosed, got %v", sendErr)
	}

	ap.pendingMu.Lock()
	n := len(ap.pending)
	ap.pendingMu.Unlock()
	if n != 0 {
		t.Fatalf("pending map not empty after Close: %d entries", n)
	}
}
