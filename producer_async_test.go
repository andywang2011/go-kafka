package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

func TestNewAsyncProducer_WithMockFactory(t *testing.T) {
	old := newSaramaAsyncProducer
	defer func() { newSaramaAsyncProducer = old }()

	newSaramaAsyncProducer = func(addrs []string, cfg *sarama.Config) (sarama.AsyncProducer, error) {
		return mocks.NewAsyncProducer(t, cfg), nil
	}

	p, err := NewAsyncProducer(context.Background(), Config{Brokers: []string{"127.0.0.1:9092"}}, ProducerConfigDefault())
	if err != nil {
		t.Fatal(err)
	}
	if p == nil {
		t.Fatal("expected producer")
	}
	_ = p.Close()
}

func TestAsyncProducer_SendMessage_Success(t *testing.T) {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.ChannelBufferSize = 1

	mp := mocks.NewAsyncProducer(t, cfg)
	mp.ExpectInputAndSucceed()

	ap := newAsyncProducer(mp)
	defer func() { _ = ap.Close() }()

	partition, offset, err := ap.SendMessage(context.Background(), &sarama.ProducerMessage{
		Topic: "t",
		Key:   sarama.StringEncoder("k"),
		Value: sarama.ByteEncoder([]byte("v")),
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if offset != 1 {
		t.Fatalf("expected offset=1, got %d", offset)
	}
	if partition < 0 {
		t.Fatalf("expected non-negative partition, got %d", partition)
	}
}

func TestAsyncProducer_SendMessage_Fail(t *testing.T) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.ChannelBufferSize = 1

	mp := mocks.NewAsyncProducer(t, cfg)
	boom := errors.New("boom")
	mp.ExpectInputAndFail(boom)

	ap := newAsyncProducer(mp)
	defer func() { _ = ap.Close() }()

	_, _, err := ap.SendMessage(context.Background(), &sarama.ProducerMessage{
		Topic: "t",
		Key:   sarama.StringEncoder("k"),
		Value: sarama.ByteEncoder([]byte("v")),
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected error to wrap %v, got %v", boom, err)
	}
}

func TestAsyncProducer_SendMessage_Closed(t *testing.T) {
	ap := &AsyncProducer{closeCh: make(chan struct{})}
	close(ap.closeCh)
	_, _, err := ap.SendMessage(context.Background(), &sarama.ProducerMessage{Topic: "t"})
	if !errors.Is(err, ErrProducerClosed) {
		t.Fatalf("got %v", err)
	}
}

func TestAsyncProducer_SendMessage_Nil(t *testing.T) {
	ap := &AsyncProducer{}
	_, _, err := ap.SendMessage(context.Background(), nil)
	if !errors.Is(err, ErrProducerClosed) {
		t.Fatalf("got %v", err)
	}
}

func TestAsyncProducer_Close_NilSafe(t *testing.T) {
	var ap *AsyncProducer
	if err := ap.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncProducer_ConcurrentSendMessage(t *testing.T) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.ChannelBufferSize = 64

	mp := mocks.NewAsyncProducer(t, cfg)
	for i := 0; i < 10; i++ {
		mp.ExpectInputAndSucceed()
	}

	ap := newAsyncProducer(mp)
	defer func() { _ = ap.Close() }()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := ap.SendMessage(context.Background(), &sarama.ProducerMessage{
				Topic: "t",
				Value: sarama.ByteEncoder([]byte("v")),
			})
			if err != nil {
				t.Errorf("SendMessage: %v", err)
			}
		}()
	}
	wg.Wait()
}
