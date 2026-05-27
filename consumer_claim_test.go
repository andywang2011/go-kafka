package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type ctxFakeSession struct {
	ctx    context.Context
	marked int
}

func (s *ctxFakeSession) Claims() map[string][]int32                  { return nil }
func (s *ctxFakeSession) MemberID() string                            { return "" }
func (s *ctxFakeSession) GenerationID() int32                         { return 0 }
func (s *ctxFakeSession) MarkOffset(string, int32, int64, string)     {}
func (s *ctxFakeSession) Commit()                                     {}
func (s *ctxFakeSession) ResetOffset(string, int32, int64, string)    {}
func (s *ctxFakeSession) MarkMessage(*sarama.ConsumerMessage, string) { s.marked++ }
func (s *ctxFakeSession) Context() context.Context                    { return s.ctx }

// blockingHandler blocks until release is closed or ctx is cancelled.
type blockingHandler struct {
	release  chan struct{}
	ready    chan struct{}
	done     chan struct{}
	readyOnce sync.Once
	doneOnce  sync.Once
}

func newBlockingHandler() *blockingHandler {
	return &blockingHandler{
		release: make(chan struct{}),
		ready:   make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (h *blockingHandler) HandleMessage(ctx context.Context, _ *sarama.ConsumerMessage) error {
	h.readyOnce.Do(func() { close(h.ready) })
	select {
	case <-h.release:
	case <-ctx.Done():
	}
	h.doneOnce.Do(func() { close(h.done) })
	return nil
}

func TestConsumer_ConsumeClaim_ReturnsOnSessionCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sess := &ctxFakeSession{ctx: ctx}
	handler := newBlockingHandler()

	msgs := make(chan *sarama.ConsumerMessage, 8)
	for i := 0; i < 8; i++ {
		msgs <- &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: int64(i)}
	}

	c := &Consumer{
		config: ConsumerConfig{
			Concurrency:             1,
			MaxRetries:              0,
			RetryDelay:              time.Millisecond,
			WorkerShutdownTimeout:   2 * time.Second,
		},
		handler: handler,
	}

	done := make(chan error, 1)
	go func() {
		done <- c.ConsumeClaim(sess, &fakeClaim{messages: msgs})
	}()

	select {
	case <-handler.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ConsumeClaim: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ConsumeClaim did not return after session ctx cancel")
	}

	select {
	case <-handler.done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit after worker context cancel")
	}
}
