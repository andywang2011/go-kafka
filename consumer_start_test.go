package kafka

import (
	"context"
	"errors"
	"testing"
	"time"
)

type errConsumerGroup struct {
	fakeConsumerGroup
	errCh chan error
}

func newErrConsumerGroup() *errConsumerGroup {
	return &errConsumerGroup{errCh: make(chan error, 1)}
}

func (e *errConsumerGroup) Errors() <-chan error { return e.errCh }

func TestConsumer_Start_PumpsGroupErrors(t *testing.T) {
	g := newErrConsumerGroup()
	hookCalled := make(chan error, 1)
	c := &Consumer{
		group: g,
		config: ConsumerConfig{
			GroupID:           "g",
			Topics:            []string{"t"},
			ConsumeRetryDelay: time.Millisecond,
			Hooks: ConsumerHooks{
				OnGroupError: func(_ context.Context, err error) {
					select {
					case hookCalled <- err:
					default:
					}
				},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = c.Start(ctx)
		close(done)
	}()

	boom := errors.New("rebalance error")
	g.errCh <- boom

	select {
	case err := <-hookCalled:
		if !errors.Is(err, boom) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnGroupError not called")
	}

	cancel()
	<-done
}
