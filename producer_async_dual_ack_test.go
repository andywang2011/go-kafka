package kafka

import (
	"errors"
	"testing"
)

func TestAsyncProducer_deliverResult_DuplicateAck(t *testing.T) {
	ap := &AsyncProducer{pending: make(map[uint64]*pendingSend)}
	p := &pendingSend{
		id: 1,
		ch: make(chan sendResult, 1),
	}
	ap.addPending(p)

	ap.deliverResult(p, sendResult{partition: 0, offset: 1})
	ap.deliverResult(p, sendResult{partition: 0, offset: 2, err: errors.New("late error")})

	select {
	case res := <-p.ch:
		if res.offset != 1 {
			t.Fatalf("expected first result offset=1, got %d", res.offset)
		}
	default:
		t.Fatal("expected first result on channel")
	}

	ap.pendingMu.Lock()
	n := len(ap.pending)
	ap.pendingMu.Unlock()
	if n != 0 {
		t.Fatalf("pending map size=%d, want 0", n)
	}
}
