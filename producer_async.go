package kafka

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/IBM/sarama"
)

type pendingSend struct {
	id uint64
	ch chan sendResult
}

type sendResult struct {
	partition int32
	offset    int64
	err       error
}

// AsyncProducer wraps sarama.AsyncProducer with a synchronous SendMessage API.
//
// SendMessage is goroutine-safe, blocks until the broker acks (or returns an error),
// and is not fire-and-forget. Use a shared instance from many HTTP handlers for throughput.
//
// ProducerMessage.Metadata is reserved; do not set it on messages passed to SendMessage.
//
// Always call Close on shutdown to flush in-flight messages.
type AsyncProducer struct {
	impl sarama.AsyncProducer

	nextID    uint64
	pendingMu sync.Mutex
	pending   map[uint64]*pendingSend

	closeOnce sync.Once
	closeCh   chan struct{}

	wg       sync.WaitGroup // success/error pump goroutines
	inflight sync.WaitGroup // in-flight SendMessage calls
}

// newSaramaAsyncProducer is swappable in tests.
var newSaramaAsyncProducer = sarama.NewAsyncProducer

// NewAsyncProducer creates an async producer with the given connection and producer config.
func NewAsyncProducer(cfg Config, pc ProducerConfig) (Producer, error) {
	saramaCfg, err := buildAsyncSaramaConfig(cfg, pc)
	if err != nil {
		return nil, err
	}
	impl, err := newSaramaAsyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("kafka async producer: %w", err)
	}
	return newAsyncProducer(impl), nil
}

func newAsyncProducer(impl sarama.AsyncProducer) *AsyncProducer {
	if impl == nil {
		return nil
	}
	ap := &AsyncProducer{
		impl:    impl,
		closeCh: make(chan struct{}),
		pending: make(map[uint64]*pendingSend),
	}

	ap.wg.Add(2)
	go func() {
		defer ap.wg.Done()
		for msg := range ap.impl.Successes() {
			if msg == nil {
				continue
			}
			p, ok := msg.Metadata.(*pendingSend)
			if !ok || p == nil {
				continue
			}
			ap.deliverResult(p, sendResult{
				partition: msg.Partition,
				offset:    msg.Offset,
			})
		}
	}()

	go func() {
		defer ap.wg.Done()
		for perr := range ap.impl.Errors() {
			if perr == nil || perr.Msg == nil {
				continue
			}
			p, ok := perr.Msg.Metadata.(*pendingSend)
			if !ok || p == nil {
				continue
			}
			ap.deliverResult(p, sendResult{
				partition: perr.Msg.Partition,
				offset:    perr.Msg.Offset,
				err:       perr.Err,
			})
		}
	}()

	return ap
}

func (ap *AsyncProducer) deliverResult(p *pendingSend, res sendResult) {
	select {
	case p.ch <- res:
	default:
		// Duplicate success/error notification; caller already unblocked.
	}
	ap.removePending(p.id)
}

func (ap *AsyncProducer) removePending(id uint64) {
	ap.pendingMu.Lock()
	defer ap.pendingMu.Unlock()
	delete(ap.pending, id)
}

func (ap *AsyncProducer) addPending(p *pendingSend) {
	ap.pendingMu.Lock()
	defer ap.pendingMu.Unlock()
	ap.pending[p.id] = p
}

// SendMessage blocks until ack, error, or producer closed.
func (ap *AsyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	if ap == nil || ap.impl == nil || msg == nil {
		return 0, 0, ErrProducerClosed
	}
	if msg.Metadata != nil {
		return 0, 0, ErrMetadataSet
	}
	select {
	case <-ap.closeCh:
		return 0, 0, ErrProducerClosed
	default:
	}

	ap.inflight.Add(1)
	defer ap.inflight.Done()

	id := atomic.AddUint64(&ap.nextID, 1)
	p := &pendingSend{
		id: id,
		ch: make(chan sendResult, 1),
	}
	ap.addPending(p)
	msg.Metadata = p

	select {
	case <-ap.closeCh:
		ap.removePending(id)
		return 0, 0, ErrProducerClosed
	case ap.impl.Input() <- msg:
	}

	select {
	case res := <-p.ch:
		return res.partition, res.offset, res.err
	case <-ap.closeCh:
		ap.removePending(id)
		return 0, 0, ErrProducerClosed
	}
}

// Close shuts down the producer and waits for background goroutines.
func (ap *AsyncProducer) Close() error {
	if ap == nil || ap.impl == nil {
		return nil
	}

	var closeErr error
	ap.closeOnce.Do(func() {
		close(ap.closeCh)

		ap.pendingMu.Lock()
		for id, p := range ap.pending {
			select {
			case p.ch <- sendResult{err: ErrProducerClosed}:
			default:
			}
			delete(ap.pending, id)
		}
		ap.pendingMu.Unlock()

		ap.inflight.Wait()
		closeErr = ap.impl.Close()
		ap.wg.Wait()
	})

	return closeErr
}

var _ Producer = (*AsyncProducer)(nil)
