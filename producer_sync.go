package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
)

// newSaramaSyncProducer is swappable in tests.
var newSaramaSyncProducer = sarama.NewSyncProducer

// syncProducer adapts sarama.SyncProducer to the context-aware Producer interface.
type syncProducer struct {
	impl sarama.SyncProducer

	closeOnce sync.Once
	closeErr  error
}

// SendMessage sends msg and blocks until the broker acks. ctx is checked before
// the send; Sarama's sync API cannot abort a request that is already in flight.
func (p *syncProducer) SendMessage(ctx context.Context, msg *sarama.ProducerMessage) (int32, int64, error) {
	if p == nil || p.impl == nil || msg == nil {
		return 0, 0, ErrProducerClosed
	}
	if err := ctxOrBackground(ctx).Err(); err != nil {
		return 0, 0, err
	}
	return p.impl.SendMessage(msg)
}

// Close closes the underlying producer; safe to call multiple times.
func (p *syncProducer) Close() error {
	if p == nil || p.impl == nil {
		return nil
	}
	p.closeOnce.Do(func() { p.closeErr = p.impl.Close() })
	return p.closeErr
}

var _ Producer = (*syncProducer)(nil)

// NewSyncProducer creates a synchronous Kafka producer. ctx bounds the initial
// broker connection.
func NewSyncProducer(ctx context.Context, cfg Config, pc ProducerConfig) (Producer, error) {
	ctx = ctxOrBackground(ctx)
	saramaCfg, err := buildSyncSaramaConfig(cfg, pc)
	if err != nil {
		return nil, err
	}
	impl, err := buildWithCtx(ctx, func() (sarama.SyncProducer, error) {
		return newSaramaSyncProducer(cfg.Brokers, saramaCfg)
	})
	if err != nil {
		return nil, fmt.Errorf("kafka sync producer: %w", err)
	}
	return &syncProducer{impl: impl}, nil
}

// NewProducer is deprecated; use NewSyncProducer with ProducerConfigDefault.
func NewProducer(ctx context.Context, cfg Config) (Producer, error) {
	return NewSyncProducer(ctx, cfg, ProducerConfigDefault())
}
