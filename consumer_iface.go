package kafka

import "context"

// GroupConsumer is the runtime abstraction for a Kafka consumer group (mock-friendly).
type GroupConsumer interface {
	Start(ctx context.Context) error
	Close() error
}

var _ GroupConsumer = (*Consumer)(nil)
