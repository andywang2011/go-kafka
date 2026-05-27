package kafka

import (
	"context"

	"github.com/IBM/sarama"
)

// ConsumerHooks optional callbacks for observability (logging, metrics).
// Nil funcs are skipped.
type ConsumerHooks struct {
	// OnHandlerError is called when the handler returns an error; attempt starts at 1.
	OnHandlerError func(ctx context.Context, msg *sarama.ConsumerMessage, err error, attempt int)
	// OnPoison is called when MaxRetries is exceeded, before DLQ or mark.
	OnPoison func(ctx context.Context, msg *sarama.ConsumerMessage, err error)
	// OnDLQFailed is called when publishing to the DLQ topic fails (message is still marked).
	OnDLQFailed func(ctx context.Context, msg *sarama.ConsumerMessage, dlqErr error)
	// OnGroupError is called for errors from ConsumerGroup.Errors() (rebalance, network, etc.).
	OnGroupError func(ctx context.Context, err error)
	// OnWorkerShutdownTimeout is called when workers do not finish within WorkerShutdownTimeout after claim ends.
	OnWorkerShutdownTimeout func(ctx context.Context)
}
