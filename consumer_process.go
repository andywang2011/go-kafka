package kafka

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
)

// processMessage handles msg with retries. It returns a non-nil error only when
// the message could not be dead-lettered; in that case the offset is NOT marked
// so the message is redelivered after the claim ends.
func (c *Consumer) processMessage(ctx context.Context, sess sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) error {
	retries := 0
	var lastErr error
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := c.handler.HandleMessage(ctx, msg)
		if err == nil {
			sess.MarkMessage(msg, "")
			return nil
		}
		lastErr = err
		if c.config.Hooks.OnHandlerError != nil {
			c.config.Hooks.OnHandlerError(ctx, msg, err, retries+1)
		}
		retries++
		if retries > c.config.MaxRetries {
			if c.config.Hooks.OnPoison != nil {
				c.config.Hooks.OnPoison(ctx, msg, lastErr)
			}
			if c.dlqProducer != nil && c.config.DLQTopic != "" {
				if dlqErr := sendDLQ(ctx, c.dlqProducer, c.config.DLQTopic, msg, lastErr); dlqErr != nil {
					if c.config.Hooks.OnDLQFailed != nil {
						c.config.Hooks.OnDLQFailed(ctx, msg, dlqErr)
					}
					return fmt.Errorf("kafka: dlq send failed for %s/%d@%d, offset not committed: %w",
						msg.Topic, msg.Partition, msg.Offset, dlqErr)
				}
			}
			sess.MarkMessage(msg, "")
			return nil
		}
		if sleepCtx(ctx, c.config.RetryDelay) != nil {
			return nil
		}
	}
}
