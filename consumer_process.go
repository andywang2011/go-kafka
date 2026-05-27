package kafka

import (
	"context"
	"time"

	"github.com/IBM/sarama"
)

func (c *Consumer) processMessage(ctx context.Context, sess sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	retries := 0
	var lastErr error
	for {
		if ctx.Err() != nil {
			return
		}
		err := c.handler.HandleMessage(ctx, msg)
		if err == nil {
			sess.MarkMessage(msg, "")
			return
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
				if dlqErr := sendDLQ(c.dlqProducer, c.config.DLQTopic, msg, lastErr); dlqErr != nil {
					if c.config.Hooks.OnDLQFailed != nil {
						c.config.Hooks.OnDLQFailed(ctx, msg, dlqErr)
					}
				}
			}
			sess.MarkMessage(msg, "")
			return
		}
		time.Sleep(c.config.RetryDelay)
	}
}
