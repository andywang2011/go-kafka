package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// ConsumerHandler handles a single Kafka message.
type ConsumerHandler interface {
	HandleMessage(ctx context.Context, msg *sarama.ConsumerMessage) error
}

// Consumer wraps a Sarama consumer group with worker-pool concurrency and retries.
type Consumer struct {
	group   sarama.ConsumerGroup
	config  ConsumerConfig
	handler ConsumerHandler

	dlqProducer Producer
}

// NewConsumer creates a consumer group without DLQ producer.
func NewConsumer(cfg Config, consumerConfig ConsumerConfig, handler ConsumerHandler) (GroupConsumer, error) {
	return NewConsumerWithDLQ(cfg, consumerConfig, handler, nil)
}

// newConsumerGroup is overridden in tests for bootstrap retry.
var newConsumerGroup = sarama.NewConsumerGroup

// NewConsumerWithDLQ creates a consumer group; dlqProducer is required when DLQTopic is set.
func NewConsumerWithDLQ(cfg Config, consumerConfig ConsumerConfig, handler ConsumerHandler, dlqProducer Producer) (GroupConsumer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, ErrHandlerNil
	}
	if err := (&consumerConfig).Validate(); err != nil {
		return nil, err
	}
	if consumerConfig.DLQTopic != "" && dlqProducer == nil {
		return nil, fmt.Errorf("%w: DLQTopic requires a non-nil dlq producer", ErrInvalidConfig)
	}

	saramaConfig, err := buildConsumerSaramaConfig(cfg, consumerConfig)
	if err != nil {
		return nil, err
	}

	var (
		group sarama.ConsumerGroup
		last  error
	)
	for attempt := 0; attempt <= consumerConfig.BootstrapMaxRetries; attempt++ {
		group, last = newConsumerGroup(cfg.Brokers, consumerConfig.GroupID, saramaConfig)
		if last == nil {
			return &Consumer{
				group:       group,
				config:      consumerConfig,
				handler:     handler,
				dlqProducer: dlqProducer,
			}, nil
		}
		if attempt < consumerConfig.BootstrapMaxRetries {
			time.Sleep(consumerConfig.BootstrapRetryDelay)
		}
	}
	return nil, fmt.Errorf("kafka consumer group: %w", last)
}

// Start blocks until ctx is cancelled.
func (c *Consumer) Start(ctx context.Context) error {
	errCh := c.group.Errors()
	go c.pumpGroupErrors(ctx, errCh)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := c.group.Consume(ctx, c.config.Topics, c); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			time.Sleep(c.config.ConsumeRetryDelay)
			continue
		}
	}
}

func (c *Consumer) pumpGroupErrors(ctx context.Context, errCh <-chan error) {
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-errCh:
			if !ok {
				return
			}
			if err != nil && c.config.Hooks.OnGroupError != nil {
				c.config.Hooks.OnGroupError(ctx, err)
			}
		}
	}
}

// Close closes the consumer group.
func (c *Consumer) Close() error {
	if c == nil || c.group == nil {
		return nil
	}
	return c.group.Close()
}

// Setup implements sarama.ConsumerGroupHandler.
func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// Cleanup implements sarama.ConsumerGroupHandler.
func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim implements sarama.ConsumerGroupHandler.
func (c *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	concurrency := c.config.Concurrency
	if concurrency < 1 {
		concurrency = 1
	}
	msgCh := make(chan *sarama.ConsumerMessage, concurrency*2)
	var wg sync.WaitGroup

	sessCtx := sess.Context()
	workerCtx, cancelWorkers := context.WithCancel(sessCtx)
	defer cancelWorkers()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range msgCh {
				c.processMessage(workerCtx, sess, msg)
			}
		}()
	}

	for sessCtx.Err() == nil {
		select {
		case <-sessCtx.Done():
			goto done
		case msg, ok := <-claim.Messages():
			if !ok {
				goto done
			}
			select {
			case msgCh <- msg:
			case <-sessCtx.Done():
				goto done
			}
		}
	}

done:
	cancelWorkers()
	close(msgCh)

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(c.config.WorkerShutdownTimeout):
		if c.config.Hooks.OnWorkerShutdownTimeout != nil {
			c.config.Hooks.OnWorkerShutdownTimeout(sessCtx)
		}
	}

	return nil
}
