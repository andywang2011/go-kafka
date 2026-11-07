package kafka

import (
	"context"
	"errors"
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

	closeOnce sync.Once
	closeErr  error
}

// NewConsumer creates a consumer group without DLQ producer.
func NewConsumer(ctx context.Context, cfg Config, consumerConfig ConsumerConfig, handler ConsumerHandler) (GroupConsumer, error) {
	return NewConsumerWithDLQ(ctx, cfg, consumerConfig, handler, nil)
}

// newConsumerGroup is overridden in tests for bootstrap retry.
var newConsumerGroup = sarama.NewConsumerGroup

// NewConsumerWithDLQ creates a consumer group; dlqProducer is required when DLQTopic is set.
// ctx bounds the connect and the bootstrap retries (including their backoff).
func NewConsumerWithDLQ(ctx context.Context, cfg Config, consumerConfig ConsumerConfig, handler ConsumerHandler, dlqProducer Producer) (GroupConsumer, error) {
	ctx = ctxOrBackground(ctx)
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

	var last error
	for attempt := 0; attempt <= consumerConfig.BootstrapMaxRetries; attempt++ {
		group, err := buildWithCtx(ctx, func() (sarama.ConsumerGroup, error) {
			return newConsumerGroup(cfg.Brokers, consumerConfig.GroupID, saramaConfig)
		})
		if err == nil {
			return &Consumer{
				group:       group,
				config:      consumerConfig,
				handler:     handler,
				dlqProducer: dlqProducer,
			}, nil
		}
		last = err
		if ctx.Err() != nil {
			break
		}
		if attempt < consumerConfig.BootstrapMaxRetries {
			if sleepErr := sleepCtx(ctx, consumerConfig.BootstrapRetryDelay); sleepErr != nil {
				last = sleepErr
				break
			}
		}
	}
	return nil, fmt.Errorf("kafka consumer group: %w", last)
}

// Start blocks until ctx is cancelled or the consumer group is closed.
func (c *Consumer) Start(ctx context.Context) error {
	pumpCtx, cancelPump := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.pumpGroupErrors(pumpCtx, c.group.Errors())
	}()
	defer func() {
		cancelPump()
		wg.Wait()
	}()

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
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return err
			}
			if sleepErr := sleepCtx(ctx, c.config.ConsumeRetryDelay); sleepErr != nil {
				return sleepErr
			}
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

// Close closes the consumer group; safe to call multiple times.
func (c *Consumer) Close() error {
	if c == nil || c.group == nil {
		return nil
	}
	c.closeOnce.Do(func() { c.closeErr = c.group.Close() })
	return c.closeErr
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

	var (
		failOnce sync.Once
		failErr  error
	)
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range msgCh {
				if err := c.processMessage(workerCtx, sess, msg); err != nil {
					// Offset must not advance past a message that could not be
					// handled or dead-lettered: stop the claim so it is redelivered.
					failOnce.Do(func() {
						failErr = err
						cancelWorkers()
					})
				}
			}
		}()
	}

	for sessCtx.Err() == nil && workerCtx.Err() == nil {
		select {
		case <-sessCtx.Done():
			goto done
		case <-workerCtx.Done():
			goto done
		case msg, ok := <-claim.Messages():
			if !ok {
				goto done
			}
			select {
			case msgCh <- msg:
			case <-sessCtx.Done():
				goto done
			case <-workerCtx.Done():
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

	shutdownTimer := time.NewTimer(c.config.WorkerShutdownTimeout)
	defer shutdownTimer.Stop()
	select {
	case <-waitDone:
	case <-shutdownTimer.C:
		if c.config.Hooks.OnWorkerShutdownTimeout != nil {
			c.config.Hooks.OnWorkerShutdownTimeout(sessCtx)
		}
		return nil
	}

	return failErr
}
