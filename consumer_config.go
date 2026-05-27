package kafka

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

// Initial offset policy names (new consumer group or partition without committed offset).
const (
	InitialOffsetNewest = "newest"
	InitialOffsetOldest = "oldest"
)

// Consumer group rebalance strategy names.
const (
	RebalanceRange      = "range"
	RebalanceSticky     = "sticky"
	RebalanceRoundRobin = "roundrobin"
)

// ConsumerConfig tunes consumer group behavior.
type ConsumerConfig struct {
	// GroupID is the consumer group id (required).
	GroupID string
	// Topics lists topics to subscribe to (required).
	Topics []string
	// Concurrency is the number of workers per partition claim (default 1).
	Concurrency int
	// MaxRetries is how many times to retry the handler after the first failure (default allows MaxRetries+1 total attempts).
	MaxRetries int
	// RetryDelay is the pause between handler retries on the same message.
	RetryDelay time.Duration

	// DLQTopic is the dead-letter topic; requires a non-nil dlq producer in NewConsumerWithDLQ.
	DLQTopic string

	// BootstrapMaxRetries is how many extra attempts to create the consumer group (0 = one try).
	BootstrapMaxRetries int
	// BootstrapRetryDelay is the pause between bootstrap retries.
	BootstrapRetryDelay time.Duration

	// ConsumeRetryDelay is the pause after Consume returns an error before retrying Start loop.
	ConsumeRetryDelay time.Duration

	// WorkerShutdownTimeout is how long to wait for workers after a claim ends (default 10s).
	WorkerShutdownTimeout time.Duration

	// InitialOffset is newest or oldest when no committed offset exists.
	InitialOffset string
	// RebalanceStrategy is range, sticky, or roundrobin.
	RebalanceStrategy string

	// Hooks optional observability callbacks.
	Hooks ConsumerHooks
}

// Validate fills defaults and validates consumer config.
func (c *ConsumerConfig) Validate() error {
	if c.GroupID == "" {
		return fmt.Errorf("%w: group id is empty", ErrInvalidConfig)
	}
	if len(c.Topics) == 0 {
		return fmt.Errorf("%w: topics is empty", ErrInvalidConfig)
	}
	if c.Concurrency < 1 {
		c.Concurrency = 1
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("%w: MaxRetries must be >= 0", ErrInvalidConfig)
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = time.Second
	}
	if c.BootstrapMaxRetries < 0 {
		return fmt.Errorf("%w: BootstrapMaxRetries must be >= 0", ErrInvalidConfig)
	}
	if c.BootstrapRetryDelay <= 0 {
		c.BootstrapRetryDelay = 2 * time.Second
	}
	if c.ConsumeRetryDelay <= 0 {
		c.ConsumeRetryDelay = 2 * time.Second
	}
	if c.WorkerShutdownTimeout <= 0 {
		c.WorkerShutdownTimeout = 10 * time.Second
	}
	if c.InitialOffset == "" {
		c.InitialOffset = InitialOffsetNewest
	}
	if c.InitialOffset != InitialOffsetNewest && c.InitialOffset != InitialOffsetOldest {
		return fmt.Errorf("%w: InitialOffset must be newest or oldest", ErrInvalidConfig)
	}
	if c.RebalanceStrategy == "" {
		c.RebalanceStrategy = RebalanceRange
	}
	if _, err := parseRebalanceStrategy(c.RebalanceStrategy); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	return nil
}

func parseRebalanceStrategy(name string) (sarama.BalanceStrategy, error) {
	switch name {
	case RebalanceRange:
		return sarama.NewBalanceStrategyRange(), nil
	case RebalanceSticky:
		return sarama.NewBalanceStrategySticky(), nil
	case RebalanceRoundRobin:
		return sarama.NewBalanceStrategyRoundRobin(), nil
	default:
		return nil, fmt.Errorf("unknown rebalance strategy %q", name)
	}
}

func buildConsumerSaramaConfig(cfg Config, cc ConsumerConfig) (*sarama.Config, error) {
	ccCopy := cc
	if err := ccCopy.Validate(); err != nil {
		return nil, err
	}
	sc := sarama.NewConfig()
	if err := applyConnection(cfg, sc); err != nil {
		return nil, err
	}
	switch ccCopy.InitialOffset {
	case InitialOffsetOldest:
		sc.Consumer.Offsets.Initial = sarama.OffsetOldest
	default:
		sc.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	strategy, err := parseRebalanceStrategy(ccCopy.RebalanceStrategy)
	if err != nil {
		return nil, err
	}
	sc.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{strategy}
	return sc, nil
}
