package kafka

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestConsumerConfig_Validate_Defaults(t *testing.T) {
	c := ConsumerConfig{
		GroupID: "g",
		Topics:  []string{"t"},
	}
	if err := (&c).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Concurrency != 1 {
		t.Fatalf("expected Concurrency=1, got %d", c.Concurrency)
	}
	if c.RetryDelay != time.Second {
		t.Fatalf("expected RetryDelay=1s, got %v", c.RetryDelay)
	}
	if c.ConsumeRetryDelay != 2*time.Second {
		t.Fatalf("expected ConsumeRetryDelay=2s, got %v", c.ConsumeRetryDelay)
	}
	if c.InitialOffset != InitialOffsetNewest {
		t.Fatalf("expected InitialOffset newest, got %s", c.InitialOffset)
	}
	if c.RebalanceStrategy != RebalanceRange {
		t.Fatalf("expected rebalance range, got %s", c.RebalanceStrategy)
	}
	if c.WorkerShutdownTimeout != 10*time.Second {
		t.Fatalf("expected WorkerShutdownTimeout=10s, got %v", c.WorkerShutdownTimeout)
	}
}

func TestConsumerConfig_Validate_Invalid(t *testing.T) {
	t.Run("empty group", func(t *testing.T) {
		c := ConsumerConfig{Topics: []string{"t"}}
		if err := (&c).Validate(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("bad offset", func(t *testing.T) {
		c := ConsumerConfig{GroupID: "g", Topics: []string{"t"}, InitialOffset: "bad"}
		if err := (&c).Validate(); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestParseRebalanceStrategy_All(t *testing.T) {
	for _, name := range []string{RebalanceRange, RebalanceSticky, RebalanceRoundRobin} {
		if _, err := parseRebalanceStrategy(name); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if _, err := parseRebalanceStrategy("bad"); err == nil {
		t.Fatal("expected error")
	}
}

func TestBuildConsumerSaramaConfig_RebalanceSticky(t *testing.T) {
	cc := ConsumerConfig{
		GroupID:           "g",
		Topics:            []string{"t"},
		RebalanceStrategy: RebalanceSticky,
	}
	sc, err := buildConsumerSaramaConfig(Config{Brokers: []string{"127.0.0.1:9092"}}, cc)
	if err != nil {
		t.Fatal(err)
	}
	if len(sc.Consumer.Group.Rebalance.GroupStrategies) != 1 {
		t.Fatal("expected one strategy")
	}
}

func TestBuildConsumerSaramaConfig_OldestOffset(t *testing.T) {
	cc := ConsumerConfig{
		GroupID:       "g",
		Topics:        []string{"t"},
		InitialOffset: InitialOffsetOldest,
	}
	sc, err := buildConsumerSaramaConfig(Config{Brokers: []string{"127.0.0.1:9092"}}, cc)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Consumer.Offsets.Initial != sarama.OffsetOldest {
		t.Fatalf("offset=%v", sc.Consumer.Offsets.Initial)
	}
}
