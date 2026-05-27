package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type fakeConsumerGroup struct{}

func (f *fakeConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	return nil
}
func (f *fakeConsumerGroup) Errors() <-chan error { return make(chan error) }
func (f *fakeConsumerGroup) Close() error         { return nil }
func (f *fakeConsumerGroup) Pause(partitions map[string][]int32) {
}
func (f *fakeConsumerGroup) Resume(partitions map[string][]int32) {}
func (f *fakeConsumerGroup) PauseAll()                            {}
func (f *fakeConsumerGroup) ResumeAll()                           {}

type dynamicConsumerGroup struct {
	consume func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error
}

func (d *dynamicConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	if d.consume != nil {
		return d.consume(ctx, topics, handler)
	}
	return nil
}
func (d *dynamicConsumerGroup) Errors() <-chan error { return make(chan error) }
func (d *dynamicConsumerGroup) Close() error         { return nil }
func (d *dynamicConsumerGroup) Pause(map[string][]int32) {
}
func (d *dynamicConsumerGroup) Resume(map[string][]int32) {}
func (d *dynamicConsumerGroup) PauseAll()                 {}
func (d *dynamicConsumerGroup) ResumeAll()                {}

type fakeHandler struct{}

func (fakeHandler) HandleMessage(_ context.Context, _ *sarama.ConsumerMessage) error { return nil }

type alwaysFailHandler struct{}

func (alwaysFailHandler) HandleMessage(_ context.Context, _ *sarama.ConsumerMessage) error {
	return errors.New("fail")
}

type captureProducer struct {
	sent int
	last *sarama.ProducerMessage
}

func (p *captureProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	p.sent++
	p.last = msg
	return 0, 0, nil
}
func (p *captureProducer) Close() error { return nil }

type captureProducerFail struct{}

func (captureProducerFail) SendMessage(*sarama.ProducerMessage) (int32, int64, error) {
	return 0, 0, errors.New("dlq send fail")
}
func (captureProducerFail) Close() error { return nil }

type fakeSession struct {
	marked int
}

func (s *fakeSession) Claims() map[string][]int32                  { return nil }
func (s *fakeSession) MemberID() string                            { return "" }
func (s *fakeSession) GenerationID() int32                         { return 0 }
func (s *fakeSession) MarkOffset(string, int32, int64, string)     {}
func (s *fakeSession) Commit()                                     {}
func (s *fakeSession) ResetOffset(string, int32, int64, string)    {}
func (s *fakeSession) MarkMessage(*sarama.ConsumerMessage, string) { s.marked++ }
func (s *fakeSession) Context() context.Context                    { return context.Background() }

type fakeClaim struct {
	messages chan *sarama.ConsumerMessage
}

func (f *fakeClaim) Topic() string                            { return "t" }
func (f *fakeClaim) Partition() int32                         { return 0 }
func (f *fakeClaim) InitialOffset() int64                     { return 0 }
func (f *fakeClaim) HighWaterMarkOffset() int64               { return 1 }
func (f *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return f.messages }

func TestNewConsumer_NilHandler(t *testing.T) {
	_, err := NewConsumer(Config{Brokers: []string{"b:9092"}}, ConsumerConfig{GroupID: "g", Topics: []string{"t"}}, nil)
	if !errors.Is(err, ErrHandlerNil) {
		t.Fatalf("expected ErrHandlerNil, got %v", err)
	}
}

func TestNewConsumerWithDLQ_BootstrapRetry(t *testing.T) {
	old := newConsumerGroup
	defer func() { newConsumerGroup = old }()

	attempts := 0
	newConsumerGroup = func(_ []string, _ string, _ *sarama.Config) (sarama.ConsumerGroup, error) {
		attempts++
		if attempts <= 2 {
			return nil, errors.New("transient failure")
		}
		return &fakeConsumerGroup{}, nil
	}

	c := ConsumerConfig{
		GroupID:             "g",
		Topics:              []string{"t"},
		BootstrapMaxRetries: 2,
		BootstrapRetryDelay: time.Millisecond,
	}

	consumer, err := NewConsumerWithDLQ(Config{Brokers: []string{"127.0.0.1:9092"}}, c, fakeHandler{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
	_ = consumer.Close()
}

func TestConsumer_Start_RetriesUntilContextDone(t *testing.T) {
	calls := 0
	c := &Consumer{
		group: &dynamicConsumerGroup{
			consume: func(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
				calls++
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return errors.New("consume error")
			},
		},
		config: ConsumerConfig{
			GroupID:           "g",
			Topics:            []string{"t"},
			ConsumeRetryDelay: 5 * time.Millisecond,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := c.Start(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v (calls=%d)", err, calls)
	}
	if calls < 2 {
		t.Fatalf("expected multiple consume attempts, got %d", calls)
	}
}

func TestConsumer_Close_NilSafe(t *testing.T) {
	var c *Consumer
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c = &Consumer{}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConsumer_ConsumeClaim_ProcessesMessages(t *testing.T) {
	msgs := make(chan *sarama.ConsumerMessage, 1)
	msgs <- &sarama.ConsumerMessage{Topic: "t", Value: []byte("ok")}
	close(msgs)

	c := &Consumer{
		config: ConsumerConfig{
			Concurrency: 1,
			MaxRetries:  0,
			RetryDelay:  time.Millisecond,
		},
		handler: fakeHandler{},
	}
	sess := &fakeSession{}
	err := c.ConsumeClaim(sess, &fakeClaim{messages: msgs})
	if err != nil {
		t.Fatal(err)
	}
}

func TestConsumer_SetupCleanup(t *testing.T) {
	c := &Consumer{}
	if err := c.Setup(nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Cleanup(nil); err != nil {
		t.Fatal(err)
	}
}

func TestConsumer_processMessage_Success(t *testing.T) {
	c := &Consumer{
		config:  ConsumerConfig{MaxRetries: 0, RetryDelay: time.Millisecond},
		handler: fakeHandler{},
	}
	sess := &fakeSession{}
	c.processMessage(context.Background(), sess, &sarama.ConsumerMessage{Topic: "t"})
	if sess.marked != 1 {
		t.Fatalf("expected marked, got %d", sess.marked)
	}
}

func TestConsumer_processMessage_HandlerErrorHook(t *testing.T) {
	called := 0
	c := &Consumer{
		config: ConsumerConfig{
			MaxRetries: 1,
			RetryDelay: time.Millisecond,
			Hooks: ConsumerHooks{
				OnHandlerError: func(_ context.Context, _ *sarama.ConsumerMessage, _ error, attempt int) {
					called++
				},
			},
		},
		handler: alwaysFailHandler{},
	}
	sess := &fakeSession{}
	c.processMessage(context.Background(), sess, &sarama.ConsumerMessage{Topic: "t"})
	if called < 1 {
		t.Fatalf("expected OnHandlerError, got %d", called)
	}
}

func TestConsumer_processMessage_DLQ(t *testing.T) {
	dlq := &captureProducer{}
	c := &Consumer{
		config: ConsumerConfig{
			GroupID:    "g",
			Topics:     []string{"t"},
			MaxRetries: 0,
			RetryDelay: time.Millisecond,
			DLQTopic:   "dlq-topic",
		},
		handler:     alwaysFailHandler{},
		dlqProducer: dlq,
	}

	sess := &fakeSession{}
	msg := &sarama.ConsumerMessage{Topic: "src", Key: []byte("k"), Value: []byte("v")}
	c.processMessage(context.Background(), sess, msg)

	if sess.marked != 1 {
		t.Fatalf("expected mark once, got %d", sess.marked)
	}
	if dlq.sent != 1 {
		t.Fatalf("expected dlq send once, got %d", dlq.sent)
	}
}

func TestConsumer_processMessage_OnPoison(t *testing.T) {
	poisonCalled := false
	c := &Consumer{
		config: ConsumerConfig{
			MaxRetries: 0,
			RetryDelay: time.Millisecond,
			Hooks: ConsumerHooks{
				OnPoison: func(_ context.Context, _ *sarama.ConsumerMessage, _ error) {
					poisonCalled = true
				},
			},
		},
		handler: alwaysFailHandler{},
	}
	sess := &fakeSession{}
	c.processMessage(context.Background(), sess, &sarama.ConsumerMessage{Topic: "t"})
	if !poisonCalled {
		t.Fatal("expected OnPoison called")
	}
}

func TestConsumer_processMessage_OnDLQFailed(t *testing.T) {
	dlqFailed := false
	c := &Consumer{
		config: ConsumerConfig{
			MaxRetries: 0,
			RetryDelay: time.Millisecond,
			DLQTopic:   "dlq",
			Hooks: ConsumerHooks{
				OnDLQFailed: func(_ context.Context, _ *sarama.ConsumerMessage, _ error) {
					dlqFailed = true
				},
			},
		},
		handler:     alwaysFailHandler{},
		dlqProducer: captureProducerFail{},
	}
	sess := &fakeSession{}
	c.processMessage(context.Background(), sess, &sarama.ConsumerMessage{Topic: "t"})
	if !dlqFailed {
		t.Fatal("expected OnDLQFailed")
	}
}
