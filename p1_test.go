package kafka

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

func testCertPair(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func testCAPEM(t *testing.T) string {
	c, _ := testCertPair(t)
	return c
}

func TestConfig_BuildTLS_mutual(t *testing.T) {
	cert, key := testCertPair(t)
	tc, err := Config{TLSEnable: true, TLSCert: cert, TLSKey: key, TLSCA: cert}.BuildTLS()
	if err != nil || tc == nil || len(tc.Certificates) != 1 || tc.RootCAs == nil {
		t.Fatal(tc, err)
	}
	// cert+key without CA must still enable TLS (system roots).
	tc, err = Config{TLSEnable: true, TLSCert: cert, TLSKey: key}.BuildTLS()
	if err != nil || tc == nil || tc.RootCAs != nil {
		t.Fatal(tc, err)
	}
	if _, err := (Config{TLSEnable: true, TLSKey: "k"}).BuildTLS(); err == nil {
		t.Fatal("expected error for key without cert")
	}
	if _, err := (Config{TLSEnable: true, TLSCA: "garbage"}).BuildTLS(); err == nil {
		t.Fatal("expected CA parse error")
	}
}

func TestParseRebalanceStrategy_names(t *testing.T) {
	for _, n := range []string{RebalanceRange, RebalanceSticky, RebalanceRoundRobin} {
		got, err := parseRebalanceStrategy(n)
		if err != nil || got == nil || got.Name() != n {
			t.Fatalf("%s: %v %v", n, got, err)
		}
	}
}

func TestSleepCtx(t *testing.T) {
	if err := sleepCtx(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if err := sleepCtx(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepCtx(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	//lint:ignore SA1012 intentional nil context in test
	if ctxOrBackground(nil) == nil {
		t.Fatal("nil ctx must become Background")
	}
}

type closeCounter struct{ closed atomic.Int32 }

func (c *closeCounter) Close() error { c.closed.Add(1); return nil }

func TestBuildWithCtx(t *testing.T) {
	v, err := buildWithCtx(context.Background(), func() (*closeCounter, error) { return &closeCounter{}, nil })
	if err != nil || v == nil {
		t.Fatal(v, err)
	}
	if _, err := buildWithCtx(context.Background(), func() (*closeCounter, error) { return nil, errors.New("x") }); err == nil {
		t.Fatal("expected error")
	}
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	if _, err := buildWithCtx(cctx, func() (*closeCounter, error) { return &closeCounter{}, nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	// Cancelled while building: late value must be closed.
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	late := &closeCounter{}
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	_, err = buildWithCtx(ctx, func() (*closeCounter, error) { <-release; return late, nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for late.closed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if late.closed.Load() != 1 {
		t.Fatal("late producer must be closed")
	}
}

type fakeSarama struct {
	sarama.SyncProducer
	sendErr   error
	closeErr  error
	closeCall atomic.Int32
}

func (f *fakeSarama) SendMessage(*sarama.ProducerMessage) (int32, int64, error) {
	return 1, 2, f.sendErr
}
func (f *fakeSarama) Close() error { f.closeCall.Add(1); return f.closeErr }

func TestNewSyncProducer_wrapperAndClose(t *testing.T) {
	old := newSaramaSyncProducer
	defer func() { newSaramaSyncProducer = old }()
	fake := &fakeSarama{closeErr: errors.New("close")}
	newSaramaSyncProducer = func([]string, *sarama.Config) (sarama.SyncProducer, error) { return fake, nil }

	p, err := NewSyncProducer(context.Background(), Config{Brokers: []string{"b:9092"}}, ProducerConfigDefault())
	if err != nil {
		t.Fatal(err)
	}
	part, off, err := p.SendMessage(context.Background(), &sarama.ProducerMessage{Topic: "t"})
	if err != nil || part != 1 || off != 2 {
		t.Fatal(part, off, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := p.SendMessage(ctx, &sarama.ProducerMessage{Topic: "t"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err := p.SendMessage(context.Background(), nil); !errors.Is(err, ErrProducerClosed) {
		t.Fatal(err)
	}
	if err := p.Close(); err == nil {
		t.Fatal("expected close error")
	}
	if err := p.Close(); err == nil { // same cached result
		t.Fatal("expected cached close error")
	}
	if fake.closeCall.Load() != 1 {
		t.Fatalf("Close ran %d times, want 1", fake.closeCall.Load())
	}
	var nilP *syncProducer
	if err := nilP.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := nilP.SendMessage(context.Background(), &sarama.ProducerMessage{}); !errors.Is(err, ErrProducerClosed) {
		t.Fatal(err)
	}
	//lint:ignore SA1012 intentional nil context in test
	if _, _, err := (&syncProducer{}).SendMessage(nil, &sarama.ProducerMessage{}); !errors.Is(err, ErrProducerClosed) {
		t.Fatal(err)
	}
}

func TestNewSyncProducer_errorsAndCancel(t *testing.T) {
	old := newSaramaSyncProducer
	defer func() { newSaramaSyncProducer = old }()
	newSaramaSyncProducer = func([]string, *sarama.Config) (sarama.SyncProducer, error) { return nil, errors.New("dial") }
	//lint:ignore SA1012 intentional nil context in test
	if _, err := NewSyncProducer(nil, Config{Brokers: []string{"b:9092"}}, ProducerConfigDefault()); err == nil {
		t.Fatal("expected error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewSyncProducer(ctx, Config{Brokers: []string{"b:9092"}}, ProducerConfigDefault()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := NewProducer(context.Background(), Config{Brokers: []string{"b:9092"}}); err == nil {
		t.Fatal("expected error")
	}
}

func TestAsyncProducer_SendMessage_ctxCancelled(t *testing.T) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	mp := mocks.NewAsyncProducer(t, cfg)
	ap := newAsyncProducer(mp)
	defer func() { _ = ap.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	msg := &sarama.ProducerMessage{Topic: "t"}
	if _, _, err := ap.SendMessage(ctx, msg); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type blockedInput struct {
	sarama.AsyncProducer
	in  chan *sarama.ProducerMessage
	suc chan *sarama.ProducerMessage
	err chan *sarama.ProducerError
}

func newBlockedInput() *blockedInput {
	return &blockedInput{
		in:  make(chan *sarama.ProducerMessage),
		suc: make(chan *sarama.ProducerMessage),
		err: make(chan *sarama.ProducerError),
	}
}
func (b *blockedInput) Input() chan<- *sarama.ProducerMessage     { return b.in }
func (b *blockedInput) Successes() <-chan *sarama.ProducerMessage { return b.suc }
func (b *blockedInput) Errors() <-chan *sarama.ProducerError      { return b.err }
func (b *blockedInput) Close() error                              { close(b.suc); close(b.err); return nil }

func TestAsyncProducer_SendMessage_ctxWhileBlockedOnInput(t *testing.T) {
	b := newBlockedInput()
	ap := newAsyncProducer(b)
	defer func() { _ = ap.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	msg := &sarama.ProducerMessage{Topic: "t"}
	if _, _, err := ap.SendMessage(ctx, msg); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if msg.Metadata != nil {
		t.Fatal("metadata must be cleared when message was not sent")
	}
}

func TestAsyncProducer_SendMessage_ctxWhileWaitingAck(t *testing.T) {
	b := newBlockedInput()
	ap := newAsyncProducer(b)
	defer func() { _ = ap.Close() }()
	go func() { <-b.in }() // accept the message, never ack
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, _, err := ap.SendMessage(ctx, &sarama.ProducerMessage{Topic: "t"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestNewAsyncProducer_cancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewAsyncProducer(ctx, Config{Brokers: []string{"b:9092"}}, ProducerConfigDefault()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// ---- consumer ----

func TestProcessMessage_DLQFailure_doesNotCommit(t *testing.T) {
	c := &Consumer{
		config:      ConsumerConfig{MaxRetries: 0, RetryDelay: time.Millisecond, DLQTopic: "dlq"},
		handler:     alwaysFailHandler{},
		dlqProducer: captureProducerFail{},
	}
	sess := &fakeSession{}
	err := c.processMessage(context.Background(), sess, &sarama.ConsumerMessage{Topic: "t"})
	if err == nil {
		t.Fatal("expected dlq failure error")
	}
	if sess.marked != 0 {
		t.Fatalf("offset must not be committed on DLQ failure, marked=%d", sess.marked)
	}
}

func TestConsumeClaim_DLQFailure_stopsClaimWithoutCommit(t *testing.T) {
	msgs := make(chan *sarama.ConsumerMessage, 3)
	for i := 0; i < 3; i++ {
		msgs <- &sarama.ConsumerMessage{Topic: "t", Offset: int64(i)}
	}
	c := &Consumer{
		config: ConsumerConfig{
			Concurrency: 1, RetryDelay: time.Millisecond,
			WorkerShutdownTimeout: time.Second, DLQTopic: "dlq",
		},
		handler:     alwaysFailHandler{},
		dlqProducer: captureProducerFail{},
	}
	sess := &fakeSession{}
	err := c.ConsumeClaim(sess, &fakeClaim{messages: msgs})
	if err == nil {
		t.Fatal("expected ConsumeClaim to fail so the message is redelivered")
	}
	if sess.marked != 0 {
		t.Fatalf("marked=%d, want 0", sess.marked)
	}
}

func TestProcessMessage_cancelDuringRetryDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{
		config: ConsumerConfig{MaxRetries: 5, RetryDelay: time.Hour,
			Hooks: ConsumerHooks{OnHandlerError: func(context.Context, *sarama.ConsumerMessage, error, int) { cancel() }}},
		handler: alwaysFailHandler{},
	}
	sess := &fakeSession{}
	done := make(chan error, 1)
	go func() { done <- c.processMessage(ctx, sess, &sarama.ConsumerMessage{Topic: "t"}) }()
	select {
	case err := <-done:
		if err != nil || sess.marked != 0 {
			t.Fatal(err, sess.marked)
		}
	case <-time.After(time.Second):
		t.Fatal("retry backoff ignored ctx")
	}
}

func TestStart_cancelDuringConsumeRetryDelay(t *testing.T) {
	c := &Consumer{
		group: &dynamicConsumerGroup{consume: func(context.Context, []string, sarama.ConsumerGroupHandler) error {
			return errors.New("boom")
		}},
		config: ConsumerConfig{Topics: []string{"t"}, ConsumeRetryDelay: time.Hour},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start ignored ctx during backoff")
	}
}

func TestStart_closedGroupStops(t *testing.T) {
	c := &Consumer{
		group: &dynamicConsumerGroup{consume: func(context.Context, []string, sarama.ConsumerGroupHandler) error {
			return sarama.ErrClosedConsumerGroup
		}},
		config: ConsumerConfig{Topics: []string{"t"}, ConsumeRetryDelay: time.Hour},
	}
	if err := c.Start(context.Background()); !errors.Is(err, sarama.ErrClosedConsumerGroup) {
		t.Fatal(err)
	}
}

func TestConsumer_Close_once(t *testing.T) {
	g := &countingGroup{err: errors.New("close")}
	c := &Consumer{group: g}
	if err := c.Close(); err == nil {
		t.Fatal("expected error")
	}
	if err := c.Close(); err == nil {
		t.Fatal("expected cached error")
	}
	if g.n.Load() != 1 {
		t.Fatalf("group closed %d times", g.n.Load())
	}
}

type countingGroup struct {
	sarama.ConsumerGroup
	n   atomic.Int32
	err error
}

func (g *countingGroup) Close() error { g.n.Add(1); return g.err }

func TestNewConsumerWithDLQ_ctxCancelStopsBootstrapRetry(t *testing.T) {
	old := newConsumerGroup
	defer func() { newConsumerGroup = old }()
	var attempts atomic.Int32
	newConsumerGroup = func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error) {
		attempts.Add(1)
		return nil, errors.New("down")
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err := NewConsumerWithDLQ(ctx, Config{Brokers: []string{"b:9092"}},
		ConsumerConfig{GroupID: "g", Topics: []string{"t"}, BootstrapMaxRetries: 100, BootstrapRetryDelay: time.Hour},
		fakeHandler{}, nil)
	if !errors.Is(err, context.Canceled) || time.Since(start) > time.Second {
		t.Fatal(err, time.Since(start))
	}
	if attempts.Load() < 1 {
		t.Fatal("expected at least one attempt")
	}

	// Already-cancelled ctx never dials.
	attempts.Store(0)
	if _, err := NewConsumerWithDLQ(ctx, Config{Brokers: []string{"b:9092"}},
		ConsumerConfig{GroupID: "g", Topics: []string{"t"}}, fakeHandler{}, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if attempts.Load() != 0 {
		t.Fatal("must not dial with cancelled ctx")
	}
	// nil ctx tolerated.
	//lint:ignore SA1012 intentional nil context in test
	if _, err := NewConsumer(nil, Config{Brokers: []string{"b:9092"}},
		ConsumerConfig{GroupID: "g", Topics: []string{"t"}}, fakeHandler{}); err == nil {
		t.Fatal("expected bootstrap error")
	}
}
