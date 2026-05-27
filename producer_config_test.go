package kafka

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestProducerConfig_Validate_Defaults(t *testing.T) {
	pc := ProducerConfig{}
	if err := (&pc).Validate(); err != nil {
		t.Fatal(err)
	}
	if pc.RequiredAcks != sarama.WaitForAll {
		t.Fatalf("acks=%v", pc.RequiredAcks)
	}
	if pc.ChannelBufferSize != 1024 {
		t.Fatalf("buffer=%d", pc.ChannelBufferSize)
	}
	if pc.Compression != CompressionNone {
		t.Fatalf("compression=%s", pc.Compression)
	}
}

func TestParseCompression_AllCodecs(t *testing.T) {
	names := []string{CompressionNone, CompressionGzip, CompressionSnappy, CompressionLZ4, CompressionZstd}
	for _, n := range names {
		if _, err := parseCompression(n); err != nil {
			t.Fatalf("%s: %v", n, err)
		}
	}
}

func TestProducerConfig_Validate_BadCompression(t *testing.T) {
	pc := ProducerConfig{Compression: "invalid"}
	if err := (&pc).Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestProducerConfigDefault(t *testing.T) {
	pc := ProducerConfigDefault()
	if pc.ChannelBufferSize != 1024 {
		t.Fatalf("buffer=%d", pc.ChannelBufferSize)
	}
}

func TestBuildSyncSaramaConfig(t *testing.T) {
	cfg := Config{Brokers: []string{"127.0.0.1:9092"}}
	pc := ProducerConfig{FlushFrequency: 100 * time.Millisecond, FlushMessages: 10}
	sc, err := buildSyncSaramaConfig(cfg, pc)
	if err != nil {
		t.Fatal(err)
	}
	if !sc.Net.SASL.Enable {
		if sc.Producer.Flush.Frequency != 100*time.Millisecond {
			t.Fatalf("flush freq=%v", sc.Producer.Flush.Frequency)
		}
	}
}

func TestBuildAsyncSaramaConfig(t *testing.T) {
	sc, err := buildAsyncSaramaConfig(Config{Brokers: []string{"127.0.0.1:9092"}}, ProducerConfigDefault())
	if err != nil {
		t.Fatal(err)
	}
	if !sc.Producer.Return.Successes || !sc.Producer.Return.Errors {
		t.Fatal("async must return successes and errors")
	}
}

func TestProducerConfig_Idempotent(t *testing.T) {
	pc := ProducerConfig{Idempotent: true}
	sc := sarama.NewConfig()
	if err := pc.applyProducer(sc); err != nil {
		t.Fatal(err)
	}
	if !sc.Producer.Idempotent || sc.Net.MaxOpenRequests != 1 {
		t.Fatal("idempotent settings not applied")
	}
}

func TestNewAsyncProducer_InvalidCompression(t *testing.T) {
	_, err := NewAsyncProducer(Config{Brokers: []string{"127.0.0.1:9092"}}, ProducerConfig{Compression: "bad"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewSyncProducer_InvalidCompression(t *testing.T) {
	_, err := NewSyncProducer(Config{Brokers: []string{"127.0.0.1:9092"}}, ProducerConfig{Compression: "bad"})
	if err == nil {
		t.Fatal("expected error")
	}
}
