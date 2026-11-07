package kafka

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

// Compression codec names for ProducerConfig.Compression.
const (
	CompressionNone   = "none"
	CompressionGzip   = "gzip"
	CompressionSnappy = "snappy"
	CompressionLZ4    = "lz4"
	CompressionZstd   = "zstd"
)

// ProducerConfig tunes producer behavior (acks, compression, buffering).
type ProducerConfig struct {
	// RequiredAcks is the broker ack level (default WaitForAll).
	RequiredAcks sarama.RequiredAcks
	// Compression is none, gzip, snappy, lz4, or zstd.
	Compression string
	// ChannelBufferSize is the async producer input buffer size (default 1024).
	ChannelBufferSize int
	// FlushFrequency enables time-based batch flush when > 0.
	FlushFrequency time.Duration
	// FlushMessages enables count-based batch flush when > 0.
	FlushMessages int
	// Idempotent enables the idempotent producer (requires Kafka 0.11+).
	Idempotent bool
}

// ProducerConfigDefault returns recommended defaults for production use.
func ProducerConfigDefault() ProducerConfig {
	pc := ProducerConfig{}
	_ = (&pc).Validate()
	return pc
}

// Validate fills defaults and checks producer config.
func (pc *ProducerConfig) Validate() error {
	if pc.RequiredAcks == 0 {
		pc.RequiredAcks = sarama.WaitForAll
	}
	if pc.Compression == "" {
		pc.Compression = CompressionNone
	}
	if _, err := parseCompression(pc.Compression); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if pc.ChannelBufferSize <= 0 {
		pc.ChannelBufferSize = 1024
	}
	if pc.FlushMessages < 0 {
		return fmt.Errorf("%w: FlushMessages must be >= 0", ErrInvalidConfig)
	}
	return nil
}

func parseCompression(name string) (sarama.CompressionCodec, error) {
	switch name {
	case CompressionNone:
		return sarama.CompressionNone, nil
	case CompressionGzip:
		return sarama.CompressionGZIP, nil
	case CompressionSnappy:
		return sarama.CompressionSnappy, nil
	case CompressionLZ4:
		return sarama.CompressionLZ4, nil
	case CompressionZstd:
		return sarama.CompressionZSTD, nil
	default:
		return sarama.CompressionNone, fmt.Errorf("unknown compression %q", name)
	}
}

func (pc *ProducerConfig) applyProducer(sc *sarama.Config) error {
	pcCopy := *pc
	if err := pcCopy.Validate(); err != nil {
		return err
	}
	codec, err := parseCompression(pcCopy.Compression)
	if err != nil {
		return err
	}
	sc.Producer.RequiredAcks = pcCopy.RequiredAcks
	sc.Producer.Compression = codec
	sc.Producer.Partitioner = sarama.NewRandomPartitioner
	sc.Producer.Idempotent = pcCopy.Idempotent
	if pcCopy.Idempotent {
		sc.Producer.RequiredAcks = sarama.WaitForAll
		sc.Net.MaxOpenRequests = 1
	}
	sc.ChannelBufferSize = pcCopy.ChannelBufferSize
	if pcCopy.FlushFrequency > 0 {
		sc.Producer.Flush.Frequency = pcCopy.FlushFrequency
	}
	if pcCopy.FlushMessages > 0 {
		sc.Producer.Flush.Messages = pcCopy.FlushMessages
	}
	return nil
}

func buildSyncSaramaConfig(cfg Config, pc ProducerConfig) (*sarama.Config, error) {
	sc := sarama.NewConfig()
	if err := applyConnection(cfg, sc); err != nil {
		return nil, err
	}
	if err := pc.applyProducer(sc); err != nil {
		return nil, err
	}
	sc.Producer.Return.Successes = true
	return sc, nil
}

func buildAsyncSaramaConfig(cfg Config, pc ProducerConfig) (*sarama.Config, error) {
	sc := sarama.NewConfig()
	if err := applyConnection(cfg, sc); err != nil {
		return nil, err
	}
	if err := pc.applyProducer(sc); err != nil {
		return nil, err
	}
	sc.Producer.Return.Successes = true
	sc.Producer.Return.Errors = true
	return sc, nil
}
