package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/IBM/sarama"
)

// Config holds connection settings (brokers, SASL, TLS) shared by producers and consumers.
type Config struct {
	// Brokers is the list of broker addresses (required), e.g. []string{"host1:9092"}.
	Brokers []string

	// KafkaVersion is the broker protocol version string (e.g. "3.6.0"). Empty defaults to V4_1_0_0.
	KafkaVersion string

	// SASLEnable enables SASL authentication (plaintext mechanism only).
	SASLEnable bool
	// SASLUser is the SASL username when SASLEnable is true.
	SASLUser string
	// SASLPassword is the SASL password when SASLEnable is true.
	SASLPassword string

	// TLSEnable enables TLS. TLSCA alone (server verification) is sufficient;
	// TLSCert and TLSKey add mutual TLS and must be set together.
	TLSEnable bool
	// TLSInsecureSkipVerify skips server certificate verification (dev/debug only).
	TLSInsecureSkipVerify bool
	// TLSCert is the client certificate PEM for mutual TLS.
	TLSCert string
	// TLSKey is the client private key PEM.
	TLSKey string
	// TLSCA is the root CA PEM used to verify broker certificates.
	TLSCA string
}

// ConfigDefault returns connection defaults; Brokers must still be set before use.
func ConfigDefault() Config { return Config{KafkaVersion: ""} }

// ValidateBrokers ensures brokers is non-empty.
func (c Config) ValidateBrokers() error {
	if len(c.Brokers) == 0 {
		return errors.New("kafka: brokers are required")
	}
	return nil
}

// Validate checks connection config.
func (c *Config) Validate() error {
	if err := c.ValidateBrokers(); err != nil {
		return err
	}
	if c.KafkaVersion != "" {
		if _, err := sarama.ParseKafkaVersion(c.KafkaVersion); err != nil {
			return fmt.Errorf("%w: invalid KafkaVersion: %v", ErrInvalidConfig, err)
		}
	}
	return nil
}

// SaramaVersion returns the parsed Kafka protocol version for Sarama.
func (c Config) SaramaVersion() sarama.KafkaVersion {
	if c.KafkaVersion == "" {
		return sarama.V4_1_0_0
	}
	v, err := sarama.ParseKafkaVersion(c.KafkaVersion)
	if err != nil {
		return sarama.V4_1_0_0
	}
	return v
}

// BuildTLS returns *tls.Config when TLSEnable is true, otherwise nil.
//
// TLSCA, when set, becomes the root pool (CA-only is valid for server-auth TLS).
// TLSCert and TLSKey must be set together for mutual TLS. With none of them set
// the system root pool is used.
func (c Config) BuildTLS() (*tls.Config, error) {
	if !c.TLSEnable {
		return nil, nil
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return nil, fmt.Errorf("kafka tls: TLSCert and TLSKey must be set together")
	}
	tc := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.TLSInsecureSkipVerify,
	}
	if c.TLSCert != "" {
		cert, err := tls.X509KeyPair([]byte(c.TLSCert), []byte(c.TLSKey))
		if err != nil {
			return nil, fmt.Errorf("kafka tls client cert: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	if c.TLSCA != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(c.TLSCA)) {
			return nil, fmt.Errorf("kafka tls ca parse failed")
		}
		tc.RootCAs = pool
	}
	return tc, nil
}
