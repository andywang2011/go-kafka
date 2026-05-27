package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
)

// applyConnection writes brokers, SASL, TLS, and protocol version into sc.
func applyConnection(cfg Config, sc *sarama.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	sc.Version = cfg.SaramaVersion()
	sc.Net.SASL.Enable = cfg.SASLEnable
	if cfg.SASLEnable {
		sc.Net.SASL.User = cfg.SASLUser
		sc.Net.SASL.Password = cfg.SASLPassword
		sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}
	if cfg.TLSEnable {
		tlsConfig, err := cfg.BuildTLS()
		if err != nil {
			return fmt.Errorf("kafka tls: %w", err)
		}
		if tlsConfig != nil {
			sc.Net.TLS.Enable = true
			sc.Net.TLS.Config = tlsConfig
		}
	}
	return nil
}
