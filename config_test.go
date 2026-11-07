package kafka

import (
	"testing"

	"github.com/IBM/sarama"
)

func TestConfig_ValidateBrokers(t *testing.T) {
	if err := (Config{Brokers: []string{"a:9092"}}).ValidateBrokers(); err != nil {
		t.Fatal(err)
	}
	if err := (Config{}).ValidateBrokers(); err == nil {
		t.Fatal("expected error")
	}
}

func TestConfig_BuildTLS_InvalidPEM(t *testing.T) {
	c := Config{TLSEnable: true, TLSCert: "x", TLSKey: "y", TLSCA: "z"}
	if _, err := c.BuildTLS(); err == nil {
		t.Fatal("expected error")
	}
}

func TestConfig_BuildTLS_Disabled(t *testing.T) {
	tls, err := (Config{}).BuildTLS()
	if err != nil || tls != nil {
		t.Fatalf("tls=%v err=%v", tls, err)
	}
}

func TestConfig_Validate_KafkaVersion(t *testing.T) {
	c := Config{Brokers: []string{"a:9092"}, KafkaVersion: "not-a-version"}
	if err := (&c).Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestConfig_SaramaVersion_Default(t *testing.T) {
	if (Config{}).SaramaVersion() != sarama.V4_1_0_0 {
		t.Fatalf("expected V4_1_0_0, got %v", (Config{}).SaramaVersion())
	}
}
