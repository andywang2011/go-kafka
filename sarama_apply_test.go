package kafka

import (
	"testing"

	"github.com/IBM/sarama"
)

func TestApplyConnection_SASL(t *testing.T) {
	sc := sarama.NewConfig()
	cfg := Config{
		Brokers:      []string{"127.0.0.1:9092"},
		SASLEnable:   true,
		SASLUser:     "u",
		SASLPassword: "p",
	}
	if err := applyConnection(cfg, sc); err != nil {
		t.Fatal(err)
	}
	if !sc.Net.SASL.Enable || sc.Net.SASL.User != "u" {
		t.Fatalf("sasl not applied: %+v", sc.Net.SASL)
	}
}

func TestApplyConnection_EmptyBrokers(t *testing.T) {
	sc := sarama.NewConfig()
	if err := applyConnection(Config{}, sc); err == nil {
		t.Fatal("expected error")
	}
}

func TestConfig_Validate(t *testing.T) {
	if err := (&Config{Brokers: []string{"a:9092"}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (&Config{}).Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestApplyConnection_TLSInvalidCert(t *testing.T) {
	sc := sarama.NewConfig()
	cfg := Config{
		Brokers:   []string{"127.0.0.1:9092"},
		TLSEnable: true,
		TLSCert:   "not-pem",
		TLSKey:    "not-pem",
		TLSCA:     "not-pem",
	}
	if err := applyConnection(cfg, sc); err == nil {
		t.Fatal("expected tls error")
	}
}

func TestApplyConnection_TLSIncompleteMaterial(t *testing.T) {
	sc := sarama.NewConfig()
	cfg := Config{Brokers: []string{"127.0.0.1:9092"}, TLSEnable: true}
	if err := applyConnection(cfg, sc); err != nil {
		t.Fatal(err)
	}
	if sc.Net.TLS.Enable {
		t.Fatal("expected tls off when cert material incomplete")
	}
}
