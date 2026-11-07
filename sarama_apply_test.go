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

func TestApplyConnection_TLSEnableSystemRoots(t *testing.T) {
	sc := sarama.NewConfig()
	cfg := Config{Brokers: []string{"127.0.0.1:9092"}, TLSEnable: true}
	if err := applyConnection(cfg, sc); err != nil {
		t.Fatal(err)
	}
	if !sc.Net.TLS.Enable || sc.Net.TLS.Config == nil || sc.Net.TLS.Config.RootCAs != nil {
		t.Fatal("expected tls on with system roots")
	}
}

func TestApplyConnection_TLSDisabled(t *testing.T) {
	sc := sarama.NewConfig()
	if err := applyConnection(Config{Brokers: []string{"127.0.0.1:9092"}}, sc); err != nil {
		t.Fatal(err)
	}
	if sc.Net.TLS.Enable {
		t.Fatal("tls must stay off without TLSEnable")
	}
}

func TestApplyConnection_TLSCAOnly(t *testing.T) {
	sc := sarama.NewConfig()
	cfg := Config{Brokers: []string{"127.0.0.1:9092"}, TLSEnable: true, TLSCA: testCAPEM(t)}
	if err := applyConnection(cfg, sc); err != nil {
		t.Fatal(err)
	}
	if !sc.Net.TLS.Enable || sc.Net.TLS.Config.RootCAs == nil || len(sc.Net.TLS.Config.Certificates) != 0 {
		t.Fatal("expected tls enabled with CA-only")
	}
}

func TestApplyConnection_TLSCertWithoutKey(t *testing.T) {
	cfg := Config{Brokers: []string{"127.0.0.1:9092"}, TLSEnable: true, TLSCert: "x"}
	if err := applyConnection(cfg, sarama.NewConfig()); err == nil {
		t.Fatal("expected error for cert without key")
	}
}
