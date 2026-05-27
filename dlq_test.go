package kafka

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/IBM/sarama"
)

func TestBuildDLQMessage(t *testing.T) {
	msg := &sarama.ConsumerMessage{
		Topic: "src", Partition: 1, Offset: 99,
		Key: []byte("k"), Value: []byte("v"),
	}
	pm, err := buildDLQMessage("dlq", msg, errors.New("boom"))
	if err != nil {
		t.Fatal(err)
	}
	if pm.Topic != "dlq" {
		t.Fatalf("topic=%s", pm.Topic)
	}
	val, _ := pm.Value.Encode()
	var env DLQEnvelope
	if err := json.Unmarshal(val, &env); err != nil {
		t.Fatal(err)
	}
	if env.OriginalTopic != "src" || env.Error != "boom" {
		t.Fatalf("env=%+v", env)
	}
}

func TestSendDLQ_NilProducer(t *testing.T) {
	if err := sendDLQ(nil, "dlq", &sarama.ConsumerMessage{}, errors.New("e")); err != nil {
		t.Fatal(err)
	}
}
