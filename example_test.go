package kafka_test

import (
	"context"
	"github.com/IBM/sarama"

	"github.com/andywang2011/go-kafka"
)

func ExampleNewAsyncProducer() {
	cfg := kafka.Config{Brokers: []string{"127.0.0.1:9092"}}
	pc := kafka.ProducerConfigDefault()

	p, err := kafka.NewAsyncProducer(context.Background(), cfg, pc)
	if err != nil {
		return
	}
	defer p.Close()

	_, _, _ = p.SendMessage(context.Background(), &sarama.ProducerMessage{
		Topic: "orders",
		Key:   sarama.StringEncoder("user-1"),
		Value: sarama.ByteEncoder([]byte(`{"id":1}`)),
	})
}
