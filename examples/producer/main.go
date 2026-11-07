package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/andywang2011/go-kafka"
)

func main() {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	if len(brokers) == 0 || brokers[0] == "" {
		brokers = []string{"127.0.0.1:9092"}
	}

	cfg := kafka.Config{Brokers: brokers}
	p, err := kafka.NewAsyncProducer(context.Background(), cfg, kafka.ProducerConfigDefault())
	if err != nil {
		log.Fatalf("NewAsyncProducer: %v", err)
	}
	defer p.Close()

	partition, offset, err := p.SendMessage(context.Background(), &sarama.ProducerMessage{
		Topic: "demo-topic",
		Key:   sarama.StringEncoder("demo-key"),
		Value: sarama.ByteEncoder("hello from go-kafka"),
	})
	if err != nil {
		log.Fatalf("SendMessage: %v", err)
	}
	fmt.Printf("sent partition=%d offset=%d\n", partition, offset)
}
