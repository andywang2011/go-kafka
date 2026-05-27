package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/andywang2011/go-kafka"
)

type handler struct{}

func (handler) HandleMessage(_ context.Context, msg *sarama.ConsumerMessage) error {
	fmt.Printf("topic=%s partition=%d offset=%d value=%s\n",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
	return nil
}

func main() {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	if len(brokers) == 0 || brokers[0] == "" {
		brokers = []string{"127.0.0.1:9092"}
	}

	cfg := kafka.Config{Brokers: brokers}
	cc := kafka.ConsumerConfig{
		GroupID:     "go-kafka-example",
		Topics:      []string{"demo-topic"},
		Concurrency: 2,
	}

	consumer, err := kafka.NewConsumer(cfg, cc, handler{})
	if err != nil {
		log.Fatalf("NewConsumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := consumer.Start(ctx); err != nil && err != context.Canceled {
			log.Printf("Start: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()
	<-done
	_ = consumer.Close()
}
