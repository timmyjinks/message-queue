package consumer

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type EmailConsumerService struct {
	Topic     string
	Partition int
}

func NewEmailConsumerService(topic string, partition int) *EmailConsumerService {
	return &EmailConsumerService{
		Topic:     topic,
		Partition: partition,
	}
}

func (c *EmailConsumerService) Start() {
	go func() {
		conn := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{"localhost:9092"},
			Partition:      c.Partition,
			GroupID:        "group-topic",
			Topic:          c.Topic,
			MaxBytes:       10e6,
			StartOffset:    kafka.LastOffset,
			CommitInterval: time.Second,
		})
		defer conn.Close()

		ctx := context.Background()

		fmt.Println("consuming emails...")
		for {
			m, err := conn.ReadMessage(ctx)
			if err != nil {
				if err != io.EOF {
					log.Println(err)
					break
				}
			}
			fmt.Printf("offset %d: %s = %s\n", m.Offset, m.Key, m.Value)
		}
	}()
}
