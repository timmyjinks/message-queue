package consumer

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/segmentio/kafka-go"
)

type ConsumerService struct {
	Topic     string
	Partition int
}

func NewConsumerService(topic string, partition int) *ConsumerService {
	return &ConsumerService{
		Topic:     topic,
		Partition: partition,
	}
}

func (c *ConsumerService) Start() {
	go func() {
		conn := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{"localhost:9092"},
			Partition:   c.Partition,
			GroupID:     "group-topic",
			Topic:       c.Topic,
			MaxBytes:    10e6,
			StartOffset: kafka.LastOffset,
		})
		defer conn.Close()

		writer := kafka.Writer{
			Addr:  kafka.TCP("localhost:9092"),
			Topic: "email",
		}

		ctx := context.Background()

		fmt.Println("consuming messages...")
		for {
			m, err := conn.ReadMessage(ctx)
			if err != nil {
				if err != io.EOF {
					log.Println(err)
					break
				}
			}
			fmt.Printf("offset %d: %s = %s\n", m.Offset, m.Key, m.Value)

			go writer.WriteMessages(ctx, kafka.Message{
				Partition: c.Partition,
				Value:     []byte("email"),
			})
		}
	}()
}
