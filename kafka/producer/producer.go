package producer

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type ProducerService struct {
	Topic     string
	Partition int
}

func NewProducerService(topic string, partition int) *ProducerService {
	return &ProducerService{
		Topic:     topic,
		Partition: partition,
	}
}

func (p *ProducerService) Send(message string) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", p.Topic, p.Partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte(message)},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	fmt.Println("message sent")

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

}
