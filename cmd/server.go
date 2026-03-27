package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/timmyjinks/message-queue/kafka/consumer"
	"github.com/timmyjinks/message-queue/kafka/producer"
)

type application struct {
	Consumer      *consumer.ConsumerService
	EmailConsumer *consumer.EmailConsumerService
	Producer      *producer.ProducerService
}

type MessageRequest struct {
	Message string `json:"message"`
}

func (app *application) Run(addr string) {
	http.HandleFunc("/message", app.Message)

	fmt.Println("Listening on localhost", addr)
	log.Println(http.ListenAndServe(addr, nil))
}

func (app *application) Message(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	var msg MessageRequest
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	app.Producer.Send(msg.Message)
}
