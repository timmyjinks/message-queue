package main

import (
	"log"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/timmyjinks/message-queue/kafka/consumer"
	"github.com/timmyjinks/message-queue/kafka/producer"
)

var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "Start server",
	Args:  cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		portFlag, err := cmd.Flags().GetInt("port")
		if err != nil {
			log.Println(err)
		}

		port := ":" + strconv.Itoa(portFlag)
		topic := "message"
		partition := 0

		c := consumer.NewConsumerService(topic, partition)
		e := consumer.NewEmailConsumerService("email", partition)
		p := producer.NewProducerService(topic, partition)

		c.Start()
		e.Start()

		app := application{
			Consumer:      c,
			EmailConsumer: e,
			Producer:      p,
		}

		app.Run(port)
	},
}

func init() {
	rootCmd.Flags().IntP("port", "p", 8080, "port to listen on")
}
