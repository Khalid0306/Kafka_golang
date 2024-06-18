package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func ConsumeMessage() {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "172.18.0.5:9092",
		"group.id":          "test-kafka",
		"auto.offset.reset": "latest", // "earliest" pour commencer la lecture depuis le début
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{"acte_metier"}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}

func main() {
	ConsumeMessage()
    // Prevent the main function from exiting immediately
    select {}
}