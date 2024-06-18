package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)


func main() {
	topic := "acte_metier"

	fmt.Println("Hello, I'm producing message...")

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "172.18.0.5"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Produce messages to topic (asynchronously)
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
					// fmt.Println(string(ev.Value))
				}
			default:
				fmt.Printf("Ignored event: %v\n", e)
			}
		}
	}()

	// Wait for message deliveries before shutting down
	p.Flush(2 * 1000)

	fmt.Println("I'm done producing message...")
}
