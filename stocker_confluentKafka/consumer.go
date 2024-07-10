package main

import (
    "fmt"
    "log"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
    KAFKA_BROKER = "172.18.0.5:9092"
    GROUP_ID = "test-kafka-golang"
    TOPIC = "acte_metier"
)

func ConsumeMessage() {
    c, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": KAFKA_BROKER,
        "group.id":          GROUP_ID,
        "auto.offset.reset": "latest",
    })

    if err != nil {
        log.Fatalf("Failed to create consumer: %s", err)
    }

    c.SubscribeTopics([]string{TOPIC}, nil)

    for {
        msg, err := c.ReadMessage(-1)
        if err == nil {
            fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
        } else {
            fmt.Printf("Consumer error: %v (%v)\n", err, msg)
        }
    }

    c.Close()
}

func main() {
	ConsumeMessage()
    // Empecher la fermeture immédiate du programme
    select {}
}