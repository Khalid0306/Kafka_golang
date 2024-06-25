package main

import (
    "context"
    "fmt"
    "log"

    "github.com/segmentio/kafka-go"
)

const (
    KAFKA_BROKER = "172.18.0.5:9092"
    TOPIC        = "acte_metier1"
    GROUP_ID     = "test-kafka-golang"
)

func main() {
    // Creer un contexte
    ctx := context.Background()

    // Kafka reader configuration
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{KAFKA_BROKER},
        Topic:   TOPIC,
        GroupID: GROUP_ID,
        StartOffset: kafka.LastOffset, // Lire les messages les plus recents
        Logger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
            log.Printf(msg, args...)
        }),
    })
    defer r.Close()

    log.Println("Starting Kafka Consumer ...")

    for {
        // Lire le message
        m, err := r.ReadMessage(ctx)
        if err != nil {
            log.Printf("Error reading message: %s", err)
            // Si une erreur est survenue, continuer
            continue
        }

        // Afficher le message
        fmt.Printf("Message at topic/partition/offset %v/%v/%v: %s = %s\n",
            m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
    }
}