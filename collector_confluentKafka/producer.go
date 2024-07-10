package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "strings"
    "time"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/khalid0306/Kafka_golang/formatter"
    "github.com/khalid0306/Kafka_golang/model"
    "runtime"
)

const (
    ENTITY_METADATA       = "App/Entity/ActeMetier"
    INTERVENTION_METADATA = "App/Entity/Intervention"
    KAFKA_BROKER          = "172.18.0.5:9092"
    TOPIC = "acte_metier"
)

var (
    insideProcessingTimesArray []float64
    outsideProcessingTime      float64
)

func main() {

    log.Println("Starting Kafka Producer ...")

    // Kafka producer configuration
    p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": KAFKA_BROKER})
    if err != nil {
        log.Fatalf("Failed to create producer: %s", err)
    }
    defer p.Close()

    formatter := formatter.NewActeMetierFormatter(nil, log.New(os.Stdout, "", log.LstdFlags))

    // Commencer le timer pour le traitement global
    outsideLoopStartTimer := time.Now()

    // Lire le payload à partir du fichier
    payload := map[string]interface{}{
        "tmpfilepath": "664daa0d1be0dDHbmbJ",
    }

    // Prepare metadata (they won't change within a file)
    entityMetadata := model.NewMetadata("entity", ENTITY_METADATA)
    interventionMetadata := model.NewMetadata("entity", INTERVENTION_METADATA)

    // Lire le ficher
    rows, err := formatter.ReadFile(payload["tmpfilepath"].(string))
    if err != nil {
        log.Fatalf("Error reading file: %s", err)
    }

    for _, row := range rows {
        if row == nil {
            continue
        }
        
        // Commencer le timer pour le traitement de chaque ligne
        insideLoopStartTimer := time.Now()

        message := model.NewMessage(row, nil, nil)
        message.AddMetadata(entityMetadata)

        payloadMessage, err := json.Marshal(message.ToDict())
        if err != nil {
            log.Fatalf("Error marshaling message to JSON: %s", err)
        }

        // Verifer la validité du JSON
        if err := json.Unmarshal(payloadMessage, &map[string]interface{}{}); err != nil {
            log.Fatalf("Invalid JSON: %s", err)
        }

        err = p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &TOPIC, Partition: kafka.PartitionAny},
            Value:          payloadMessage,
        }, nil)
        if err != nil {
            log.Fatalf("Failed to produce message: %s", err)
        }

        go func() {
            for e := range p.Events() {
                switch ev := e.(type) {
                case *kafka.Message:
                    if ev.TopicPartition.Error != nil {
                        log.Printf("Delivery failed: %v\n", ev.TopicPartition)
                    } else {
                        log.Printf("Delivered message to %v\n", ev.TopicPartition)
                    }
                }
            }
        }()

        // Verifie 'IdentifiantVICR' et envoie un autre message si présent
        identifiantVICR := fmt.Sprintf("%v", row["IdentifiantVICR"])
        if identifiantVICR != "" {
            intervention := map[string]interface{}{
                "Id":              row["InterventionId"],
                "IdentifiantVICR": identifiantVICR,
            }
            messageIntervention := model.NewMessage(intervention, nil, nil)
            messageIntervention.AddMetadata(interventionMetadata)

            payloadMessageIntervention, err := json.Marshal(messageIntervention.ToDict())
            if err != nil {
                log.Fatalf("Error marshaling intervention message to JSON: %s", err)
            }

            // Verifer la validité du JSON
            if err := json.Unmarshal(payloadMessageIntervention, &map[string]interface{}{}); err != nil {
                log.Fatalf("Invalid JSON for intervention message: %s", err)
            }

            err = p.Produce(&kafka.Message{
                TopicPartition: kafka.TopicPartition{Topic: &TOPIC, Partition: kafka.PartitionAny},
                Value:          payloadMessageIntervention,
            }, nil)
            if err != nil {
                log.Fatalf("Failed to produce intervention message: %s", err)
            }

            go func() {
                for e := range p.Events() {
                    switch ev := e.(type) {
                    case *kafka.Message:
                        if ev.TopicPartition.Error != nil {
                            log.Printf("Delivery failed: %v\n", ev.TopicPartition)
                        } else {
                            log.Printf("Delivered intervention message to %v\n", ev.TopicPartition)
                        }
                    }
                }
            }()

        }

        // Fin du timer pour le traitement de chaque ligne
        insideLoopEndTimer := time.Now()
        insideProcessingTime := insideLoopEndTimer.Sub(insideLoopStartTimer).Seconds()
        insideProcessingTimesArray = append(insideProcessingTimesArray, insideProcessingTime)

        // Limite le nombre de messages envoyés pour les tests
        if len(insideProcessingTimesArray) >= 500000 {
            break
        }
    }

    // Fin du timer pour le traitement global
    outsideLoopEndTimer := time.Now()
    outsideProcessingTime = outsideLoopEndTimer.Sub(outsideLoopStartTimer).Seconds()

    writeToFile()

    // Flush les messages et ferme le producer
    p.Flush(5 * 1000)
    log.Println("Producer finished")
}

// Méthode pour écrire les temps de traitement dans un fichier
func writeToFile() {
    if len(insideProcessingTimesArray) == 0 {
        return
    }

    avgTime := calculateAverage(insideProcessingTimesArray)

    var displayTestDuration string
    if outsideProcessingTime <= 60 {
        displayTestDuration = fmt.Sprintf("Le temps écoulé pour ce test est de : %.2f secondes.", outsideProcessingTime)
    } else {
        displayTestDuration = fmt.Sprintf("Le temps écoulé pour ce test est de : %.2f minutes.", outsideProcessingTime/60)
    }

    memoryUsage := fmt.Sprintf("Memory usage: %d KB", getMemoryUsage())

    filePath := "../collectorOutputTest/processing_times.txt"

    lines := []string{}
    for _, time := range insideProcessingTimesArray {
        lines = append(lines, fmt.Sprintf("%.6f", time))
    }
    lines = append(lines, fmt.Sprintf("Le temps moyen de traitement d'une ligne est de : %.6f secondes.", avgTime))
    lines = append(lines, displayTestDuration)
    lines = append(lines, memoryUsage)

    err := ioutil.WriteFile(filePath, []byte(strings.Join(lines, "\n")), 0644)
    if err != nil {
        log.Fatalf("Error writing to file: %s", err)
    }
}

// Méthode pour calculer la moyenne des temps de traitement
func calculateAverage(times []float64) float64 {
    sum := 0.0
    for _, t := range times {
        sum += t
    }
    return sum / float64(len(times))
}

// Méthodes pour obtenir l'utilisation de la mémoire
func getMemoryUsage() uint64 {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    return m.Alloc / 1024
}
