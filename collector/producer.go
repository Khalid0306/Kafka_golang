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
)

var (
    insideProcessingTimesArray []float64
    outsideProcessingTime      float64
)

func main() {
    log.Println("Starting Kafka Producer")

    // Kafka producer configuration
    p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": KAFKA_BROKER})
    if err != nil {
        log.Fatalf("Failed to create producer: %s", err)
    }
    defer p.Close()

    // Formatter setup (you need to define this according to your formatter implementation)
    formatter := formatter.NewActeMetierFormatter(nil, log.New(os.Stdout, "", log.LstdFlags))

    // Start time to measure overall processing time
    outsideLoopStartTimer := time.Now()

    // Read payload from file
    payload := map[string]interface{}{
        "tmpfilepath": "664daa0d1be0dDHbmbJ",
    }

    // Prepare metadata (they won't change within a file)
    entityMetadata := model.NewMetadata("entity", ENTITY_METADATA)
    interventionMetadata := model.NewMetadata("entity", INTERVENTION_METADATA)

    // Read file contents
    rows, err := formatter.ReadFile(payload["tmpfilepath"].(string))
    if err != nil {
        log.Fatalf("Error reading file: %s", err)
    }

    // Define the topic variable
    topic := "acte_metier"

    // Iterate over rows
    for _, row := range rows {
        if row == nil {
            continue
        }

        // Start timer for individual row processing
        insideLoopStartTimer := time.Now()

        // Construct Message from row data
        message := model.NewMessage(row, nil, nil)
        message.AddMetadata(entityMetadata)

        // Convert message to JSON
        payloadMessage, err := json.Marshal(message.ToDict())
        if err != nil {
            log.Fatalf("Error marshaling message to JSON: %s", err)
        }

        // Send message to Kafka topic 'acte_metier'
        deliveryChan := make(chan kafka.Event)
        err = p.Produce(&kafka.Message{
            TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
            Value:          payloadMessage,
        }, deliveryChan)
        if err != nil {
            log.Fatalf("Failed to produce message: %s", err)
        }

        // Wait for delivery report
        e := <-deliveryChan
        m := e.(*kafka.Message)
        if m.TopicPartition.Error != nil {
            log.Printf("Delivery failed: %v\n", m.TopicPartition)
        } else {
            log.Printf("Delivered message to %v\n", m.TopicPartition)
        }

        // Check JSON validity
        if err := json.Unmarshal(payloadMessage, &map[string]interface{}{}); err != nil {
            log.Fatalf("Invalid JSON: %s", err)
        }

        // Log message details (uncomment if needed)
        // log.Printf("Sending message: %s\n", payloadMessage)

        // Check for 'IdentifiantVICR' and send another message if present
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

            // Send message to Kafka topic 'acte_metier'
            err = p.Produce(&kafka.Message{
                TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
                Value:          payloadMessageIntervention,
            }, deliveryChan)
            if err != nil {
                log.Fatalf("Failed to produce intervention message: %s", err)
            }

            // Wait for delivery report
            e := <-deliveryChan
            m := e.(*kafka.Message)
            if m.TopicPartition.Error != nil {
                log.Printf("Delivery failed: %v\n", m.TopicPartition)
            } else {
                log.Printf("Delivered intervention message to %v\n", m.TopicPartition)
            }

            // Check JSON validity
            if err := json.Unmarshal(payloadMessageIntervention, &map[string]interface{}{}); err != nil {
                log.Fatalf("Invalid JSON for intervention message: %s", err)
            }
        }

        // End timer for individual row processing
        insideLoopEndTimer := time.Now()
        insideProcessingTime := insideLoopEndTimer.Sub(insideLoopStartTimer).Seconds()
        insideProcessingTimesArray = append(insideProcessingTimesArray, insideProcessingTime)

        // Limit number of messages sent
        if len(insideProcessingTimesArray) >= 5000 {
            break
        }
    }

    // End timer for overall processing
    outsideLoopEndTimer := time.Now()
    outsideProcessingTime = outsideLoopEndTimer.Sub(outsideLoopStartTimer).Seconds()

    // Write processing times to file
    writeToFile()

    // Flush messages and close producer
    p.Flush(5 * 1000)
    log.Println("Producer finished")
}

// Function to write processing times to a file
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

    // File path
    filePath := "../collectorOutputTest/processing_times.txt"

    // Prepare lines to write to file
    lines := []string{}
    for _, time := range insideProcessingTimesArray {
        lines = append(lines, fmt.Sprintf("%.6f", time))
    }
    lines = append(lines, fmt.Sprintf("Le temps moyen de traitement d'une ligne est de : %.6f secondes.", avgTime))
    lines = append(lines, displayTestDuration)
    lines = append(lines, memoryUsage)

    // Write lines to file
    err := ioutil.WriteFile(filePath, []byte(strings.Join(lines, "\n")), 0644)
    if err != nil {
        log.Fatalf("Error writing to file: %s", err)
    }
}

// Function to calculate average time from an array of times
func calculateAverage(times []float64) float64 {
    sum := 0.0
    for _, t := range times {
        sum += t
    }
    return sum / float64(len(times))
}

// Function to get memory usage
func getMemoryUsage() uint64 {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    return m.Alloc / 1024
}
