package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "strings"
    "time"
    "context"
    "runtime"
    "sync" 

    kafka "github.com/segmentio/kafka-go"
    "github.com/khalid0306/Kafka_golang/formatter"
    "github.com/khalid0306/Kafka_golang/model"
)

const (
    ENTITY_METADATA       = "App/Entity/ActeMetier"
    INTERVENTION_METADATA = "App/Entity/Intervention"
    KAFKA_BROKER          = "172.18.0.5:9092"
)

var (
    insideProcessingTimesArray []float64
    outsideProcessingTime      float64
    wg sync.WaitGroup
)

func main() {

    log.Println("Starting Kafka Producer ...")

    topic := "acte_metier1"

    // configuration du producer Kafka
    w := kafka.NewWriter(kafka.WriterConfig{
        Brokers:  []string{KAFKA_BROKER},
        Topic:    topic,
        Balancer: &kafka.LeastBytes{},
    })
    defer w.Close()

    formatter := formatter.NewActeMetierFormatter(nil, log.New(os.Stdout, "", log.LstdFlags))

    // Commencer le timer pour le traitement global
    outsideLoopStartTimer := time.Now()

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

    errChan := make(chan error, len(rows)*2)

    for _, row := range rows {
        if row == nil {
            continue
        }

        // Commencer le timer pour le traitement de chaque ligne
        insideLoopStartTimer := time.Now()

        message := model.NewMessage(row, nil, nil)
        message.AddMetadata(entityMetadata)

        // Convertir le message en JSON
        payloadMessage, err := json.Marshal(message.ToDict())
        if err != nil {
            log.Fatalf("Error marshaling message to JSON: %s", err)
        }

        // Verifer la validité du JSON
        if err := json.Unmarshal(payloadMessage, &map[string]interface{}{}); err != nil {
            log.Fatalf("Invalid JSON: %s", err)
        }
        
        wg.Add(1) // Ajouter 1 pour chaque goroutine lancée

        // Envoyer les messages au topic 'acte_metier'
        go func(msg kafka.Message) {
            defer wg.Done() // Marquer la fin de la goroutine à la fin de son exécution

            if err := w.WriteMessages(context.Background(), msg); err != nil {
                errChan <- err
            } else {
                log.Printf("Delivered message to %v\n", w.Topic)
                errChan <- nil
            }
        }(kafka.Message{
            Key:   []byte("test-key"),
            Value: payloadMessage,
        })

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

            if err := json.Unmarshal(payloadMessageIntervention, &map[string]interface{}{}); err != nil {
                log.Fatalf("Invalid JSON for intervention message: %s", err)
            }

            wg.Add(1)

            go func(msg kafka.Message) {
                defer wg.Done()
                
                if err := w.WriteMessages(context.Background(), msg); err != nil {
                    errChan <- err
                } else {
                    log.Printf("Delivered intervention message to %v\n", w.Topic)
                    errChan <- nil
                }
            }(kafka.Message{
                Key:   []byte("test-key"),
                Value: payloadMessageIntervention,
            })    

        }

        // Fin du timer pour le traitement de chaque ligne
        insideLoopEndTimer := time.Now()
        insideProcessingTime := insideLoopEndTimer.Sub(insideLoopStartTimer).Seconds()
        insideProcessingTimesArray = append(insideProcessingTimesArray, insideProcessingTime)

        // Limite le nombre de messages envoyés pour les tests
        if len(insideProcessingTimesArray) >= 5000 {
            break
        }
    }

    go func() {
        wg.Wait() // Attendre ici que toutes les goroutines soient terminées
        close(errChan) // Fermer le canal une fois que toutes les goroutines sont terminées
    }()

    // Attendre que tous les messages soient envoyés
    for err := range errChan {
        if err != nil {
            log.Printf("Error producing message: %s", err)
        }
    }    

    log.Println("Producer shuting down... ")

    // Fin du timer pour le traitement global
    outsideLoopEndTimer := time.Now()
    outsideProcessingTime = outsideLoopEndTimer.Sub(outsideLoopStartTimer).Seconds()

    writeToFile()
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
