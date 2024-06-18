package main

import (
    "github.com/khalid0306/Kafka_golang/collector" 
)

func main() {
    go collector.ProduceMessage()
    // Prevent the main function from exiting immediately
    select {}
}