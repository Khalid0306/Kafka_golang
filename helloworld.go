package main

import (
	"./collector"
	"./stocker"
)

func main() {
	go collector.produceMessage()

	// Prevent the main function from exiting immediately
	select {}
}
