package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func runTest(choice string) {
	if choice == "Setup1" {
		for i := 1; i <= 5; i++ {
			// Change directory
			err := os.Chdir(filepath.Join(os.Getenv("HOME"), "Testprojet/go-kafka/collector"))
			if err != nil {
				fmt.Printf("Error changing directory: %s\n", err)
				os.Exit(1)
			}

			// Run the producer script
			cmd := exec.Command("go", "run", "producer.go")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			if err != nil {
				fmt.Printf("Error running producer script: %s\n", err)
				os.Exit(1)
			}

			// Change directory to the output directory
			err = os.Chdir(filepath.Join(os.Getenv("HOME"), "Testprojet/go-kafka/collectorOutputTest"))
			if err != nil {
				fmt.Printf("Error changing directory: %s\n", err)
				os.Exit(1)
			}

			// Rename the file
			oldName := "processing_times.txt"
			newName := fmt.Sprintf("ConfluentGoKafka_processing_times_%s_500000_%d.txt", choice, i)
			err = os.Rename(oldName, newName)
			if err != nil {
				fmt.Printf("Error renaming file: %s\n", err)
				os.Exit(1)
			}

			// Clear the terminal
			cmd = exec.Command("clear")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			if err != nil {
				fmt.Printf("Error clearing terminal: %s\n", err)
				os.Exit(1)
			}

			// Reset the terminal
			cmd = exec.Command("reset")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			if err != nil {
				fmt.Printf("Error resetting terminal: %s\n", err)
				os.Exit(1)
			}
		}
	} else {
		// Invalid input
		fmt.Println("L'option entrée n'est pas valide ou n'existe pas (l'option valide est : Setup1)")
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Vous devez indiquer le setup que vous souhaitez utiliser.")
		os.Exit(2)
	}

	setupChoice := os.Args[1]
	runTest(strings.TrimSpace(setupChoice))
}
