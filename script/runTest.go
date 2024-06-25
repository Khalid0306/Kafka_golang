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
            // Change de répertoire pour le répertoire du producer
			err := os.Chdir(filepath.Join(os.Getenv("HOME"), "Testprojet/go-kafka/collector_kafka-go"))
			if err != nil {
				fmt.Printf("Error changing directory: %s\n", err)
				os.Exit(1)
			}

			// Lance le producer
			cmd := exec.Command("go", "run", "producer_kafka-go.go")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			if err != nil {
				fmt.Printf("Error running producer script: %s\n", err)
				os.Exit(1)
			}

			// Change le repertoire pour le repertoire des fichiers de test
			err = os.Chdir(filepath.Join(os.Getenv("HOME"), "Testprojet/go-kafka/collectorOutputTest"))
			if err != nil {
				fmt.Printf("Error changing directory: %s\n", err)
				os.Exit(1)
			}

			// Change le nom du fichier
			oldName := "processing_times.txt"
			newName := fmt.Sprintf("kafka-go_processing_times_%s_5000_%d.txt", choice, i)
			err = os.Rename(oldName, newName)
			if err != nil {
				fmt.Printf("Error renaming file: %s\n", err)
				os.Exit(1)
			}

			// Nettoie le cache
			cmd = exec.Command("go", "clean", "-cache")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			if err != nil {
				fmt.Printf("Error clearing cache: %s\n", err)
				os.Exit(1)
			}

			// Nettoie le terminal
			cmd = exec.Command("clear")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			if err != nil {
				fmt.Printf("Error clearing terminal: %s\n", err)
				os.Exit(1)
			}

			// Reset le terminal
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
		// Choix invalide
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
