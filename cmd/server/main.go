package main

import (
	"fmt"
	"log"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
)

func main() {
	targets, err := config.LoadTargets("configs/targets.json")
	if err != nil {
		log.Fatalf("error: could not read target json: %s", err.Error())
	}
	fmt.Printf("Monitoring %d targets from file configs/targets.json\n", len(targets))
	for i, tar := range targets {
		fmt.Printf("%d: %s\n", i, tar.URL)
	}

}
