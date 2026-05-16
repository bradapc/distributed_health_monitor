package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
	"github.com/bradapc/distributed_health_monitor.git/internal/monitor"
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
	dispatcher := monitor.NewDispatcher(targets)
	ctx := context.Background()

	for _, t := range targets {
		dispatcher.StartWorker(ctx, t)
	}

	poller := monitor.NewPoller("configs/targets.json", 1*time.Second)
	poller.RunPoller(ctx)

	select {}
}
