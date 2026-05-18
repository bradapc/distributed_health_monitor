package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
	"github.com/bradapc/distributed_health_monitor.git/internal/logger"
	"github.com/bradapc/distributed_health_monitor.git/internal/monitor"
	"github.com/bradapc/distributed_health_monitor.git/internal/telemetry"
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

	logger, cleanup, err := logger.NewLogger("log.jsonl")
	if err != nil {
		log.Fatalf("fatal error in main: %s", err.Error())
	}
	defer cleanup()

	dispatcher := monitor.NewDispatcher(targets, logger, telemetry.NewMetricsRegistry())
	ctx := context.Background()

	for _, t := range targets {
		dispatcher.StartWorker(ctx, t)
	}

	poller := monitor.NewPoller("configs/targets.json", 1*time.Second, dispatcher)
	go poller.RunPoller(ctx)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	sig := <-sigChan

	fmt.Printf("%s signal received, stopping workers...\n", sig.String())
	dispatcher.Stop()

	fmt.Println("System clean exit successful.")
}

/*
Suggestions to Add:
Status tracking via api (current active workers, total network errors, token pool, responce latency data)
*/
