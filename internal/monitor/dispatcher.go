package monitor

import (
	"context"
	"sync"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
)

//Manage pool of workers and config diffing

type Dispatcher struct {
	activeWorkers map[string]context.CancelFunc
	wg            sync.WaitGroup
}

func (d *Dispatcher) StartWorker(ctx context.Context, target config.MonitorTarget) {
	workerCtx, cancel := context.WithCancel(ctx)
	d.activeWorkers[target.URL] = cancel

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		RunWorker(workerCtx, target)
	}()
}
