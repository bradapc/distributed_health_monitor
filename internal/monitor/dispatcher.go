package monitor

import (
	"context"
	"net/http"
	"sync"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
	"github.com/bradapc/distributed_health_monitor.git/internal/httpclient"
)

//Manage pool of workers and config diffing

type Dispatcher struct {
	cancelWorkers map[string]context.CancelFunc
	activeWorkers map[string]*Worker
	wg            sync.WaitGroup
	client        *http.Client
	targets       []config.MonitorTarget
}

func NewDispatcher(targets []config.MonitorTarget) *Dispatcher {
	return &Dispatcher{
		cancelWorkers: make(map[string]context.CancelFunc),
		activeWorkers: make(map[string]*Worker),
		targets:       targets,
		client:        httpclient.NewHTTPClient(),
	}
}

func (d *Dispatcher) StartWorker(ctx context.Context, target config.MonitorTarget) {
	workerCtx, cancel := context.WithCancel(ctx)
	d.cancelWorkers[target.URL] = cancel
	worker := Worker{
		Target: target,
		Client: d.client,
	}
	d.activeWorkers[target.URL] = &worker

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		worker.RunWorker(workerCtx)
	}()
}
