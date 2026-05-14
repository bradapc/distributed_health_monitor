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
	activeWorkers map[string]context.CancelFunc
	wg            sync.WaitGroup
	client        *http.Client
	targets       []config.MonitorTarget
}

func NewDispatcher(targets []config.MonitorTarget) *Dispatcher {
	return &Dispatcher{
		activeWorkers: make(map[string]context.CancelFunc),
		targets:       targets,
		client:        httpclient.NewHTTPClient(),
	}
}

func (d *Dispatcher) StartWorker(ctx context.Context, target config.MonitorTarget) {
	workerCtx, cancel := context.WithCancel(ctx)
	d.activeWorkers[target.URL] = cancel

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		RunWorker(workerCtx, target, d.client)
	}()
}
