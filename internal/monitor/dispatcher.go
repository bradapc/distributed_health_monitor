package monitor

import (
	"context"
	"net/http"
	"sync"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
	"github.com/bradapc/distributed_health_monitor.git/internal/httpclient"
)

//Manage pool of workers and config diffing

// Dispatcher represents an object that handles creation and management of Worker structs
type Dispatcher struct {
	cancelWorkers map[string]context.CancelFunc
	activeWorkers map[string]*Worker
	wg            sync.WaitGroup
	client        *http.Client
	targets       []config.MonitorTarget
	tokens        chan struct{}
}

const WorkerPoolLimit int = 50

// Creates a new dispatcher with the specified slice of targets to monitor
func NewDispatcher(targets []config.MonitorTarget) *Dispatcher {
	return &Dispatcher{
		cancelWorkers: make(map[string]context.CancelFunc),
		activeWorkers: make(map[string]*Worker),
		targets:       targets,
		client:        httpclient.NewHTTPClient(),
		tokens:        make(chan struct{}, WorkerPoolLimit),
	}
}

/*
	Starts a Worker and assigns a MonitorTarget to the Worker. This function handles

creation of workers from the Dispatcher view.
*/
func (d *Dispatcher) StartWorker(ctx context.Context, target config.MonitorTarget) {
	workerCtx, cancel := context.WithCancel(ctx)
	d.cancelWorkers[target.URL] = cancel
	worker := Worker{
		Target: target,
		Client: d.client,
		tokens: d.tokens,
	}
	d.activeWorkers[target.URL] = &worker

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		worker.RunWorker(workerCtx)
	}()
}
