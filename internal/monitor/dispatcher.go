package monitor

import (
	"context"
	"net/http"
	"sync"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
	"github.com/bradapc/distributed_health_monitor.git/internal/httpclient"
)

//Manage pool of workers and config diffing

/*
Dispatcher represents an object that handles creation and management of Worker structs
*/
type Dispatcher struct {
	cancelWorkers map[string]context.CancelFunc
	activeWorkers map[string]*Worker
	wg            sync.WaitGroup
	client        *http.Client
	targets       []config.MonitorTarget
	tokens        chan struct{}
	mapMu         sync.Mutex
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
	worker := Worker{
		Target: target,
		Client: d.client,
		tokens: d.tokens,
	}

	d.mapMu.Lock()
	d.cancelWorkers[target.URL] = cancel
	d.activeWorkers[target.URL] = &worker
	d.mapMu.Unlock()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		worker.RunWorker(workerCtx)
	}()
}

func (d *Dispatcher) ReloadTargets(newTargets []config.MonitorTarget, ctx context.Context) error {

	d.mapMu.Lock()
	addedTargets, removedTargetUrls := d.diffTargets(newTargets)

	for _, url := range removedTargetUrls {
		d.cancelWorkers[url]()
		delete(d.activeWorkers, url)
	}

	d.mapMu.Unlock()
	for _, t := range addedTargets {
		d.StartWorker(ctx, t)
	}
	return nil
}

// TODO: Compare changes for interval and poll time instead of just URL
func (d *Dispatcher) diffTargets(newTargets []config.MonitorTarget) ([]config.MonitorTarget, []string) {
	addedTargets := make([]config.MonitorTarget, 0)
	removedTargets := make([]string, 0)

	newTargetsMap := make(map[string]config.MonitorTarget)

	for _, t := range newTargets {
		newTargetsMap[t.URL] = t
	}

	for url := range d.activeWorkers {
		_, ok := newTargetsMap[url]
		if !ok {
			removedTargets = append(removedTargets, url)
		}
	}

	for url, mt := range newTargetsMap {
		_, ok := d.activeWorkers[url]
		if !ok {
			addedTargets = append(addedTargets, mt)
		}
	}

	return addedTargets, removedTargets
}
