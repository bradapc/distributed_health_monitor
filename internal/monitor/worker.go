package monitor

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
)

//Per-URL loop logic and state machine

type Worker struct {
	Target       config.MonitorTarget
	Client       *http.Client
	FailureCount int
	LastFailure  time.Time
}

func (w *Worker) RunWorker(ctx context.Context) {
	ticker := time.NewTicker(w.Target.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.performCheck(ctx)
		}
	}
}

func (w *Worker) performCheck(ctx context.Context) {
	reqCtx, cancel := context.WithTimeout(ctx, w.Target.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", w.Target.URL, nil)
	if err != nil {
		return
	}

	//start := time.Now()
	resp, err := w.Client.Do(req)
	//latency := time.Since(start)

	if err != nil {
		//HandleError(err)
		return
	}

	defer resp.Body.Close()
	fmt.Printf("[%s] Status: %d\n", w.Target.URL, resp.StatusCode)

	//LogResult(resp.StatusCode, latency)
}
