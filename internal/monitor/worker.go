package monitor

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
)

//Per-URL loop logic and state machine

type State int

const (
	Open State = iota
	HalfOpen
	Closed
)

const FailureThreshold int = 3
const TimeoutThreshold int = 10

type Worker struct {
	Target config.MonitorTarget
	Client *http.Client

	CurrentState State
	FailureCount int
	LastFailure  time.Time
}

func (w *Worker) RunWorker(ctx context.Context) {
	ticker := time.NewTicker(w.Target.Interval)
	w.CurrentState = Closed
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
	if w.CurrentState == Open {
		timeElapsed := time.Since(w.LastFailure)
		if timeElapsed.Seconds() >= float64(TimeoutThreshold) {
			fmt.Printf("%s cooldown finished, retrying...\n", w.Target.URL)
			w.CurrentState = HalfOpen
		} else {
			return
		}
	}

	reqCtx, cancel := context.WithTimeout(ctx, w.Target.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", w.Target.URL, nil)
	if err != nil {
		return
	}

	start := time.Now()
	resp, err := w.Client.Do(req)
	latency := time.Since(start)

	if err != nil {
		fmt.Println(err.Error())
		w.HandleFailure()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 && resp.StatusCode < 600 {
		fmt.Printf("[%s] soft failure, status code in 5xx range\n", w.Target.URL)
		w.HandleFailure()
		return
	}

	if w.CurrentState == HalfOpen {
		w.CurrentState = Closed
	}

	w.FailureCount = 0

	fmt.Printf("[%s] Status: %d in time %s\n", w.Target.URL, resp.StatusCode, latency)

	//LogResult(resp.StatusCode, latency)
}

func (w *Worker) HandleFailure() {
	w.FailureCount++
	if w.CurrentState == HalfOpen {
		w.CurrentState = Open
		fmt.Printf("[%s] retry failed, cooling down again...\n", w.Target.URL)
	} else if w.FailureCount == FailureThreshold {
		w.CurrentState = Open
		fmt.Printf("[%s] timed out 3x, cooling down...\n", w.Target.URL)
	}
	w.LastFailure = time.Now()
}
