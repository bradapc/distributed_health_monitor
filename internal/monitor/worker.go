package monitor

import (
	"context"
	"net/http"
	"time"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
	"github.com/bradapc/distributed_health_monitor.git/internal/logger"
	"github.com/bradapc/distributed_health_monitor.git/internal/telemetry"
)

//Per-URL loop logic and state machine

type State int

const (
	Open State = iota
	HalfOpen
	Closed
)

// Number of failure attempts before cooling down
const FailureThreshold int = 3

// Cooldown time after FailureThreshold attempts are reached
const TimeoutThreshold int = 60

// Worker monitors a URL target's health and tracks failures
type Worker struct {
	Target config.MonitorTarget
	Client *http.Client

	CurrentState State
	FailureCount int
	LastFailure  time.Time

	tokens chan struct{}

	logger  *logger.Logger
	metrics *telemetry.TargetSummary

	recordEvent func(telemetry.TargetEvent)
}

// Core work loop for a worker where health checks are performed at regular interval until termination
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

// Creates and fires the http request to monitor a URL
func (w *Worker) performCheck(ctx context.Context) {
	if w.CurrentState == Open {
		timeElapsed := time.Since(w.LastFailure)
		if timeElapsed.Seconds() >= float64(TimeoutThreshold) {
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

	w.tokens <- struct{}{}
	defer func() {
		<-w.tokens
	}()
	start := time.Now()
	resp, err := w.Client.Do(req)
	latency := time.Since(start).Milliseconds()

	if err != nil {
		w.logger.LogErrorFailure("worker_execution_error", latency, w.FailureCount+1, w.Target.URL, err.Error())
		w.HandleFailure(err.Error())
		w.updateMetrics(latency, true)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 && resp.StatusCode < 600 {
		w.logger.LogNonErrorFailure("worker_execution_failed", resp.StatusCode, latency, w.FailureCount+1, w.Target.URL)
		w.HandleFailure("Error: Network code within 500-600 range")
		w.updateMetrics(latency, true)
		return
	}

	if w.CurrentState == HalfOpen {
		w.CurrentState = Closed
	}

	w.FailureCount = 0
	w.updateMetrics(latency, false)
	w.logger.LogMessage("target_check_success", resp.StatusCode, latency, w.Target.URL)
}

// Finite state machine for handling failure depending on stage in the FSM
func (w *Worker) HandleFailure(err string) {
	w.FailureCount++
	oldState := w.CurrentState
	if w.CurrentState == HalfOpen {
		w.CurrentState = Open
	} else if w.FailureCount == FailureThreshold {
		w.CurrentState = Open
	}
	w.recordEvent(telemetry.TargetEvent{
		URL:          w.Target.URL,
		OldState:     int(oldState),
		NewState:     int(w.CurrentState),
		NetworkError: err,
	})
	w.LastFailure = time.Now()
}

func (w *Worker) updateMetrics(latency int64, isError bool) {
	w.metrics.Mu.Lock()
	defer w.metrics.Mu.Unlock()
	w.metrics.State = int(w.CurrentState)
	w.metrics.FailureCount = w.FailureCount
	w.metrics.LastChecked = time.Now()
	w.metrics.LastCheckLatency = latency
	w.metrics.TimesChecked++
	if isError {
		w.metrics.TimesErrored++
	}
}
