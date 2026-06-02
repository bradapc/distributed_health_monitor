package telemetry

import (
	"sync"
	"time"
)

type Telemetry struct {
	Status             string                   `json:"status"`
	Uptime             int                      `json:"total_uptime"`
	ConcurrencySummary ConcurrencySummary       `json:"concurrency"`
	AggregateSummary   AggregateSummary         `json:"aggregate"`
	Targets            map[string]TargetSummary `json:"targets"`
	RecentEventLog     RecentEventLog           `json:"event_log"`
}

type ConcurrencySummary struct {
	ActiveWorkers   int `json:"active_workers"`
	MaxWorkerLimit  int `json:"max_worker_limit"`
	AvailableTokens int `json:"available_tokens"`
}

type AggregateSummary struct {
	TotalChecks        int `json:"total_checks_performed"`
	TotalNetworkErrors int `json:"total_network_errors"`
}

type TargetSummary struct {
	Mu               sync.RWMutex
	State            int       `json:"state"`
	FailureCount     int       `json:"failure_count"`
	LastCheckLatency int64     `json:"last_check_latency_ms"`
	LastChecked      time.Time `json:"last_checked"`
	TimesChecked     int       `json:"times_checked"`
	TimesErrored     int       `json:"times_errored"`
	PercentSuccess   float64   `json:"percent_success"`
}

type TargetEvent struct {
	URL          string    `json:"affected_url"`
	OldState     int       `json:"old_state"`
	NewState     int       `json:"new_state"`
	NetworkError string    `json:"network_error"`
	FailureTime  time.Time `json:"failure_time"`
}

type RecentEventLog []TargetEvent

type MetricsRegistry struct {
	mapMu              sync.RWMutex
	Targets            map[string]*TargetSummary
	ConcurrencySummary ConcurrencySummary

	elMu           sync.RWMutex
	RecentEventLog RecentEventLog

	telMu          sync.RWMutex
	TargetEventLog map[string][]TargetEvent
}

// NewMetricsRegistry creates a metrics registry
func NewMetricsRegistry() *MetricsRegistry {
	return &MetricsRegistry{
		Targets:            make(map[string]*TargetSummary),
		ConcurrencySummary: ConcurrencySummary{},
		RecentEventLog:     make(RecentEventLog, 0, 50),
		TargetEventLog:     make(map[string][]TargetEvent),
	}
}

func (r *MetricsRegistry) GetTargetEventSummary(url string) []TargetEvent {
	r.telMu.RLock()
	defer r.telMu.RUnlock()
	return r.TargetEventLog[url]
}

// RecordEvent atomically adds an event to the event log, removing the oldest event if it is full
// Also adds the event to a url specific bucket
func (r *MetricsRegistry) RecordEvent(te TargetEvent) {
	r.elMu.Lock()
	if len(r.RecentEventLog) == 50 {
		r.RecentEventLog = r.RecentEventLog[1:]
	}
	r.RecentEventLog = append(r.RecentEventLog, te)
	r.elMu.Unlock()
	r.telMu.Lock()
	defer r.telMu.Unlock()
	eventBucket, ok := r.TargetEventLog[te.URL]
	if !ok {
		r.TargetEventLog[te.URL] = make([]TargetEvent, 0, 10)
	}
	r.TargetEventLog[te.URL] = append(eventBucket, te)
}

// GetOrCreateBucket atomically creates or returns a TargetSummary for metrics gathering for a specified target
func (r *MetricsRegistry) GetOrCreateBucket(url string) *TargetSummary {
	r.mapMu.Lock()
	defer r.mapMu.Unlock()

	if bucket, ok := r.Targets[url]; ok {
		return bucket
	}

	newBucket := &TargetSummary{}
	r.Targets[url] = newBucket
	return newBucket
}

// RemoveBucket atomically deletes a TargetSummary for when a target is no longer actively monitored
func (r *MetricsRegistry) RemoveBucket(url string) {
	r.mapMu.Lock()
	defer r.mapMu.Unlock()
	delete(r.Targets, url)
}

// GetSnapshot returns a snapshot of the current system state for all targets monitored
func (r *MetricsRegistry) GetSnapshot(tokenChan chan struct{}) *Telemetry {
	r.mapMu.RLock()
	defer r.mapMu.RUnlock()

	snapshot := make(map[string]TargetSummary)

	agg := AggregateSummary{}

	for url, bucket := range r.Targets {
		bucket.Mu.RLock()
		snapshot[url] = TargetSummary{
			State:            bucket.State,
			FailureCount:     bucket.FailureCount,
			LastCheckLatency: bucket.LastCheckLatency,
			LastChecked:      bucket.LastChecked,
			TimesChecked:     bucket.TimesChecked,
			TimesErrored:     bucket.TimesErrored,
			PercentSuccess: func() float64 {
				if bucket.TimesChecked > 0 {
					return float64((bucket.TimesChecked-bucket.TimesErrored)*100) / float64(bucket.TimesChecked)
				}
				return 0
			}(),
		}

		bucket.Mu.RUnlock()

		agg.TotalChecks += snapshot[url].TimesChecked
		agg.TotalNetworkErrors += snapshot[url].TimesErrored
	}

	r.elMu.RLock()
	eventsCopy := make(RecentEventLog, len(r.RecentEventLog))
	copy(eventsCopy, r.RecentEventLog)
	r.elMu.RUnlock()

	telemetry := &Telemetry{
		Status: "active",
		Uptime: -1,
		ConcurrencySummary: ConcurrencySummary{
			ActiveWorkers:   r.ConcurrencySummary.ActiveWorkers,
			MaxWorkerLimit:  int(cap(tokenChan)),
			AvailableTokens: int(cap(tokenChan) - len(tokenChan)),
		},
		AggregateSummary: agg,
		Targets:          snapshot,
		RecentEventLog:   eventsCopy,
	}
	return telemetry
}
