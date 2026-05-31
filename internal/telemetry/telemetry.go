package telemetry

import (
	"math"
	"sync"
	"time"
)

type Telemetry struct {
	Status             string                   `json:"status"`
	Uptime             int                      `json:"total_uptime"`
	ConcurrencySummary ConcurrencySummary       `json:"concurrency"`
	AggregateSummary   AggregateSummary         `json:"aggregate"`
	Targets            map[string]TargetSummary `json:"targets"`
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

type MetricsRegistry struct {
	mapMu              sync.RWMutex
	Targets            map[string]*TargetSummary
	ConcurrencySummary ConcurrencySummary
}

func NewMetricsRegistry() *MetricsRegistry {
	return &MetricsRegistry{
		Targets:            make(map[string]*TargetSummary),
		ConcurrencySummary: ConcurrencySummary{},
	}
}

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

func (r *MetricsRegistry) RemoveBucket(url string) {
	r.mapMu.Lock()
	defer r.mapMu.Unlock()
	delete(r.Targets, url)
}

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
					return 100 * math.Floor(float64((bucket.TimesChecked-bucket.TimesErrored))/float64(bucket.TimesChecked))
				}
				return 100
			}(),
		}

		bucket.Mu.RUnlock()

		agg.TotalChecks += snapshot[url].TimesChecked
		agg.TotalNetworkErrors += snapshot[url].TimesErrored
	}

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
	}
	return telemetry
}
