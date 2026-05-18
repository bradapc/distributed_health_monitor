package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/bradapc/distributed_health_monitor.git/internal/telemetry"
)

type Server struct {
	registry  *telemetry.MetricsRegistry
	tokenChan chan struct{}
	startTime time.Time
}

func NewServer(reg *telemetry.MetricsRegistry, tokenChan chan struct{}) *Server {
	return &Server{
		registry:  reg,
		tokenChan: tokenChan,
		startTime: time.Now(),
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return mux
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snapshot := s.registry.GetSnapshot(s.tokenChan)
	snapshot.Uptime = int(time.Since(s.startTime).Seconds())
	if snapshot.ConcurrencySummary.ActiveWorkers > 0 {
		snapshot.Status = "healthy"
	} else {
		snapshot.Status = "idle"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(snapshot); err != nil {
		http.Error(w, "internal server error formatting json", http.StatusInternalServerError)
	}
}
