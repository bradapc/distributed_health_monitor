package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bradapc/distributed_health_monitor.git/internal/config"
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
	mux.HandleFunc("GET /stream", s.handleEventStream)
	mux.HandleFunc("GET /config", s.handleGetConfig)
	mux.HandleFunc("POST /config", s.handlePostConfig)
	return enableCORS(mux)
}

func (s *Server) handlePostConfig(w http.ResponseWriter, r *http.Request) {
	targetsPayload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	err = config.ReplaceFile(targetsPayload)
	if err != nil {
		http.Error(w, "Failed to replace targets", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	http.ServeFile(w, r, "configs/targets.json")
}

func (s *Server) handleEventStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	clientGone := r.Context().Done()

	fmt.Println("Establishing connection with client ", r.RemoteAddr)

	rc := http.NewResponseController(w)
	_ = rc.Flush()

	err := rc.SetWriteDeadline(time.Time{})
	if err != nil {
		fmt.Println("Error removing write deadline optimization:", err)
	}

	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-clientGone:
			fmt.Println("Client disconnected: ", r.RemoteAddr)
			return
		case <-t.C:
			snapshot := s.getSnapshotPayload()
			payloadBytes, err := json.Marshal(snapshot)
			if err != nil {
				fmt.Println("JSON marshal error:", err)
				continue
			}
			_, err = fmt.Fprintf(w, "data: %s\n\n", string(payloadBytes))
			if err != nil {
				fmt.Println("write error:", err)
				return
			}
			flushErr := rc.Flush()
			if flushErr != nil {
				fmt.Println("flush error: ", flushErr)
				continue
			}
		}
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snapshot := s.getSnapshotPayload()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(snapshot); err != nil {
		http.Error(w, "internal server error formatting json", http.StatusInternalServerError)
	}
}

func (s *Server) getSnapshotPayload() *telemetry.Telemetry {
	snapshot := s.registry.GetSnapshot(s.tokenChan)
	snapshot.Uptime = int(time.Since(s.startTime).Seconds())
	if snapshot.ConcurrencySummary.ActiveWorkers > 0 {
		snapshot.Status = "healthy"
	} else {
		snapshot.Status = "idle"
	}
	return snapshot
}

func enableCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:5173")

		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
