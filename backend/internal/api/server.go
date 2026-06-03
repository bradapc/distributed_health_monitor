package api

import (
	"encoding/base64"
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

// NewServer creates a new api server with an associated MetricsRegistry
func NewServer(reg *telemetry.MetricsRegistry, tokenChan chan struct{}) *Server {
	return &Server{
		registry:  reg,
		tokenChan: tokenChan,
		startTime: time.Now(),
	}
}

// Routes returns a mux handling all server routes with CORS enabled
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("GET /stream", s.handleEventStream)
	mux.HandleFunc("GET /config", s.handleGetConfig)
	mux.HandleFunc("POST /config", s.handlePostConfig)
	mux.HandleFunc("GET /targets/{id}", s.handleGetTargetEvents)
	return enableCORS(mux)
}

func (s *Server) handleGetTargetEvents(w http.ResponseWriter, r *http.Request) {
	targetBase64 := r.PathValue("id")
	targetUrlBytes, err := base64.StdEncoding.DecodeString(targetBase64)
	if err != nil {
		http.Error(w, "error decoding target url", http.StatusBadRequest)
		return
	}
	targetUrlStr := string(targetUrlBytes)
	payload := s.registry.GetTargetEventSummary(targetUrlStr)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		http.Error(w, "error encoding target event summary", http.StatusInternalServerError)
		return
	}
}

// handlePostConfig handles the POST /config endpoint. It reads the request payload and delegates file replacement of the target json file with the request payload.
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

// handleGetConfig handles GET /config which returns the contents of the target json file.
func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	http.ServeFile(w, r, "configs/targets.json")
}

// handleEventStream handles GET /config which continuously streams snapshots of the system's current state to the user via SSE. The endpoint sends snapshots on a 1s interval.
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

// handleMetrics handles GET /metrics which sends a snapshot via REST of the current system state
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snapshot := s.getSnapshotPayload()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(snapshot); err != nil {
		http.Error(w, "internal server error formatting json", http.StatusInternalServerError)
	}
}

// getSnapshotPayload gets a snapshot of the system state and appends the uptime and system status
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

// enableCORS middleware configures the http handler to accept requests from the frontend
func enableCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost")

		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
