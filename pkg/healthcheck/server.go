package healthcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types"
)

const (
	defaultHealthCheckTimeout = 30 * time.Second
	shutdownTimeout           = 5 * time.Second
)

// Config holds the configuration for the health check server.
type Config struct {
	Enabled     bool
	Port        int
	BindAddress string
}

// HealthResponse represents the JSON response for health check endpoints.
type HealthResponse struct {
	Status    string            `json:"status"`
	Timestamp string            `json:"timestamp"`
	Details   map[string]string `json:"details,omitempty"`
}

// ClientFunc is a function that returns a ConnectorClient.
type ClientFunc func(context.Context) (types.ConnectorClient, error)

// Server manages the HTTP health check server lifecycle.
type Server struct {
	cfg        Config
	clientFunc ClientFunc
	server     *http.Server
	mu         sync.Mutex
	started    bool
	ctx        context.Context
}

// NewServer creates a new health check server.
func NewServer(cfg Config, clientFunc ClientFunc) *Server {
	return &Server{
		cfg:        cfg,
		clientFunc: clientFunc,
	}
}

// Start starts the HTTP health check server.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("health check server already started")
	}

	s.ctx = ctx
	l := ctxzap.Extract(ctx)

	mux := http.NewServeMux()

	// Register health check endpoints
	mux.HandleFunc("/health", s.healthHandler)
	mux.HandleFunc("/ready", s.readyHandler)
	mux.HandleFunc("/live", s.liveHandler)

	addr := net.JoinHostPort(s.cfg.BindAddress, strconv.Itoa(s.cfg.Port))
	lc := &net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to create health check listener: %w", err)
	}

	s.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	s.started = true

	go func() {
		l.Info("health check server starting", zap.String("address", addr))
		if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			l.Error("health check server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully shuts down the health check server.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started || s.server == nil {
		return nil
	}

	l := ctxzap.Extract(ctx)
	l.Info("stopping health check server")

	shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()

	if err := s.server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("failed to shutdown health check server: %w", err)
	}

	s.started = false
	return nil
}

// healthHandler handles the /health endpoint.
// It calls Validate() on the connector and returns the health status.
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	ctx := s.ctx
	if ctx == nil {
		ctx = r.Context()
	}
	l := ctxzap.Extract(ctx)

	response := HealthResponse{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Details:   make(map[string]string),
	}

	// Get the connector client
	client, err := s.clientFunc(ctx)
	if err != nil {
		l.Warn("health check failed: could not get connector client", zap.Error(err))
		response.Status = "unhealthy"
		response.Details["error"] = "failed to get connector client"
		s.writeJSON(w, http.StatusServiceUnavailable, response)
		return
	}

	// Call Validate() on the connector with a timeout
	validateCtx, cancel := context.WithTimeout(ctx, defaultHealthCheckTimeout)
	defer cancel()

	_, err = client.Validate(validateCtx, &v2.ConnectorServiceValidateRequest{})
	if err != nil {
		l.Warn("health check failed: connector validation failed", zap.Error(err))
		response.Status = "unhealthy"
		response.Details["error"] = "connector validation failed"
		response.Details["validation_error"] = err.Error()
		s.writeJSON(w, http.StatusServiceUnavailable, response)
		return
	}

	response.Status = "healthy"
	s.writeJSON(w, http.StatusOK, response)
}

// readyHandler handles the /ready endpoint.
// It checks if the connector client can be obtained (ready to serve).
func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	ctx := s.ctx
	if ctx == nil {
		ctx = r.Context()
	}
	l := ctxzap.Extract(ctx)

	response := HealthResponse{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Details:   make(map[string]string),
	}

	// Try to get the connector client
	_, err := s.clientFunc(ctx)
	if err != nil {
		l.Warn("readiness check failed: could not get connector client", zap.Error(err))
		response.Status = "not_ready"
		response.Details["error"] = "failed to get connector client"
		s.writeJSON(w, http.StatusServiceUnavailable, response)
		return
	}

	response.Status = "ready"
	s.writeJSON(w, http.StatusOK, response)
}

// liveHandler handles the /live endpoint.
// It always returns HTTP 200 to indicate the process is alive.
func (s *Server) liveHandler(w http.ResponseWriter, _ *http.Request) {
	response := HealthResponse{
		Status:    "alive",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	s.writeJSON(w, http.StatusOK, response)
}

// writeJSON writes a JSON response with the given status code.
func (s *Server) writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		// If encoding fails, we can't do much about it
		return
	}
}
