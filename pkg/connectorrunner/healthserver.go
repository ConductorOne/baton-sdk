package connectorrunner

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

type HealthServer struct {
	server *http.Server
}

type LivenessResponse struct {
	Status string `json:"status"`
}

type ReadinessResponse struct {
	Status string `json:"status"`
}

type HealthResponse struct {
	Status string `json:"status"`
	// Tasks []tasks.Task `json:"tasks"`
}

func NewHealthServer(ctx context.Context, addr string, cw types.ClientWrapper, tasks tasks.Manager, cfg *runnerConfig) (*HealthServer, error) {
	server := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: time.Duration(10) * time.Second,
	}

	// TODO: Use a goroutine so we don't block forever in server.Serve()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		hr := HealthResponse{
			Status: "ok",
		}
		body, err := json.MarshalIndent(hr, "", "  ")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, err = w.Write(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	err = server.Serve(l)
	if err != nil {
		return nil, err
	}

	return &HealthServer{
		server: server,
	}, nil
}

func (hs *HealthServer) Close(ctx context.Context) error {
	err := hs.server.Shutdown(ctx)
	return err
}
