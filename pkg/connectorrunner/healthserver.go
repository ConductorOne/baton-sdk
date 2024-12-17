package connectorrunner

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/tasks/c1api"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type HealthServer struct {
	server *http.Server
}

type ReadinessResponse struct {
	Status string `json:"status"`
}

type HealthResponse struct {
	Status string `json:"status"`
	// Tasks []tasks.Task `json:"tasks"`
	Config       runnerConfig     `json:"config"`
	HealthInfo   tasks.HealthInfo `json:"health_info"`
	CurrentTasks []*v1.Task       `json:"current_tasks"`
}

func NewHealthServer(ctx context.Context, addr string, runner *connectorRunner, cfg *runnerConfig) (*HealthServer, error) {
	server := &http.Server{
		Addr:              addr,
		ReadHeaderTimeout: time.Duration(10) * time.Second,
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		// TODO: check runner to see if it's ready before returning 200 OK
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/info", func(w http.ResponseWriter, r *http.Request) {
		c1ApiTaskManager, ok := runner.taskManager.(*c1api.C1ApiTaskManager)
		var healthInfo tasks.HealthInfo
		if ok {
			healthInfo = c1ApiTaskManager.GetHealthInfo(ctx)
		}
		currentTasks := make([]*v1.Task, 0)
		for _, task := range runner.tasks {
			currentTasks = append(currentTasks, task)
		}

		hr := HealthResponse{
			Status:       "ok",
			Config:       *cfg,
			HealthInfo:   healthInfo,
			CurrentTasks: currentTasks,
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

	return &HealthServer{
		server: server,
	}, nil
}

func (hs *HealthServer) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", hs.server.Addr)
	if err != nil {
		return err
	}
	l := ctxzap.Extract(ctx)

	// Use a goroutine so we don't block forever in server.Serve()
	go func() {
		err = hs.server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			l.Info("Health server closed")
			return
		}
		if err != nil {
			l.Error("Error serving health server", zap.Error(err))
			return
		}
	}()
	go func() {
		<-ctx.Done()
		_ = hs.Close(context.Background())
	}()
	return nil
}

func (hs *HealthServer) Close(ctx context.Context) error {
	err := hs.server.Shutdown(ctx)
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return err
}
