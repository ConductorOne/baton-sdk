package healthcheck

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockConnectorClient struct {
	types.ConnectorClient
	validateErr error
}

func (m *mockConnectorClient) Validate(ctx context.Context, req *v2.ConnectorServiceValidateRequest, opts ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	if m.validateErr != nil {
		return nil, m.validateErr
	}
	return &v2.ConnectorServiceValidateResponse{}, nil
}

func TestHealthHandler_Healthy(t *testing.T) {
	client := &mockConnectorClient{}
	s := &Server{
		cfg: Config{
			Enabled:     true,
			Port:        8081,
			Path:        "/health",
			BindAddress: "127.0.0.1",
		},
		clientFunc: func(ctx context.Context) (types.ConnectorClient, error) {
			return client, nil
		},
		ctx: context.Background(),
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	s.healthHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var response HealthResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	require.Equal(t, "healthy", response.Status)
}

func TestHealthHandler_Unhealthy_ClientError(t *testing.T) {
	s := &Server{
		cfg: Config{
			Enabled:     true,
			Port:        8081,
			Path:        "/health",
			BindAddress: "127.0.0.1",
		},
		clientFunc: func(ctx context.Context) (types.ConnectorClient, error) {
			return nil, errors.New("client error")
		},
		ctx: context.Background(),
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	s.healthHandler(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)

	var response HealthResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	require.Equal(t, "unhealthy", response.Status)
	require.Contains(t, response.Details["error"], "failed to get connector client")
}

func TestHealthHandler_Unhealthy_ValidationError(t *testing.T) {
	client := &mockConnectorClient{
		validateErr: errors.New("validation failed"),
	}
	s := &Server{
		cfg: Config{
			Enabled:     true,
			Port:        8081,
			Path:        "/health",
			BindAddress: "127.0.0.1",
		},
		clientFunc: func(ctx context.Context) (types.ConnectorClient, error) {
			return client, nil
		},
		ctx: context.Background(),
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	s.healthHandler(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)

	var response HealthResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	require.Equal(t, "unhealthy", response.Status)
	require.Contains(t, response.Details["error"], "connector validation failed")
	require.Contains(t, response.Details["validation_error"], "validation failed")
}

func TestReadyHandler_Ready(t *testing.T) {
	client := &mockConnectorClient{}
	s := &Server{
		cfg: Config{
			Enabled:     true,
			Port:        8081,
			Path:        "/health",
			BindAddress: "127.0.0.1",
		},
		clientFunc: func(ctx context.Context) (types.ConnectorClient, error) {
			return client, nil
		},
		ctx: context.Background(),
	}

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	s.readyHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var response HealthResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	require.Equal(t, "ready", response.Status)
}

func TestReadyHandler_NotReady(t *testing.T) {
	s := &Server{
		cfg: Config{
			Enabled:     true,
			Port:        8081,
			Path:        "/health",
			BindAddress: "127.0.0.1",
		},
		clientFunc: func(ctx context.Context) (types.ConnectorClient, error) {
			return nil, errors.New("not ready")
		},
		ctx: context.Background(),
	}

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()

	s.readyHandler(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)

	var response HealthResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	require.Equal(t, "not_ready", response.Status)
}

func TestLiveHandler(t *testing.T) {
	s := &Server{
		cfg: Config{
			Enabled:     true,
			Port:        8081,
			Path:        "/health",
			BindAddress: "127.0.0.1",
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/live", nil)
	w := httptest.NewRecorder()

	s.liveHandler(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var response HealthResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	require.Equal(t, "alive", response.Status)
}

func TestServerStartStop(t *testing.T) {
	client := &mockConnectorClient{}
	cfg := Config{
		Enabled:     true,
		Port:        0, // Use port 0 to let the OS assign an available port
		Path:        "/health",
		BindAddress: "127.0.0.1",
	}

	s := NewServer(cfg, func(ctx context.Context) (types.ConnectorClient, error) {
		return client, nil
	})

	ctx := context.Background()

	// Test double start
	err := s.Start(ctx)
	require.NoError(t, err)

	err = s.Start(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already started")

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Stop should work
	err = s.Stop(ctx)
	require.NoError(t, err)

	// Double stop should be safe
	err = s.Stop(ctx)
	require.NoError(t, err)
}

func TestNewServer(t *testing.T) {
	cfg := Config{
		Enabled:     true,
		Port:        8081,
		Path:        "/health",
		BindAddress: "127.0.0.1",
	}

	clientFunc := func(ctx context.Context) (types.ConnectorClient, error) {
		return nil, nil
	}

	s := NewServer(cfg, clientFunc)
	require.NotNil(t, s)
	require.Equal(t, cfg, s.cfg)
	require.NotNil(t, s.clientFunc)
}
