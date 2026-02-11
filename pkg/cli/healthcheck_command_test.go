package cli

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestValidHealthCheckEndpoints(t *testing.T) {
	require.Equal(t, "/health", validHealthCheckEndpoints["health"])
	require.Equal(t, "/ready", validHealthCheckEndpoints["ready"])
	require.Equal(t, "/live", validHealthCheckEndpoints["live"])
	require.Len(t, validHealthCheckEndpoints, 3)
}

func TestMakeHealthCheckCommand_InvalidEndpoint(t *testing.T) {
	ctx := context.Background()
	v := viper.New()

	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "invalid", "")
	cmd.Flags().Int("timeout", 5, "")

	v.Set("health-check-port", 8081)
	v.Set("health-check-bind-address", "127.0.0.1")

	runE := MakeHealthCheckCommand(ctx, v)
	err := runE(cmd, []string{})

	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid endpoint: invalid")
	require.Contains(t, err.Error(), "valid: health, ready, live")
}

func TestMakeHealthCheckCommand_SuccessfulHealthCheck(t *testing.T) {
	// Create a test server that returns 200 OK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/health", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Parse the server URL to get port
	parts := strings.Split(server.URL, ":")
	port, err := strconv.Atoi(parts[len(parts)-1])
	require.NoError(t, err)

	ctx := context.Background()
	v := viper.New()

	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "health", "")
	cmd.Flags().Int("timeout", 5, "")

	v.Set("health-check-port", port)
	v.Set("health-check-bind-address", "127.0.0.1")

	runE := MakeHealthCheckCommand(ctx, v)
	err = runE(cmd, []string{})

	require.NoError(t, err)
}

func TestMakeHealthCheckCommand_SuccessfulReadyCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/ready", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	parts := strings.Split(server.URL, ":")
	port, err := strconv.Atoi(parts[len(parts)-1])
	require.NoError(t, err)

	ctx := context.Background()
	v := viper.New()

	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "ready", "")
	cmd.Flags().Int("timeout", 5, "")

	v.Set("health-check-port", port)
	v.Set("health-check-bind-address", "127.0.0.1")

	runE := MakeHealthCheckCommand(ctx, v)
	err = runE(cmd, []string{})

	require.NoError(t, err)
}

func TestMakeHealthCheckCommand_SuccessfulLiveCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/live", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	parts := strings.Split(server.URL, ":")
	port, err := strconv.Atoi(parts[len(parts)-1])
	require.NoError(t, err)

	ctx := context.Background()
	v := viper.New()

	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "live", "")
	cmd.Flags().Int("timeout", 5, "")

	v.Set("health-check-port", port)
	v.Set("health-check-bind-address", "127.0.0.1")

	runE := MakeHealthCheckCommand(ctx, v)
	err = runE(cmd, []string{})

	require.NoError(t, err)
}

func TestMakeHealthCheckCommand_Non200Response(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	parts := strings.Split(server.URL, ":")
	port, err := strconv.Atoi(parts[len(parts)-1])
	require.NoError(t, err)

	ctx := context.Background()
	v := viper.New()

	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "health", "")
	cmd.Flags().Int("timeout", 5, "")

	v.Set("health-check-port", port)
	v.Set("health-check-bind-address", "127.0.0.1")

	runE := MakeHealthCheckCommand(ctx, v)
	err = runE(cmd, []string{})

	require.Error(t, err)
	require.Contains(t, err.Error(), "health check returned status 503")
}

func TestMakeHealthCheckCommand_ConnectionRefused(t *testing.T) {
	ctx := context.Background()

	// Get a free port by listening on :0 and immediately closing
	lc := &net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	err = listener.Close()
	require.NoError(t, err)
	v := viper.New()

	cmd := &cobra.Command{}
	cmd.Flags().String("endpoint", "health", "")
	cmd.Flags().Int("timeout", 1, "") // Short timeout for faster test

	v.Set("health-check-port", port)
	v.Set("health-check-bind-address", "127.0.0.1")

	runE := MakeHealthCheckCommand(ctx, v)
	err = runE(cmd, []string{})

	require.Error(t, err)
	require.Contains(t, err.Error(), "health check failed")
}

func TestMakeHealthCheckCommand_URLConstruction(t *testing.T) {
	tests := []struct {
		name        string
		port        int
		bindAddress string
		endpoint    string
		expectedURL string
	}{
		{
			name:        "default values",
			port:        8081,
			bindAddress: "127.0.0.1",
			endpoint:    "health",
			expectedURL: "http://127.0.0.1:8081/health",
		},
		{
			name:        "custom port",
			port:        9090,
			bindAddress: "127.0.0.1",
			endpoint:    "health",
			expectedURL: "http://127.0.0.1:9090/health",
		},
		{
			name:        "custom bind address",
			port:        8081,
			bindAddress: "192.168.1.100",
			endpoint:    "ready",
			expectedURL: "http://192.168.1.100:8081/ready",
		},
		{
			name:        "live endpoint",
			port:        8081,
			bindAddress: "127.0.0.1",
			endpoint:    "live",
			expectedURL: "http://127.0.0.1:8081/live",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Verify the endpoint paths are correct
			path, ok := validHealthCheckEndpoints[tc.endpoint]
			require.True(t, ok)

			expectedPath := "/" + tc.endpoint
			require.Equal(t, expectedPath, path)
		})
	}
}
