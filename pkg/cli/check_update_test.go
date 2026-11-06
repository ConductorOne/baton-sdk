package cli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckUpdate(t *testing.T) {
	tests := []struct {
		name       string
		installed  string
		stable     string
		wantStatus string
	}{
		{name: "older", installed: "v1.2.0", stable: "v1.3.0", wantStatus: "update available"},
		{name: "equal", installed: "v1.3.0", stable: "v1.3.0", wantStatus: "up to date"},
		{name: "candidate ahead of stable", installed: "v1.4.0", stable: "v1.3.0", wantStatus: "newer than stable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api/v1/connectors/ConductorOne/baton-test/summary", r.URL.Path)
				_, err := fmt.Fprintf(w, `{"connector":{"stable":%q}}`, tt.stable)
				require.NoError(t, err)
			}))
			defer server.Close()

			cmd := NewCheckUpdateCommand(context.Background(), "baton-test", tt.installed)
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetArgs([]string{"--registry-url", server.URL})
			require.NoError(t, cmd.Execute())
			require.Equal(t, fmt.Sprintf("Installed: %s\nStable: %s\nStatus: %s\n", tt.installed, tt.stable, tt.wantStatus), output.String())
		})
	}
}

func TestCheckUpdateUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte(`{"connector":{"stable":""}}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	cmd := NewCheckUpdateCommand(context.Background(), "baton-test", "v1.2.0")
	cmd.SetArgs([]string{"--registry-url", server.URL})
	require.ErrorContains(t, cmd.Execute(), "no stable release")

	cmd = NewCheckUpdateCommand(context.Background(), "baton-test", "dev")
	cmd.SetArgs([]string{"--registry-url", server.URL})
	require.ErrorContains(t, cmd.Execute(), "installed version")
}
