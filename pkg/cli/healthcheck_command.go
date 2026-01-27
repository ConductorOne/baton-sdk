package cli

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var validHealthCheckEndpoints = map[string]string{
	"health": "/health",
	"ready":  "/ready",
	"live":   "/live",
}

func MakeHealthCheckCommand(
	ctx context.Context,
	v *viper.Viper,
) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		err := v.BindPFlags(cmd.Flags())
		if err != nil {
			return err
		}

		// Get configuration from persistent parent flags
		port := v.GetInt("health-check-port")
		bindAddress := v.GetString("health-check-bind-address")

		// Get subcommand-specific flags
		endpoint, _ := cmd.Flags().GetString("endpoint")
		timeout, _ := cmd.Flags().GetInt("timeout")

		// Validate endpoint
		path, ok := validHealthCheckEndpoints[endpoint]
		if !ok {
			return fmt.Errorf("invalid endpoint: %s (valid: health, ready, live)", endpoint)
		}

		// Construct URL
		u := &url.URL{
			Scheme: "http",
			Host:   net.JoinHostPort(bindAddress, strconv.Itoa(port)),
			Path:   path,
		}

		// Create HTTP client using baton-sdk uhttp package
		client, err := uhttp.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("failed to create http client: %w", err)
		}
		// Override the default 5-minute timeout with our configured timeout
		client.Timeout = time.Duration(timeout) * time.Second

		// Make request
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("health check failed: %w", err)
		}
		defer resp.Body.Close()

		// Check response status
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("health check returned status %d", resp.StatusCode)
		}

		return nil
	}
}
