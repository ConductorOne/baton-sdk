//go:build baton_lambda_support

package cli

import (
	"testing"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLambdaLogLevelConfigExpiresDebug(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	v := viper.New()
	v.Set("log-level", "debug")
	v.Set("log-level-debug-expires-at", now.Add(-time.Minute).Format(time.RFC3339))

	config, err := lambdaLogLevelConfigFromViper(v)
	if err != nil {
		t.Fatalf("lambdaLogLevelConfigFromViper: %v", err)
	}
	if got := config.effective(now); got != "info" {
		t.Fatalf("effective log level = %q, want info", got)
	}
}

func TestLambdaLogLevelConfigKeepsUnexpiredDebug(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	v := viper.New()
	v.Set("log-level", "debug")
	v.Set("log-level-debug-expires-at", now.Add(time.Minute).Format(time.RFC3339))

	config, err := lambdaLogLevelConfigFromViper(v)
	if err != nil {
		t.Fatalf("lambdaLogLevelConfigFromViper: %v", err)
	}
	if got := config.effective(now); got != "debug" {
		t.Fatalf("effective log level = %q, want debug", got)
	}
}

func TestLambdaLogLevelConfigRejectsInvalidLevel(t *testing.T) {
	t.Parallel()

	v := viper.New()
	v.Set("log-level", "verbose")

	if _, err := lambdaLogLevelConfigFromViper(v); err == nil {
		t.Fatal("expected invalid log level to fail")
	}
}

func TestLambdaConnectorConfigVersionPrefersOpaqueVersion(t *testing.T) {
	t.Parallel()

	lastUpdated := time.Date(2026, 5, 19, 15, 0, 0, 123, time.UTC)
	config := &v1.GetConnectorConfigResponse{
		LastUpdated: timestamppb.New(lastUpdated),
		Version:     "runtime=7;deployment=dep-1:sha256:abc;config=2026-05-19T15:00:00Z",
	}

	if got := lambdaConnectorConfigVersion(config); got != config.Version {
		t.Fatalf("lambdaConnectorConfigVersion = %q, want %q", got, config.Version)
	}
}

func TestLambdaConnectorConfigVersionFallsBackToLastUpdated(t *testing.T) {
	t.Parallel()

	lastUpdated := time.Date(2026, 5, 19, 15, 0, 0, 123, time.UTC)
	config := &v1.GetConnectorConfigResponse{
		LastUpdated: timestamppb.New(lastUpdated),
	}

	want := "2026-05-19T15:00:00.000000123Z"
	if got := lambdaConnectorConfigVersion(config); got != want {
		t.Fatalf("lambdaConnectorConfigVersion = %q, want %q", got, want)
	}
}

func TestLambdaConnectorConfigVersionHandlesEmptyConfig(t *testing.T) {
	t.Parallel()

	if got := lambdaConnectorConfigVersion(nil); got != "" {
		t.Fatalf("lambdaConnectorConfigVersion(nil) = %q, want empty", got)
	}
	if got := lambdaConnectorConfigVersion(&v1.GetConnectorConfigResponse{}); got != "" {
		t.Fatalf("lambdaConnectorConfigVersion(empty) = %q, want empty", got)
	}
}
