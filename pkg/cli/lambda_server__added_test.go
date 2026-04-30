//go:build baton_lambda_support

package cli

import (
	"testing"
	"time"

	"github.com/spf13/viper"
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
