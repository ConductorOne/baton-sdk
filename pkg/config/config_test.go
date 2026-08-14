package config

import (
	"context"
	"errors"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/stretchr/testify/require"
)

// testConfig is a minimal field.Configurable stub. The constraint at
// pkg/field/validation.go requires exactly these five getters; all return
// zero values because no test here reads configuration.
type testConfig struct{}

func (testConfig) GetString(key string) string            { return "" }
func (testConfig) GetBool(key string) bool                { return false }
func (testConfig) GetInt(key string) int                  { return 0 }
func (testConfig) GetStringSlice(key string) []string     { return nil }
func (testConfig) GetStringMap(key string) map[string]any { return nil }

var _ field.Configurable = testConfig{}

func TestConnectorOptsFromRunTime(t *testing.T) {
	policy := &cli.EgressPolicy{Enforce: true, HTTPSOnly: true}
	runTimeOpts := cli.RunTimeOpts{
		TokenSource:         nil,
		SelectedAuthMethod:  "oauth2",
		SyncResourceTypeIDs: []string{"user", "group"},
		EgressPolicy:        policy,
	}

	opts := connectorOptsFromRunTime(runTimeOpts)
	require.NotNil(t, opts)
	require.Equal(t, runTimeOpts.TokenSource, opts.TokenSource)
	require.Equal(t, runTimeOpts.SelectedAuthMethod, opts.SelectedAuthMethod)
	require.Equal(t, runTimeOpts.SyncResourceTypeIDs, opts.SyncResourceTypeIDs)
	require.Same(t, policy, opts.EgressPolicy, "EgressPolicy must be forwarded pointer-equal")
}

func TestRunConnectorFuncPassesEgressPolicy(t *testing.T) {
	errStop := errors.New("stop")
	policy := &cli.EgressPolicy{Enforce: true, HTTPSOnly: true}

	var captured *cli.ConnectorOpts
	stubCf := func(ctx context.Context, cfg testConfig, opts *cli.ConnectorOpts) (connectorbuilder.ConnectorBuilderV2, []connectorbuilder.Opt, error) {
		captured = opts
		return nil, nil, errStop
	}

	// runConnectorFunc returns cli.GetConnectorFunc2[testConfig]; the stub's
	// signature must satisfy cli.NewConnector[testConfig]. The error return
	// makes the closure exit before connectorbuilder.NewConnector, so no real
	// builder is needed.
	_, err := runConnectorFunc[testConfig](stubCf)(context.Background(), testConfig{}, cli.RunTimeOpts{EgressPolicy: policy})
	require.ErrorIs(t, err, errStop)
	require.NotNil(t, captured, "the connector must receive the ConnectorOpts")
	require.Same(t, policy, captured.EgressPolicy, "EgressPolicy must reach the connector pointer-equal")
}
