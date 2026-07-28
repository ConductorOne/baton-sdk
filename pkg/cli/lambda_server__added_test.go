//go:build baton_lambda_support

package cli

import (
	"testing"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestEgressPolicyFromResponse(t *testing.T) {
	t.Parallel()

	newResp := func(cv string, env *v1.ServedPolicyEnvelope) *v1.GetConnectorConfigResponse {
		r := &v1.GetConnectorConfigResponse{}
		if cv != "" {
			r.SetConfigVersion(cv)
		}
		if env != nil {
			r.SetServedPolicyEnvelope(env)
		}
		return r
	}
	goodEnvelope := func(cv string) *v1.ServedPolicyEnvelope {
		return v1.ServedPolicyEnvelope_builder{
			EnvelopeVersion: servedPolicyEnvelopeVersion,
			ConfigVersion:   cv,
			Egress: v1.EgressSection_builder{
				SchemaVersion: egressSectionSchemaVersion,
				HttpsOnly:     true,
				AllowedHosts:  []string{"api.example.com"},
			}.Build(),
		}.Build()
	}

	t.Run("no envelope is ungoverned", func(t *testing.T) {
		require.Nil(t, egressPolicyFromResponse(newResp("cv-1", nil)))
		require.Nil(t, egressPolicyFromResponse(nil))
	})

	t.Run("valid envelope surfaces hosts and https_only", func(t *testing.T) {
		p := egressPolicyFromResponse(newResp("cv-1", goodEnvelope("cv-1")))
		require.NotNil(t, p)
		require.True(t, p.Governed)
		require.True(t, p.HTTPSOnly)
		require.Equal(t, []string{"api.example.com"}, p.AllowedHosts)
	})

	t.Run("binding mismatch is governed deny-all", func(t *testing.T) {
		// Envelope config_version differs from the response's.
		p := egressPolicyFromResponse(newResp("cv-1", goodEnvelope("cv-2")))
		require.NotNil(t, p)
		require.True(t, p.Governed)
		require.Empty(t, p.AllowedHosts)
	})

	t.Run("empty response config_version is governed deny-all", func(t *testing.T) {
		p := egressPolicyFromResponse(newResp("", goodEnvelope("")))
		require.NotNil(t, p)
		require.True(t, p.Governed)
		require.Empty(t, p.AllowedHosts)
	})

	t.Run("unsupported envelope version is governed deny-all", func(t *testing.T) {
		env := goodEnvelope("cv-1")
		env.SetEnvelopeVersion(999)
		p := egressPolicyFromResponse(newResp("cv-1", env))
		require.NotNil(t, p)
		require.True(t, p.Governed)
		require.Empty(t, p.AllowedHosts)
	})

	t.Run("unsupported egress schema version is governed deny-all", func(t *testing.T) {
		env := goodEnvelope("cv-1")
		env.GetEgress().SetSchemaVersion(999)
		p := egressPolicyFromResponse(newResp("cv-1", env))
		require.NotNil(t, p)
		require.True(t, p.Governed)
		require.Empty(t, p.AllowedHosts)
	})
}

func TestLambdaLogLevelConfigExpiresDebug(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	v := viper.New()
	v.Set("log-level", "debug")
	v.Set("log-level-debug-expires-at", now.Add(-time.Minute).Format(time.RFC3339))

	config, err := lambdaLogLevelConfigFromViper(v)
	require.NoError(t, err, "lambdaLogLevelConfigFromViper")
	require.Equal(t, "info", config.effective(now), "effective log level")
}

func TestLambdaLogLevelConfigKeepsUnexpiredDebug(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	v := viper.New()
	v.Set("log-level", "debug")
	v.Set("log-level-debug-expires-at", now.Add(time.Minute).Format(time.RFC3339))

	config, err := lambdaLogLevelConfigFromViper(v)
	require.NoError(t, err, "lambdaLogLevelConfigFromViper")
	require.Equal(t, "debug", config.effective(now), "effective log level")
}

func TestLambdaLogLevelConfigRejectsInvalidLevel(t *testing.T) {
	t.Parallel()

	v := viper.New()
	v.Set("log-level", "verbose")

	_, err := lambdaLogLevelConfigFromViper(v)
	require.Error(t, err, "expected invalid log level to fail")
}
