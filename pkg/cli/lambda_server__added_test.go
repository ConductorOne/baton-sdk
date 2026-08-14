//go:build baton_lambda_support

package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/ugrpc"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newResp builds a GetConnectorConfigResponse with the given config_version and
// optional served-policy envelope. It is shared by the egress-policy tests and
// the rejection-log tests.
func newResp(cv string, env *v1.ServedPolicyEnvelope) *v1.GetConnectorConfigResponse {
	r := &v1.GetConnectorConfigResponse{}
	if cv != "" {
		r.SetConfigVersion(cv)
	}
	if env != nil {
		r.SetServedPolicyEnvelope(env)
	}
	return r
}

// goodEnvelope builds a valid served-policy envelope bound to the given
// config_version, with a single allowed host and https_only set.
func goodEnvelope(cv string) *v1.ServedPolicyEnvelope {
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

func TestEgressPolicyFromResponse(t *testing.T) {
	t.Parallel()

	t.Run("no envelope is ungoverned", func(t *testing.T) {
		t.Parallel()
		require.Nil(t, egressPolicyFromResponse(context.Background(), newResp("cv-1", nil)))
		require.Nil(t, egressPolicyFromResponse(context.Background(), nil))
	})

	t.Run("valid envelope surfaces hosts and https_only", func(t *testing.T) {
		t.Parallel()
		p := egressPolicyFromResponse(context.Background(), newResp("cv-1", goodEnvelope("cv-1")))
		require.NotNil(t, p)
		require.True(t, p.HTTPSOnly)
		require.Equal(t, []string{"api.example.com"}, p.AllowedHosts)
		require.False(t, p.Enforce)
	})

	t.Run("binding mismatch is governed deny-all", func(t *testing.T) {
		t.Parallel()
		// Envelope config_version differs from the response's.
		p := egressPolicyFromResponse(context.Background(), newResp("cv-1", goodEnvelope("cv-2")))
		require.NotNil(t, p)
		require.Empty(t, p.AllowedHosts)
		require.True(t, p.Enforce)
		require.True(t, p.HTTPSOnly)
	})

	t.Run("empty response config_version is governed deny-all", func(t *testing.T) {
		t.Parallel()
		p := egressPolicyFromResponse(context.Background(), newResp("", goodEnvelope("")))
		require.NotNil(t, p)
		require.Empty(t, p.AllowedHosts)
		require.True(t, p.Enforce)
		require.True(t, p.HTTPSOnly)
	})

	t.Run("unsupported envelope version is governed deny-all", func(t *testing.T) {
		t.Parallel()
		env := goodEnvelope("cv-1")
		env.SetEnvelopeVersion(999)
		p := egressPolicyFromResponse(context.Background(), newResp("cv-1", env))
		require.NotNil(t, p)
		require.Empty(t, p.AllowedHosts)
		require.True(t, p.Enforce)
		require.True(t, p.HTTPSOnly)
	})

	t.Run("unsupported egress schema version is governed deny-all", func(t *testing.T) {
		t.Parallel()
		env := goodEnvelope("cv-1")
		env.GetEgress().SetSchemaVersion(999)
		p := egressPolicyFromResponse(context.Background(), newResp("cv-1", env))
		require.NotNil(t, p)
		require.Empty(t, p.AllowedHosts)
		require.True(t, p.Enforce)
		require.True(t, p.HTTPSOnly)
	})
	t.Run("nil egress section is governed deny-all", func(t *testing.T) {
		t.Parallel()
		env := goodEnvelope("cv-1")
		env.SetEgress(nil)
		p := egressPolicyFromResponse(context.Background(), newResp("cv-1", env))
		require.NotNil(t, p)
		require.Empty(t, p.AllowedHosts)
		require.True(t, p.Enforce)
		require.True(t, p.HTTPSOnly)
	})

	t.Run("mode narrows to Enforce", func(t *testing.T) {
		t.Parallel()
		// Only an explicit ENFORCE blocks. Absent (a server predating the field)
		// and any unrecognized future posture both observe, so a runtime never
		// has to guess at a value it does not understand.
		for _, tt := range []struct {
			name    string
			mode    v1.EgressMode
			enforce bool
		}{
			{"absent", v1.EgressMode_EGRESS_MODE_UNSPECIFIED, false},
			{"report", v1.EgressMode_EGRESS_MODE_REPORT, false},
			{"enforce", v1.EgressMode_EGRESS_MODE_ENFORCE, true},
			{"unrecognized", v1.EgressMode(999), false},
		} {
			t.Run(tt.name, func(t *testing.T) {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetMode(tt.mode)
				p := egressPolicyFromResponse(context.Background(), newResp("cv-1", env))
				require.NotNil(t, p, tt.name)
				require.Equal(t, tt.enforce, p.Enforce, tt.name)
				// The projection must not disturb the envelope's hosts or scheme
				// gate: mode only narrows Enforce, everything else survives.
				require.Equal(t, []string{"api.example.com"}, p.AllowedHosts, tt.name)
				require.True(t, p.HTTPSOnly, tt.name)
			})
		}
	})

	t.Run("mode does not survive a failed binding check", func(t *testing.T) {
		t.Parallel()
		// The synthetic deny-all fallback enforces (Enforce=true) regardless of
		// the rejected envelope's mode: empty AllowedHosts + Enforce=true = block all.
		env := goodEnvelope("cv-2")
		env.GetEgress().SetMode(v1.EgressMode_EGRESS_MODE_ENFORCE)
		p := egressPolicyFromResponse(context.Background(), newResp("cv-1", env))
		require.NotNil(t, p)
		require.Empty(t, p.AllowedHosts)
		require.True(t, p.Enforce)
		require.True(t, p.HTTPSOnly)
	})

	t.Run("https_only false survives projection", func(t *testing.T) {
		t.Parallel()
		// The valid path overrides the deny-all fallback's forced HTTPSOnly=true
		// from the envelope, so a false https_only must project as false.
		env := goodEnvelope("cv-1")
		env.GetEgress().SetHttpsOnly(false)
		p := egressPolicyFromResponse(context.Background(), newResp("cv-1", env))
		require.NotNil(t, p)
		require.False(t, p.HTTPSOnly)
	})

	t.Run("projection does not alias the envelope's allowlist", func(t *testing.T) {
		t.Parallel()
		// The projection clones AllowedHosts, so mutating the retained envelope
		// must not change the projected policy.
		env := goodEnvelope("cv-1")
		p := egressPolicyFromResponse(context.Background(), newResp("cv-1", env))
		require.NotNil(t, p)
		env.GetEgress().GetAllowedHosts()[0] = "mutated.example.com"
		require.Equal(t, []string{"api.example.com"}, p.AllowedHosts)
	})
}

// TestEgressPolicyFromResponseRejectionLogs pins the kill-switch's only operator
// signal: the three envelope-rejection Warns and the unrecognized-mode Warn.
// ctxzap.Extract no-ops under a bare context.Background(), so these are asserted
// through an observer core attached via ctxzap.ToContext.
func TestEgressPolicyFromResponseRejectionLogs(t *testing.T) {
	cases := []struct {
		name        string
		buildResp   func() *v1.GetConnectorConfigResponse
		wantMessage string
		wantFields  map[string]any
		wantWarns   int
	}{
		{
			name: "envelope version mismatch",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.SetEnvelopeVersion(999)
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: served-policy envelope rejected; egress deny-all",
			wantFields: map[string]any{
				"reason":                "envelope-version-mismatch",
				"envelope_version":      uint32(999),
				"want_envelope_version": uint32(servedPolicyEnvelopeVersion),
			},
		},
		{
			name: "config version binding mismatch",
			buildResp: func() *v1.GetConnectorConfigResponse {
				return newResp("cv-1", goodEnvelope("cv-2"))
			},
			wantMessage: "connector_authoring: served-policy envelope rejected; egress deny-all",
			wantFields: map[string]any{
				"reason":                  "config-version-binding-mismatch",
				"envelope_config_version": "cv-2",
				"response_config_version": "cv-1",
			},
		},
		{
			name: "egress section unsupported",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetSchemaVersion(999)
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: served-policy envelope rejected; egress deny-all",
			wantFields: map[string]any{
				"reason":                     "egress-section-unsupported",
				"egress_present":             true,
				"egress_schema_version":      uint32(999),
				"want_egress_schema_version": uint32(egressSectionSchemaVersion),
			},
		},
		{
			name: "unrecognized egress mode observes",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetMode(v1.EgressMode(999))
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: unrecognized egress mode; observing",
			wantFields: map[string]any{
				"mode": int32(999),
			},
		},
		{
			name: "nil egress section unsupported",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.SetEgress(nil)
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: served-policy envelope rejected; egress deny-all",
			wantFields: map[string]any{
				"reason":                     "egress-section-unsupported",
				"egress_present":             false,
				"egress_schema_version":      uint32(0),
				"want_egress_schema_version": uint32(egressSectionSchemaVersion),
			},
		},
		{
			name: "allowlist wildcard invalidates envelope per contract",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetMode(v1.EgressMode_EGRESS_MODE_ENFORCE)
				env.GetEgress().SetAllowedHosts([]string{"*"})
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: egress allowlist contains an empty, wildcard, or IP-literal host; envelope is invalid per contract",
			wantFields: map[string]any{
				"host": "*",
			},
		},
		{
			name: "allowlist subdomain wildcard invalidates envelope per contract",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetMode(v1.EgressMode_EGRESS_MODE_ENFORCE)
				env.GetEgress().SetAllowedHosts([]string{"*.example.com"})
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: egress allowlist contains an empty, wildcard, or IP-literal host; envelope is invalid per contract",
			wantFields: map[string]any{
				"host": "*.example.com",
			},
		},
		{
			name: "allowlist empty host invalidates envelope per contract",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetMode(v1.EgressMode_EGRESS_MODE_ENFORCE)
				env.GetEgress().SetAllowedHosts([]string{""})
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: egress allowlist contains an empty, wildcard, or IP-literal host; envelope is invalid per contract",
			wantFields: map[string]any{
				"host": "",
			},
		},
		{
			name: "allowlist IPv4 literal invalidates envelope per contract",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetMode(v1.EgressMode_EGRESS_MODE_ENFORCE)
				env.GetEgress().SetAllowedHosts([]string{"192.168.1.1"})
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: egress allowlist contains an empty, wildcard, or IP-literal host; envelope is invalid per contract",
			wantFields: map[string]any{
				"host": "192.168.1.1",
			},
		},
		{
			name: "allowlist bare IPv6 literal invalidates envelope per contract",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetMode(v1.EgressMode_EGRESS_MODE_ENFORCE)
				env.GetEgress().SetAllowedHosts([]string{"2001:db8::1"})
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: egress allowlist contains an empty, wildcard, or IP-literal host; envelope is invalid per contract",
			wantFields: map[string]any{
				"host": "2001:db8::1",
			},
		},
		{
			name: "allowlist bracketed IPv6 literal invalidates envelope per contract",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetMode(v1.EgressMode_EGRESS_MODE_ENFORCE)
				env.GetEgress().SetAllowedHosts([]string{"[2001:db8::1]"})
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: egress allowlist contains an empty, wildcard, or IP-literal host; envelope is invalid per contract",
			wantFields: map[string]any{
				"host": "[2001:db8::1]",
			},
		},
		{
			name: "allowlist wildcard in REPORT mode observes",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetMode(v1.EgressMode_EGRESS_MODE_REPORT)
				env.GetEgress().SetAllowedHosts([]string{"*"})
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: egress allowlist contains an empty, wildcard, or IP-literal host; envelope is invalid per contract",
			wantFields: map[string]any{
				"host": "*",
			},
		},
		{
			name: "allowlist IPv4 with port invalidates envelope per contract",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetMode(v1.EgressMode_EGRESS_MODE_ENFORCE)
				env.GetEgress().SetAllowedHosts([]string{"192.168.1.1:443"})
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: egress allowlist contains an empty, wildcard, or IP-literal host; envelope is invalid per contract",
			wantFields: map[string]any{
				"host": "192.168.1.1:443",
			},
		},
		{
			name: "allowlist bracketed IPv6 with port invalidates envelope per contract",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetMode(v1.EgressMode_EGRESS_MODE_ENFORCE)
				env.GetEgress().SetAllowedHosts([]string{"[2001:db8::1]:443"})
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: egress allowlist contains an empty, wildcard, or IP-literal host; envelope is invalid per contract",
			wantFields: map[string]any{
				"host": "[2001:db8::1]:443",
			},
		},
		{
			name: "allowlist multiple invalid entries in REPORT mode observes all",
			buildResp: func() *v1.GetConnectorConfigResponse {
				env := goodEnvelope("cv-1")
				env.GetEgress().SetMode(v1.EgressMode_EGRESS_MODE_REPORT)
				env.GetEgress().SetAllowedHosts([]string{"*", "10.0.0.5"})
				return newResp("cv-1", env)
			},
			wantMessage: "connector_authoring: egress allowlist contains an empty, wildcard, or IP-literal host; envelope is invalid per contract",
			wantFields: map[string]any{
				"host": "*",
			},
			wantWarns: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			core, observed := observer.New(zapcore.WarnLevel)
			logger := zap.New(core)
			ctx := ctxzap.ToContext(context.Background(), logger)

			p := egressPolicyFromResponse(ctx, tc.buildResp())

			entries := observed.All()
			wantWarns := tc.wantWarns
			if wantWarns == 0 {
				wantWarns = 1
			}
			require.Len(t, entries, wantWarns, "expected %d Warn(s)", wantWarns)
			require.Equal(t, tc.wantMessage, entries[0].Message)
			ctxMap := entries[0].ContextMap()
			for k, want := range tc.wantFields {
				require.Equal(t, want, ctxMap[k], "field %q", k)
			}

			// The unrecognized-mode case is a valid envelope: the projection must
			// still observe (Enforce=false) and keep the envelope's hosts and
			// scheme gate.
			if tc.name == "unrecognized egress mode observes" {
				require.NotNil(t, p)
				require.False(t, p.Enforce)
				require.Equal(t, []string{"api.example.com"}, p.AllowedHosts)
				require.True(t, p.HTTPSOnly)
			}
			// The wildcard/subdomain-wildcard/empty-host/IP-literal cases fail closed
			// under ENFORCE like every other v1 invariant: the projection is the
			// synthetic deny-all (empty allowlist, Enforce=true, HTTPSOnly=true),
			// not the envelope's hosts verbatim — under ENFORCE a wildcard entry
			// would otherwise read as allow-all to a wildcard-matching connector.
			switch tc.name {
			case "allowlist wildcard invalidates envelope per contract",
				"allowlist subdomain wildcard invalidates envelope per contract",
				"allowlist empty host invalidates envelope per contract",
				"allowlist IPv4 literal invalidates envelope per contract",
				"allowlist bare IPv6 literal invalidates envelope per contract",
				"allowlist bracketed IPv6 literal invalidates envelope per contract",
				"allowlist IPv4 with port invalidates envelope per contract",
				"allowlist bracketed IPv6 with port invalidates envelope per contract":
				require.NotNil(t, p)
				require.True(t, p.Enforce)
				require.True(t, p.HTTPSOnly)
				require.Empty(t, p.AllowedHosts)
			// Under REPORT/absent mode the allowlist is observed, not enforced, so
			// a contract-invalid entry must not take egress offline: the
			// projection is unchanged (Enforce=false, hosts verbatim).
			case "allowlist wildcard in REPORT mode observes":
				require.NotNil(t, p)
				require.False(t, p.Enforce)
				require.Equal(t, []string{"*"}, p.AllowedHosts)
				require.True(t, p.HTTPSOnly)
			case "allowlist multiple invalid entries in REPORT mode observes all":
				require.NotNil(t, p)
				require.False(t, p.Enforce)
				require.Equal(t, []string{"*", "10.0.0.5"}, p.AllowedHosts)
				require.True(t, p.HTTPSOnly)
				// Both offending entries are surfaced in one projection.
				require.Equal(t, "connector_authoring: egress allowlist contains an empty, wildcard, or IP-literal host; envelope is invalid per contract", entries[1].Message)
				require.Equal(t, "10.0.0.5", entries[1].ContextMap()["host"])
			}
		})
	}
}

// TestEgressModeNameKeysPinned pins the EgressMode enum's name->number mapping,
// which the egress projection switch in egressPolicyFromResponse depends on.
// Adding a new enum value or renumbering an existing one fails this test until
// the switch is revisited, so a regenerated-but-unupdated SDK cannot silently
// observe a new enforcing posture and a renumber cannot break wire
// compatibility unnoticed.
func TestEgressModeNameKeysPinned(t *testing.T) {
	require.Equal(t, map[string]int32{
		"EGRESS_MODE_UNSPECIFIED": 0,
		"EGRESS_MODE_REPORT":      1,
		"EGRESS_MODE_ENFORCE":     2,
	}, v1.EgressMode_value)
}

// TestGenerationVersion pins the version choice and the mismatch Warn that
// generationVersion owns: a generation is labeled with the served config_version
// (falling back to the requested version), and a served/requested mismatch is
// surfaced so the reload no-op guard re-fetches on the next invocation.
func TestGenerationVersion(t *testing.T) {
	t.Run("requested equals served returns served without warning", func(t *testing.T) {
		core, observed := observer.New(zapcore.WarnLevel)
		ctx := ctxzap.ToContext(context.Background(), zap.New(core))
		require.Equal(t, "v1", generationVersion(ctx, "v1", newResp("v1", nil)))
		require.Empty(t, observed.All())
	})
	t.Run("served differs from requested warns and serves the served version", func(t *testing.T) {
		core, observed := observer.New(zapcore.WarnLevel)
		ctx := ctxzap.ToContext(context.Background(), zap.New(core))
		require.Equal(t, "v1", generationVersion(ctx, "v2", newResp("v1", nil)))
		entries := observed.All()
		require.Len(t, entries, 1)
		require.Equal(t, "connector_authoring: served config_version differs from requested; will retry on a later invocation", entries[0].Message)
		cm := entries[0].ContextMap()
		require.Equal(t, "v2", cm["requested_version"])
		require.Equal(t, "v1", cm["served_version"])
	})
	t.Run("served empty falls back to requested without warning", func(t *testing.T) {
		core, observed := observer.New(zapcore.WarnLevel)
		ctx := ctxzap.ToContext(context.Background(), zap.New(core))
		require.Equal(t, "v2", generationVersion(ctx, "v2", newResp("", nil)))
		require.Empty(t, observed.All())
	})
	t.Run("both empty returns empty without warning", func(t *testing.T) {
		core, observed := observer.New(zapcore.WarnLevel)
		ctx := ctxzap.ToContext(context.Background(), zap.New(core))
		require.Equal(t, "", generationVersion(ctx, "", newResp("", nil)))
		require.Empty(t, observed.All())
	})
	t.Run("present-but-empty config_version falls back to requested without warning", func(t *testing.T) {
		// newResp skips SetConfigVersion for an empty cv, so build the response
		// directly: a present-but-empty config_version must not label the
		// generation with the empty string (which would defeat the reload no-op
		// guard and rebuild on every invocation).
		resp := &v1.GetConnectorConfigResponse{}
		resp.SetConfigVersion("")
		core, observed := observer.New(zapcore.WarnLevel)
		ctx := ctxzap.ToContext(context.Background(), zap.New(core))
		require.Equal(t, "v2", generationVersion(ctx, "v2", resp))
		require.Empty(t, observed.All())
	})
}

// TestReloadRefetchesWhenServedVersionDiffersFromRequested pins the reload
// no-op guard against pinning a stale posture: a generation labeled with the
// served config_version (which differs from the requested header) fails the
// guard on the next version-stamped invocation and is re-fetched, not pinned.
func TestReloadRefetchesWhenServedVersionDiffersFromRequested(t *testing.T) {
	// The reloader swaps the process-wide log level through the deferred guard,
	// so this test cannot run in parallel.
	// Disable the reload rate-cap: this test pins the guard's re-fetch behavior
	// (two identical cv-2 invocations each rebuild), which the rate-cap would
	// otherwise bound to one.
	old := lambdaConnectorReloadMinInterval
	lambdaConnectorReloadMinInterval = 0
	defer func() { lambdaConnectorReloadMinInterval = old }()

	// The stub build applies level:"error" on a successful reload and the
	// deferred guard does not restore it, so initialize a known process-wide
	// level and restore it on the way out.
	_, err := logging.Init(context.Background(),
		logging.WithLogLevel("info"),
		logging.WithOutputPaths([]string{os.DevNull}),
	)
	require.NoError(t, err, "logging.Init")
	t.Cleanup(func() {
		require.NoError(t, logging.SetLogLevel("info"))
	})

	activeConnector := &stubConnectorServer{}
	server := c1_lambda_grpc.NewServer(lambdaUnaryInterceptorChain())
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Service",
		HandlerType: (*types.ConnectorServer)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Method",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					return &v1.GetConnectorConfigResponse{}, nil
				},
			},
		},
	}, activeConnector)

	buildCalls := 0
	r := &lambdaConnectorReloader{
		server:  server,
		current: &lambdaConnectorGeneration{version: "cv-1", connector: activeConnector, logging: lambdaLogLevelConfig{level: "error"}},
		build: func(ctx context.Context, version string) (*lambdaConnectorGeneration, error) {
			buildCalls++
			// A real build labels the generation with the served config_version
			// (generationVersion), which here differs from the requested header.
			resp := &v1.GetConnectorConfigResponse{}
			resp.SetConfigVersion("cv-1") // served version differs from the requested cv-2 header
			return &lambdaConnectorGeneration{
				version:   generationVersion(ctx, version, resp),
				connector: &stubConnectorServer{},
				logging:   lambdaLogLevelConfig{level: "error"},
			}, nil
		},
	}

	req, err := c1_lambda_grpc.NewRequest(
		"/test.Service/Method",
		&v1.GetConnectorConfigRequest{},
		metadata.Pairs(lambdaConnectorConfigVersionHeader, "cv-2"),
	)
	require.NoError(t, err, "NewRequest")

	for i := 0; i < 2; i++ {
		resp, err := r.Handler(context.Background(), req)
		require.NoError(t, err, "invocation %d", i)
		require.NotNil(t, resp)
	}

	// A generation labeled with the served version (cv-1) fails the reload
	// no-op guard on the next cv-2 invocation and is re-fetched, not pinned.
	require.Equal(t, 2, buildCalls, "a served-version-labeled generation must be re-fetched on the next version-stamped invocation")
}

// TestBuildGenerationPolicyAndVersion pins the lambda build path's two
// security-load-bearing compositions — egress-policy delivery and served-version
// labeling — through the single seam egressPolicyAndGenerationVersion. A revert
// of either composition in buildConnectorGeneration ships green unless this test
// catches it.
func TestBuildGenerationPolicyAndVersion(t *testing.T) {
	t.Run("deny-all envelope projects deny-all and labels served version", func(t *testing.T) {
		// Config-version binding mismatch: the envelope is bound to cv-2 but the
		// response serves cv-1, so the projection fails closed (deny-all).
		policy, version := egressPolicyAndGenerationVersion(context.Background(), "cv-2", newResp("cv-1", goodEnvelope("cv-2")))
		require.NotNil(t, policy)
		require.True(t, policy.Enforce)
		require.True(t, policy.HTTPSOnly)
		require.Empty(t, policy.AllowedHosts)
		require.Equal(t, "cv-1", version, "labeled with the served version, not the requested cv-2")
	})
	t.Run("valid ENFORCE envelope projects enforce and labels served version", func(t *testing.T) {
		env := goodEnvelope("cv-1")
		env.GetEgress().SetMode(v1.EgressMode_EGRESS_MODE_ENFORCE)
		policy, version := egressPolicyAndGenerationVersion(context.Background(), "cv-2", newResp("cv-1", env))
		require.NotNil(t, policy)
		require.True(t, policy.Enforce)
		require.Equal(t, []string{"api.example.com"}, policy.AllowedHosts)
		require.True(t, policy.HTTPSOnly)
		require.Equal(t, "cv-1", version)
	})
	t.Run("valid REPORT envelope projects observe and labels served version", func(t *testing.T) {
		policy, version := egressPolicyAndGenerationVersion(context.Background(), "cv-2", newResp("cv-1", goodEnvelope("cv-1")))
		require.NotNil(t, policy)
		require.False(t, policy.Enforce)
		require.Equal(t, "cv-1", version)
	})
}

// TestReloadRateCapsPersistentMismatch pins the reload rate-cap: when the
// server persistently serves a config_version different from the requested
// header, the served-version labeling makes the no-op guard never match, so the
// cap bounds rebuilds to one per interval. Interval expiry re-enables rebuild.
func TestReloadRateCapsPersistentMismatch(t *testing.T) {
	// The reloader swaps the process-wide log level through the deferred guard,
	// so this test cannot run in parallel.
	old := lambdaConnectorReloadMinInterval
	defer func() { lambdaConnectorReloadMinInterval = old }()
	lambdaConnectorReloadMinInterval = time.Hour

	// The stub build applies level:"error" on a successful reload and the
	// deferred guard does not restore it, so initialize a known process-wide
	// level and restore it on the way out.
	_, err := logging.Init(context.Background(),
		logging.WithLogLevel("info"),
		logging.WithOutputPaths([]string{os.DevNull}),
	)
	require.NoError(t, err, "logging.Init")
	t.Cleanup(func() {
		require.NoError(t, logging.SetLogLevel("info"))
	})

	activeConnector := &stubConnectorServer{}
	server := c1_lambda_grpc.NewServer(lambdaUnaryInterceptorChain())
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Service",
		HandlerType: (*types.ConnectorServer)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Method",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					return &v1.GetConnectorConfigResponse{}, nil
				},
			},
		},
	}, activeConnector)

	buildCalls := 0
	r := &lambdaConnectorReloader{
		server:  server,
		current: &lambdaConnectorGeneration{version: "cv-1", connector: activeConnector, logging: lambdaLogLevelConfig{level: "error"}},
		build: func(ctx context.Context, version string) (*lambdaConnectorGeneration, error) {
			buildCalls++
			// The server persistently serves cv-1 while the invoker requests
			// cv-2, so the served-version labeling makes the no-op guard never
			// match and every invocation would otherwise rebuild.
			return &lambdaConnectorGeneration{version: "cv-1", connector: &stubConnectorServer{}, logging: lambdaLogLevelConfig{level: "error"}}, nil
		},
	}

	req, err := c1_lambda_grpc.NewRequest(
		"/test.Service/Method",
		&v1.GetConnectorConfigRequest{},
		metadata.Pairs(lambdaConnectorConfigVersionHeader, "cv-2"),
	)
	require.NoError(t, err, "NewRequest")

	// Attach an observer core so the rate-cap Warn on the capped path is pinned.
	core, observed := observer.New(zapcore.WarnLevel)
	ctx := ctxzap.ToContext(context.Background(), zap.New(core))

	// Three identical cv-2 invocations under a persistent served/requested skew:
	// the rate-cap bounds the rebuild to one.
	for i := range 3 {
		resp, err := r.Handler(ctx, req)
		require.NoError(t, err, "invocation %d", i)
		require.NotNil(t, resp)
	}
	require.Equal(t, 1, buildCalls, "the 2nd and 3rd invocations are rate-capped")
	// The capped path warns once per rebuild interval (not per invocation), with
	// the requested version, the current generation version, and the remaining
	// interval.
	require.Len(t, observed.All(), 1, "one rate-cap Warn per rebuild interval")
	capEntry := observed.All()[0]
	require.Equal(t, "lambda-run: reload rate-capped; serving previous generation", capEntry.Message)
	require.Equal(t, "cv-2", capEntry.ContextMap()["requested_version"])
	require.Equal(t, "cv-1", capEntry.ContextMap()["current_version"])
	require.Greater(t, capEntry.ContextMap()["remaining_interval"].(time.Duration), time.Duration(0))

	// Interval expiry re-enables rebuild: backdate lastRebuildAt so a positive
	// interval has elapsed, then the next invocation rebuilds.
	r.lastRebuildAt = time.Now().Add(-2 * time.Hour)
	resp, err := r.Handler(ctx, req)
	require.NoError(t, err, "invocation after interval expiry")
	require.NotNil(t, resp)
	require.Equal(t, 2, buildCalls, "interval expiry re-enables rebuild")

	// Cap-disabled path: with the interval set to 0, the next two invocations
	// each rebuild.
	lambdaConnectorReloadMinInterval = 0
	for i := range 2 {
		resp, err := r.Handler(ctx, req)
		require.NoError(t, err, "invocation %d", i)
		require.NotNil(t, resp)
	}
	require.Equal(t, 4, buildCalls, "cap-disabled path rebuilds on every invocation")
}

func TestLambdaConnectorConfigVersion(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 7, 27, 10, 30, 0, 0, time.UTC)

	// config_version stamped: preferred even when last_updated is also present.
	preferConfigVersion := &v1.GetConnectorConfigResponse{}
	preferConfigVersion.SetConfigVersion("cv-1")
	preferConfigVersion.SetLastUpdated(timestamppb.New(ts))

	// Older server: no config_version, falls back to last_updated.
	lastUpdatedOnly := &v1.GetConnectorConfigResponse{}
	lastUpdatedOnly.SetLastUpdated(timestamppb.New(ts))

	for _, tc := range []struct {
		name   string
		config *v1.GetConnectorConfigResponse
		want   string
	}{
		{"nil config", nil, ""},
		{"config_version preferred over last_updated", preferConfigVersion, "cv-1"},
		{"falls back to last_updated when config_version absent", lastUpdatedOnly, ts.Format(time.RFC3339Nano)},
		{"empty when neither is set", &v1.GetConnectorConfigResponse{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, lambdaConnectorConfigVersion(tc.config))
		})
	}
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

// Connector construction on a config-version reload runs before the interceptor
// chain, so the reloader is the one place a panic can still reach the lambda
// runtime and be reported as a statusless crashed invocation.
func TestLambdaConnectorReloaderRecoversBuildPanic(t *testing.T) {
	// reloadIfNeeded's deferred guard restores the active generation's log level
	// through the process-wide handle, so this test cannot run in parallel.
	r := &lambdaConnectorReloader{
		current: &lambdaConnectorGeneration{version: "cv-1"},
		build: func(ctx context.Context, version string) (*lambdaConnectorGeneration, error) {
			panic("connector construction exploded")
		},
	}

	req, err := c1_lambda_grpc.NewRequest(
		"/test.Service/Method",
		&v1.GetConnectorConfigRequest{},
		metadata.Pairs(lambdaConnectorConfigVersionHeader, "cv-2"),
	)
	require.NoError(t, err, "NewRequest")

	logged := &bytes.Buffer{}
	ctx := ctxzap.ToContext(context.Background(), newCaptureLogger(logged))

	resp, err := r.Handler(ctx, req)
	require.NoError(t, err, "a recovered panic must not propagate as an invocation error")
	require.NotNil(t, resp)

	st, err := resp.Status()
	require.NoError(t, err, "Status")
	require.Equal(t, codes.Internal, st.Code())

	// This panic happened before the interceptor chain could run, and the log
	// message is shared with in-chain panics, so the site field is what tells an
	// operator which one they are looking at.
	require.Contains(t, logged.String(), "connector construction exploded")
	require.Contains(t, logged.String(), `"recovery_site":"`+ugrpc.RecoverySiteLambdaHandler+`"`)
}

// A build that returns an error rather than panicking must keep its own
// classification: the recovery wrapper is not allowed to turn ordinary failures
// into Internal.
func TestLambdaConnectorReloaderPassesThroughBuildError(t *testing.T) {
	// reloadIfNeeded's deferred guard restores the active generation's log level
	// through the process-wide handle, so this test cannot run in parallel.
	r := &lambdaConnectorReloader{
		current: &lambdaConnectorGeneration{version: "cv-1"},
		build: func(ctx context.Context, version string) (*lambdaConnectorGeneration, error) {
			return nil, errors.New("config fetch failed")
		},
	}

	req, err := c1_lambda_grpc.NewRequest(
		"/test.Service/Method",
		&v1.GetConnectorConfigRequest{},
		metadata.Pairs(lambdaConnectorConfigVersionHeader, "cv-2"),
	)
	require.NoError(t, err, "NewRequest")

	resp, err := r.Handler(context.Background(), req)
	require.NoError(t, err)

	st, err := resp.Status()
	require.NoError(t, err, "Status")
	require.Equal(t, codes.Unavailable, st.Code(), "a build error must not be reclassified as Internal")
}

// reloadIfNeeded's log level obligation at every exit. The refactor collapsed
// three explicit reverts into one r.current != active condition, so each exit is
// a cell here rather than a sampled case: build applies the incoming generation's
// level before it can fail, and only an exit that actually activated that
// generation may keep it.
func TestLambdaConnectorReloaderLogLevelAtEveryExit(t *testing.T) {
	// Every case sets the process-wide log level, so none of this runs in parallel.
	activeConnector := &stubConnectorServer{}
	nextGeneration := func(version string) (*lambdaConnectorGeneration, error) {
		return &lambdaConnectorGeneration{
			version:   version,
			connector: &stubConnectorServer{},
			logging:   lambdaLogLevelConfig{level: "error"},
		}, nil
	}

	for _, tc := range []struct {
		name              string
		registerActive    bool
		build             func(version string) (*lambdaConnectorGeneration, error)
		wantActiveVersion string
		wantLevelRestored bool
	}{
		{
			name:              "build returns an error",
			registerActive:    true,
			build:             func(string) (*lambdaConnectorGeneration, error) { return nil, errors.New("config fetch failed") },
			wantActiveVersion: "cv-1",
			wantLevelRestored: true,
		},
		{
			name:              "build panics",
			registerActive:    true,
			build:             func(string) (*lambdaConnectorGeneration, error) { panic("connector construction exploded") },
			wantActiveVersion: "cv-1",
			wantLevelRestored: true,
		},
		{
			// ReplaceServiceImplementation rejects a nil implementation.
			name:           "built generation carries no connector",
			registerActive: true,
			build: func(version string) (*lambdaConnectorGeneration, error) {
				return &lambdaConnectorGeneration{version: version, logging: lambdaLogLevelConfig{level: "error"}}, nil
			},
			wantActiveVersion: "cv-1",
			wantLevelRestored: true,
		},
		{
			// Nothing registered, so no service matches the active generation and
			// ReplaceServiceImplementation reports zero replacements.
			name:              "no registered service matches",
			registerActive:    false,
			build:             nextGeneration,
			wantActiveVersion: "cv-1",
			wantLevelRestored: true,
		},
		{
			name:              "successful swap keeps the new level",
			registerActive:    true,
			build:             nextGeneration,
			wantActiveVersion: "cv-2",
			wantLevelRestored: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Discard the output: these cases assert on the logger's level, never
			// on what it wrote, and zap keeps a sink open for the life of the
			// process, so a file under t.TempDir() would block the directory's
			// cleanup on Windows, where an open handle cannot be deleted.
			ctx, err := logging.Init(context.Background(),
				logging.WithLogLevel("debug"),
				logging.WithOutputPaths([]string{os.DevNull}),
			)
			require.NoError(t, err, "logging.Init")
			logger := ctxzap.Extract(ctx)
			require.True(t, logger.Core().Enabled(zapcore.DebugLevel), "precondition: debug is enabled")

			server := c1_lambda_grpc.NewServer(lambdaUnaryInterceptorChain())
			if tc.registerActive {
				server.RegisterService(&grpc.ServiceDesc{
					ServiceName: "test.ConnectorService",
					HandlerType: (*types.ConnectorServer)(nil),
				}, activeConnector)
			}

			r := &lambdaConnectorReloader{
				server:  server,
				current: &lambdaConnectorGeneration{version: "cv-1", connector: activeConnector, logging: lambdaLogLevelConfig{level: "debug"}},
				build: func(ctx context.Context, version string) (*lambdaConnectorGeneration, error) {
					// Mirror a real build, which applies the incoming level first.
					require.NoError(t, applyLambdaLogLevel(lambdaLogLevelConfig{level: "error"}, time.Now()))
					return tc.build(version)
				},
			}

			// A panic is one exit among these; turning it into a status is the
			// handler's job and is asserted separately.
			func() {
				defer func() { _ = recover() }()
				_ = r.reloadIfNeeded(ctx, "cv-2")
			}()

			require.Equal(t, tc.wantActiveVersion, r.current.version, "active generation")
			require.Equal(t, tc.wantLevelRestored, logger.Core().Enabled(zapcore.DebugLevel), "log level restored")
		})
	}
}

// The restore lives in reloadIfNeeded's defer and the recovery in Handler; the
// every-exit table reaches the panic exit with a local recover, so this is the
// one test that pins the seam between them: a panic recovered by Handler must
// return Internal and leave the log level restored, in the same invocation.
func TestLambdaConnectorReloaderRecoveredPanicRestoresLogLevel(t *testing.T) {
	// Sets the process-wide log level, so this test cannot run in parallel.
	ctx, err := logging.Init(context.Background(),
		logging.WithLogLevel("debug"),
		logging.WithOutputPaths([]string{os.DevNull}),
	)
	require.NoError(t, err, "logging.Init")
	logger := ctxzap.Extract(ctx)
	require.True(t, logger.Core().Enabled(zapcore.DebugLevel), "precondition: debug is enabled")

	r := &lambdaConnectorReloader{
		current: &lambdaConnectorGeneration{version: "cv-1", logging: lambdaLogLevelConfig{level: "debug"}},
		build: func(ctx context.Context, version string) (*lambdaConnectorGeneration, error) {
			// Mirror a real build, which applies the incoming level first.
			require.NoError(t, applyLambdaLogLevel(lambdaLogLevelConfig{level: "error"}, time.Now()))
			panic("connector construction exploded")
		},
	}

	req, err := c1_lambda_grpc.NewRequest(
		"/test.Service/Method",
		&v1.GetConnectorConfigRequest{},
		metadata.Pairs(lambdaConnectorConfigVersionHeader, "cv-2"),
	)
	require.NoError(t, err, "NewRequest")

	resp, err := r.Handler(ctx, req)
	require.NoError(t, err, "a recovered panic must not propagate as an invocation error")
	require.NotNil(t, resp)

	st, err := resp.Status()
	require.NoError(t, err, "Status")
	require.Equal(t, codes.Internal, st.Code())
	require.True(t, logger.Core().Enabled(zapcore.DebugLevel), "log level restored after a recovered reload panic")
}

// stubConnectorServer embeds the interface so it satisfies types.ConnectorServer
// without implementing it; these tests only ever use it as an identity to swap.
type stubConnectorServer struct {
	types.ConnectorServer
}

func newCaptureLogger(buf *bytes.Buffer) *zap.Logger {
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "" // keep test output deterministic
	return zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encCfg),
		zapcore.AddSync(buf),
		zap.DebugLevel,
	))
}

// The lambda transport reports an escaping panic as a crashed invocation with no
// gRPC status, so recovery has to be in this chain and has to sit outside the
// other interceptors. Both properties are asserted here because the defect being
// guarded against is recovery silently missing from the lambda path while the
// normal gRPC path has it.
func TestLambdaUnaryInterceptorChainRecoversPanics(t *testing.T) {
	t.Parallel()

	okHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "response", nil
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Method"}

	t.Run("panic in the handler", func(t *testing.T) {
		t.Parallel()

		chain := lambdaUnaryInterceptorChain()
		_, err := chain(context.Background(), nil, info, func(ctx context.Context, req interface{}) (interface{}, error) {
			panic("connector exploded")
		})

		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("panic in a wrapped interceptor", func(t *testing.T) {
		t.Parallel()

		panicking := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			panic("auth exploded")
		}

		chain := lambdaUnaryInterceptorChain(panicking)
		_, err := chain(context.Background(), nil, info, okHandler)

		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("wrapped interceptors still run", func(t *testing.T) {
		t.Parallel()

		called := false
		marker := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			called = true
			return handler(ctx, req)
		}

		chain := lambdaUnaryInterceptorChain(marker)
		resp, err := chain(context.Background(), nil, info, okHandler)

		require.NoError(t, err)
		require.Equal(t, "response", resp)
		require.True(t, called, "wrapped interceptor did not run")
	})
}
