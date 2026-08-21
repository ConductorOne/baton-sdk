package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	"github.com/conductorone/baton-sdk/internal/testtier"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func TestChaosConnectorReferentialCorpus(t *testing.T) {
	testtier.RequireNightly(t)
	for _, corpusCase := range chaosconnector.ReferentialCorpus() {
		t.Run(corpusCase.Name, func(t *testing.T) {
			for _, transport := range []chaosTransport{chaosTransportDirect, chaosTransportGRPC} {
				t.Run(transport.String(), func(t *testing.T) {
					runReferentialCorpusCase(t, corpusCase, transport)
				})
			}
		})
	}
}

func runReferentialCorpusCase(
	t *testing.T,
	corpusCase chaosconnector.ReferentialCase,
	transport chaosTransport,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "referential.c1z")

	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	require.NoError(t, corpusCase.Apply(scenario))
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, transport)
	concreteSyncer, ok := harness.Syncer.(*syncer)
	require.True(t, ok)

	syncErr := harness.Syncer.Sync(ctx)
	switch corpusCase.Policy {
	case chaosconnector.DataPolicyFail, chaosconnector.DataPolicyRejectRPC:
		require.Error(t, syncErr)
	case chaosconnector.DataPolicyAccept, chaosconnector.DataPolicyNormalize,
		chaosconnector.DataPolicySkipReport, chaosconnector.DataPolicyWarnRetain:
		require.NoError(t, syncErr)
	case chaosconnector.DataPolicyUnresolved:
		t.Fatalf("referential corpus case %q has unresolved policy", corpusCase.Name)
	default:
		t.Fatalf("referential corpus case %q has unknown policy %q", corpusCase.Name, corpusCase.Policy)
	}

	if corpusCase.Policy == chaosconnector.DataPolicySkipReport {
		switch corpusCase.Entity {
		case chaosconnector.ReferentialResource:
			require.Zero(t, concreteSyncer.ingestFilterStats.entitlementsDropped.Load())
			require.Zero(t, concreteSyncer.ingestFilterStats.grantsDropped.Load())
		case chaosconnector.ReferentialEntitlement:
			if corpusCase.Reference == chaosconnector.ReferenceTypeUnknown {
				require.EqualValues(t, 1, concreteSyncer.ingestFilterStats.entitlementsDropped.Load())
			} else {
				require.Zero(t, concreteSyncer.ingestFilterStats.entitlementsDropped.Load())
			}
		case chaosconnector.ReferentialGrant:
			require.EqualValues(t, 1, concreteSyncer.ingestFilterStats.grantsDropped.Load())
		}
	}
	require.NoError(t, harness.Close(t.Context()))
	require.NoError(t, run.Runtime().VerifyRequired())

	store, err := dotc1z.NewStore(
		t.Context(),
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(t.Context())) }()

	latest, err := store.SyncMeta().LatestFullSync(t.Context())
	require.NoError(t, err)
	if corpusCase.Policy == chaosconnector.DataPolicyFail ||
		corpusCase.Policy == chaosconnector.DataPolicyRejectRPC {
		require.Nil(t, latest, "hard-invalid corpus case must not seal")
		return
	}
	require.NotNil(t, latest, "non-fatal corpus case must seal")

	present, err := referentialIdentityPresent(t.Context(), store, corpusCase)
	require.NoError(t, err)
	switch corpusCase.Policy {
	case chaosconnector.DataPolicySkipReport:
		require.False(t, present, "dropped corpus row survived")
	case chaosconnector.DataPolicyAccept, chaosconnector.DataPolicyNormalize,
		chaosconnector.DataPolicyWarnRetain:
		require.True(t, present, "accepted corpus row is missing")
	case chaosconnector.DataPolicyFail, chaosconnector.DataPolicyRejectRPC,
		chaosconnector.DataPolicyUnresolved:
		t.Fatalf("non-sealing policy reached sealed-store assertion")
	}
}

func referentialIdentityPresent(
	ctx context.Context,
	store c1zstore.Store,
	corpusCase chaosconnector.ReferentialCase,
) (bool, error) {
	pageToken := ""
	for {
		switch corpusCase.Entity {
		case chaosconnector.ReferentialResource:
			response, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
				PageToken: pageToken,
			}.Build())
			if err != nil {
				return false, err
			}
			for _, item := range response.GetList() {
				if item.GetId().GetResourceType()+"\x00"+item.GetId().GetResource() == corpusCase.Identity {
					return true, nil
				}
			}
			pageToken = response.GetNextPageToken()
		case chaosconnector.ReferentialEntitlement:
			response, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
				PageToken: pageToken,
			}.Build())
			if err != nil {
				return false, err
			}
			for _, item := range response.GetList() {
				if item.GetId() == corpusCase.Identity {
					return true, nil
				}
			}
			pageToken = response.GetNextPageToken()
		case chaosconnector.ReferentialGrant:
			response, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
				PageToken: pageToken,
			}.Build())
			if err != nil {
				return false, err
			}
			for _, item := range response.GetList() {
				if item.GetId() == corpusCase.Identity {
					return true, nil
				}
			}
			pageToken = response.GetNextPageToken()
		default:
			return false, nil
		}
		if pageToken == "" {
			return false, nil
		}
	}
}
