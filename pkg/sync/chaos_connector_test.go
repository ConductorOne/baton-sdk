package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func TestChaosConnectorCleanSyncMatchesManifest(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			c1zPath := filepath.Join(tmpDir, "chaos-clean.c1z")

			scenario, err := chaosconnector.NewFullScenario()
			require.NoError(t, err)
			run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
			require.NoError(t, err)
			harness := newChaosHarness(
				t, ctx, run, c1zPath, tmpDir, chaosTransportDirect, WithWorkerCount(workers),
			)
			harness.SyncAndClose(t, ctx)

			manifest, err := scenario.Manifest(scenario.InitialEpoch)
			require.NoError(t, err)
			expected := chaosoracle.ExpectedIdentities(manifest)

			store, err := dotc1z.NewStore(
				ctx,
				c1zPath,
				dotc1z.WithEngine(c1zstore.EnginePebble),
				dotc1z.WithTmpDir(tmpDir),
				dotc1z.WithReadOnly(true),
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
			actual, err := chaosoracle.ReadIdentities(ctx, store)
			require.NoError(t, err)
			require.NoError(t, chaosoracle.CompareIdentities(expected, actual))
		})
	}
}

func TestChaosConnectorGRPCSyncMatchesManifest(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "chaos-grpc.c1z")
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	harness := newChaosHarness(
		t, ctx, run, c1zPath, tmpDir, chaosTransportGRPC, WithWorkerCount(4),
	)
	harness.SyncAndClose(t, ctx)

	manifest, err := scenario.Manifest(scenario.InitialEpoch)
	require.NoError(t, err)
	assertChaosStoreMatches(t, c1zPath, tmpDir, chaosoracle.ExpectedIdentities(manifest))
}

func TestChaosConnectorRetryableErrorConverges(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "chaos-retry.c1z")
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "resources-unavailable-once",
		Match: chaosconnector.Matcher{
			Service: chaosconnector.ExactString("ResourcesService"),
			Method:  chaosconnector.ExactString("ListResources"),
			Attempt: 1,
			Phase:   chaosconnector.PhaseBeforeCall,
		},
		Effects: []chaosconnector.Effect{{
			Kind:    chaosconnector.EffectError,
			Code:    codes.Unavailable,
			Message: "injected transient",
		}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, chaosTransportDirect)
	harness.SyncAndClose(t, ctx)

	require.NoError(t, chaosoracle.VerifyTrace(run.Trace().Events(), chaosoracle.TraceExpectation{
		Name: "retryable ListResources retried",
		Match: chaosconnector.Matcher{
			Service: chaosconnector.ExactString("ResourcesService"),
			Method:  chaosconnector.ExactString("ListResources"),
		},
		Outcomes: []chaosconnector.Outcome{
			chaosconnector.OutcomeReturned,
			chaosconnector.OutcomeErrored,
		},
		Min: 2,
	}))

	manifest, err := scenario.Manifest(scenario.InitialEpoch)
	require.NoError(t, err)
	assertChaosStoreMatches(t, c1zPath, tmpDir, chaosoracle.ExpectedIdentities(manifest))
}

func TestChaosConnectorLostResponseConvergesWithoutDuplicateRows(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "chaos-lost-response.c1z")
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "lose-entitlements-after-delegate",
		Match: chaosconnector.Matcher{
			Service: chaosconnector.ExactString("EntitlementsService"),
			Method:  chaosconnector.ExactString("ListEntitlements"),
			Attempt: 1,
			Phase:   chaosconnector.PhaseAfterDelegate,
		},
		Effects:  []chaosconnector.Effect{{Kind: chaosconnector.EffectLoseResponse}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, chaosTransportDirect)
	harness.SyncAndClose(t, ctx)

	require.NoError(t, chaosoracle.VerifyTrace(run.Trace().Events(), chaosoracle.TraceExpectation{
		Name: "lost entitlement response was retried",
		Match: chaosconnector.Matcher{
			Service: chaosconnector.ExactString("EntitlementsService"),
			Method:  chaosconnector.ExactString("ListEntitlements"),
		},
		Outcomes: []chaosconnector.Outcome{
			chaosconnector.OutcomeReturned,
			chaosconnector.OutcomeErrored,
		},
		Min: 2,
	}))
	manifest, err := scenario.Manifest(scenario.InitialEpoch)
	require.NoError(t, err)
	assertChaosStoreMatches(t, c1zPath, tmpDir, chaosoracle.ExpectedIdentities(manifest))
}

func TestChaosConnectorUnknownResponseDataIsIgnored(t *testing.T) {
	for _, mutation := range []string{
		chaosconnector.MutationUnknownAnnotation,
		chaosconnector.MutationUnknownProtoField,
	} {
		t.Run(mutation, func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			c1zPath := filepath.Join(tmpDir, "chaos-unknown-response.c1z")
			scenario, err := chaosconnector.NewFullScenario()
			require.NoError(t, err)
			run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
				ID: "mutate-list-resources-" + mutation,
				Match: chaosconnector.Matcher{
					Service: chaosconnector.ExactString("ResourcesService"),
					Method:  chaosconnector.ExactString("ListResources"),
					Attempt: 1,
					Phase:   chaosconnector.PhaseBeforeResponse,
				},
				Effects:  []chaosconnector.Effect{{Kind: chaosconnector.EffectMutate, Mutation: mutation}},
				MinFires: 1,
				MaxFires: 1,
			}))
			require.NoError(t, err)
			harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, chaosTransportDirect)
			harness.SyncAndClose(t, ctx)

			manifest, err := scenario.Manifest(scenario.InitialEpoch)
			require.NoError(t, err)
			assertChaosStoreMatches(t, c1zPath, tmpDir, chaosoracle.ExpectedIdentities(manifest))
		})
	}
}

func TestChaosConnectorDuplicateResourceIsIdempotent(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "chaos-duplicate-resource.c1z")
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "duplicate-first-resource",
		Match: chaosconnector.Matcher{
			Service: chaosconnector.ExactString("ResourcesService"),
			Method:  chaosconnector.ExactString("ListResources"),
			Attempt: 1,
			Phase:   chaosconnector.PhaseBeforeResponse,
		},
		Effects: []chaosconnector.Effect{{
			Kind:     chaosconnector.EffectMutate,
			Mutation: chaosconnector.MutationDuplicateFirstItem,
		}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, chaosTransportDirect)
	harness.SyncAndClose(t, ctx)

	manifest, err := scenario.Manifest(scenario.InitialEpoch)
	require.NoError(t, err)
	assertChaosStoreMatches(t, c1zPath, tmpDir, chaosoracle.ExpectedIdentities(manifest))
}

func TestChaosConnectorEmptyResourceFailsWithoutSealing(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "chaos-empty-resource.c1z")
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "clear-first-resource",
		Match: chaosconnector.Matcher{
			Service: chaosconnector.ExactString("ResourcesService"),
			Method:  chaosconnector.ExactString("ListResources"),
			Attempt: 1,
			Phase:   chaosconnector.PhaseBeforeResponse,
		},
		Effects: []chaosconnector.Effect{{
			Kind:     chaosconnector.EffectMutate,
			Mutation: chaosconnector.MutationClearFirstItem,
		}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, chaosTransportDirect)
	syncErr := harness.Syncer.Sync(ctx)
	require.ErrorContains(t, syncErr, "resource with missing identity")
	require.Equal(t, codes.Internal, status.Code(syncErr))
	require.NoError(t, harness.Close(ctx))
	require.NoError(t, run.Runtime().VerifyRequired())

	store, err := dotc1z.NewStore(
		ctx,
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	latest, err := store.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.Nil(t, latest, "empty connector records must not seal a sync")
}

func TestChaosConnectorEmptyResourceTypeFailsWithoutSealing(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "chaos-empty-resource-type.c1z")
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "clear-first-resource-type",
		Match: chaosconnector.Matcher{
			Service: chaosconnector.ExactString("ResourceTypesService"),
			Method:  chaosconnector.ExactString("ListResourceTypes"),
			Attempt: 1,
			Phase:   chaosconnector.PhaseBeforeResponse,
		},
		Effects: []chaosconnector.Effect{{
			Kind:     chaosconnector.EffectMutate,
			Mutation: chaosconnector.MutationClearFirstItem,
		}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, chaosTransportDirect)
	syncErr := harness.Syncer.Sync(ctx)
	require.ErrorContains(t, syncErr, "resource type with missing identity")
	require.Equal(t, codes.Internal, status.Code(syncErr))
	require.NoError(t, harness.Close(ctx))
	require.NoError(t, run.Runtime().VerifyRequired())

	store, err := dotc1z.NewStore(
		ctx,
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	latest, err := store.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.Nil(t, latest, "an unkeyable resource type must not seal a sync")
}

func TestChaosConnectorDuplicateEnqueueAnnotationFailsWithoutSealing(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "chaos-duplicate-enqueue.c1z")
	scenario, err := chaosconnector.NewGeneratedSyncScenario(73)
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "duplicate-root-enqueue-annotation",
		Match: chaosconnector.Matcher{
			Service: chaosconnector.ExactString("EntitlementsService"),
			Method:  chaosconnector.ExactString("ListEntitlements"),
			Attempt: 1,
			Phase:   chaosconnector.PhaseBeforeResponse,
		},
		Effects: []chaosconnector.Effect{{
			Kind:     chaosconnector.EffectMutate,
			Mutation: chaosconnector.MutationDuplicateAnnotation,
		}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	harness := newChaosHarness(
		t, ctx, run, c1zPath, tmpDir, chaosTransportDirect, WithWorkerCount(4),
	)
	syncErr := harness.Syncer.Sync(ctx)
	require.ErrorContains(t, syncErr, "multiple EnqueuePageTokens annotations")
	require.NoError(t, harness.Close(ctx))
	require.NoError(t, run.Runtime().VerifyRequired())

	store, err := dotc1z.NewStore(
		ctx,
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	latest, err := store.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.Nil(t, latest, "invalid control annotations must not seal a sync")
}

func TestChaosConnectorCyclicPageTokensTerminateWithoutDuplicateRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "chaos-cyclic-pages.c1z")
	scenario, err := chaosconnector.NewGeneratedSyncScenario(19)
	require.NoError(t, err)
	dataset := scenario.Epochs[scenario.InitialEpoch]
	original := dataset.Entitlements[chaosconnector.FullCapabilityResourceTypeID]
	tokens := make([]string, 0, len(original))
	for token := range original {
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	slices.Sort(tokens)
	var records []*v2.Entitlement
	for _, token := range tokens {
		page := original[token]
		if len(page.List) != 0 {
			records = append(records, page.List[0])
			if len(records) == 2 {
				break
			}
		}
	}
	require.Len(t, records, 2)
	dataset.Entitlements[chaosconnector.FullCapabilityResourceTypeID] =
		chaosconnector.Pages[*v2.Entitlement]{
			"":        {Spawn: []string{"cycle-a"}},
			"cycle-a": {List: []*v2.Entitlement{records[0]}, Next: "cycle-b"},
			"cycle-b": {List: []*v2.Entitlement{records[1]}, Next: "cycle-a"},
		}

	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	harness := newChaosHarness(
		t, ctx, run, c1zPath, tmpDir, chaosTransportDirect,
		WithWorkerCount(4), WithSkipGrants(true),
	)
	harness.SyncAndClose(t, ctx)

	manifest, err := scenario.Manifest(scenario.InitialEpoch)
	require.NoError(t, err)
	expected := chaosoracle.ExpectedIdentities(manifest)
	expected.Grants = nil
	assertChaosStoreMatches(t, c1zPath, tmpDir, expected)
}

func TestChaosConnectorFatalErrorDoesNotSeal(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "chaos-fatal.c1z")
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "resources-fatal",
		Match: chaosconnector.Matcher{
			Service: chaosconnector.ExactString("ResourcesService"),
			Method:  chaosconnector.ExactString("ListResources"),
			Phase:   chaosconnector.PhaseBeforeCall,
		},
		Effects: []chaosconnector.Effect{{
			Kind:    chaosconnector.EffectError,
			Code:    codes.InvalidArgument,
			Message: "injected fatal",
		}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, chaosTransportDirect)
	syncErr := harness.Syncer.Sync(ctx)
	require.Error(t, syncErr)
	require.Equal(t, codes.InvalidArgument, status.Code(syncErr))
	require.NoError(t, harness.Close(ctx))
	require.NoError(t, run.Runtime().VerifyRequired())

	store, err := dotc1z.NewStore(
		ctx,
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	latest, err := store.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.Nil(t, latest, "fatal connector error must not produce a sealed full sync")
}

func TestChaosConnectorWarnAndDropIsTaggedAndExact(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "chaos-drop.c1z")
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	dataset := scenario.Epochs[scenario.InitialEpoch]
	root := dataset.Entitlements[chaosconnector.FullCapabilityResourceTypeID][""]
	require.Len(t, root.List, 1)
	entitlement := root.List[0]
	dropped := entitlement.GetId()
	root.List = nil
	root.Spawn = []string{"dropped"}
	dataset.Entitlements[chaosconnector.FullCapabilityResourceTypeID][""] = root
	dataset.Entitlements[chaosconnector.FullCapabilityResourceTypeID]["dropped"] =
		chaosconnector.Page[*v2.Entitlement]{List: []*v2.Entitlement{entitlement}}

	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule(chaosconnector.Rule{
		ID: "drop-spawned-entitlement-page",
		Match: chaosconnector.Matcher{
			Service:   chaosconnector.ExactString("EntitlementsService"),
			Method:    chaosconnector.ExactString("ListEntitlements"),
			PageToken: chaosconnector.ExactString("dropped"),
			Phase:     chaosconnector.PhaseBeforeCall,
		},
		Effects: []chaosconnector.Effect{{
			Kind:    chaosconnector.EffectError,
			Code:    codes.NotFound,
			Message: "injected declared skip",
		}},
		MinFires: 1,
		MaxFires: 1,
	}))
	require.NoError(t, err)
	harness := newChaosHarness(
		t, ctx, run, c1zPath, tmpDir, chaosTransportDirect, WithSkipGrants(true),
	)
	harness.SyncAndClose(t, ctx)

	manifest, err := scenario.Manifest(scenario.InitialEpoch)
	require.NoError(t, err)
	expected := chaosoracle.ExpectedIdentities(manifest)
	expected.Entitlements = removeString(expected.Entitlements, dropped)
	expected.Grants = nil
	assertChaosStoreMatches(t, c1zPath, tmpDir, expected)
}

func TestChaosConnectorMalformedEntitlementFailsCleanly(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "chaos-malformed-entitlement.c1z")
	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	var corpusCase chaosconnector.CorpusCase
	for _, candidate := range chaosconnector.InitialDataCorpus() {
		if candidate.Name == "entitlement-missing-resource" {
			corpusCase = candidate
			break
		}
	}
	require.NotNil(t, corpusCase.Apply, "entitlement-missing-resource corpus case must exist")
	require.Equal(t, chaosconnector.DataPolicyFail, corpusCase.Policy)
	require.NoError(t, corpusCase.Apply(scenario))

	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	harness := newChaosHarness(t, ctx, run, c1zPath, tmpDir, chaosTransportDirect)
	syncErr := harness.Syncer.Sync(ctx)
	require.ErrorContains(t, syncErr, "missing resource")
	require.NoError(t, harness.Close(ctx))
	store, err := dotc1z.NewStore(
		ctx,
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	latest, err := store.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.Nil(t, latest, "malformed connector data must not seal")
}

func TestChaosConnectorSeededFanoutWithRetries(t *testing.T) {
	iterations := 1
	if value := os.Getenv("BATON_CHAOS_ITERATIONS"); value != "" {
		parsed, err := strconv.Atoi(value)
		require.NoError(t, err)
		require.Positive(t, parsed)
		iterations = parsed
	}
	seedBase := int64(1)
	if value := os.Getenv("BATON_CHAOS_SEED"); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		require.NoError(t, err)
		seedBase = parsed
	}

	for i := range iterations {
		seed := seedBase + int64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			c1zPath := filepath.Join(tmpDir, "chaos-generated.c1z")
			scenario, err := chaosconnector.NewGeneratedSyncScenario(seed)
			require.NoError(t, err)
			run, err := chaosconnector.NewRun(scenario, chaosconnector.GeneratedRetrySchedule(scenario))
			require.NoError(t, err)
			harness := newChaosHarness(
				t, ctx, run, c1zPath, tmpDir, chaosTransportDirect, WithWorkerCount(4),
			)
			harness.SyncAndClose(t, ctx)

			manifest, err := scenario.Manifest(scenario.InitialEpoch)
			require.NoError(t, err)
			assertChaosStoreMatches(t, c1zPath, tmpDir, chaosoracle.ExpectedIdentities(manifest))
		})
	}
}

func assertChaosStoreMatches(
	t *testing.T,
	c1zPath string,
	tmpDir string,
	expected chaosoracle.IdentitySnapshot,
) {
	t.Helper()
	ctx := t.Context()
	store, err := dotc1z.NewStore(
		ctx,
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	actual, err := chaosoracle.ReadIdentities(ctx, store)
	require.NoError(t, err)
	require.NoError(t, chaosoracle.CompareIdentities(expected, actual))
}

func removeString(values []string, target string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != target {
			out = append(out, value)
		}
	}
	return out
}
