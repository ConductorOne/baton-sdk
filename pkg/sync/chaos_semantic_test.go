package sync //nolint:revive,nolintlint // backwards-compatible package name

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func TestChaosConnectorSemanticCorpus(t *testing.T) {
	for _, corpusCase := range chaosconnector.SemanticCorpus() {
		t.Run(corpusCase.Name, func(t *testing.T) {
			runSemanticCorpusCase(t, corpusCase)
		})
	}
}

func TestChaosConnectorRetryDriftCorpus(t *testing.T) {
	for _, corpusCase := range chaosconnector.TemporalCorpus() {
		t.Run(corpusCase.Name, func(t *testing.T) {
			runTemporalCorpusCase(t, corpusCase)
		})
	}
}

func TestChaosConnectorConcurrentDuplicateCompletionOrder(t *testing.T) {
	for _, corpusCase := range chaosconnector.ConcurrentDuplicateCorpus() {
		t.Run(corpusCase.Name, func(t *testing.T) {
			runConcurrentDuplicateCase(t, corpusCase)
		})
	}
}

func TestChaosConnectorConcurrentDuplicateResumeOrder(t *testing.T) {
	for _, corpusCase := range chaosconnector.ConcurrentDuplicateCorpus() {
		t.Run(corpusCase.Name, func(t *testing.T) {
			runConcurrentDuplicateResumeCase(t, corpusCase)
		})
	}
}

func runSemanticCorpusCase(t *testing.T, corpusCase chaosconnector.SemanticCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "semantic.c1z")

	scenario, err := chaosconnector.NewFullScenario()
	require.NoError(t, err)
	require.NoError(t, corpusCase.Apply(scenario))
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	builder, err := chaosconnector.NewBuilder(run)
	require.NoError(t, err)
	server, err := builder.Server(ctx)
	require.NoError(t, err)
	sdkSyncer, err := NewSyncer(
		ctx,
		chaosconnector.NewDirectClient(ctx, server, run),
		WithC1ZPath(c1zPath),
		WithTmpDir(tmpDir),
		WithStorageEngine(c1zstore.EnginePebble),
		WithDontExpandGrants(),
	)
	require.NoError(t, err)
	require.NoError(t, sdkSyncer.Sync(ctx))
	require.NoError(t, sdkSyncer.Close(t.Context()))
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
	require.NotNil(t, latest)
	assertSemanticExpectation(t, t.Context(), store, corpusCase.Expectation)
}

func runTemporalCorpusCase(t *testing.T, corpusCase chaosconnector.TemporalCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "temporal.c1z")

	scenario, err := corpusCase.Build()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, corpusCase.Schedule)
	require.NoError(t, err)
	builder, err := chaosconnector.NewBuilder(run)
	require.NoError(t, err)
	server, err := builder.Server(ctx)
	require.NoError(t, err)
	sdkSyncer, err := NewSyncer(
		ctx,
		chaosconnector.NewDirectClient(ctx, server, run),
		WithC1ZPath(c1zPath),
		WithTmpDir(tmpDir),
		WithStorageEngine(c1zstore.EnginePebble),
		WithDontExpandGrants(),
	)
	require.NoError(t, err)
	require.NoError(t, sdkSyncer.Sync(ctx))
	require.NoError(t, sdkSyncer.Close(t.Context()))
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

	assertSemanticExpectation(t, t.Context(), store, corpusCase.Expectation)
	if corpusCase.AbsentCanonicalIdentity != "" {
		assertCanonicalIdentityAbsent(
			t,
			t.Context(),
			store,
			corpusCase.Expectation.Entity,
			corpusCase.AbsentCanonicalIdentity,
		)
	}
}

func runConcurrentDuplicateCase(t *testing.T, corpusCase chaosconnector.ConcurrentDuplicateCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "concurrent-duplicate.c1z")

	scenario, err := chaosconnector.NewConcurrentDuplicateScenario()
	require.NoError(t, err)
	run, err := chaosconnector.NewRun(scenario, corpusCase.Schedule)
	require.NoError(t, err)
	defer func() {
		if t.Failed() {
			t.Logf("chaos trace: %+v", run.Trace().Events())
		}
	}()
	builder, err := chaosconnector.NewBuilder(run)
	require.NoError(t, err)
	server, err := builder.Server(ctx)
	require.NoError(t, err)
	sdkSyncer, err := NewSyncer(
		ctx,
		chaosconnector.NewDirectClient(ctx, server, run),
		WithC1ZPath(c1zPath),
		WithTmpDir(tmpDir),
		WithStorageEngine(c1zstore.EnginePebble),
		WithDontExpandGrants(),
		WithWorkerCount(2),
	)
	require.NoError(t, err)
	concreteSyncer, ok := sdkSyncer.(*syncer)
	require.True(t, ok)

	done := make(chan error, 1)
	go func() {
		done <- sdkSyncer.Sync(ctx)
	}()

	waitForConcurrentEntitlement(t, ctx, concreteSyncer, run, corpusCase.FirstToken)

	run.Runtime().ReleaseBarrier("release-" + corpusCase.BlockedToken)
	require.NoError(t, <-done)
	require.NoError(t, sdkSyncer.Close(t.Context()))
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
	assertSemanticExpectation(t, t.Context(), store, chaosconnector.SemanticExpectation{
		Entity:            chaosconnector.ReferentialEntitlement,
		CanonicalIdentity: "chaos-user:user-1:member",
		Multiplicity:      1,
		DisplayName:       corpusCase.ExpectedName,
	})
}

func runConcurrentDuplicateResumeCase(t *testing.T, corpusCase chaosconnector.ConcurrentDuplicateCase) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	tmpDir := t.TempDir()
	c1zPath := filepath.Join(tmpDir, "concurrent-duplicate-resume.c1z")

	scenario, err := chaosconnector.NewConcurrentDuplicateScenario()
	require.NoError(t, err)
	firstRun, err := chaosconnector.NewRun(scenario, corpusCase.CrashSchedule)
	require.NoError(t, err)
	builder, err := chaosconnector.NewBuilder(firstRun)
	require.NoError(t, err)
	server, err := builder.Server(ctx)
	require.NoError(t, err)
	firstSyncer, err := NewSyncer(
		ctx,
		chaosconnector.NewDirectClient(ctx, server, firstRun),
		WithC1ZPath(c1zPath),
		WithTmpDir(tmpDir),
		WithStorageEngine(c1zstore.EnginePebble),
		WithDontExpandGrants(),
		WithWorkerCount(2),
	)
	require.NoError(t, err)
	concreteFirst, ok := firstSyncer.(*syncer)
	require.True(t, ok)

	done := make(chan error, 1)
	go func() {
		done <- firstSyncer.Sync(ctx)
	}()
	waitForConcurrentEntitlement(t, ctx, concreteFirst, firstRun, corpusCase.FirstToken)
	firstRun.Runtime().ReleaseBarrier("release-" + corpusCase.BlockedToken)
	require.ErrorIs(t, <-done, chaosconnector.ErrCrashRequested)
	require.NoError(t, firstSyncer.Close(t.Context()))
	require.NoError(t, firstRun.Runtime().VerifyRequired())

	resumeScenario, err := chaosconnector.NewConcurrentDuplicateScenario()
	require.NoError(t, err)
	resumeRun, err := chaosconnector.NewRun(resumeScenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	resumeBuilder, err := chaosconnector.NewBuilder(resumeRun)
	require.NoError(t, err)
	resumeServer, err := resumeBuilder.Server(ctx)
	require.NoError(t, err)
	resumeSyncer, err := NewSyncer(
		ctx,
		chaosconnector.NewDirectClient(ctx, resumeServer, resumeRun),
		WithC1ZPath(c1zPath),
		WithTmpDir(tmpDir),
		WithStorageEngine(c1zstore.EnginePebble),
		WithDontExpandGrants(),
		WithWorkerCount(1),
	)
	require.NoError(t, err)
	require.NoError(t, resumeSyncer.Sync(ctx))
	require.NoError(t, resumeSyncer.Close(t.Context()))
	resumedTokens := make(map[string]bool)
	for _, event := range resumeRun.Trace().Events() {
		if event.Operation.Method == "ListEntitlements" && event.Outcome == chaosconnector.OutcomeReturned {
			resumedTokens[event.Operation.PageToken] = true
		}
	}
	require.True(t, resumedTokens["left"], "resume did not replay left sibling")
	require.True(t, resumedTokens["right"], "resume did not replay right sibling")

	store, err := dotc1z.NewStore(
		t.Context(),
		c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(t.Context())) }()
	assertSemanticExpectation(t, t.Context(), store, chaosconnector.SemanticExpectation{
		Entity:            chaosconnector.ReferentialEntitlement,
		CanonicalIdentity: "chaos-user:user-1:member",
		Multiplicity:      1,
		DisplayName:       corpusCase.ResumeExpectedName,
	})
}

func waitForConcurrentEntitlement(
	t *testing.T,
	ctx context.Context,
	concreteSyncer *syncer,
	run *chaosconnector.Run,
	token string,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, event := range run.Trace().Events() {
			if event.Operation.Method == "ListEntitlements" &&
				event.Operation.PageToken == token &&
				event.Outcome == chaosconnector.OutcomeReturned {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "unblocked sibling did not return")

	expectedName := "Concurrent observation from " + token
	require.Eventually(t, func() bool {
		response, err := concreteSyncer.store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
			EntitlementId: "chaos-user:user-1:member",
		}.Build())
		return err == nil && response != nil && response.GetEntitlement() != nil &&
			response.GetEntitlement().GetDisplayName() == expectedName
	}, 5*time.Second, 10*time.Millisecond, "unblocked sibling was not persisted")
}

func assertSemanticExpectation(
	t *testing.T,
	ctx context.Context,
	store c1zstore.Store,
	expectation chaosconnector.SemanticExpectation,
) {
	t.Helper()
	switch expectation.Entity {
	case chaosconnector.ReferentialResource:
		response, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{}.Build())
		require.NoError(t, err)
		var matches []*v2.Resource
		for _, item := range response.GetList() {
			if resourceIdentityKey(item.GetId()) == expectation.CanonicalIdentity {
				matches = append(matches, item)
			}
		}
		require.Len(t, matches, expectation.Multiplicity)
		require.Equal(t, expectation.DisplayName, matches[0].GetDisplayName())
		if expectation.ParentIdentity != "" {
			require.Equal(t, expectation.ParentIdentity, resourceIdentityKey(matches[0].GetParentResourceId()))
		}
	case chaosconnector.ReferentialEntitlement:
		response, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
		require.NoError(t, err)
		var matches []*v2.Entitlement
		for _, item := range response.GetList() {
			if item.GetId() == expectation.CanonicalIdentity {
				matches = append(matches, item)
			}
		}
		require.Len(t, matches, expectation.Multiplicity)
		require.Equal(t, expectation.DisplayName, matches[0].GetDisplayName())
	case chaosconnector.ReferentialGrant:
		response, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
		require.NoError(t, err)
		var matches []*v2.Grant
		for _, item := range response.GetList() {
			identity := item.GetEntitlement().GetId() + "\x00" + resourceIdentityKey(item.GetPrincipal().GetId())
			if identity == expectation.CanonicalIdentity {
				matches = append(matches, item)
			}
		}
		require.Len(t, matches, expectation.Multiplicity)
		require.Equal(t, expectation.ExternalID, matches[0].GetId())
	}
}

func assertCanonicalIdentityAbsent(
	t *testing.T,
	ctx context.Context,
	store c1zstore.Store,
	entity chaosconnector.ReferentialEntity,
	identity string,
) {
	t.Helper()
	switch entity {
	case chaosconnector.ReferentialResource:
		response, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{}.Build())
		require.NoError(t, err)
		for _, item := range response.GetList() {
			require.NotEqual(t, identity, resourceIdentityKey(item.GetId()))
		}
	case chaosconnector.ReferentialEntitlement:
		response, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
		require.NoError(t, err)
		for _, item := range response.GetList() {
			require.NotEqual(t, identity, item.GetId())
		}
	case chaosconnector.ReferentialGrant:
		response, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
		require.NoError(t, err)
		for _, item := range response.GetList() {
			actual := item.GetEntitlement().GetId() + "\x00" + resourceIdentityKey(item.GetPrincipal().GetId())
			require.NotEqual(t, identity, actual)
		}
	}
}

func resourceIdentityKey(id *v2.ResourceId) string {
	if id == nil {
		return ""
	}
	return id.GetResourceType() + "\x00" + id.GetResource()
}
