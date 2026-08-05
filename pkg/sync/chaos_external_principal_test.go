package sync

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	"github.com/conductorone/baton-sdk/internal/testtier"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
)

var errChaosExternalPrincipalCut = errors.New("chaos: external principal rewrite cut")

type chaosExternalPrincipalCutStore struct {
	c1zstore.Store
	failDeleteAt      int64
	deleteCalls       atomic.Int64
	failEntitlementAt int64
	entitlementCalls  atomic.Int64
	failResourceAt    int64
	resourceCalls     atomic.Int64
}

func (s *chaosExternalPrincipalCutStore) DeleteGrantByRefs(
	ctx context.Context,
	grant *v2.Grant,
) error {
	if s.deleteCalls.Add(1) == s.failDeleteAt {
		return errChaosExternalPrincipalCut
	}
	deleter, ok := s.Store.(grantByRefsDeleter)
	if !ok {
		return errors.New("chaos: store lacks refs-based grant deletion")
	}
	return deleter.DeleteGrantByRefs(ctx, grant)
}

func (s *chaosExternalPrincipalCutStore) DeleteResourceRecord(
	ctx context.Context,
	resourceTypeID string,
	resourceID string,
) error {
	if s.resourceCalls.Add(1) == s.failResourceAt {
		return errChaosExternalPrincipalCut
	}
	deleter, ok := s.Store.(resourceRecordDeleter)
	if !ok {
		return errors.New("chaos: store lacks resource-record deletion")
	}
	return deleter.DeleteResourceRecord(ctx, resourceTypeID, resourceID)
}

func (s *chaosExternalPrincipalCutStore) DeleteEntitlementByRefs(
	ctx context.Context,
	entitlement *v2.Entitlement,
) error {
	if s.entitlementCalls.Add(1) == s.failEntitlementAt {
		return errChaosExternalPrincipalCut
	}
	deleter, ok := s.Store.(entitlementRecordDeleter)
	if !ok {
		return errors.New("chaos: store lacks entitlement-record deletion")
	}
	return deleter.DeleteEntitlementByRefs(ctx, entitlement)
}

func TestChaosConnectorExternalPrincipalCorpus(t *testing.T) {
	testtier.RequireNightly(t)
	for _, corpusCase := range chaosconnector.ExternalPrincipalCorpus() {
		for _, transport := range []chaosTransport{chaosTransportDirect, chaosTransportGRPC} {
			t.Run(corpusCase.Name+"/"+transport.String(), func(t *testing.T) {
				ctx := t.Context()
				tmpDir := t.TempDir()
				externalPath := filepath.Join(tmpDir, "external.c1z")
				internalPath := filepath.Join(tmpDir, "internal.c1z")

				externalScenario, internalScenario, err := corpusCase.Build()
				require.NoError(t, err)
				externalRun, err := chaosconnector.NewRun(
					externalScenario,
					chaosconnector.NewSchedule(),
				)
				require.NoError(t, err)
				externalHarness := newChaosHarness(
					t,
					ctx,
					externalRun,
					externalPath,
					tmpDir,
					chaosTransportDirect,
					WithWorkerCount(1),
				)
				externalHarness.SyncAndClose(t, ctx)
				if corpusCase.Name == "external-principal/group-profile-case-fold" {
					externalStore, openErr := dotc1z.NewStore(
						ctx,
						externalPath,
						dotc1z.WithEngine(c1zstore.EnginePebble),
						dotc1z.WithTmpDir(tmpDir),
						dotc1z.WithReadOnly(true),
					)
					require.NoError(t, openErr)
					response, listErr := externalStore.ListResources(
						ctx,
						v2.ResourcesServiceListResourcesRequest_builder{
							ResourceTypeId: chaosconnector.ExternalGroupTypeID,
						}.Build(),
					)
					require.NoError(t, listErr)
					require.Len(t, response.GetList(), 1)
					value, present := rs.GetProfileStringValue(
						rs.GetProfile(response.GetList()[0]),
						corpusCase.Key,
					)
					require.True(t, present)
					require.Equal(t, corpusCase.Value, value)
					require.NoError(t, externalStore.Close(ctx))
				}

				internalRun, err := chaosconnector.NewRun(
					internalScenario,
					chaosconnector.NewSchedule(),
				)
				require.NoError(t, err)
				internalHarness := newChaosHarness(
					t,
					ctx,
					internalRun,
					internalPath,
					tmpDir,
					transport,
					WithWorkerCount(1),
					WithExternalResourceC1ZPath(externalPath),
				)
				internalHarness.SyncAndClose(t, ctx)

				manifest, err := internalScenario.Manifest(internalScenario.InitialEpoch)
				require.NoError(t, err)
				require.Len(t, manifest.Entitlements, 1)

				store, err := dotc1z.NewStore(
					ctx,
					internalPath,
					dotc1z.WithEngine(c1zstore.EnginePebble),
					dotc1z.WithTmpDir(tmpDir),
					dotc1z.WithReadOnly(true),
				)
				require.NoError(t, err)
				defer func() { require.NoError(t, store.Close(ctx)) }()
				observation, err := chaosoracle.ReadExternalPrincipal(
					ctx,
					store,
					manifest.Entitlements[0].GetId(),
					chaosconnector.ExternalPlaceholderID,
				)
				require.NoError(t, err)
				require.NoError(t, chaosoracle.CompareExternalPrincipal(
					externalPrincipalExpectation(t, corpusCase, externalScenario),
					observation,
				))
			})
		}
	}
}

func TestChaosConnectorExternalPrincipalCorpusResumesAfterRewriteCut(t *testing.T) {
	testtier.RequireNightly(t)
	for _, corpusCase := range chaosconnector.ExternalPrincipalCorpus() {
		t.Run(corpusCase.Name, func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			externalPath := filepath.Join(tmpDir, "external.c1z")
			internalPath := filepath.Join(tmpDir, "internal.c1z")

			externalScenario, internalScenario, err := corpusCase.Build()
			require.NoError(t, err)
			runExternalPrincipalSource(t, externalScenario, externalPath, tmpDir)
			baselineContent := runExternalPrincipalInternalBaseline(
				t,
				internalScenario,
				externalPath,
				filepath.Join(tmpDir, "internal-baseline.c1z"),
				tmpDir,
			)

			internalStore, err := dotc1z.NewStore(
				ctx,
				internalPath,
				dotc1z.WithEngine(c1zstore.EnginePebble),
				dotc1z.WithTmpDir(tmpDir),
			)
			require.NoError(t, err)
			cutStore := &chaosExternalPrincipalCutStore{
				Store:        internalStore,
				failDeleteAt: 1,
			}
			cutRun, err := chaosconnector.NewRun(
				internalScenario,
				chaosconnector.NewSchedule(),
			)
			require.NoError(t, err)
			cutHarness := newChaosHarness(
				t,
				ctx,
				cutRun,
				internalPath,
				tmpDir,
				chaosTransportDirect,
				WithWorkerCount(1),
				WithExternalResourceC1ZPath(externalPath),
				WithConnectorStore(cutStore),
			)
			require.ErrorIs(t, cutHarness.Syncer.Sync(ctx), errChaosExternalPrincipalCut)
			require.NoError(t, cutHarness.Close(ctx))
			interruptedRuns := readChaosSyncRuns(t, ctx, internalPath, tmpDir)
			require.Len(t, interruptedRuns, 1)
			require.Nil(t, interruptedRuns[0].EndedAt)
			interruptedSyncID := interruptedRuns[0].ID

			manifest, err := internalScenario.Manifest(internalScenario.InitialEpoch)
			require.NoError(t, err)
			require.Len(t, manifest.Entitlements, 1)
			interrupted := readExternalPrincipalObservation(
				t,
				internalPath,
				tmpDir,
				manifest.Entitlements[0].GetId(),
			)
			require.False(t, interrupted.Sealed)
			require.Equal(t, 1, interrupted.CarrierCount)

			resumeRun, err := chaosconnector.NewRun(
				internalScenario,
				chaosconnector.NewSchedule(),
			)
			require.NoError(t, err)
			resumeHarness := newChaosHarness(
				t,
				ctx,
				resumeRun,
				internalPath,
				tmpDir,
				chaosTransportDirect,
				WithWorkerCount(1),
				WithExternalResourceC1ZPath(externalPath),
			)
			resumeHarness.SyncAndClose(t, ctx)

			final := readExternalPrincipalObservation(
				t,
				internalPath,
				tmpDir,
				manifest.Entitlements[0].GetId(),
			)
			require.NoError(t, chaosoracle.CompareExternalPrincipal(
				externalPrincipalExpectation(t, corpusCase, externalScenario),
				final,
			))
			finalRuns := readChaosSyncRuns(t, ctx, internalPath, tmpDir)
			require.Len(t, finalRuns, 1)
			require.NotNil(t, finalRuns[0].EndedAt)
			require.Equal(t, interruptedSyncID, finalRuns[0].ID)
			finalContent := readChaosLogicalContent(t, ctx, internalPath, tmpDir)
			require.NoError(t, chaosoracle.CompareLogicalContent(baselineContent, finalContent),
				"resumed external-principal rewrite must equal uninterrupted execution")
		})
	}
}

func TestChaosConnectorExternalPrincipalResumeUsesCurrentExternalAnswer(t *testing.T) {
	var corpusCase chaosconnector.ExternalPrincipalCase
	for _, candidate := range chaosconnector.ExternalPrincipalCorpus() {
		if candidate.Name == "external-principal/all-users" {
			corpusCase = candidate
			break
		}
	}
	require.NotEmpty(t, corpusCase.Name)

	ctx := t.Context()
	tmpDir := t.TempDir()
	firstExternalPath := filepath.Join(tmpDir, "external-first.c1z")
	resumeExternalPath := filepath.Join(tmpDir, "external-resume.c1z")
	internalPath := filepath.Join(tmpDir, "internal.c1z")

	firstExternal, internalScenario, err := corpusCase.Build()
	require.NoError(t, err)
	runExternalPrincipalSource(t, firstExternal, firstExternalPath, tmpDir)

	internalStore, err := dotc1z.NewStore(
		ctx,
		internalPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	cutStore := &chaosExternalPrincipalCutStore{
		Store:        internalStore,
		failDeleteAt: 1,
	}
	cutRun, err := chaosconnector.NewRun(internalScenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	cutHarness := newChaosHarness(
		t,
		ctx,
		cutRun,
		internalPath,
		tmpDir,
		chaosTransportDirect,
		WithWorkerCount(1),
		WithExternalResourceC1ZPath(firstExternalPath),
		WithConnectorStore(cutStore),
	)
	require.ErrorIs(t, cutHarness.Syncer.Sync(ctx), errChaosExternalPrincipalCut)
	require.NoError(t, cutHarness.Close(ctx))
	seedStaleExternalPrincipalDependencies(t, internalPath, tmpDir)

	resumeExternal, _, err := corpusCase.Build()
	require.NoError(t, err)
	resumeDataset := resumeExternal.Epochs[resumeExternal.InitialEpoch]
	users := resumeDataset.Resources[chaosconnector.ExternalUserTypeID][""]
	require.Len(t, users.List, 2)
	users.List = users.List[1:]
	resumeDataset.Resources[chaosconnector.ExternalUserTypeID][""] = users
	runExternalPrincipalSource(t, resumeExternal, resumeExternalPath, tmpDir)
	baselineContent := runExternalPrincipalInternalBaseline(
		t,
		internalScenario,
		resumeExternalPath,
		filepath.Join(tmpDir, "internal-current-answer-baseline.c1z"),
		tmpDir,
	)
	interruptedRuns := readChaosSyncRuns(t, ctx, internalPath, tmpDir)
	require.Len(t, interruptedRuns, 1)
	require.Nil(t, interruptedRuns[0].EndedAt)
	interruptedSyncID := interruptedRuns[0].ID

	resumeStore, err := dotc1z.NewStore(
		ctx,
		internalPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	dependencyCutStore := &chaosExternalPrincipalCutStore{
		Store:        resumeStore,
		failDeleteAt: 2,
	}
	resumeRun, err := chaosconnector.NewRun(internalScenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	resumeHarness := newChaosHarness(
		t,
		ctx,
		resumeRun,
		internalPath,
		tmpDir,
		chaosTransportDirect,
		WithWorkerCount(1),
		WithExternalResourceC1ZPath(resumeExternalPath),
		WithConnectorStore(dependencyCutStore),
	)
	require.ErrorIs(t, resumeHarness.Syncer.Sync(ctx), errChaosExternalPrincipalCut)
	require.NoError(t, resumeHarness.Close(ctx))
	require.Equal(t, int64(2), dependencyCutStore.deleteCalls.Load(),
		"dependency cleanup cut must occur after a committed grant deletion")
	partiallyCleanedRuns := readChaosSyncRuns(t, ctx, internalPath, tmpDir)
	require.Len(t, partiallyCleanedRuns, 1)
	require.Equal(t, interruptedSyncID, partiallyCleanedRuns[0].ID)
	require.Nil(t, partiallyCleanedRuns[0].EndedAt)

	entitlementCutStore := runExternalPrincipalCleanupCut(
		t, internalScenario, internalPath, resumeExternalPath, tmpDir,
		&chaosExternalPrincipalCutStore{failEntitlementAt: 2},
	)
	require.Equal(t, int64(2), entitlementCutStore.entitlementCalls.Load(),
		"entitlement cleanup cut must occur after a committed entitlement deletion")
	resourceCutStore := runExternalPrincipalCleanupCut(
		t, internalScenario, internalPath, resumeExternalPath, tmpDir,
		&chaosExternalPrincipalCutStore{failResourceAt: 1},
	)
	require.Equal(t, int64(1), resourceCutStore.resourceCalls.Load(),
		"resource cleanup cut must fire before the stale principal deletion")

	finalResumeRun, err := chaosconnector.NewRun(internalScenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	finalResumeHarness := newChaosHarness(
		t,
		ctx,
		finalResumeRun,
		internalPath,
		tmpDir,
		chaosTransportDirect,
		WithWorkerCount(1),
		WithExternalResourceC1ZPath(resumeExternalPath),
	)
	finalResumeHarness.SyncAndClose(t, ctx)

	manifest, err := internalScenario.Manifest(internalScenario.InitialEpoch)
	require.NoError(t, err)
	require.Len(t, manifest.Entitlements, 1)
	final := readExternalPrincipalObservation(
		t,
		internalPath,
		tmpDir,
		manifest.Entitlements[0].GetId(),
	)
	require.NoError(t, chaosoracle.CompareExternalPrincipal(
		chaosoracle.ExternalPrincipalExpectation{
			PrincipalIDs:  []string{"external-user-2"},
			RequireSealed: true,
		},
		final,
	))
	finalRuns := readChaosSyncRuns(t, ctx, internalPath, tmpDir)
	require.Len(t, finalRuns, 1)
	require.NotNil(t, finalRuns[0].EndedAt)
	require.Equal(t, interruptedSyncID, finalRuns[0].ID)
	finalContent := readChaosLogicalContent(t, ctx, internalPath, tmpDir)
	require.NoError(t, chaosoracle.CompareLogicalContent(baselineContent, finalContent),
		"resume against changed external data must equal a clean run against that data")
}

func TestChaosConnectorSQLiteExternalPrincipalResumeDegradesWithoutFailure(t *testing.T) {
	var corpusCase chaosconnector.ExternalPrincipalCase
	for _, candidate := range chaosconnector.ExternalPrincipalCorpus() {
		if candidate.Name == "external-principal/id-match" {
			corpusCase = candidate
			break
		}
	}
	require.NotEmpty(t, corpusCase.Name)

	ctx := t.Context()
	tmpDir := t.TempDir()
	firstExternalPath := filepath.Join(tmpDir, "sqlite-external-first.c1z")
	resumeExternalPath := filepath.Join(tmpDir, "sqlite-external-resume.c1z")
	internalPath := filepath.Join(tmpDir, "sqlite-internal.c1z")
	firstExternal, internalScenario, err := corpusCase.Build()
	require.NoError(t, err)
	runExternalPrincipalSource(t, firstExternal, firstExternalPath, tmpDir)

	internalStore, err := dotc1z.NewStore(
		ctx,
		internalPath,
		dotc1z.WithEngine(c1zstore.EngineSQLite),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	cutStore := &chaosExternalPrincipalCutStore{Store: internalStore, failDeleteAt: 1}
	firstRun, err := chaosconnector.NewRun(internalScenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	firstHarness := newChaosHarness(
		t, ctx, firstRun, internalPath, tmpDir, chaosTransportDirect,
		WithWorkerCount(1),
		WithExternalResourceC1ZPath(firstExternalPath),
		WithConnectorStore(cutStore),
	)
	require.ErrorIs(t, firstHarness.Syncer.Sync(ctx), errChaosExternalPrincipalCut)
	require.NoError(t, firstHarness.Close(ctx))

	interruptedStore, err := dotc1z.NewStore(
		ctx,
		internalPath,
		dotc1z.WithEngine(c1zstore.EngineSQLite),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	interruptedLister, ok := interruptedStore.(interface {
		ListSyncRuns(context.Context, string, uint32) ([]*c1zstore.SyncRun, string, error)
	})
	require.True(t, ok)
	interruptedRuns, _, err := interruptedLister.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, interruptedRuns, 1)
	require.Nil(t, interruptedRuns[0].EndedAt)
	interruptedSyncID := interruptedRuns[0].ID
	require.NoError(t, interruptedStore.Close(ctx))

	resumeExternal, _, err := corpusCase.Build()
	require.NoError(t, err)
	resumeDataset := resumeExternal.Epochs[resumeExternal.InitialEpoch]
	users := resumeDataset.Resources[chaosconnector.ExternalUserTypeID][""]
	require.Len(t, users.List, 2)
	users.List = users.List[1:]
	resumeDataset.Resources[chaosconnector.ExternalUserTypeID][""] = users
	runExternalPrincipalSource(t, resumeExternal, resumeExternalPath, tmpDir)

	resumeRun, err := chaosconnector.NewRun(internalScenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	resumeHarness := newChaosHarness(
		t, ctx, resumeRun, internalPath, tmpDir, chaosTransportDirect,
		WithWorkerCount(1),
		WithExternalResourceC1ZPath(resumeExternalPath),
		WithStorageEngine(c1zstore.EngineSQLite),
	)
	resumeHarness.SyncAndClose(t, ctx)

	finalStore, err := dotc1z.NewStore(
		ctx,
		internalPath,
		dotc1z.WithEngine(c1zstore.EngineSQLite),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, finalStore.Close(ctx)) }()
	finalLister, ok := finalStore.(interface {
		ListSyncRuns(context.Context, string, uint32) ([]*c1zstore.SyncRun, string, error)
	})
	require.True(t, ok)
	finalRuns, _, err := finalLister.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	require.Len(t, finalRuns, 1)
	require.Equal(t, interruptedSyncID, finalRuns[0].ID)
	require.NotNil(t, finalRuns[0].EndedAt)
	resources, err := finalStore.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{}.Build())
	require.NoError(t, err)
	var currentPrincipalPresent bool
	for _, resource := range resources.GetList() {
		id := resource.GetId()
		if id.GetResourceType() == chaosconnector.ExternalUserTypeID &&
			id.GetResource() == "external-user-2" {
			currentPrincipalPresent = true
			break
		}
	}
	require.True(t, currentPrincipalPresent,
		"SQLite degradation must still ingest the current external answer")
}

func runExternalPrincipalCleanupCut(
	t *testing.T,
	scenario *chaosconnector.Scenario,
	internalPath string,
	externalPath string,
	tmpDir string,
	cutStore *chaosExternalPrincipalCutStore,
) *chaosExternalPrincipalCutStore {
	t.Helper()
	ctx := t.Context()
	store, err := dotc1z.NewStore(
		ctx,
		internalPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	cutStore.Store = store
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	harness := newChaosHarness(
		t,
		ctx,
		run,
		internalPath,
		tmpDir,
		chaosTransportDirect,
		WithWorkerCount(1),
		WithExternalResourceC1ZPath(externalPath),
		WithConnectorStore(cutStore),
	)
	require.ErrorIs(t, harness.Syncer.Sync(ctx), errChaosExternalPrincipalCut)
	require.NoError(t, harness.Close(ctx))
	return cutStore
}

func seedStaleExternalPrincipalDependencies(t *testing.T, path string, tmpDir string) {
	t.Helper()
	ctx := t.Context()
	store, err := dotc1z.NewStore(
		ctx,
		path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	principal := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: chaosconnector.ExternalUserTypeID,
			Resource:     "external-user-1",
		}.Build(),
		DisplayName: "Stale external principal",
	}.Build()
	staleEntitlement := v2.Entitlement_builder{
		Id:          "stale-external-entitlement",
		DisplayName: "Stale external entitlement",
		Resource:    principal,
	}.Build()
	staleGrant := v2.Grant_builder{
		Id:          "stale-external-grant",
		Entitlement: staleEntitlement,
		Principal:   principal,
	}.Build()
	secondEntitlement := v2.Entitlement_builder{
		Id:          "stale-external-entitlement-2",
		DisplayName: "Second stale external entitlement",
		Resource:    principal,
	}.Build()
	secondGrant := v2.Grant_builder{
		Id:          "stale-external-grant-2",
		Entitlement: secondEntitlement,
		Principal:   principal,
	}.Build()
	require.NoError(t, store.PutEntitlements(ctx, staleEntitlement, secondEntitlement))
	require.NoError(t, store.PutGrants(ctx, staleGrant, secondGrant))
}

func runExternalPrincipalInternalBaseline(
	t *testing.T,
	scenario *chaosconnector.Scenario,
	externalPath string,
	internalPath string,
	tmpDir string,
) chaosoracle.LogicalContentSnapshot {
	t.Helper()
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	harness := newChaosHarness(
		t,
		t.Context(),
		run,
		internalPath,
		tmpDir,
		chaosTransportDirect,
		WithWorkerCount(1),
		WithExternalResourceC1ZPath(externalPath),
	)
	harness.SyncAndClose(t, t.Context())
	return readChaosLogicalContent(t, t.Context(), internalPath, tmpDir)
}

func runExternalPrincipalSource(
	t *testing.T,
	scenario *chaosconnector.Scenario,
	path string,
	tmpDir string,
) {
	t.Helper()
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	harness := newChaosHarness(
		t,
		t.Context(),
		run,
		path,
		tmpDir,
		chaosTransportDirect,
		WithWorkerCount(1),
	)
	harness.SyncAndClose(t, t.Context())
}

func externalPrincipalExpectation(
	t *testing.T,
	corpusCase chaosconnector.ExternalPrincipalCase,
	externalScenario *chaosconnector.Scenario,
) chaosoracle.ExternalPrincipalExpectation {
	t.Helper()
	expectation := chaosoracle.ExternalPrincipalExpectation{
		PrincipalIDs:  corpusCase.ExpectedPrincipalIDs,
		RequireSealed: true,
	}
	if !corpusCase.Expandable {
		return expectation
	}
	manifest, err := externalScenario.Manifest(externalScenario.InitialEpoch)
	require.NoError(t, err)
	for _, entitlement := range manifest.Entitlements {
		if entitlement.GetResource().GetId().GetResource() == corpusCase.Value {
			expectation.ExpandableEntitlementIDs = append(
				expectation.ExpandableEntitlementIDs,
				entitlement.GetId(),
			)
		}
	}
	require.NotEmpty(t, expectation.ExpandableEntitlementIDs)
	return expectation
}

func readExternalPrincipalObservation(
	t *testing.T,
	path string,
	tmpDir string,
	entitlementID string,
) chaosoracle.ExternalPrincipalObservation {
	t.Helper()
	store, err := dotc1z.NewStore(
		t.Context(),
		path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(t.Context())) }()
	observation, err := chaosoracle.ReadExternalPrincipal(
		t.Context(),
		store,
		entitlementID,
		chaosconnector.ExternalPlaceholderID,
	)
	require.NoError(t, err)
	return observation
}
