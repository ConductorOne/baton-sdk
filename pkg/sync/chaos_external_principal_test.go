package sync

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
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

// DeleteGrantsByRefs puts this wrapper on the batched production path that
// processGrantsWithExternalPrincipals now prefers. Without it the type
// assertion for grantsByRefsBatchDeleter fails and the syncer silently falls
// back to the singular loop, so the crash-resume corpus would stop covering
// the code path it is meant to cut.
//
// The cut stays per-GRANT, counted in the same order and at the same
// granularity the singular DeleteGrantByRefs would use: deleteCalls advances
// once per grant, the failDeleteAt-th grant returns before its delete is
// issued, and every grant before it is committed by its own call into the
// real store. That keeps "everything before the cut is durable, the cut grant
// and everything after it is not" — the interruption shape the resume-to-
// baseline oracle asserts on — byte-identical to the pre-batching behavior.
//
// With no delete cut armed there is nothing to interleave, so the batch is
// handed to the store whole and actually exercises multi-grant chunking.
//
// The syncer resolves grantsByRefsBatchDeleter on THIS wrapper at attach, so
// declaring the method routes every engine here — including SQLite, which has no
// batched delete. Degrade to the wrapped store's singular delete in that case
// instead of advertising batch support it does not have, so the SQLite
// scenarios keep the one-commit-per-grant shape production would give them.
func (s *chaosExternalPrincipalCutStore) DeleteGrantsByRefs(
	ctx context.Context,
	grants ...*v2.Grant,
) error {
	batchDeleter, canBatch := s.Store.(grantsByRefsBatchDeleter)
	if canBatch && s.failDeleteAt <= 0 {
		s.deleteCalls.Add(int64(len(grants)))
		return batchDeleter.DeleteGrantsByRefs(ctx, grants...)
	}
	for _, grant := range grants {
		// Count and cut BEFORE resolving the wrapped store's capability, the
		// same order DeleteGrantByRefs uses. SQLite implements neither delete
		// interface, so its scenarios only ever get past this line for grants
		// the cut does not claim.
		if s.deleteCalls.Add(1) == s.failDeleteAt {
			return errChaosExternalPrincipalCut
		}
		var err error
		switch singularDeleter, canSingular := s.Store.(grantByRefsDeleter); {
		case canBatch:
			err = batchDeleter.DeleteGrantsByRefs(ctx, grant)
		case canSingular:
			err = singularDeleter.DeleteGrantByRefs(ctx, grant)
		default:
			err = errors.New("chaos: store lacks refs-based grant deletion")
		}
		if err != nil {
			return err
		}
	}
	return nil
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

// recordingBatchDeleteStore records the batches it is handed so the cut
// granularity above can be asserted without a chaos scenario.
type recordingBatchDeleteStore struct {
	c1zstore.Store
	batches [][]string
}

// SyncMeta answers the capability resolution setStore performs at attach; see
// legacyPaginatedCheckpointStore.SyncMeta.
func (s *recordingBatchDeleteStore) SyncMeta() c1zstore.SyncMeta { return nil }

// Grants answers the same attach-time capability resolution; see
// legacyPaginatedCheckpointStore.Grants.
func (s *recordingBatchDeleteStore) Grants() c1zstore.GrantStore { return nil }

func (s *recordingBatchDeleteStore) DeleteGrantsByRefs(_ context.Context, grants ...*v2.Grant) error {
	ids := make([]string, 0, len(grants))
	for _, grant := range grants {
		ids = append(ids, grant.GetId())
	}
	s.batches = append(s.batches, ids)
	return nil
}

// recordingSingularDeleteStore supports only the singular delete, standing in
// for an engine (SQLite) that never implements grantsByRefsBatchDeleter.
type recordingSingularDeleteStore struct {
	c1zstore.Store
	deleted []string
}

// SyncMeta answers the capability resolution setStore performs at attach; see
// legacyPaginatedCheckpointStore.SyncMeta.
func (s *recordingSingularDeleteStore) SyncMeta() c1zstore.SyncMeta { return nil }

// Grants answers the same attach-time capability resolution; see
// legacyPaginatedCheckpointStore.Grants.
func (s *recordingSingularDeleteStore) Grants() c1zstore.GrantStore { return nil }

func (s *recordingSingularDeleteStore) DeleteGrantByRefs(_ context.Context, grant *v2.Grant) error {
	s.deleted = append(s.deleted, grant.GetId())
	return nil
}

func chaosCutTestGrants(n int) []*v2.Grant {
	grants := make([]*v2.Grant, 0, n)
	for i := 0; i < n; i++ {
		grants = append(grants, v2.Grant_builder{Id: fmt.Sprintf("grant-%d", i)}.Build())
	}
	return grants
}

// TestChaosExternalPrincipalCutStoreBatchDeleteKeepsCutPerGrant guards the
// property the crash-resume corpus depends on but cannot itself observe: its
// fixtures only ever produce a single pending delete per call, so a batched
// wrapper that collapsed N grants into one opaque store call would still pass
// every scenario above while silently losing the per-grant cut point.
func TestChaosExternalPrincipalCutStoreBatchDeleteKeepsCutPerGrant(t *testing.T) {
	t.Run("cut armed stops between grants", func(t *testing.T) {
		underlying := &recordingBatchDeleteStore{}
		cutStore := &chaosExternalPrincipalCutStore{Store: underlying, failDeleteAt: 2}

		err := cutStore.DeleteGrantsByRefs(t.Context(), chaosCutTestGrants(3)...)

		require.ErrorIs(t, err, errChaosExternalPrincipalCut)
		require.Equal(t, int64(2), cutStore.deleteCalls.Load(),
			"the cut must be counted per grant, not per batch")
		require.Equal(t, [][]string{{"grant-0"}}, underlying.batches,
			"only grants before the cut may reach the store, each in its own durable call")
	})

	t.Run("no cut armed delegates the whole batch", func(t *testing.T) {
		underlying := &recordingBatchDeleteStore{}
		cutStore := &chaosExternalPrincipalCutStore{Store: underlying}

		require.NoError(t, cutStore.DeleteGrantsByRefs(t.Context(), chaosCutTestGrants(3)...))
		require.Equal(t, int64(3), cutStore.deleteCalls.Load())
		require.Equal(t, [][]string{{"grant-0", "grant-1", "grant-2"}}, underlying.batches,
			"an unarmed wrapper must exercise real multi-grant chunking")
	})

	// Declaring DeleteGrantsByRefs on the wrapper routes every engine through
	// it, so a store without batch support must degrade to its singular
	// delete rather than surface a "store lacks batching" error where the
	// SQLite scenarios expect the injected cut.
	t.Run("degrades to singular deletes without batch support", func(t *testing.T) {
		underlying := &recordingSingularDeleteStore{}
		cutStore := &chaosExternalPrincipalCutStore{Store: underlying, failDeleteAt: 2}

		err := cutStore.DeleteGrantsByRefs(t.Context(), chaosCutTestGrants(3)...)

		require.ErrorIs(t, err, errChaosExternalPrincipalCut,
			"the injected cut must still be what surfaces on a non-batching store")
		require.Equal(t, int64(2), cutStore.deleteCalls.Load())
		require.Equal(t, []string{"grant-0"}, underlying.deleted)
	})
}

func TestChaosConnectorExternalPrincipalCorpus(t *testing.T) {
	skipChaosInShort(t)
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
	skipChaosInShort(t)
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
	skipChaosInShort(t)
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
	skipChaosInShort(t)
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
