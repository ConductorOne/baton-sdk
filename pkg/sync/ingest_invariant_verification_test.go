package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

func TestIngestInvariantVerificationCoverageByEngine(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name       string
		engine     c1zstore.Engine
		syncType   connectorstore.SyncType
		compaction bool
		failFast   bool
		resources  bool
		want       []string
		wantMode   c1zstore.IngestInvariantVerificationMode
	}{
		{
			name:     "SQLite records its engine-independent I5 coverage",
			engine:   c1zstore.EngineSQLite,
			syncType: connectorstore.SyncTypeFull,
			want:     []string{"I5"},
			wantMode: c1zstore.IngestInvariantVerificationModeConnector,
		},
		{
			name:     "Pebble full records the referential family",
			engine:   c1zstore.EnginePebble,
			syncType: connectorstore.SyncTypeFull,
			want:     []string{"I5", "I7", "I3", "I8", "I9"},
			wantMode: c1zstore.IngestInvariantVerificationModeConnector,
		},
		{
			name:     "Pebble partial records only all-sync-type checks",
			engine:   c1zstore.EnginePebble,
			syncType: connectorstore.SyncTypePartial,
			want:     []string{"I5"},
			wantMode: c1zstore.IngestInvariantVerificationModeConnector,
		},
		{
			name:      "fail-fast records I4 when schedule evidence exists",
			engine:    c1zstore.EngineSQLite,
			syncType:  connectorstore.SyncTypeFull,
			failFast:  true,
			resources: true,
			want:      []string{"I5", "I4"},
			wantMode:  c1zstore.IngestInvariantVerificationModeConnectorFailFast,
		},
		{
			name:       "compaction mode is explicit",
			engine:     c1zstore.EnginePebble,
			syncType:   connectorstore.SyncTypeFull,
			compaction: true,
			want:       []string{"I5", "I7", "I3", "I8", "I9"},
			wantMode:   c1zstore.IngestInvariantVerificationModeCompactionMerge,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "verification.c1z")
			store, err := dotc1z.NewStore(ctx, path,
				dotc1z.WithEngine(tc.engine),
				dotc1z.WithTmpDir(tmpDir),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close(ctx) })

			syncID, err := store.StartNewSync(ctx, tc.syncType, "")
			require.NoError(t, err)
			s := &syncer{
				store:                 store,
				syncID:                syncID,
				syncType:              tc.syncType,
				compactionMergedStore: tc.compaction,
				failFastInvariants:    tc.failFast,
				resourcesPhaseRanHere: tc.resources,
			}
			require.NoError(t, s.runIngestionInvariants(ctx))
			// The production seam checkpoints after validation. Pebble must
			// preserve provenance while updating the sync token.
			require.NoError(t, store.CheckpointSync(ctx, "after-verification"))
			require.NoError(t, store.EndSync(ctx))
			// Production ordering: the marker is persisted only after the
			// seal, so a crash before EndSync leaves the sync unverified.
			require.NoError(t, s.persistIngestInvariantVerification(ctx))
			require.NoError(t, store.Close(ctx))

			store, err = dotc1z.NewStore(ctx, path,
				dotc1z.WithEngine(tc.engine),
				dotc1z.WithTmpDir(tmpDir),
			)
			require.NoError(t, err)

			run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
			require.NoError(t, err)
			require.NotNil(t, run)
			require.Equal(t, IngestInvariantGeneration, run.Generation)
			require.Equal(t, tc.want, run.Coverage)
			require.Equal(t, tc.wantMode, run.Mode)
		})
	}
}

// TestCrashBetweenInvariantPassAndSealLeavesNoMarker pins the marker's core
// promise: verification provenance must never be readable on an UNFINISHED
// sync. The seam between the invariant pass and the final checkpoint/EndSync
// is a real crash window (haltStageInvariantsComplete). A marker persisted
// before the seal would claim "verified" over a sync the resume machinery is
// about to rewrite wholesale (resume pushes InitOp and re-collects), and any
// consumer reading the crash image meanwhile would trust data that never
// sealed. The marker must therefore be written only after EndSync: a crash
// anywhere before the seal leaves the sync unverified (fail-closed).
func TestCrashBetweenInvariantPassAndSealLeavesNoMarker(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			ctx := context.Background()
			tempDir := t.TempDir()
			c1zPath := filepath.Join(tempDir, "marker-crash.c1z")

			mc := newMockConnector()
			mc.rtDB = append(mc.rtDB, userResourceType, groupResourceType)
			user, err := rs.NewUserResource("user1", userResourceType, "user1", nil)
			require.NoError(t, err)
			mc.AddResource(ctx, user)
			group, _, err := mc.AddGroup(ctx, "group1")
			require.NoError(t, err)
			mc.AddGroupMember(ctx, group, user)

			errCrash := errors.New("injected crash after invariant pass, before seal")
			crashed, err := NewSyncer(ctx, mc,
				WithC1ZPath(c1zPath),
				WithTmpDir(tempDir),
				WithStorageEngine(engine),
			)
			require.NoError(t, err)
			crashed.(*syncer).testIngestHaltHook = func(stage string) error {
				if stage == haltStageInvariantsComplete {
					return errCrash
				}
				return nil
			}
			require.ErrorIs(t, crashed.Sync(ctx), errCrash)
			require.NoError(t, crashed.Close(ctx))

			// The crash image: exactly one unfinished sync, and it must
			// read as UNVERIFIED.
			store, err := dotc1z.NewStore(ctx, c1zPath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tempDir))
			require.NoError(t, err)
			lister, ok := store.(interface {
				ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
			})
			require.True(t, ok)
			runs, _, err := lister.ListSyncRuns(ctx, "", 100)
			require.NoError(t, err)
			require.Len(t, runs, 1, "the crash image must hold exactly the one aborted sync")
			require.Nil(t, runs[0].EndedAt, "the aborted sync must be unfinished")
			require.False(t, runs[0].IsVerified(),
				"an unfinished sync must never carry verification provenance")
			require.NoError(t, store.Close(ctx))

			// The resume seals the sync and the marker lands with it —
			// the production mark point still fires end to end.
			resumed, err := NewSyncer(ctx, mc,
				WithC1ZPath(c1zPath),
				WithTmpDir(tempDir),
				WithStorageEngine(engine),
			)
			require.NoError(t, err)
			require.NoError(t, resumed.Sync(ctx))
			require.NoError(t, resumed.Close(ctx))

			store, err = dotc1z.NewStore(ctx, c1zPath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tempDir))
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close(ctx)) }()
			run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
			require.NoError(t, err)
			require.NotNil(t, run, "the resumed sync must have sealed")
			require.True(t, run.IsVerified(), "the sealed sync must carry the verification marker")
			require.Equal(t, IngestInvariantGeneration, run.Generation)
		})
	}
}

// grantFailingConnector fails ListGrants on demand so a re-collection
// can be aborted mid-flight, after resource/entitlement writes have
// already mutated the store.
type grantFailingConnector struct {
	*mockConnector
	failGrants bool
}

func (c *grantFailingConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	if c.failGrants {
		return nil, errors.New("injected connector failure during re-collection")
	}
	return c.mockConnector.ListGrants(ctx, in, opts...)
}

// TestRebindingSealedSyncClearsStaleVerification pins the rebind
// hazard: WithSyncID can bind an already-sealed, VERIFIED sync (the
// compactor's expansion pass does exactly this; a reused syncer can
// too) and re-collection then rewrites the data the marker vouched
// for. The stale proof must be invalidated at sync start, before any
// collection write — a run that aborts mid-collection must leave the
// sync unverified, not verified-over-rewritten-data.
func TestRebindingSealedSyncClearsStaleVerification(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			ctx := context.Background()
			tempDir := t.TempDir()
			c1zPath := filepath.Join(tempDir, "rebind.c1z")

			mc := newMockConnector()
			mc.rtDB = append(mc.rtDB, userResourceType, groupResourceType)
			user, err := rs.NewUserResource("user1", userResourceType, "user1", nil)
			require.NoError(t, err)
			mc.AddResource(ctx, user)
			group, _, err := mc.AddGroup(ctx, "group1")
			require.NoError(t, err)
			mc.AddGroupMember(ctx, group, user)
			conn := &grantFailingConnector{mockConnector: mc}

			// Run 1: a clean sync seals and marks.
			first, err := NewSyncer(ctx, conn,
				WithC1ZPath(c1zPath), WithTmpDir(tempDir), WithStorageEngine(engine))
			require.NoError(t, err)
			require.NoError(t, first.Sync(ctx))
			require.NoError(t, first.Close(ctx))

			sealedID := func() string {
				store, err := dotc1z.NewStore(ctx, c1zPath,
					dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tempDir))
				require.NoError(t, err)
				defer func() { require.NoError(t, store.Close(ctx)) }()
				run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
				require.NoError(t, err)
				require.NotNil(t, run)
				require.True(t, run.IsVerified(), "run 1 must have sealed verified (precondition)")
				return run.ID
			}()

			// Run 2: rebind the sealed sync and abort mid-collection,
			// after the store has already been rewritten under the marker.
			conn.failGrants = true
			second, err := NewSyncer(ctx, conn,
				WithC1ZPath(c1zPath), WithTmpDir(tempDir), WithStorageEngine(engine),
				WithSyncID(sealedID))
			require.NoError(t, err)
			require.Error(t, second.Sync(ctx), "the rebound run must abort at the injected failure")
			require.NoError(t, second.Close(ctx))

			store, err := dotc1z.NewStore(ctx, c1zPath,
				dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tempDir))
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close(ctx)) }()
			lister, ok := store.(interface {
				ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
			})
			require.True(t, ok)
			runs, _, err := lister.ListSyncRuns(ctx, "", 100)
			require.NoError(t, err)
			require.NotEmpty(t, runs)
			for _, run := range runs {
				if run.ID != sealedID {
					continue
				}
				require.False(t, run.IsVerified(),
					"a sealed sync whose data was rewritten by a rebound run must not still read as verified")
			}
		})
	}
}

func TestFailedIngestInvariantPassDoesNotWriteVerification(t *testing.T) {
	ctx := context.Background()
	store, syncID := newInvariantTestStore(ctx, t)

	r1, err := rs.NewResource("user1", userResourceType, "user1")
	require.NoError(t, err)
	r2, err := rs.NewResource("user2", userResourceType, "user2")
	require.NoError(t, err)
	require.NoError(t, store.PutEntitlements(ctx,
		et.NewPermissionEntitlement(r1, "viewer", et.WithExclusionGroupDefault("role", 1)),
		et.NewPermissionEntitlement(r2, "admin", et.WithExclusionGroupDefault("role", 2)),
	))

	// The stale-marker shape the pass-start clear exists for: a sealed,
	// MARKED sync rebound for a re-run (the writer refuses unfinished
	// syncs, so this is the only way a marker can pre-exist the pass).
	require.NoError(t, store.EndSync(ctx))
	verificationWriter, ok := store.SyncMeta().(c1zstore.IngestInvariantVerificationWriter)
	require.True(t, ok)
	require.NoError(t, verificationWriter.MarkIngestInvariantsVerified(ctx, syncID, c1zstore.IngestInvariantVerification{
		Generation: "old-generation",
		Coverage:   []string{"I5"},
		Mode:       c1zstore.IngestInvariantVerificationModeConnector,
	}))
	require.NoError(t, store.SetCurrentSync(ctx, syncID))

	s := &syncer{store: store, syncID: syncID, syncType: connectorstore.SyncTypeFull}
	require.Error(t, s.runIngestionInvariants(ctx))

	// The failed pass must have invalidated the inherited proof, and no
	// new verification may have been staged.
	require.Nil(t, s.pendingInvariantVerification)
	run, err := store.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.Empty(t, run.Generation)
	require.Empty(t, run.Coverage)
	require.Empty(t, run.Mode)
}
