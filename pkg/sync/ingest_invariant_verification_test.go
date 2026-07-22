package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

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

	s := &syncer{store: store, syncID: syncID, syncType: connectorstore.SyncTypeFull}
	verificationWriter, ok := store.SyncMeta().(c1zstore.IngestInvariantVerificationWriter)
	require.True(t, ok)
	require.NoError(t, verificationWriter.MarkIngestInvariantsVerified(ctx, syncID, c1zstore.IngestInvariantVerification{
		Generation: "old-generation",
		Coverage:   []string{"I5"},
		Mode:       c1zstore.IngestInvariantVerificationModeConnector,
	}))
	require.Error(t, s.runIngestionInvariants(ctx))

	// Seal manually to make the absence observable. Production returns before
	// EndSync on this error, so a violating run is never published.
	require.NoError(t, store.EndSync(ctx))
	run, err := store.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, run)
	require.Empty(t, run.Generation)
	require.Empty(t, run.Coverage)
	require.Empty(t, run.Mode)
}
