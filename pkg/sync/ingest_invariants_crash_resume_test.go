package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Syncer-level crash/resume sweep across the invariant seams (the
// "Tier 1" harness): the pass-level halt sweep proves the PASS is
// idempotent, but nothing proved a whole SYNC aborted at each seam
// resumes to a complete, invariant-clean artifact through the real
// resume machinery (unfinished-sync discovery + token replay) on a
// REOPENED store. The crash model is the local runner's: the process
// stops after its last checkpointed save (Close persists the c1z with
// the unfinished sync), a fresh process reopens the same path and
// resumes. Engine-level crash images (fsync-truncated state) are the
// errorfs sweep's job; this sweeps the syncer orchestration above it.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

func TestSyncerCrashResumeAtInvariantSeams(t *testing.T) {
	errCrash := errors.New("injected crash at invariant seam")

	// Every seam the pass can stop at, plus the whole-pass syncer seam.
	stages := ingestInvariantHaltStages()

	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			tempDir := t.TempDir()
			c1zPath := filepath.Join(tempDir, "crash-resume.c1z")

			mc := newMockConnector()
			mc.rtDB = append(mc.rtDB, userResourceType, groupResourceType)
			user, err := rs.NewUserResource("user1", userResourceType, "user1", nil)
			require.NoError(t, err)
			mc.AddResource(ctx, user)
			group, groupEnt, err := mc.AddGroup(ctx, "group1")
			require.NoError(t, err)
			mc.AddGroupMember(ctx, group, user)

			newSyncerAt := func(hook func(string) error) Syncer {
				s, err := NewSyncer(ctx, mc,
					WithC1ZPath(c1zPath),
					WithTmpDir(tempDir),
					// Pebble: the engine with the full inspection
					// surface, so every invariant (and the I9-indexes
					// seam) actually runs.
					WithStorageEngine(c1zstore.EnginePebble),
				)
				require.NoError(t, err)
				s.(*syncer).testIngestHaltHook = hook
				return s
			}

			// Run 1: "crash" at the seam. The hook error aborts the
			// sync; Close persists the c1z with the UNFINISHED sync —
			// the local runner's stop-after-checkpoint shape.
			crashed := newSyncerAt(func(got string) error {
				if got == stage {
					return fmt.Errorf("%w: %s", errCrash, got)
				}
				return nil
			})
			err = crashed.Sync(ctx)
			require.ErrorIs(t, err, errCrash, "the sync must abort at the injected seam")
			require.NoError(t, crashed.Close(ctx))

			// Capture the crashed run's sync id: Run 2 must seal THIS
			// sync, not quietly start a fresh one that rebuilds the
			// same fixture (which would pass every content assertion
			// below without proving resume at all — review finding,
			// round 3).
			crashedID := func() string {
				s, err := dotc1z.NewStore(ctx, c1zPath,
					dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(tempDir))
				require.NoError(t, err)
				defer func() { require.NoError(t, s.Close(ctx)) }()
				lister, ok := s.(interface {
					ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
				})
				require.True(t, ok)
				runs, _, err := lister.ListSyncRuns(ctx, "", 100)
				require.NoError(t, err)
				require.Len(t, runs, 1, "the crashed store must hold exactly the one aborted sync")
				require.Nil(t, runs[0].EndedAt, "the aborted sync must be unfinished")
				return runs[0].ID
			}()

			// Run 2: a FRESH syncer on the same path — the real resume
			// machinery must discover the unfinished sync and seal it.
			resumed := newSyncerAt(nil)
			require.NoError(t, resumed.Sync(ctx), "the resumed sync must converge at seam %s", stage)
			require.NoError(t, resumed.Close(ctx))

			// Oracle: the sealed artifact is content-complete and
			// passes the whole invariant pass under FAIL-FAST — a
			// resume that duplicated, dropped, or half-applied state
			// fails here.
			store, err := dotc1z.NewStore(ctx, c1zPath,
				dotc1z.WithEngine(c1zstore.EnginePebble),
				dotc1z.WithTmpDir(tempDir),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close(ctx)) }()

			run, err := store.SyncMeta().LatestFullSync(ctx)
			require.NoError(t, err)
			require.NotNil(t, run, "the resumed sync must have sealed")
			require.Equal(t, crashedID, run.ID,
				"run 2 must RESUME the crashed sync, not start a fresh one")
			lister, ok := store.(interface {
				ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
			})
			require.True(t, ok)
			runs, _, err := lister.ListSyncRuns(ctx, "", 100)
			require.NoError(t, err)
			require.Len(t, runs, 1, "exactly one sync run: the resumed one — no shadow restart")
			require.NoError(t, store.SetCurrentSync(ctx, run.ID))

			require.NoError(t, RunIngestInvariants(ctx, store, IngestInvariantsPolicy{
				ActiveSyncID: run.ID,
				SyncType:     connectorstore.SyncTypeFull,
				FailFast:     true,
			}), "the sealed artifact must pass the full invariant pass under fail-fast")

			rresp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{}.Build())
			require.NoError(t, err)
			require.Len(t, rresp.GetList(), 2, "resource content must survive crash/resume")
			gresp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
			require.NoError(t, err)
			require.Len(t, gresp.GetList(), 1, "grant content must survive crash/resume")
			_, err = store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
				EntitlementId: groupEnt.GetId(),
			}.Build())
			require.NoError(t, err, "entitlement content must survive crash/resume")

			_ = os.Remove(c1zPath)
		})
	}
}
