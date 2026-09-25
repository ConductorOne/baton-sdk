package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/stretchr/testify/require"
)

func TestLedgerUploadedUnexpandedSync(t *testing.T) {
	for _, workers := range []int{1, 4} {
		for _, debug := range []bool{false, true} {
			for _, ending := range []string{"sealed", "early-end", "unfinished"} {
				t.Run(fmt.Sprintf("workers-%d/debug-%t/%s", workers, debug, ending), func(t *testing.T) {
					ctx := t.Context()
					source := newMockConnector()
					source.rtDB = []*v2.ResourceType{userResourceType, groupResourceType}
					user, err := source.AddUser(ctx, "alice")
					require.NoError(t, err)
					groups := make([]*v2.Resource, 3)
					ents := make([]*v2.Entitlement, 3)
					for i, name := range []string{"a", "b", "c"} {
						groups[i], ents[i], err = source.AddGroup(ctx, name)
						require.NoError(t, err)
					}
					source.AddGroupMember(ctx, groups[0], user)
					source.AddGroupMember(ctx, groups[1], groups[0], ents[0])
					source.AddGroupMember(ctx, groups[2], groups[1], ents[1])
					want := []string{
						gt.NewGrant(groups[0], "member", user).GetId(),
						gt.NewGrant(groups[1], "member", groups[0]).GetId(),
						gt.NewGrant(groups[2], "member", groups[1]).GetId(),
					}
					expanded := append(append([]string{}, want...),
						gt.NewGrant(groups[1], "member", user).GetId(),
						gt.NewGrant(groups[2], "member", user).GetId(),
						gt.NewGrant(groups[2], "member", groups[0]).GetId())
					f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), "connector.c1z"), false)
					first, err := NewSyncer(ctx, source, WithConnectorStore(f.store), WithDontExpandGrants(), WithWorkerCount(workers), WithLedgerDebug(debug))
					require.NoError(t, err)
					collecting := first.(*syncer)
					interrupted := errors.New("interrupted before seal")
					if ending != "sealed" {
						collecting.testHooks.ingestHaltHook = func(stage string) error {
							if stage == haltStageInvariantsComplete {
								return interrupted
							}
							return nil
						}
						require.ErrorIs(t, first.Sync(ctx), interrupted)
						pending, initialized, err := f.ledger.PendingWork(ctx, 0, 1)
						require.NoError(t, err)
						require.True(t, initialized)
						require.Empty(t, pending)
						if ending == "early-end" {
							require.NoError(t, f.store.EndSync(ctx))
						}
					} else {
						require.NoError(t, first.Sync(ctx))
					}
					id := collecting.syncID
					completed := collecting.run.getActionCount(SyncGrantExpansionOp).CompletedCount
					grants, err := f.store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
					require.NoError(t, err)
					var ids []string
					for _, g := range grants.GetList() {
						ids = append(ids, g.GetId())
					}
					require.ElementsMatch(t, want, ids)
					require.NoError(t, f.store.Close(ctx))
					root, err := os.OpenRoot(filepath.Dir(f.path))
					require.NoError(t, err)
					data, err := root.ReadFile(filepath.Base(f.path))
					require.NoError(t, err)
					require.NoError(t, root.Close())
					uploaded := filepath.Join(t.TempDir(), "uploaded.c1z")
					require.NoError(t, writeLedgerTestFile(uploaded, data, 0600))
					passes := 1
					if ending == "unfinished" {
						passes = 2
					}
					for pass := range passes {
						f = openLedgerFixtureAt(t, uploaded, false)
						host, err := NewSyncer(ctx, ledgerExpansionConnector{mockConnector: newMockConnector()},
							WithConnectorStore(f.store), WithSyncID(id), WithOnlyExpandGrants(), WithLedgerDebug(debug))
						require.NoError(t, err)
						observeLedgerRestore(t, host.(*syncer), f)
						require.NoError(t, host.Sync(ctx))
						expected := expanded
						expectedCompleted := completed + 1
						if ending == "unfinished" && pass == 0 {
							expected = want
							expectedCompleted = completed
						}
						actualCompleted := host.(*syncer).run.getActionCount(SyncGrantExpansionOp).CompletedCount
						require.NoError(t, f.store.Close(ctx))
						f = openLedgerFixtureAt(t, uploaded, false)
						grants, err = f.store.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{})
						require.NoError(t, err)
						ids = nil
						for _, g := range grants.GetList() {
							ids = append(ids, g.GetId())
						}
						require.ElementsMatch(t, expected, ids)
						require.Equal(t, expectedCompleted, actualCompleted)
						record, err := f.engine.GetSyncRunRecord(ctx, id)
						require.NoError(t, err)
						require.Equal(t, id, record.GetSyncId())
						require.NotNil(t, record.GetEndedAt())
						require.NoError(t, f.store.Close(ctx))
					}
				})
			}
		}
	}
}
