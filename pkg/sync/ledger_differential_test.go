package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"testing/synctest"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestLedgerDeterministicTimeFixture(t *testing.T) {
	seedPath := filepath.Join(t.TempDir(), "seed.c1z")
	synctest.Test(t, func(t *testing.T) {
		f := newLedgerFixtureAt(t, seedPath)
		require.NoError(t, f.store.Close(t.Context()))
	})
	seed, err := os.ReadFile(seedPath)
	require.NoError(t, err)
	var snapshots [][]ledgerKV
	for i := range 2 {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("arm-%d.c1z", i))
		require.NoError(t, os.WriteFile(path, seed, 0600))
		synctest.Test(t, func(t *testing.T) {
			f := openLedgerFixtureAt(t, path, false)
			runtime, err := newLedgerRuntime(t.Context(), f.ledger, "attempt")
			require.NoError(t, err)
			f.audit.enter(ledgerHandler)
			require.NoError(t, runLedgerSchedulerFixture(t, runtime, ledgerListingFixtureRoots(), 1, func(ctx context.Context, s *syncer, action *Action, page *ledgerPage) error {
				if err := page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "type"}.Build()); err != nil {
					return err
				}
				return ledgerFixtureTransition(ctx, s, action, "")
			}))
			f.audit.enter(ledgerLifecycle)
			require.NoError(t, runtime.prepareSeal(t.Context(), c1zstore.LedgerCounters{}))
			require.NoError(t, runtime.seal(t.Context()))
			snapshot, err := canonicalLedgerSnapshot(ledgerRawSnapshot(t, f.engine))
			require.NoError(t, err)
			snapshots = append(snapshots, snapshot)
			require.NoError(t, f.store.Close(t.Context()))
			artifact, err := os.ReadFile(path)
			require.NoError(t, err)
			t.Logf("raw artifact SHA-256 %x; byte equality is not asserted", sha256.Sum256(artifact))
		})
	}
	require.Equal(t, snapshots[0], snapshots[1])
}

func TestLedgerResumeLogicalDifferential(t *testing.T) {
	seedPath := filepath.Join(t.TempDir(), "seed.c1z")
	synctest.Test(t, func(t *testing.T) {
		f := newLedgerFixtureAt(t, seedPath)
		require.NoError(t, f.store.Close(t.Context()))
	})
	seed, err := os.ReadFile(seedPath)
	require.NoError(t, err)
	reference := make(map[uint32][]ledgerKV)
	for _, cut := range []int{-1, 0, 1, 2} {
		for _, workers := range []uint32{1, 4} {
			path := filepath.Join(t.TempDir(), fmt.Sprintf("cut-%d-workers-%d.c1z", cut, workers))
			require.NoError(t, os.WriteFile(path, seed, 0600))
			synctest.Test(t, func(t *testing.T) {
				f := openLedgerFixtureAt(t, path, false)
				runtime, err := newLedgerRuntime(t.Context(), f.ledger, "first")
				require.NoError(t, err)
				failed := false
				stopped := fmt.Errorf("injected before commit of page %d", cut)
				handler := func(ctx context.Context, s *syncer, action *Action, page *ledgerPage) error {
					if action.Op == SyncResourceTypesOp {
						if err := page.writer.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "type"}.Build()); err != nil {
							return err
						}
						return ledgerFixtureTransition(ctx, s, action, "",
							c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-resources", ResourceTypeID: "type", ResourceID: "stream-0"}},
							c1zstore.LedgerChild{Identity: c1zstore.LedgerActionIdentity{Op: "list-resources", ResourceTypeID: "type", ResourceID: "stream-1"}})
					}
					index := 0
					if action.PageToken != "" {
						var err error
						index, err = strconv.Atoi(action.PageToken)
						if err != nil {
							return err
						}
					}
					name := fmt.Sprintf("%s-record-%d", action.ResourceID, index)
					resource := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "type", Resource: name}.Build(), DisplayName: name}.Build()
					entitlement := v2.Entitlement_builder{Id: "ent-" + name, Resource: resource, DisplayName: name}.Build()
					grant := v2.Grant_builder{Id: "grant-" + name, Entitlement: entitlement, Principal: resource}.Build()
					if err := page.writer.PutResources(ctx, resource); err != nil {
						return err
					}
					if err := page.writer.PutEntitlements(ctx, entitlement); err != nil {
						return err
					}
					if err := page.writer.PutGrants(ctx, grant); err != nil {
						return err
					}
					if err := page.setFact("observed-data"); err != nil {
						return err
					}
					page.observations.Counters = map[string]uint64{"records": 3}
					if action.ResourceID == "stream-0" && index == cut && !failed {
						failed = true
						return stopped
					}
					next := ""
					if index < 2 {
						next = strconv.Itoa(index + 1)
					}
					return ledgerFixtureTransition(ctx, s, action, next)
				}
				f.audit.enter(ledgerHandler)
				err = runLedgerSchedulerFixture(t, runtime, ledgerListingFixtureRoots(), workers, handler)
				f.audit.enter(ledgerLifecycle)
				if cut >= 0 {
					require.ErrorIs(t, err, stopped)
					require.True(t, failed)
					require.NoError(t, f.store.Close(t.Context()))
					f = openLedgerFixtureAt(t, path, false)
					runtime, err = newLedgerRuntime(t.Context(), f.ledger, "resumed")
					require.NoError(t, err)
					f.audit.enter(ledgerHandler)
					err = runLedgerSchedulerFixture(t, runtime, ledgerListingFixtureRoots(), workers, handler)
					f.audit.enter(ledgerLifecycle)
				}
				require.NoError(t, err)
				require.NoError(t, runtime.prepareSeal(t.Context(), c1zstore.LedgerCounters{}))
				require.NoError(t, runtime.seal(t.Context()))
				canonical, err := canonicalLedgerSnapshot(ledgerRawSnapshot(t, f.engine))
				require.NoError(t, err)
				if reference[workers] == nil {
					reference[workers] = canonical
				} else {
					require.Equal(t, reference[workers], canonical)
				}
				require.NoError(t, f.store.Close(t.Context()))
				artifact, err := os.ReadFile(path)
				require.NoError(t, err)
				t.Logf("cut=%d workers=%d raw artifact SHA-256=%x; comparison is logical", cut, workers, sha256.Sum256(artifact))
			})
		}
	}
}
