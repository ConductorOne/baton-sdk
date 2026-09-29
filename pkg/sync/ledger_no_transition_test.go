package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

type ledgerPassSnapshot struct {
	facts     map[string]string
	rows      uint64
	completed uint64
	phase     c1zstore.LedgerQueuePhase
}

func snapshotLedgerPass(t *testing.T, f *ledgerFixture, id string) ledgerPassSnapshot {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, f.store.SetCurrentSync(ctx, id))
	facts, err := f.ledger.LedgerFacts(ctx)
	require.NoError(t, err)
	var rows uint64
	for _, kv := range ledgerRawSnapshot(t, f.engine) {
		if len(kv.key) >= 3 && kv.key[0] == 0x03 && kv.key[1] == 0x0c && kv.key[2] == 0 {
			rows++
		}
	}
	counters, err := f.ledger.LedgerCounters(ctx)
	require.NoError(t, err)
	_, phase, err := f.ledger.PendingWork(ctx, 0, 1)
	require.NoError(t, err)
	return ledgerPassSnapshot{facts: facts, rows: rows, completed: counters.Counters[ledgerCompletedActions], phase: phase}
}

// Interrupt a retained-mode sync at each lifecycle point. The images are the
// durable states of CO-039 §1 as a real run reaches them.
func ledgerInterruptedImage(t *testing.T, image string) (string, string) {
	t.Helper()
	ctx := t.Context()
	source, _, _ := ledgerUnexpandedSource(t)
	f := openLedgerFixtureAt(t, filepath.Join(t.TempDir(), image+".c1z"), false)
	created, err := NewSyncer(ctx, source, WithConnectorStore(f.store), WithLedgerDebug(true))
	require.NoError(t, err)
	s := created.(*syncer)
	switch image {
	case "collecting":
		s.caps.pageLedger = ledgerPageCommitFailsForOp{PageLedgerStore: f.ledger, op: SyncGrantsOp}
	case "expanding":
		t.Setenv("BATON_PEBBLE_SYNTH_LAYER_SEGMENT_ROWS", "1")
		s.caps.expandedGrantLayer = &ledgerExpansionLayerObserver{expandedGrantLayerStorer: s.caps.expandedGrantLayer, failFinish: true}
	case "drained":
		s.testHooks.ingestHaltHook = func(stage string) error {
			if stage == haltStageInvariantsComplete {
				return errLedgerInjectedPage
			}
			return nil
		}
	case "sealing":
		s.caps.pageLedger = ledgerExpansionSealFailure{PageLedgerStore: f.ledger}
	case "sealed":
	default:
		t.Fatalf("unknown image %q", image)
	}
	err = created.Sync(ctx)
	if image == "sealed" {
		require.NoError(t, err)
	} else {
		require.ErrorIs(t, err, errLedgerInjectedPage)
	}
	require.NoError(t, f.store.Close(ctx))
	return f.path, s.syncID
}

// CO-039 §5: an attempt writes to the fact family only when it commits a
// page or begins a pass. A resumer that finds nothing to do, or is refused,
// leaves the facts as it found them, whatever its own configuration says.
func TestLedgerResumeWithoutWorkWritesNoFacts(t *testing.T) {
	flips := map[string][]SyncOpt{
		"same":                 {WithLedgerDebug(true)},
		"debug-off":            {},
		"only-expand":          {WithLedgerDebug(true), WithOnlyExpandGrants()},
		"dont-expand":          {WithLedgerDebug(true), WithDontExpandGrants()},
		"skip-grants":          {WithLedgerDebug(true), WithSkipGrants(true)},
		"skip-ents-and-grants": {WithLedgerDebug(true), WithSkipEntitlementsAndGrants(true)},
		"workers-4":            {WithLedgerDebug(true), WithWorkerCount(4)},
	}
	for _, image := range []string{"collecting", "drained", "expanding", "sealing", "sealed"} {
		for name, opts := range flips {
			t.Run(image+"/"+name, func(t *testing.T) {
				ctx := t.Context()
				path, id := ledgerInterruptedImage(t, image)
				f := openLedgerFixtureAt(t, path, false)
				before := snapshotLedgerPass(t, f, id)
				source, _, _ := ledgerUnexpandedSource(t)
				resumer, err := NewSyncer(ctx, source, append([]SyncOpt{WithConnectorStore(f.store), WithSyncID(id)}, opts...)...)
				require.NoError(t, err)
				err = resumer.Sync(ctx)
				if err != nil && !errors.Is(err, ErrLedgerStateConflict) {
					t.Fatalf("resume failed for a reason other than a state conflict: %v", err)
				}
				after := snapshotLedgerPass(t, f, id)
				// A pass that ran to completion can leave the same phase and row
				// count it found; its completed actions cannot stay put.
				worked := after.rows != before.rows || after.completed != before.completed ||
					(before.phase == c1zstore.LedgerQueueAbsent && after.phase != c1zstore.LedgerQueueAbsent)
				if !worked {
					require.Equal(t, before.facts, after.facts,
						fmt.Sprintf("image %s resumed with %s committed no page and began no pass, yet the fact family changed", image, name))
				}
			})
		}
	}
}
