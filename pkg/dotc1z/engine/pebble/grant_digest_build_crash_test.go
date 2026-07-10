package pebble

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// Crash-window tests for the grant digest build's durable pending
// marker (encodeGrantDigestBuildPendingKey). The build commits digest
// nodes in bounded batches DURING the merge and again at fold.finish()
// — including the global root — before the hash-index IngestAndExcise,
// and committed WAL writes survive a process kill. Without the marker,
// a resumed EndSync's RepairMissingGrantDigests trusted every root the
// crashed build left behind (or fast-pathed on its global root) and
// sealed the file with correct-looking digests over an empty hash
// index — the silent skip-entitlements-at-uplift failure mode.
//
// The kill is simulated by driving the REAL standalone build to a
// sentinel error at a named point (testDigestBuildHook), bypassing
// BuildGrantDigests's in-process drop handler — exactly a process
// death's durable footprint (a clean Close then persists everything
// committed, the maximal state a WAL replay could resurrect).

var errDigestBuildKilled = errors.New("test: digest build killed")

// seedDigestCrashRecords writes a deterministic entitlement/grant set —
// identical bytes on every engine it is applied to, so the recovered
// build can be compared byte-for-byte against a from-scratch reference.
func seedDigestCrashRecords(t *testing.T, e *Engine) {
	t.Helper()
	ctx := context.Background()
	for entID, principals := range map[string][]string{
		"ent-a":    {"alice", "bob", "carol"},
		"ent-b":    {"dave", "erin"},
		"ent-c":    {"frank"},
		"ent-zero": nil,
	} {
		putEnt(t, e, ctx, entID)
		for _, p := range principals {
			g := makeGrant("", "g-"+entID+"-"+p, entID, p)
			require.NoError(t, e.PutGrantRecords(ctx, g), "PutGrantRecords(%s/%s)", entID, p)
		}
	}
}

// dumpKeyRangeTest snapshots a keyspace range as key→value bytes for
// byte-for-byte comparison between engines.
func dumpKeyRangeTest(t *testing.T, e *Engine, lo, hi []byte) map[string][]byte {
	t.Helper()
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	require.NoError(t, err, "NewIter")
	defer iter.Close()
	out := map[string][]byte{}
	for iter.First(); iter.Valid(); iter.Next() {
		out[string(iter.Key())] = append([]byte(nil), iter.Value()...)
	}
	require.NoError(t, iter.Error(), "iter")
	return out
}

func rawKeyPresent(t *testing.T, e *Engine, key []byte) bool {
	t.Helper()
	_, closer, err := e.db.Get(key)
	if err == nil {
		closer.Close()
		return true
	}
	require.ErrorIs(t, err, pebble.ErrNotFound)
	return false
}

// TestGrantDigestBuildCrashMidMerge kills the standalone build right
// after a mid-merge digest-node batch commit: some partitions' roots
// are durable, the global root and the hash-index ingest are not.
func TestGrantDigestBuildCrashMidMerge(t *testing.T) {
	testGrantDigestBuildCrash(t,
		func(e *Engine) {
			// Commit (and fire the hook) on every partition close so a
			// small dataset exercises the mid-merge commit path; die
			// after the first committed batch.
			e.testDigestNodeFlushBytes = 1
			fired := 0
			e.testDigestBuildHook = func(stage string) error {
				if stage != "node-batch-committed" {
					return nil
				}
				fired++
				if fired == 1 {
					return errDigestBuildKilled
				}
				return nil
			}
		},
		func(t *testing.T, e *Engine) {
			// Partial: at least one partition's nodes committed, but the
			// global root only rides fold.finish()'s final batch.
			require.NotZero(t, countKeyRangeTest(t, e, DigestLowerBound(), DigestUpperBound()),
				"mid-merge kill must leave committed digest nodes behind")
			require.False(t, rawKeyPresent(t, e, globalGrantDigestNodeKey()),
				"the global root must not be durable before fold.finish()")
		},
	)
}

// TestGrantDigestBuildCrashPostFinish kills the build between
// fold.finish() and the hash-index IngestAndExcise: EVERY digest node
// including the global root is durable over an index that was never
// ingested — the exact state RepairMissingGrantDigests's global-root
// fast path would have trusted wholesale.
func TestGrantDigestBuildCrashPostFinish(t *testing.T) {
	testGrantDigestBuildCrash(t,
		func(e *Engine) {
			e.testDigestBuildHook = func(stage string) error {
				if stage == "post-finish" {
					return errDigestBuildKilled
				}
				return nil
			}
		},
		func(t *testing.T, e *Engine) {
			require.True(t, rawKeyPresent(t, e, globalGrantDigestNodeKey()),
				"post-finish kill must leave the global root durable")
		},
	)
}

func testGrantDigestBuildCrash(t *testing.T, arm func(*Engine), assertPoisoned func(*testing.T, *Engine)) {
	ctx := context.Background()
	dbDir := filepath.Join(t.TempDir(), "db")

	// Process A: sync some grants (inline-index writes — the deferred
	// marker is never armed, so EndSync would take the standalone
	// RepairMissingGrantDigests→BuildGrantDigests path), then die inside
	// the digest build.
	e, err := Open(ctx, dbDir)
	require.NoError(t, err)
	a := NewAdapter(e)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	seedDigestCrashRecords(t, e)

	arm(e)
	buildErr := e.withWriteAllowSealed(func() error { return e.buildGrantDigestsStandaloneLocked(ctx) })
	require.ErrorIs(t, buildErr, errDigestBuildKilled)

	// The poisoned durable state the marker exists for: the marker and
	// (some or all) digest nodes are committed, the ingest never ran.
	require.True(t, rawKeyPresent(t, e, encodeGrantDigestBuildPendingKey()),
		"the build-pending marker must be armed before any node commit")
	require.Zero(t, countKeyRangeTest(t, e, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()),
		"the hash index must be empty — the ingest never ran")
	assertPoisoned(t, e)

	// Even in-process, the root getters must refuse the crashed build's
	// nodes while the pending flag is set.
	_, ok, gerr := e.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, gerr)
	require.False(t, ok, "a pending build's global root must read as absent")

	// The "crash": stop without any cleanup. A clean Close persists
	// every committed batch — the maximal state WAL replay could hand a
	// real crash's successor.
	require.NoError(t, e.Close())

	// Process B: reopen. The marker must be consumed by dropping ALL
	// digest state — a crash always converts to "digests absent".
	e2, err := Open(ctx, dbDir)
	require.NoError(t, err)
	defer e2.Close()
	require.False(t, e2.grantDigestBuildPending.Load(), "reopen must consume the durable marker")
	require.False(t, rawKeyPresent(t, e2, encodeGrantDigestBuildPendingKey()))
	require.Zero(t, countKeyRangeTest(t, e2, DigestLowerBound(), DigestUpperBound()),
		"reopen must drop every digest node the crashed build committed")
	require.False(t, e2.grantDigestsPresent.Load())

	// Resume the sync and rerun EndSync — the standalone build runs
	// again, from scratch.
	a2 := NewAdapter(e2)
	require.NoError(t, a2.SetCurrentSync(ctx, syncID))
	require.NoError(t, a2.EndSync(ctx))
	require.False(t, e2.grantDigestBuildPending.Load(), "a successful build must clear the marker")
	require.False(t, rawKeyPresent(t, e2, encodeGrantDigestBuildPendingKey()))

	// The recovered digest state must be byte-for-byte identical to a
	// from-scratch build over the same records — digest nodes, hash
	// index, and global root carry no sync-relative bytes.
	ref, _ := newTestEngine(t)
	aRef := NewAdapter(ref)
	_, err = aRef.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	seedDigestCrashRecords(t, ref)
	require.NoError(t, aRef.EndSync(ctx))

	require.Equal(t,
		dumpKeyRangeTest(t, ref, DigestLowerBound(), DigestUpperBound()),
		dumpKeyRangeTest(t, e2, DigestLowerBound(), DigestUpperBound()),
		"digest nodes after crash recovery must match a from-scratch build")
	require.Equal(t,
		dumpKeyRangeTest(t, ref, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()),
		dumpKeyRangeTest(t, e2, GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound()),
		"hash-index rows after crash recovery must match a from-scratch build")

	root, rootOK, err := e2.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.True(t, rootOK, "global root must be present after the recovered seal")
	require.EqualValues(t, 6, root.Count)
}
