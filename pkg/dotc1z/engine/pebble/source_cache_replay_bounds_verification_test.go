package pebble

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/internal/testtier"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// The ordinary-CI half of the replay-bound proof: the default path resolves to
// the production constant, while TestVerificationReplayCommittedPrefixRetryAllKinds
// cheaply proves that the live replay loop honors the resolved limit. The
// extra-tier 10,001-row fixture below retains direct production-scale evidence.
func TestVerificationReplayProductionBatchLimitWiring(t *testing.T) {
	e := &Engine{}
	require.Equal(t, replayBatchRows, e.sourceCacheReplayBatchLimit())

	e.test.sourceCacheReplayBatchRows = 2
	require.Equal(t, 2, e.sourceCacheReplayBatchLimit())
}

// C10/C12: the replay commit seam supplies deterministic evidence that live
// batch cardinality is fixed, and lets retry be cut after one landed chunk.
func TestVerificationReplayBatchBoundAndInterruptedRetry(t *testing.T) {
	testtier.RequireExtra(t)
	ctx := t.Context()
	const rows = replayBatchRows + 1

	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	const writeChunk = 1_000
	for start := 0; start < rows; start += writeChunk {
		end := min(start+writeChunk, rows)
		batch := make([]*v3.ResourceRecord, 0, end-start)
		for i := start; i < end; i++ {
			batch = append(batch, v3.ResourceRecord_builder{
				ResourceTypeId: "user",
				ResourceId:     fmt.Sprintf("user-%05d", i),
				SourceScopeKey: "scope-a",
			}.Build())
		}
		require.NoError(t, prev.PebbleEngine().PutResourceRecords(ctx, batch...))
	}
	sealReplaySource(ctx, t, prev.PebbleEngine(), sourcecache.RowKindResources, "scope-a")

	bounded := newAdapter(t)
	_, err = bounded.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	var commitCalls, highWater int
	bounded.PebbleEngine().test.sourceCacheReplayCommitHook = func(kind string, batchRows int, _ bool) error {
		require.Equal(t, "resources", kind)
		commitCalls++
		highWater = max(highWater, batchRows)
		return nil
	}
	res, err := bounded.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	require.Equal(t, int64(rows), res.Rows)
	require.Equal(t, 2, commitCalls)
	require.NoError(t, validateBatchHighWater(highWater, replayBatchRows))
	require.Equal(t, replayBatchRows, highWater)
	require.Equal(t, rows, countKeys(t, bounded.PebbleEngine(), encodeResourcePrefix()))

	interruptedEngine, interruptedDir := newTestEngine(t)
	interrupted := NewAdapter(interruptedEngine)
	interruptedSyncID, err := interrupted.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	injected := errors.New("verification replay cut")
	commitCalls = 0
	interrupted.PebbleEngine().test.sourceCacheReplayCommitHook = func(_ string, _ int, _ bool) error {
		commitCalls++
		if commitCalls == 2 {
			return injected
		}
		return nil
	}
	_, err = interrupted.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.ErrorIs(t, err, injected)
	require.Equal(t, replayBatchRows, countKeys(t, interrupted.PebbleEngine(), encodeResourcePrefix()),
		"only the first complete replay chunk may be visible after the injected cut")
	require.Equal(t, replayBatchRows, countKeys(t, interrupted.PebbleEngine(), ResourceBySourceScopeLowerBound()))
	require.Equal(t, rows, countKeys(t, prev.PebbleEngine(), encodeResourcePrefix()), "failed replay mutated source primaries")
	require.NoError(t, auditSourceScopeBiconditional(prev.PebbleEngine()))

	require.NoError(t, interrupted.PebbleEngine().Close())
	reopened, err := Open(ctx, filepath.Join(interruptedDir, "db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	interrupted = NewAdapter(reopened)
	require.NoError(t, interrupted.SetCurrentSync(ctx, interruptedSyncID))
	res, err = interrupted.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	require.Equal(t, int64(rows), res.Rows)
	require.Equal(t, rows, countKeys(t, interrupted.PebbleEngine(), encodeResourcePrefix()))
	require.Equal(t, rows, countKeys(t, interrupted.PebbleEngine(), ResourceBySourceScopeLowerBound()))
	require.NoError(t, auditSourceScopeBiconditional(interrupted.PebbleEngine()))

	cancelled := newAdapter(t)
	_, err = cancelled.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	cancelCtx, cancel := context.WithCancel(ctx)
	cancelled.PebbleEngine().test.sourceCacheReplayCommitHook = func(_ string, _ int, final bool) error {
		if !final {
			cancel()
		}
		return nil
	}
	_, err = cancelled.PebbleEngine().ReplaySourceCacheResources(cancelCtx, prev.PebbleEngine(), "scope-a")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, replayBatchRows, countKeys(t, cancelled.PebbleEngine(), encodeResourcePrefix()))
	require.Equal(t, rows, countKeys(t, prev.PebbleEngine(), encodeResourcePrefix()), "cancelled replay mutated source primaries")
	require.NoError(t, auditSourceScopeBiconditional(prev.PebbleEngine()))
	cancelled.PebbleEngine().test.sourceCacheReplayCommitHook = nil
	res, err = cancelled.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	require.Equal(t, int64(rows), res.Rows)
	require.Equal(t, rows, countKeys(t, cancelled.PebbleEngine(), encodeResourcePrefix()))
	require.NoError(t, auditSourceScopeBiconditional(cancelled.PebbleEngine()))

	readFailed := newAdapter(t)
	_, err = readFailed.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	readErr := errors.New("verification source iterator failure")
	readFailed.PebbleEngine().test.sourceCacheReplayReadHook = func(kind string, row int) error {
		require.Equal(t, "resources", kind)
		if row == replayBatchRows {
			return readErr
		}
		return nil
	}
	_, err = readFailed.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.ErrorIs(t, err, readErr)
	require.Equal(t, replayBatchRows, countKeys(t, readFailed.PebbleEngine(), encodeResourcePrefix()))
	require.Equal(t, rows, countKeys(t, prev.PebbleEngine(), encodeResourcePrefix()), "source iterator failure mutated source")
	readFailed.PebbleEngine().test.sourceCacheReplayReadHook = nil
	res, err = readFailed.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), "scope-a")
	require.NoError(t, err)
	require.Equal(t, int64(rows), res.Rows)
	require.Equal(t, rows, countKeys(t, readFailed.PebbleEngine(), encodeResourcePrefix()))
	require.NoError(t, auditSourceScopeBiconditional(readFailed.PebbleEngine()))
}

// C10/C23/C27: every row kind exercises a real committed-prefix cut, hard
// reopen, and convergent retry. TestVerificationReplayProductionBatchLimitWiring
// pins the production default in ordinary CI; this matrix proves the live loop
// honors that resolved limit while lowering only the test seam to keep
// row-kind closure cheap. The extra-tier fixture above additionally exercises
// the production cardinality directly.
func TestVerificationReplayCommittedPrefixRetryAllKinds(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{
		sourcecache.RowKindResources,
		sourcecache.RowKindEntitlements,
		sourcecache.RowKindGrants,
	} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := t.Context()
			prev := newAdapter(t)
			_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			switch kind {
			case sourcecache.RowKindResources:
				var rows []*v3.ResourceRecord
				for i := range 3 {
					rows = append(rows, v3.ResourceRecord_builder{
						ResourceTypeId: "user",
						ResourceId:     fmt.Sprintf("user-%d", i),
						SourceScopeKey: "scope-a",
					}.Build())
				}
				require.NoError(t, prev.PebbleEngine().PutResourceRecords(ctx, rows...))
			case sourcecache.RowKindEntitlements:
				var rows []*v3.EntitlementRecord
				for i := range 3 {
					rows = append(rows, v3.EntitlementRecord_builder{
						ExternalId: fmt.Sprintf("group:g%d:member", i),
						Resource: v3.ResourceRef_builder{
							ResourceTypeId: "group",
							ResourceId:     fmt.Sprintf("g%d", i),
						}.Build(),
						SourceScopeKey: "scope-a",
					}.Build())
				}
				require.NoError(t, prev.PebbleEngine().PutEntitlementRecords(ctx, rows...))
			case sourcecache.RowKindGrants:
				var rows []*v3.GrantRecord
				for i := range 3 {
					rows = append(rows, v3.GrantRecord_builder{
						ExternalId: fmt.Sprintf("group:g0:member:user:user-%d", i),
						Entitlement: v3.EntitlementRef_builder{
							ResourceTypeId: "group",
							ResourceId:     "g0",
							EntitlementId:  "group:g0:member",
						}.Build(),
						Principal: v3.PrincipalRef_builder{
							ResourceTypeId: "user",
							ResourceId:     fmt.Sprintf("user-%d", i),
						}.Build(),
						SourceScopeKey: "scope-a",
					}.Build())
				}
				require.NoError(t, prev.PebbleEngine().PutGrantRecords(ctx, rows...))
			}
			// Seal before the byte-dump so the untouched-source
			// comparisons below still hold.
			sealReplaySource(ctx, t, prev.PebbleEngine(), kind, "scope-a")
			sourceBefore := dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil)

			replay := func(replayCtx context.Context, dst *Engine) (SourceCacheReplayResult, error) {
				switch kind {
				case sourcecache.RowKindResources:
					return dst.ReplaySourceCacheResources(replayCtx, prev.PebbleEngine(), "scope-a")
				case sourcecache.RowKindEntitlements:
					return dst.ReplaySourceCacheEntitlements(replayCtx, prev.PebbleEngine(), "scope-a")
				case sourcecache.RowKindGrants:
					return dst.ReplaySourceCacheGrants(replayCtx, prev.PebbleEngine(), "scope-a")
				default:
					t.Fatalf("unsupported row kind %q", kind)
					return SourceCacheReplayResult{}, nil
				}
			}
			var family sourceScopeAuditFamily
			for _, candidate := range sourceScopeAuditFamilies() {
				if candidate.name == string(kind) {
					family = candidate
					break
				}
			}
			require.NotEmpty(t, family.name)

			interruptedEngine, interruptedDir := newTestEngine(t)
			interrupted := NewAdapter(interruptedEngine)
			syncID, err := interrupted.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			interruptedEngine.test.sourceCacheReplayBatchRows = 2
			commitCalls := 0
			injected := errors.New("verification all-kind committed-prefix cut")
			interruptedEngine.test.sourceCacheReplayCommitHook = func(_ string, _ int, _ bool) error {
				commitCalls++
				if commitCalls == 2 {
					return injected
				}
				return nil
			}
			interruptedRes, err := replay(ctx, interruptedEngine)
			require.ErrorIs(t, err, injected)
			// The error result reports committed progress: the first 2-row
			// batch landed, the failing final batch's staged row did not.
			require.Equal(t, int64(2), interruptedRes.Rows)
			require.Equal(t, 2, countKeys(t, interruptedEngine, family.primaryLo))
			require.Equal(t, 2, countKeys(t, interruptedEngine, family.indexLo))
			require.NoError(t, auditSourceScopeBiconditional(interruptedEngine))
			require.Equal(t, sourceBefore, dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil))

			require.NoError(t, interruptedEngine.Close())
			reopened, err := Open(ctx, filepath.Join(interruptedDir, "db"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			require.NoError(t, reopened.SetCurrentSync(ctx, syncID))
			require.Equal(t, 2, countKeys(t, reopened, family.primaryLo))
			reopened.test.sourceCacheReplayBatchRows = 2
			res, err := replay(ctx, reopened)
			require.NoError(t, err)
			require.Equal(t, int64(3), res.Rows)
			require.Equal(t, 3, countKeys(t, reopened, family.primaryLo))
			require.Equal(t, 3, countKeys(t, reopened, family.indexLo))
			require.NoError(t, auditSourceScopeBiconditional(reopened))
			require.Equal(t, sourceBefore, dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil))

			for cut := range 3 {
				t.Run(fmt.Sprintf("read-error-cut-%d", cut), func(t *testing.T) {
					dst := newAdapter(t)
					_, err := dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
					require.NoError(t, err)
					dst.PebbleEngine().test.sourceCacheReplayBatchRows = 2
					injected := errors.New("verification all-kind source read cut")
					dst.PebbleEngine().test.sourceCacheReplayReadHook = func(_ string, row int) error {
						if row == cut {
							return injected
						}
						return nil
					}
					_, err = replay(ctx, dst.PebbleEngine())
					require.ErrorIs(t, err, injected)
					require.Equal(t, (cut/2)*2, countKeys(t, dst.PebbleEngine(), family.primaryLo))
					require.NoError(t, auditSourceScopeBiconditional(dst.PebbleEngine()))
					require.Equal(t, sourceBefore, dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil))
					dst.PebbleEngine().test.sourceCacheReplayReadHook = nil
					res, err := replay(ctx, dst.PebbleEngine())
					require.NoError(t, err)
					require.Equal(t, int64(3), res.Rows)
					require.Equal(t, 3, countKeys(t, dst.PebbleEngine(), family.primaryLo))
				})
			}

			t.Run("iterator-terminal-error", func(t *testing.T) {
				dst := newAdapter(t)
				_, err := dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				dst.PebbleEngine().test.sourceCacheReplayBatchRows = 2
				injected := errors.New("verification all-kind iterator terminal error")
				dst.PebbleEngine().test.sourceCacheReplayIteratorErrorHook = func(gotKind string) error {
					require.Equal(t, string(kind), gotKind)
					return injected
				}
				_, err = replay(ctx, dst.PebbleEngine())
				require.ErrorIs(t, err, injected)
				require.Equal(t, 2, countKeys(t, dst.PebbleEngine(), family.primaryLo),
					"the final staged row must not commit after Iterator.Error")
				require.NoError(t, auditSourceScopeBiconditional(dst.PebbleEngine()))
				require.Equal(t, sourceBefore, dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil))

				dst.PebbleEngine().test.sourceCacheReplayIteratorErrorHook = nil
				res, err := replay(ctx, dst.PebbleEngine())
				require.NoError(t, err)
				require.Equal(t, int64(3), res.Rows)
				require.Equal(t, 3, countKeys(t, dst.PebbleEngine(), family.primaryLo))
			})

			for cut := range 3 {
				t.Run(fmt.Sprintf("cancel-cut-%d", cut), func(t *testing.T) {
					dst := newAdapter(t)
					_, err := dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
					require.NoError(t, err)
					dst.PebbleEngine().test.sourceCacheReplayBatchRows = 2
					cancelCtx, cancel := context.WithCancel(ctx)
					dst.PebbleEngine().test.sourceCacheReplayReadHook = func(_ string, row int) error {
						if row == cut {
							cancel()
						}
						return nil
					}
					_, err = replay(cancelCtx, dst.PebbleEngine())
					require.ErrorIs(t, err, context.Canceled)
					require.Equal(t, ((cut+1)/2)*2, countKeys(t, dst.PebbleEngine(), family.primaryLo))
					require.NoError(t, auditSourceScopeBiconditional(dst.PebbleEngine()))
					require.Equal(t, sourceBefore, dumpKeyRangeTest(t, prev.PebbleEngine(), nil, nil))
					dst.PebbleEngine().test.sourceCacheReplayReadHook = nil
					res, err := replay(ctx, dst.PebbleEngine())
					require.NoError(t, err)
					require.Equal(t, int64(3), res.Rows)
					require.Equal(t, 3, countKeys(t, dst.PebbleEngine(), family.primaryLo))
				})
			}
		})
	}
}

// The entitlement replay COPY loop mutates the entitlement keyspace in
// bounded chunks just like the tombstone paths, and bare-id readers take
// only entIDLookupMu — so each landed chunk must invalidate the cached
// lookup map as it commits, not at function exit (the twin of
// TestVerificationEntitlementDeleteBumpsLookupGenPerChunk). Deterministic
// pin: with 2-row chunks, chunk 2's pre-commit hook runs strictly after
// chunk 1 committed and must already observe a bumped generation.
func TestVerificationEntitlementReplayBumpsLookupGenPerChunk(t *testing.T) {
	ctx := t.Context()
	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	records := make([]*v3.EntitlementRecord, 0, 5)
	for i := range 5 {
		records = append(records, v3.EntitlementRecord_builder{
			ExternalId: fmt.Sprintf("group:g%d:member", i),
			Resource: v3.ResourceRef_builder{
				ResourceTypeId: "group",
				ResourceId:     fmt.Sprintf("g%d", i),
			}.Build(),
			SourceScopeKey: "scope-a",
		}.Build())
	}
	require.NoError(t, prev.PebbleEngine().PutEntitlementRecords(ctx, records...))
	sealReplaySource(ctx, t, prev.PebbleEngine(), sourcecache.RowKindEntitlements, "scope-a")

	dst := newAdapter(t)
	_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := dst.PebbleEngine()
	e.test.sourceCacheReplayBatchRows = 2
	genBefore := e.entIDLookupGen.Load()
	intermediates := 0
	sawMidLoopBump := false
	e.test.sourceCacheReplayCommitHook = func(kind string, _ int, final bool) error {
		require.Equal(t, "entitlements", kind)
		if final {
			return nil
		}
		intermediates++
		if intermediates == 2 {
			sawMidLoopBump = e.entIDLookupGen.Load() > genBefore
		}
		return nil
	}
	res, err := e.ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), "scope-a")
	e.test.sourceCacheReplayCommitHook = nil
	require.NoError(t, err)
	require.Equal(t, int64(5), res.Rows)
	require.GreaterOrEqual(t, intermediates, 2, "the 5-row replay must chunk at 2 rows")
	require.True(t, sawMidLoopBump,
		"chunk 1 landed before chunk 2's pre-commit hook; the lookup generation must already be invalidated")
}
