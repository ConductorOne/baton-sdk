package expand

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

// TestTopologicalMergeResumeIdempotent simulates an interrupt-and-resume of the
// topological algorithms.
//
// The projection/streaming expanders run the whole graph in one RunSingleStep
// and only mark edges expanded at the very end, so an interruption leaves the
// checkpoint with edges unexpanded — meaning resume re-runs the ENTIRE
// algorithm over a store that already contains the expanded grants from the
// interrupted attempt. This test exercises exactly that: it runs an algorithm
// to completion, then runs it AGAIN over the same store with a freshly rebuilt
// (unexpanded) graph, and asserts the grant set is byte-for-byte identical on
// the second pass. A non-idempotent re-derivation (duplicated/dropped sources,
// directness flips, spurious grants) would diverge here.
func TestTopologicalMergeResumeIdempotent(t *testing.T) {
	algos := []struct {
		name string
		run  func(ctx context.Context, e *Expander) error
	}{
		{"projection", func(ctx context.Context, e *Expander) error { return e.RunTopologicalMergeProjection(ctx) }},
		{"streaming", func(ctx context.Context, e *Expander) error { return e.RunTopologicalMergeStreaming(ctx) }},
	}

	for _, engine := range []dotc1z.Engine{dotc1z.EnginePebble, dotc1z.EngineSQLite} {
		for _, algo := range algos {
			for _, tc := range append(parityCases(), cyclicCases()...) {
				tc := tc
				label := string(engine) + "/" + algo.name
				t.Run(label+"/"+tc.name, func(t *testing.T) {
					ctx := context.Background()
					path := filepath.Join(t.TempDir(), "resume.c1z")

					store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
					require.NoError(t, err)
					_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
					require.NoError(t, err)
					seedSQLiteBaseData(t, ctx, store, tc)

					es := benchmarkExpanderStore{store: store}

					// First pass: expand to completion.
					graph1 := buildGraphFromCase(t, ctx, tc)
					require.NoError(t, algo.run(ctx, NewExpander(es, graph1)))
					first := snapshotOpenStoreGrants(t, ctx, store)

					// Second pass (resume): a fresh, unexpanded graph over the
					// SAME store, which now holds the first pass's output.
					graph2 := buildGraphFromCase(t, ctx, tc)
					require.NoError(t, algo.run(ctx, NewExpander(es, graph2)))
					second := snapshotOpenStoreGrants(t, ctx, store)

					assertStoreSnapshotsEqual(t, first, second, label+"/resume")

					require.NoError(t, store.EndSync(ctx))
					require.NoError(t, store.Close(ctx))
				})
			}
		}
	}
}

// snapshotOpenStoreGrants reads every grant from a store mid-sync (without
// ending it), so the resume test can compare the grant set across two
// expansion passes on the same open store.
func snapshotOpenStoreGrants(t *testing.T, ctx context.Context, store dotc1z.C1ZStore) map[string]storeGrantSnapshot {
	t.Helper()
	return readBackGrantSnapshot(t, ctx, store)
}

// TestTopologicalMergeCyclic verifies the topological algorithms on collapsed
// cycles (super-nodes). The current source-batched expander is nondeterministic
// here (it can drop grants and attribute provenance to an arbitrary cycle
// member depending on map-iteration order), so it is NOT a usable oracle.
// Instead this asserts the properties that matter: each new algorithm is
// deterministic across repeated runs, and the three algorithms agree with each
// other (so a fleet can switch between them without changing results).
func TestTopologicalMergeCyclic(t *testing.T) {
	runs := []struct {
		name string
		run  func(ctx context.Context, e *Expander) error
	}{
		{"projection", func(ctx context.Context, e *Expander) error { return e.RunTopologicalMergeProjection(ctx) }},
		{"streaming", func(ctx context.Context, e *Expander) error { return e.RunTopologicalMergeStreaming(ctx) }},
	}

	for _, engine := range []dotc1z.Engine{dotc1z.EnginePebble, dotc1z.EngineSQLite} {
		for _, tc := range cyclicCases() {
			tc := tc
			t.Run(string(engine)+"/"+tc.name, func(t *testing.T) {
				ctx := context.Background()

				var reference map[string]storeGrantSnapshot
				for _, algo := range runs {
					// Determinism: two independent runs of the same algorithm
					// must match.
					first := runExpansion(t, ctx, tc, engine, 0, func(e *Expander) error { return algo.run(ctx, e) })
					second := runExpansion(t, ctx, tc, engine, 0, func(e *Expander) error { return algo.run(ctx, e) })
					assertStoreSnapshotsEqual(t, first, second, string(engine)+"/"+algo.name+"/deterministic")

					// Cross-algorithm agreement against the first algorithm.
					if reference == nil {
						reference = first
					} else {
						assertStoreSnapshotsEqual(t, reference, first, string(engine)+"/"+algo.name+"/agrees-with-projection")
					}
				}
			})
		}
	}
}

var errStoreInterrupted = errors.New("test: store interrupted")

// interruptAfterNStore lets the first n StoreExpandedGrants batches through,
// then fails every subsequent one — simulating a process that dies partway
// through expansion, leaving a subset of destinations written.
type interruptAfterNStore struct {
	inner ExpanderStore
	n     int
	count *int
}

func (s interruptAfterNStore) GetEntitlement(
	ctx context.Context,
	req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest,
) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	return s.inner.GetEntitlement(ctx, req)
}

func (s interruptAfterNStore) ListGrantsForEntitlement(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	return s.inner.ListGrantsForEntitlement(ctx, req)
}

func (s interruptAfterNStore) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	if *s.count >= s.n {
		return errStoreInterrupted
	}
	*s.count++
	return s.inner.StoreExpandedGrants(ctx, grants...)
}

func (s interruptAfterNStore) GrantsForEntitlementPrincipalSorted() bool {
	return s.inner.GrantsForEntitlementPrincipalSorted()
}

// TestTopologicalMergePartialInterruptResume proves that interrupting an
// expansion after only some destinations have been written, then resuming to
// completion, yields exactly the same grant set as a clean uninterrupted run.
//
// This is the realistic crash/resume path for the monolithic projection and
// streaming expanders: the first attempt commits some destinations and then
// dies (no edges marked, no checkpoint of progress); the resume re-runs the
// whole algorithm over the partially-written store and must converge.
func TestTopologicalMergePartialInterruptResume(t *testing.T) {
	algos := []struct {
		name string
		run  func(ctx context.Context, e *Expander) error
	}{
		{"projection", func(ctx context.Context, e *Expander) error { return e.RunTopologicalMergeProjection(ctx) }},
		{"streaming", func(ctx context.Context, e *Expander) error { return e.RunTopologicalMergeStreaming(ctx) }},
	}

	for _, engine := range []dotc1z.Engine{dotc1z.EnginePebble, dotc1z.EngineSQLite} {
		for _, algo := range algos {
			for _, tc := range append(parityCases(), cyclicCases()...) {
				tc := tc
				label := string(engine) + "/" + algo.name
				t.Run(label+"/"+tc.name, func(t *testing.T) {
					ctx := context.Background()

					// Reference: a clean, uninterrupted run on its own store.
					reference := runExpansion(t, ctx, tc, engine, 0, func(e *Expander) error { return algo.run(ctx, e) })

					// Interrupted+resumed run on a single store.
					path := filepath.Join(t.TempDir(), "partial.c1z")
					store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
					require.NoError(t, err)
					syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
					require.NoError(t, err)
					seedSQLiteBaseData(t, ctx, store, tc)

					// First pass: allow a single destination batch through, then fail.
					count := 0
					interrupting := interruptAfterNStore{inner: benchmarkExpanderStore{store: store}, n: 1, count: &count}
					graph1 := buildGraphFromCase(t, ctx, tc)
					err = algo.run(ctx, NewExpander(interrupting, graph1))
					if err != nil {
						require.ErrorIs(t, err, errStoreInterrupted)
					}

					// Resume: fresh graph, healthy store, run to completion.
					graph2 := buildGraphFromCase(t, ctx, tc)
					require.NoError(t, algo.run(ctx, NewExpander(benchmarkExpanderStore{store: store}, graph2)))

					require.NoError(t, store.EndSync(ctx))
					require.NoError(t, store.Close(ctx))

					ro, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
					require.NoError(t, err)
					defer func() { require.NoError(t, ro.Close(ctx)) }()
					require.NoError(t, ro.SetCurrentSync(ctx, syncID))
					resumed := readBackGrantSnapshot(t, ctx, ro)

					assertStoreSnapshotsEqual(t, reference, resumed, label+"/partial-resume")
				})
			}
		}
	}
}
