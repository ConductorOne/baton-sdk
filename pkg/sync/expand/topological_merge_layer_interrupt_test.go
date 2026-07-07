package expand

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// layerInterruptStore is an interrupt wrapper that — unlike
// interruptAfterNStore — implements the layer-session fast-path interfaces
// by embedding benchmarkExpanderStore, so on Pebble the expander routes
// synthesized rows through Begin/Add/Finish layer sessions instead of
// silently degrading to the generic StoreExpandedGrants path. It fails the
// (failAddAfter+1)-th AddExpandedGrantLayerContributions (mid-layer: the
// driver must AbortExpandedGrantLayer; segments already ingested by the
// engine remain in the DB) or the (failFinishAfter+1)-th
// FinishExpandedGrantLayer (layer boundary: the layer's rows are being
// published when the process dies).
type layerInterruptStore struct {
	benchmarkExpanderStore
	failAddAfter    int // -1: never fail Add
	failFinishAfter int // -1: never fail Finish
	addCount        *int
	finishCount     *int
	layerBegun      *bool
}

func (s layerInterruptStore) BeginExpandedGrantLayer(ctx context.Context) (bool, error) {
	ok, err := s.benchmarkExpanderStore.BeginExpandedGrantLayer(ctx)
	if ok {
		*s.layerBegun = true
	}
	return ok, err
}

func (s layerInterruptStore) AddExpandedGrantLayerContributions(
	ctx context.Context,
	dest *v2.Entitlement,
	principals []*v3.PrincipalRef,
	sources []batonGrant.Sources,
) error {
	if s.failAddAfter >= 0 && *s.addCount >= s.failAddAfter {
		return errStoreInterrupted
	}
	*s.addCount++
	return s.benchmarkExpanderStore.AddExpandedGrantLayerContributions(ctx, dest, principals, sources)
}

func (s layerInterruptStore) FinishExpandedGrantLayer(ctx context.Context) error {
	if s.failFinishAfter >= 0 && *s.finishCount >= s.failFinishAfter {
		return errStoreInterrupted
	}
	*s.finishCount++
	return s.benchmarkExpanderStore.FinishExpandedGrantLayer(ctx)
}

// TestTopologicalMergeLayerSessionInterruptResume closes the last
// manual-trace-only resume invariant: interrupting a Pebble layer session
// mid-layer (Add) or at the layer boundary (Finish), then resuming with a
// healthy store, must converge to exactly the clean-run artifact. The
// stranded state the first pass leaves behind is real: an aborted session
// keeps already-ingested SST segments in the DB, so the resume sees some of
// its "synthesized" grants as pre-existing base grants and must fold them
// without duplication or discovered_at churn.
func TestTopologicalMergeLayerSessionInterruptResume(t *testing.T) {
	algos := []struct {
		name string
		run  func(ctx context.Context, e *Expander) error
	}{
		{"projection", func(ctx context.Context, e *Expander) error { return e.RunTopologicalMergeProjection(ctx) }},
		{"streaming", func(ctx context.Context, e *Expander) error { return e.RunTopologicalMergeStreaming(ctx) }},
	}
	scenarios := []struct {
		name            string
		failAddAfter    int
		failFinishAfter int
	}{
		// Mid-wave: first Add succeeds (a segment may already be
		// ingested), second Add dies.
		{"fail-second-add", 1, -1},
		// Wave boundary: all Adds succeed, the first Finish dies.
		{"fail-first-finish", -1, 0},
	}

	// Pebble only: layer sessions are a Pebble fast path; SQLite routes
	// through the generic path already covered by
	// TestTopologicalMergePartialInterruptResume.
	engine := dotc1z.EnginePebble
	for _, algo := range algos {
		for _, sc := range scenarios {
			for _, tc := range append(parityCases(), cyclicCases()...) {
				tc := tc
				label := algo.name + "/" + sc.name
				t.Run(label+"/"+tc.name, func(t *testing.T) {
					ctx := context.Background()

					// Reference: clean, uninterrupted run on its own store.
					reference := runExpansion(t, ctx, tc, engine, 0, func(e *Expander) error { return algo.run(ctx, e) })

					// Interrupted run on a single store.
					path := filepath.Join(t.TempDir(), "layer-interrupt.c1z")
					store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
					require.NoError(t, err)
					syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
					require.NoError(t, err)
					seedSQLiteBaseData(t, ctx, store, tc)

					var addCount, finishCount int
					var layerBegun bool
					interrupting := layerInterruptStore{
						benchmarkExpanderStore: benchmarkExpanderStore{store: store},
						failAddAfter:           sc.failAddAfter,
						failFinishAfter:        sc.failFinishAfter,
						addCount:               &addCount,
						finishCount:            &finishCount,
						layerBegun:             &layerBegun,
					}
					graph1 := buildGraphFromCase(t, ctx, tc, engine)
					err = algo.run(ctx, NewExpander(interrupting, graph1))
					if err != nil {
						require.ErrorIs(t, err, errStoreInterrupted, "first pass must die on the injected interrupt, not something else")
					}
					// The wrapper exists to exercise the LAYER path: if the
					// expander never opened a session, the test is testing
					// nothing (this is exactly how interruptAfterNStore
					// silently degraded).
					require.True(t, layerBegun, "expander did not open a layer session; interrupt wrapper is being bypassed")

					// Resume: fresh graph, healthy store, run to completion.
					graph2 := buildGraphFromCase(t, ctx, tc, engine)
					require.NoError(t, algo.run(ctx, NewExpander(benchmarkExpanderStore{store: store}, graph2)))

					require.NoError(t, store.EndSync(ctx))
					require.NoError(t, store.Close(ctx))

					ro, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
					require.NoError(t, err)
					defer func() { require.NoError(t, ro.Close(ctx)) }()
					require.NoError(t, ro.SetCurrentSync(ctx, syncID))
					resumed := readBackGrantSnapshot(t, ctx, ro)

					assertStoreSnapshotsEqual(t, reference, resumed, label+"/layer-interrupt-resume")
				})
			}
		}
	}
}
