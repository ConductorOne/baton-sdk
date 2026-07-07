package pebble

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// These tests exercise the synthesized-grant layer session at the ENGINE
// level with a tiny segment limit, reaching the paths the expander-level
// interrupt tests cannot: real segment cuts (the default limit is 8M rows,
// far above any test fixture), the background worker's error propagation
// into Add/Finish, and abort-after-ingest semantics (already-ingested
// segments remain; a retry converges by idempotent overwrite).

func synthLayerRow(n int) synthesizedGrantRecord {
	id := "u" + strconv.Itoa(n)
	return synthesizedGrantRecord{
		id: grantIdentity{
			entitlement:     entitlementIdentityFromParts("group", "eng", "group:eng:member"),
			principalTypeID: "user",
			principalID:     id,
		},
		entitlement: v3.EntitlementRef_builder{ResourceTypeId: "group", ResourceId: "eng", EntitlementId: "group:eng:member"}.Build(),
		principal:   v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: id}.Build(),
		sources:     batonGrant.Sources{{EntitlementID: "group:eng:admin", IsDirect: true}},
	}
}

func synthLayerEngine(t *testing.T, ctx context.Context) *Engine {
	t.Helper()
	e, err := Open(ctx, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	a := NewAdapter(e)
	_, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	return e
}

func readSynthLayerPrincipals(t *testing.T, ctx context.Context, e *Engine) map[string]int {
	t.Helper()
	got := map[string]int{}
	require.NoError(t, e.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		got[r.GetPrincipal().GetResourceId()]++
		return true
	}))
	return got
}

// TestSynthLayerSessionMultiSegment forces segment cuts every 2 rows and
// verifies every row across every ingested segment (plus the tail segment
// flushed by Finish) is readable afterward.
func TestSynthLayerSessionMultiSegment(t *testing.T) {
	t.Setenv("BATON_PEBBLE_SYNTH_LAYER_SEGMENT_ROWS", "2")
	ctx := context.Background()
	e := synthLayerEngine(t, ctx)

	ok, err := e.BeginSynthesizedGrantLayer(ctx)
	require.NoError(t, err)
	require.True(t, ok, "Pebble must serve a layer session with a sync open")

	const rows = 7 // 3 cut segments + a 1-row tail flushed at Finish
	for i := 0; i < rows; i++ {
		require.NoError(t, e.AddSynthesizedGrantLayerContributions(ctx, []synthesizedGrantRecord{synthLayerRow(i)}))
	}
	require.NoError(t, e.FinishSynthesizedGrantLayer(ctx))

	got := readSynthLayerPrincipals(t, ctx, e)
	require.Len(t, got, rows, "row count after multi-segment finish")
	for i := 0; i < rows; i++ {
		require.Equal(t, 1, got["u"+strconv.Itoa(i)], "principal u%d", i)
	}

	// The session is closed: a new Begin must succeed (no leaked session).
	ok, err = e.BeginSynthesizedGrantLayer(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, e.AbortSynthesizedGrantLayer(ctx))
}

// TestSynthLayerSessionAbortAfterIngestThenRetry pins the documented abort
// contract: segments already ingested by the background worker REMAIN in the
// DB, and a retry session re-adding every row converges to exactly the full
// set — the identity keys make the re-adds idempotent overwrites, not
// duplicates.
func TestSynthLayerSessionAbortAfterIngestThenRetry(t *testing.T) {
	t.Setenv("BATON_PEBBLE_SYNTH_LAYER_SEGMENT_ROWS", "1")
	ctx := context.Background()
	e := synthLayerEngine(t, ctx)

	const rows = 5
	ok, err := e.BeginSynthesizedGrantLayer(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	// Every Add cuts+queues a 1-row segment; some subset is ingested before
	// the abort lands.
	for i := 0; i < rows-1; i++ {
		require.NoError(t, e.AddSynthesizedGrantLayerContributions(ctx, []synthesizedGrantRecord{synthLayerRow(i)}))
	}
	require.NoError(t, e.AbortSynthesizedGrantLayer(ctx))

	stranded := readSynthLayerPrincipals(t, ctx, e)
	require.LessOrEqual(t, len(stranded), rows-1, "abort must not invent rows")
	for p, n := range stranded {
		require.Equal(t, 1, n, "stranded principal %s duplicated", p)
	}

	// Retry: a fresh session re-adds ALL rows (the stranded ones included)
	// and finishes.
	ok, err = e.BeginSynthesizedGrantLayer(ctx)
	require.NoError(t, err, "Begin after abort")
	require.True(t, ok)
	for i := 0; i < rows; i++ {
		require.NoError(t, e.AddSynthesizedGrantLayerContributions(ctx, []synthesizedGrantRecord{synthLayerRow(i)}))
	}
	require.NoError(t, e.FinishSynthesizedGrantLayer(ctx))

	got := readSynthLayerPrincipals(t, ctx, e)
	require.Len(t, got, rows, "retry must converge to exactly the full set")
	for i := 0; i < rows; i++ {
		require.Equal(t, 1, got["u"+strconv.Itoa(i)], "principal u%d after retry (stranded rows must be overwritten, not duplicated)", i)
	}
}

// TestSynthLayerSessionWorkerErrorPropagates pins the worker→producer error
// channel: a failure stored by the background worker (setErr is exactly what
// its merge/ingest error branch calls) must surface from the NEXT Add and
// from Finish — never be swallowed — the failed session must be abortable
// without hanging on the dead worker, and the engine must serve a fresh
// session afterward.
func TestSynthLayerSessionWorkerErrorPropagates(t *testing.T) {
	t.Setenv("BATON_PEBBLE_SYNTH_LAYER_SEGMENT_ROWS", "1")
	ctx := context.Background()

	t.Run("surfaces at Add", func(t *testing.T) {
		e := synthLayerEngine(t, ctx)
		ok, err := e.BeginSynthesizedGrantLayer(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, e.AddSynthesizedGrantLayerContributions(ctx, []synthesizedGrantRecord{synthLayerRow(0)}))

		injected := errors.New("test: synth layer worker ingest failed")
		e.loadSynthLayer().setErr(injected)

		err = e.AddSynthesizedGrantLayerContributions(ctx, []synthesizedGrantRecord{synthLayerRow(1)})
		require.ErrorIs(t, err, injected, "worker error must surface at the next Add")
		// The session is still attached after a failed Add; Abort must
		// tear it down without hanging.
		require.NoError(t, e.AbortSynthesizedGrantLayer(ctx))

		// Engine remains fully usable for a fresh session.
		ok, err = e.BeginSynthesizedGrantLayer(ctx)
		require.NoError(t, err, "Begin after worker failure")
		require.True(t, ok)
		require.NoError(t, e.AddSynthesizedGrantLayerContributions(ctx, []synthesizedGrantRecord{synthLayerRow(99)}))
		require.NoError(t, e.FinishSynthesizedGrantLayer(ctx))
		got := readSynthLayerPrincipals(t, ctx, e)
		require.Equal(t, 1, got["u99"], "post-recovery session must write normally")
	})

	t.Run("surfaces at Finish", func(t *testing.T) {
		e := synthLayerEngine(t, ctx)
		ok, err := e.BeginSynthesizedGrantLayer(ctx)
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, e.AddSynthesizedGrantLayerContributions(ctx, []synthesizedGrantRecord{synthLayerRow(0)}))

		injected := errors.New("test: synth layer worker ingest failed")
		e.loadSynthLayer().setErr(injected)

		// Finish waits the worker out and must report the stored error,
		// not success.
		require.ErrorIs(t, e.FinishSynthesizedGrantLayer(ctx), injected, "worker error must surface at Finish")

		ok, err = e.BeginSynthesizedGrantLayer(ctx)
		require.NoError(t, err, "Begin after failed Finish")
		require.True(t, ok)
		require.NoError(t, e.AbortSynthesizedGrantLayer(ctx))
	})
}
