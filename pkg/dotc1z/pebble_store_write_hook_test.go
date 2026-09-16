package dotc1z

// The write hook shipped with no test of its own: nothing exercised
// StrictWriteHook, SetWriteHook, WithOpenPage or WithPageWriteBypass, so
// a guard wired after its write, or a method returning before it reaches
// writeHook(), would have gone unnoticed. Covering the contract needs no
// syncer — a direct store write and a context are enough.
//
// writeHook() has four outcomes, and all four matter:
//
//	hook installed  page open  bypass registered  result
//	no              -          -                  write proceeds, PageOpen not reached
//	yes             no         -                  write proceeds (not inside a page)
//	yes             yes        no                 ErrUnregisteredPageWrite, write refused
//	yes             yes        yes                write proceeds, event recorded
//
// I6/C23 in docs/verification/page-ledger/plan.md.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func TestWriteHookOutcomes(t *testing.T) {
	ctx := context.Background()

	// newHookStore returns a store mid-sync plus the events the installed
	// hook saw. install=false leaves the hook absent.
	newHookStore := func(t *testing.T, install bool) (c1zstore.Store, *[]c1zstore.WriteHookEvent) {
		t.Helper()
		store, err := NewStore(ctx, filepath.Join(t.TempDir(), "hook.c1z"), WithEngine(c1zstore.EnginePebble))
		require.NoError(t, err)
		t.Cleanup(func() { _ = store.Close(ctx) })
		_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)

		var seen []c1zstore.WriteHookEvent
		if install {
			hooked, ok := store.(c1zstore.WriteHookStore)
			require.True(t, ok, "the pebble store exposes the write hook")
			hooked.SetWriteHook(c1zstore.StrictWriteHook(func(ev c1zstore.WriteHookEvent) {
				seen = append(seen, ev)
			}))
		}
		return store, &seen
	}

	// A direct store write, the thing the hook guards. PutGrants is one of
	// the methods a page would otherwise route through its PageWriter.
	directWrite := func(ctx context.Context, store c1zstore.Store) error {
		return store.PutGrants(ctx, mkV2Grant("g1", "ent", "user", "alice"))
	}

	t.Run("no hook: write proceeds", func(t *testing.T) {
		store, seen := newHookStore(t, false)
		// Inside a page, which is what makes this the interesting cell:
		// with no hook the hook must not consult PageOpen at all.
		require.NoError(t, directWrite(c1zstore.WithOpenPage(ctx), store))
		require.Empty(t, *seen)
	})

	t.Run("hook, no page: write proceeds", func(t *testing.T) {
		store, seen := newHookStore(t, true)
		require.NoError(t, directWrite(ctx, store))
		require.Empty(t, *seen, "a write outside a page is not a bypass and must not reach the hook")
	})

	t.Run("hook, page open, unregistered: refused", func(t *testing.T) {
		store, seen := newHookStore(t, true)
		err := directWrite(c1zstore.WithOpenPage(ctx), store)
		require.ErrorIs(t, err, c1zstore.ErrUnregisteredPageWrite)
		require.Len(t, *seen, 1)
		require.False(t, (*seen)[0].Registered())
		require.NotEmpty(t, (*seen)[0].Method, "the event must name the method so a failure is actionable")
	})

	t.Run("hook, page open, registered bypass: proceeds and is recorded", func(t *testing.T) {
		store, seen := newHookStore(t, true)
		const reason = "asset blob: precedes the row and is idempotent on re-run"
		pageCtx := c1zstore.WithPageWriteBypass(c1zstore.WithOpenPage(ctx), reason)
		require.NoError(t, directWrite(pageCtx, store))
		require.Len(t, *seen, 1)
		require.True(t, (*seen)[0].Registered())
		require.Equal(t, reason, (*seen)[0].Bypass)
	})

	t.Run("removing the hook restores the no-hook outcome", func(t *testing.T) {
		store, seen := newHookStore(t, true)
		store.(c1zstore.WriteHookStore).SetWriteHook(nil)
		require.NoError(t, directWrite(c1zstore.WithOpenPage(ctx), store))
		require.Empty(t, *seen)
	})
}

// The context helpers are what the store's check reads, so their own
// behaviour is worth pinning separately: an empty bypass reason must not
// register, or a caller could silence the hook by passing "".
func TestWriteHookContextHelpers(t *testing.T) {
	ctx := context.Background()
	require.False(t, c1zstore.PageOpen(ctx))
	require.True(t, c1zstore.PageOpen(c1zstore.WithOpenPage(ctx)))

	_, ok := c1zstore.PageWriteBypass(ctx)
	require.False(t, ok)

	reason, ok := c1zstore.PageWriteBypass(c1zstore.WithPageWriteBypass(ctx, "why"))
	require.True(t, ok)
	require.Equal(t, "why", reason)

	_, ok = c1zstore.PageWriteBypass(c1zstore.WithPageWriteBypass(ctx, ""))
	require.False(t, ok, "an empty reason must not count as a registration")

	// StrictWriteHook tolerates a nil recorder.
	require.NoError(t, c1zstore.StrictWriteHook(nil)(ctx, c1zstore.WriteHookEvent{Method: "M", Bypass: "r"}))
	require.ErrorIs(t,
		c1zstore.StrictWriteHook(nil)(ctx, c1zstore.WriteHookEvent{Method: "M"}),
		c1zstore.ErrUnregisteredPageWrite)
}
