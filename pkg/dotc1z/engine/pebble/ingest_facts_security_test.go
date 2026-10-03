package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestSecurity_IngestScansTerminateOnNonCanonicalKeys pins C1Z-SEC-006:
// the prefix-skip scans must make forward progress on ANY key bytes.
// Two hostile shapes previously re-encoded BELOW the current key, so
// SeekGE never advanced and the loop spun at 100% CPU until the context
// was cancelled — and the syncer runs the invariant pass under the
// outer context, so a compaction worker ingesting a hostile v3 c1z hung
// until externally killed:
//
//   - key[2] != the tuple separator: the scans bound the iterator at the
//     2-byte type header but skip headerLen=3 assuming the separator,
//     so every re-encoded prefix (which always emits the separator)
//     sorts strictly below the current key.
//   - an identity flag component other than "0"/"1": the dangling scan
//     folded every non-"1" value to stripped=false, re-encoding "0"
//     where the key holds e.g. "2".
//
// The scans now reject both shapes with the malformed-key error. Each
// subtest carries its own 30s deadline: pre-fix, the hostile subtests
// fail with context.DeadlineExceeded (the hang itself); post-fix they
// return the malformed-key error in microseconds.
func TestSecurity_IngestScansTerminateOnNonCanonicalKeys(t *testing.T) {
	// Per-subtest hang guard, so one spinning subtest cannot eat the
	// budget of the next (pre-fix, each hostile subtest burns its full
	// 30s; the error is context.DeadlineExceeded, never "malformed").
	hangGuard := func(t *testing.T) context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		t.Cleanup(cancel)
		return ctx
	}

	// Grant-family key whose third byte is not the tuple separator.
	nonCanonicalHeader := append(encodeGrantPrefix(), 0x05, 'a', 0x00, 'b')
	// Grant key with flag component "2" in the identity slot: canonical
	// header, non-canonical flag. Four identity segments suffice — the
	// flag check runs after the 4-component decode, before any
	// principal segment is read.
	nonCanonicalFlag := append(encodeGrantPrefix(), 0x00, 'a', 0x00, 'b', 0x00, '2', 0x00, 'c')

	t.Run("grant scans reject non-canonical header", func(t *testing.T) {
		ctx := hangGuard(t)
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e := a.PebbleEngine()
		require.NoError(t, e.db.UnsafeForTesting().Set(nonCanonicalHeader, nil, nil))
		require.NoError(t, e.db.UnsafeForTesting().Set(nonCanonicalFlag, nil, nil))

		err = e.ForEachDistinctGrantEntitlementResource(ctx, func(string, string) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "malformed")
		require.NotErrorIs(t, err, context.DeadlineExceeded, "the scan must reject the key, not hang on it")

		err = e.ForEachDanglingGrantEntitlement(ctx, func(string, string, string) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "malformed")
		require.NotErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("dangling-entitlement scan rejects non-canonical flag", func(t *testing.T) {
		ctx := hangGuard(t)
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e := a.PebbleEngine()
		require.NoError(t, e.db.UnsafeForTesting().Set(nonCanonicalFlag, nil, nil))

		err = e.ForEachDanglingGrantEntitlement(ctx, func(string, string, string) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "malformed")
		require.NotErrorIs(t, err, context.DeadlineExceeded, "the scan must reject the key, not hang on it")
	})

	t.Run("entitlement scan rejects non-canonical header", func(t *testing.T) {
		ctx := hangGuard(t)
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e := a.PebbleEngine()
		require.NoError(t, e.db.UnsafeForTesting().Set(append(encodeEntitlementPrefix(), 0x05, 'a', 0x00, 'b'), nil, nil))

		err = e.ForEachDistinctEntitlementResource(ctx, func(string, string) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "malformed")
		require.NotErrorIs(t, err, context.DeadlineExceeded, "the scan must reject the key, not hang on it")
	})

	t.Run("canonical keys still skip correctly (control)", func(t *testing.T) {
		ctx := hangGuard(t)
		a := newAdapter(t)
		_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoError(t, err)
		e := a.PebbleEngine()

		// Two grants on one entitlement resource with DIFFERENT identity
		// flags: "app:github:member" begins with "app:github:" so it
		// stores stripped (flag "1", tail "member"); "raw-opaque-ent"
		// does not, so it stores opaque (flag "0"). The skip must treat
		// them as two distinct identities, not fold the flag.
		mkGrant := func(entID, principalID string) *v2.Grant {
			return v2.Grant_builder{
				Id: entID + ":user:" + principalID,
				Entitlement: v2.Entitlement_builder{
					Id: entID,
					Resource: v2.Resource_builder{
						Id: v2.ResourceId_builder{
							ResourceType: "app",
							Resource:     "github",
						}.Build(),
					}.Build(),
				}.Build(),
				Principal: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: "user",
						Resource:     principalID,
					}.Build(),
				}.Build(),
			}.Build()
		}
		require.NoError(t, a.PutGrants(ctx,
			mkGrant("app:github:member", "alice"),
			mkGrant("raw-opaque-ent", "bob"),
		))

		// I3: one visit per distinct entitlement resource.
		var resources [][2]string
		require.NoError(t, e.ForEachDistinctGrantEntitlementResource(ctx, func(rt, rid string) error {
			resources = append(resources, [2]string{rt, rid})
			return nil
		}))
		require.Equal(t, [][2]string{{"app", "github"}}, resources,
			"both grants share one entitlement resource; the skip must visit it exactly once")

		// I8: both identities are dangling (no entitlement rows); each
		// visited exactly once, opaque ("0") before stripped ("1") in
		// key order.
		var dangling []string
		require.NoError(t, e.ForEachDanglingGrantEntitlement(ctx, func(entID, _, _ string) error {
			dangling = append(dangling, entID)
			return nil
		}))
		require.Equal(t, []string{"raw-opaque-ent", "app:github:member"}, dangling,
			"the two flags are two distinct identities; each must be visited exactly once")

		// With entitlement rows present, the dangling scan visits
		// nothing, and I7 visits the shared resource exactly once.
		mkEnt := func(entID string) *v2.Entitlement {
			return v2.Entitlement_builder{
				Id: entID,
				Resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: "app",
						Resource:     "github",
					}.Build(),
				}.Build(),
			}.Build()
		}
		require.NoError(t, a.PutEntitlements(ctx, mkEnt("app:github:member"), mkEnt("raw-opaque-ent")))

		dangling = nil
		require.NoError(t, e.ForEachDanglingGrantEntitlement(ctx, func(string, string, string) error {
			dangling = append(dangling, "visited")
			return nil
		}))
		require.Empty(t, dangling, "existing entitlement rows make both identities non-dangling")

		resources = nil
		require.NoError(t, e.ForEachDistinctEntitlementResource(ctx, func(rt, rid string) error {
			resources = append(resources, [2]string{rt, rid})
			return nil
		}))
		require.Equal(t, [][2]string{{"app", "github"}}, resources,
			"both entitlements share one resource; the skip must visit it exactly once")
	})
}
