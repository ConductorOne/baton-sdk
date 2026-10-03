package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// Every invariant scan must reject a key that does not parse as its keyspace's
// tuple shape. Seek targets re-encoded from such keys used to sort below the
// key, so the scan revisited it until the context ended; the deadline turns
// that regression into a fast failure instead of a hung test.
func TestSecurity_IngestScansRejectNonCanonicalKeys(t *testing.T) {
	scans := map[string]func(context.Context, *Engine) error{
		"I3": func(ctx context.Context, e *Engine) error {
			return e.ForEachDistinctGrantEntitlementResource(ctx, func(string, string) error { return nil })
		},
		"I7": func(ctx context.Context, e *Engine) error {
			return e.ForEachDistinctEntitlementResource(ctx, func(string, string) error { return nil })
		},
		"I8": func(ctx context.Context, e *Engine) error {
			return e.ForEachDanglingGrantEntitlement(ctx, func(string, string, string) error { return nil })
		},
		"I9": func(ctx context.Context, e *Engine) error {
			return e.ForEachDanglingGrantPrincipal(ctx, func(string, string, bool, int64) error { return nil })
		},
	}
	grant := func(tail ...byte) []byte { return append([]byte{versionV3, typeGrant}, tail...) }
	for _, tc := range []struct {
		name  string
		key   []byte
		scans []string
	}{
		{"grant key without the header separator", grant(0x05, 'a', 0, 'b', 0, '1', 0, 'c', 0, 'u', 0, 'p'), []string{"I3", "I8"}},
		{"grant key with identity flag 2", grant(0, 'a', 0, 'b', 0, '2', 0, 'c', 0, 'u', 0, 'p'), []string{"I8"}},
		{"grant key ending inside the entitlement identity", grant(0, 'a', 0, 'b', 0, '1', 0, 'c'), []string{"I8"}},
		{"entitlement key without the header separator", []byte{versionV3, typeEntitlement, 0x05, 'a', 0, 'b', 0, '1', 0, 'c'}, []string{"I7"}},
		{"by_principal key without the header separator", []byte{versionV3, typeIndex, idxGrantByPrincipal, 0x05, 'u', 0, 'p', 0, 'a'}, []string{"I9"}},
	} {
		for _, scan := range tc.scans {
			t.Run(tc.name+"/"+scan, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				a := newAdapter(t)
				_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				e := a.PebbleEngine()
				require.NoError(t, e.db.UnsafeForTesting().Set(tc.key, nil, nil))

				err = scans[scan](ctx, e)
				require.NotErrorIs(t, err, context.DeadlineExceeded, "the scan revisited the key instead of rejecting it")
				require.ErrorContains(t, err, "malformed")
			})
		}
	}
}

// Canonical keys still group correctly: one visit per entitlement resource
// and one per entitlement identity, with stripped ("1") and opaque ("0")
// identities on the same resource kept apart.
func TestIngestScansVisitEachGroupOnce(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	resource := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build()}.Build()
	grant := func(entID, principalID string) *v2.Grant {
		return v2.Grant_builder{
			Id:          entID + ":user:" + principalID,
			Entitlement: v2.Entitlement_builder{Id: entID, Resource: resource}.Build(),
			Principal:   v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: principalID}.Build()}.Build(),
		}.Build()
	}
	// "app:github:member" is stored stripped, "raw-opaque-ent" opaque.
	require.NoError(t, a.PutGrants(ctx,
		grant("app:github:member", "alice"),
		grant("app:github:member", "bob"),
		grant("raw-opaque-ent", "alice"),
		grant("raw-opaque-ent", "bob"),
	))

	var resources [][2]string
	require.NoError(t, e.ForEachDistinctGrantEntitlementResource(ctx, func(rt, rid string) error {
		resources = append(resources, [2]string{rt, rid})
		return nil
	}))
	require.Equal(t, [][2]string{{"app", "github"}}, resources)

	require.NoError(t, e.EnsureGrantIndexes(ctx))
	var principals []string
	require.NoError(t, e.ForEachDanglingGrantPrincipal(ctx, func(rt, rid string, _ bool, _ int64) error {
		principals = append(principals, rt+"/"+rid)
		return nil
	}))
	require.Equal(t, []string{"user/alice", "user/bob"}, principals)

	var dangling []string
	require.NoError(t, e.ForEachDanglingGrantEntitlement(ctx, func(entID, _, _ string) error {
		dangling = append(dangling, entID)
		return nil
	}))
	require.Equal(t, []string{"raw-opaque-ent", "app:github:member"}, dangling)

	require.NoError(t, a.PutEntitlements(ctx,
		v2.Entitlement_builder{Id: "app:github:member", Resource: resource}.Build(),
		v2.Entitlement_builder{Id: "raw-opaque-ent", Resource: resource}.Build(),
	))
	dangling = nil
	require.NoError(t, e.ForEachDanglingGrantEntitlement(ctx, func(entID, _, _ string) error {
		dangling = append(dangling, entID)
		return nil
	}))
	require.Empty(t, dangling)

	resources = nil
	require.NoError(t, e.ForEachDistinctEntitlementResource(ctx, func(rt, rid string) error {
		resources = append(resources, [2]string{rt, rid})
		return nil
	}))
	require.Equal(t, [][2]string{{"app", "github"}}, resources)
}
