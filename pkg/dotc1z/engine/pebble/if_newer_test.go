package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// TestDiscoveredAtIsNewer locks in the SQLite-parity contract for
// the engine-level IfNewer predicate.
//
// Adapter-level PutXxxIfNewer methods stamp DiscoveredAt with
// time.Now() before calling here (see pkg/dotc1z/engine/pebble/
// adapter_grants_store.go), so the nil-incoming branch is
// unreachable through the public API today. The branch exists as a
// contract for direct engine callers (e.g. a future migration or
// compactor that bypasses the adapter) and as a NULL-propagation
// match for SQLite's `EXCLUDED.discovered_at > X.discovered_at`
// upsert semantic.
//
// This test pins the contract so a future edit that "simplifies"
// the nil branch doesn't silently regress to the pre-fix behavior
// of writing on nil incoming (which would let any direct caller
// clobber existing records with stamp-less writes).
func TestDiscoveredAtIsNewer(t *testing.T) {
	t1 := timestamppb.New(time.Unix(1_700_000_000, 0))
	t2 := timestamppb.New(time.Unix(1_700_000_100, 0)) // strictly after t1

	tests := []struct {
		name     string
		incoming *timestamppb.Timestamp
		existing *timestamppb.Timestamp
		want     bool
	}{
		// SQLite `NULL > anything` is NULL (don't write). The
		// pre-fix Pebble behavior returned true here, which would
		// have let any direct engine caller without a DiscoveredAt
		// clobber an existing record.
		{"nil incoming, nil existing — do not write", nil, nil, false},
		{"nil incoming, real existing — do not write", nil, t1, false},

		// No prior record at this key → SQLite's INSERT-on-conflict
		// reduces to a plain INSERT. Write unconditionally.
		{"real incoming, nil existing — write", t1, nil, true},

		// Strict After: equal timestamps are NOT newer (matches
		// SQLite's > operator, exercised end-to-end through the full
		// PutXxxRecordsIfNewer path by TestPutGrantRecordsIfNewerSkipsStale
		// and TestPutRecordsIfNewerRejectsOlderAllTypes below).
		{"strictly newer incoming — write", t2, t1, true},
		{"equal timestamps — do not write", t1, t1, false},
		{"older incoming — do not write", t1, t2, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := discoveredAtIsNewer(tt.incoming, tt.existing)
			require.Equal(t, tt.want, got, "discoveredAtIsNewer(incoming=%v, existing=%v)", tt.incoming, tt.existing)
		})
	}
}

func ifNewerGrant(principal string, at time.Time) *v3.GrantRecord {
	return ifNewerGrantExternal("g-1", principal, at)
}

func ifNewerGrantExternal(externalID, principal string, at time.Time) *v3.GrantRecord {
	return v3.GrantRecord_builder{
		ExternalId: externalID,
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: "ent-A",
		}.Build(),
		Principal:    v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: principal}.Build(),
		DiscoveredAt: timestamppb.New(at),
	}.Build()
}

func grantsByPrincipal(t *testing.T, ctx context.Context, e *Engine, principal string) []string {
	t.Helper()
	var ids []string
	require.NoError(t, e.IterateGrantsByPrincipal(ctx, "user", principal, func(g *v3.GrantRecord) bool {
		ids = append(ids, g.GetExternalId())
		return true
	}))
	return ids
}

// TestPutGrantRecordsIfNewerSkipsStale is the end-to-end guard the
// TestDiscoveredAtIsNewer predicate test points at: it drives the full
// PutGrantRecordsIfNewer path (read incumbent, compare discovered_at,
// conditionally write + swap derived index keys) rather than the bare
// predicate. It pins that a stale (older or equal) replay never
// regresses the stored grant OR its by_principal index, and that a
// strictly-newer record both replaces the value and moves the index
// entry off the old principal — the index-swap step a value-only check
// would miss.
func TestPutGrantRecordsIfNewerSkipsStale(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	require.NoError(t, e.SetCurrentSync(ksuid.New().String()))

	older := time.Unix(1000, 0).UTC()
	newer := time.Unix(2000, 0).UTC()
	newest := time.Unix(3000, 0).UTC()

	// Seed the incumbent at `newer`.
	require.NoError(t, e.PutGrantRecordsIfNewer(ctx, ifNewerGrant("winner", newer)))
	grantID := canonicalTestGrantID("ent-A", "user", "winner")
	got, err := e.GetGrantRecord(ctx, grantID)
	require.NoError(t, err)
	require.Equal(t, "winner", got.GetPrincipal().GetResourceId())

	// Older replay: dropped — value and index unchanged.
	require.NoError(t, e.PutGrantRecordsIfNewer(ctx, ifNewerGrantExternal("g-stale-older", "winner", older)))
	got, err = e.GetGrantRecord(ctx, grantID)
	require.NoError(t, err)
	require.Equal(t, "winner", got.GetPrincipal().GetResourceId(), "older replay must not regress the grant")

	// Equal replay: dropped — strict `>` keeps the incumbent.
	require.NoError(t, e.PutGrantRecordsIfNewer(ctx, ifNewerGrantExternal("g-stale-tie", "winner", newer)))
	got, err = e.GetGrantRecord(ctx, grantID)
	require.NoError(t, err)
	require.Equal(t, "winner", got.GetPrincipal().GetResourceId(), "equal discovered_at must keep the incumbent")

	// No stale index keys leaked from the dropped writes.
	require.Equal(t, []string{"g-1"}, grantsByPrincipal(t, ctx, e, "winner"))

	// Strictly newer for the same identity: replaces the retained value.
	require.NoError(t, e.PutGrantRecordsIfNewer(ctx, ifNewerGrantExternal("g-newest", "winner", newest)))
	got, err = e.GetGrantRecord(ctx, grantID)
	require.NoError(t, err)
	require.Equal(t, "g-newest", got.GetExternalId(), "strictly newer must replace")
	require.Equal(t, []string{"g-newest"}, grantsByPrincipal(t, ctx, e, "winner"))
}

// TestPutRecordsIfNewerRejectsOlderAllTypes guards the per-type
// discovered_at field constants in the *IfNewer path
// (grant/resource/entitlement/resource_type each scan a different proto
// field): for every primary type, seeding a newer record then replaying
// an older one must keep the newer incumbent. A transposed field
// constant would read the wrong bytes and silently let the older replay
// win for that one type.
func TestPutRecordsIfNewerRejectsOlderAllTypes(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	require.NoError(t, e.SetCurrentSync(ksuid.New().String()))

	older := time.Unix(1000, 0).UTC()
	newer := time.Unix(2000, 0).UTC()

	t.Run("resource_type", func(t *testing.T) {
		newRec := func(dn string, at time.Time) *v3.ResourceTypeRecord {
			return v3.ResourceTypeRecord_builder{ExternalId: "rt-1", DisplayName: dn, DiscoveredAt: timestamppb.New(at)}.Build()
		}
		require.NoError(t, e.PutResourceTypeRecordsIfNewer(ctx, newRec("kept", newer)))
		require.NoError(t, e.PutResourceTypeRecordsIfNewer(ctx, newRec("stale", older)))
		got, err := e.GetResourceTypeRecord(ctx, "rt-1")
		require.NoError(t, err)
		require.Equal(t, "kept", got.GetDisplayName())
	})

	t.Run("resource", func(t *testing.T) {
		newRec := func(dn string, at time.Time) *v3.ResourceRecord {
			return v3.ResourceRecord_builder{ResourceTypeId: "user", ResourceId: "u1", DisplayName: dn, DiscoveredAt: timestamppb.New(at)}.Build()
		}
		require.NoError(t, e.PutResourceRecordsIfNewer(ctx, newRec("kept", newer)))
		require.NoError(t, e.PutResourceRecordsIfNewer(ctx, newRec("stale", older)))
		got, err := e.GetResourceRecord(ctx, "user", "u1")
		require.NoError(t, err)
		require.Equal(t, "kept", got.GetDisplayName())
	})

	t.Run("entitlement", func(t *testing.T) {
		newRec := func(dn string, at time.Time) *v3.EntitlementRecord {
			return v3.EntitlementRecord_builder{
				ExternalId:   "e-1",
				Resource:     v3.ResourceRef_builder{ResourceTypeId: "user", ResourceId: "u1"}.Build(),
				DisplayName:  dn,
				DiscoveredAt: timestamppb.New(at),
			}.Build()
		}
		require.NoError(t, e.PutEntitlementRecordsIfNewer(ctx, newRec("kept", newer)))
		require.NoError(t, e.PutEntitlementRecordsIfNewer(ctx, newRec("stale", older)))
		got, err := e.GetEntitlementRecord(ctx, "user:u1:custom:e-1")
		require.NoError(t, err)
		require.Equal(t, "kept", got.GetDisplayName())
	})

	t.Run("grant", func(t *testing.T) {
		require.NoError(t, e.PutGrantRecordsIfNewer(ctx, ifNewerGrant("kept", newer)))
		require.NoError(t, e.PutGrantRecordsIfNewer(ctx, ifNewerGrant("stale", older)))
		got, err := e.GetGrantRecord(ctx, canonicalTestGrantID("ent-A", "user", "kept"))
		require.NoError(t, err)
		require.Equal(t, "kept", got.GetPrincipal().GetResourceId())
	})
}
