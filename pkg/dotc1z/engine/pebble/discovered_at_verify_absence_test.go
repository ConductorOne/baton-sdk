package pebble

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// === V-06 + V-20: absence → nil, NEVER a fresh now() ===
//
// The highest-value trap (plan §5 "Absence"). A grant with NO stored
// discovered_at must read back as nil/absent through the v3 reader — never a
// synthesized time.Now(). V-20 is the premise gate: we first assert at the
// RAW v3.GrantRecord layer (GetGrantRecord) that the stored record genuinely
// lacks a discovered_at, so the reader assertion below cannot pass vacuously.
//
// Seeding: PutGrantRecord writes the v3.GrantRecord as-is with no now() stamp
// (only the v2->v3 translateGrants path stamps). That is the only write path
// that can produce a genuinely-absent discovered_at — see the implementation
// addendum §B.
func TestV3ReadAbsentDiscoveredAtReturnsNilNeverNow(t *testing.T) {
	ctx := context.Background()
	e, r := newV3GrantReader(ctx, t)

	// The window the fabrication trap rejects: any wall-clock stamp the read
	// path could invent lands at-or-after windowStart.
	windowStart := time.Now().Add(-time.Second)

	// Seed a grant with NO discovered_at, straight through the engine.
	rec := v3.GrantRecord_builder{
		ExternalId: "g-absent",
		Entitlement: v3.EntitlementRef_builder{
			ResourceTypeId: "app", ResourceId: "github", EntitlementId: canonicalTestEntID("ent-A"),
		}.Build(),
		Principal: v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "alice"}.Build(),
	}.Build()
	require.Nil(t, rec.GetDiscoveredAt(), "fixture must start with nil discovered_at")
	require.NoError(t, e.PutGrantRecord(ctx, rec))

	// V-20 PREMISE GATE: the RAW stored record genuinely lacks discovered_at.
	// If a write path had silently stamped it, this fails and the reader
	// assertions below would be vacuous.
	raw, err := e.GetGrantRecord(ctx, "g-absent")
	require.NoError(t, err)
	require.Nil(t, raw.GetDiscoveredAt(),
		"V-20 premise: raw stored GrantRecord must genuinely lack discovered_at (absence case is real, not vacuous)")

	assertAbsent := func(t *testing.T, got *v3.GrantRecord, where string) {
		t.Helper()
		require.NotNil(t, got, "%s: reader must still return the grant", where)
		// The load-bearing assertion: absence stays absence.
		require.Nil(t, got.GetDiscoveredAt(),
			"%s: absent discovered_at must read back nil, never a synthesized now()", where)
		// Wall-clock fabrication trap: even if a future regression returned a
		// non-nil value, it must not be a fresh wall-clock instant.
		if got.GetDiscoveredAt() != nil {
			require.False(t, got.GetDiscoveredAt().AsTime().After(windowStart),
				"%s: discovered_at fell inside the test wall-clock window — a now() reflex on the read path", where)
		}
	}

	// GetGrant (point read).
	gg, err := r.GetGrant(ctx, reader_v3.GrantsReaderServiceGetGrantRequest_builder{GrantId: "g-absent"}.Build())
	require.NoError(t, err)
	assertAbsent(t, gg.GetGrant(), "v3 GetGrant")

	// ListGrantsForEntitlement (scan read) — different code path (iterate vs point-get).
	lg, err := r.ListGrantsForEntitlement(ctx, reader_v3.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v3TestEntStub("ent-A"),
		PageSize:    100,
	}.Build())
	require.NoError(t, err)
	require.Len(t, lg.GetList(), 1)
	assertAbsent(t, lg.GetList()[0], "v3 ListGrantsForEntitlement")

	// ListGrants (unfiltered, arena path with reconcileAbsentFields).
	lgAll, err := e.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageSize: 100}.Build())
	require.NoError(t, err)
	require.Len(t, lgAll.GetList(), 1, "sanity: the grant is on the unfiltered list path too")
}

// === V-15: the read path contains no time.Now()/timestamppb.Now() ===
//
// Cheap static grep-gate over the source files that make up the grant read
// path. These files carry zero legitimate now() (all writes/lifecycle stamps
// live elsewhere — addendum §C). A regression that adds a wall-clock read on
// the read path trips this without needing to execute the branch.
func TestV3ReadPathHasNoWallClockReflex(t *testing.T) {
	readPathFiles := []string{
		"adapter_reader_v3.go", // the entire v3 read surface
		"grant_read_arena.go",  // the read decode arena
		"paginate.go",          // the shared grant paginators + point-get
	}
	banned := []string{"time.Now(", "timestamppb.Now("}
	for _, f := range readPathFiles {
		src, err := os.ReadFile(f)
		require.NoError(t, err, "read %s", f)
		text := string(src)
		for _, b := range banned {
			require.NotContains(t, text, b,
				"read-path file %s must not call %s — a wall-clock read on the grant read path violates C1/C2 (never now() at read)", f, b)
		}
	}
}

// === V-12(a) + V-06 invalid: a decode fault surfaces as an ERROR, never a
// silent zero discovered_at ===
//
// Plant corrupt bytes under a grant's primary key (raw pebble Set, bypassing
// the record codec) and confirm every v3 read method returns a non-nil error
// rather than a "successful" GrantRecord with a zero/absent discovered_at. A
// swallowed decode error masquerading as absence is exactly the silent-failure
// mode C18/C2 forbid.
func TestV3ReadCorruptRecordSurfacesErrorNeverSilentZero(t *testing.T) {
	ctx := context.Background()
	e, r := newV3GrantReader(ctx, t)

	seeded := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	seedGrantWithDiscoveredAt(ctx, t, e, "g-corrupt", "ent-A", "alice", seeded)

	// Resolve the record's primary identity key and clobber its VALUE with
	// bytes that fail proto decode. The key stays valid so the id-index still
	// resolves and the read reaches the record decode.
	recRaw, err := e.GetGrantRecord(ctx, "g-corrupt")
	require.NoError(t, err)
	id, err := grantIdentityFromRecord(recRaw)
	require.NoError(t, err)
	key := encodeGrantIdentityKey(id)

	// Invalid protobuf wire: a bare field-tag byte with no payload / bad
	// wiretype. unmarshalRecord (UnmarshalVT) must reject this.
	require.NoError(t, e.db.UnsafeForTesting().Set(key, []byte{0xff, 0xff, 0xff, 0xff}, pebble.Sync))

	// Point read: must error, not return a zero-discovered_at grant.
	gg, err := r.GetGrant(ctx, reader_v3.GrantsReaderServiceGetGrantRequest_builder{GrantId: "g-corrupt"}.Build())
	require.Error(t, err, "v3 GetGrant over a corrupt record must return an error, not a silent zero discovered_at")
	if gg != nil {
		require.Nil(t, gg.GetGrant(), "no partial GrantRecord may be returned alongside a decode fault")
	}

	// Scan read: a swallowed decode error must not surface as a page whose
	// grant carries a silent zero/absent discovered_at.
	lg, err := r.ListGrantsForEntitlement(ctx, reader_v3.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v3TestEntStub("ent-A"),
		PageSize:    100,
	}.Build())
	require.Error(t, err, "v3 ListGrantsForEntitlement over a corrupt record must return an error, not a silent partial page")
	if lg != nil {
		require.Empty(t, lg.GetList(), "no partial page may accompany a decode fault")
	}
}
