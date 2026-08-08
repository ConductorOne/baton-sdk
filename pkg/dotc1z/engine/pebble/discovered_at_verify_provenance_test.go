package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// === V-08: provenance preservation (C9) ===
//
// The read is one choke point (addendum §A), so per-class correctness reduces
// to: each WRITE path commits the right discovered_at, and the single v3 read
// path returns it verbatim. Each subtest drives a distinct provenance class
// then reads back through the v3 reader's GetGrant.
//
// Classes driven here:
//   - connector-observed  : PutGrants (v2->v3 translate stamps now at write)
//   - synthesized-explicit: PutSynthesizedGrantRecords with a supplied instant
//   - synthesized-nil     : PutSynthesizedGrantRecords with nil -> stamped now
//   - expander preserve   : StoreExpandedGrants over an existing grant -> prior kept
//
// The bulk-import / SQLite->Pebble migration class ("rides source value") is a
// dotc1z-level path (to_pebble.go carries the source discovered_at, see
// addendum §G); it is asserted at the storage layer by the existing converter
// suite and is NOT re-driven here — recorded as a scoped note in the evidence.
func TestV3ReadProvenanceClasses(t *testing.T) {
	readBack := func(t *testing.T, r connectorstore.V3GrantReader, extID string) *v3.GrantRecord {
		t.Helper()
		resp, err := r.GetGrant(context.Background(), reader_v3.GrantsReaderServiceGetGrantRequest_builder{
			GrantId: extID,
		}.Build())
		require.NoError(t, err)
		return resp.GetGrant()
	}

	t.Run("connector-observed_PutGrants_stamps_at_write", func(t *testing.T) {
		ctx := context.Background()
		e, r := newV3GrantReader(ctx, t)
		a := NewAdapter(e)

		before := time.Now().Add(-time.Second)
		require.NoError(t, a.PutGrants(ctx, mkV2Grant("", "ent-A", "user", "alice")))
		after := time.Now().Add(time.Second)

		id := canonicalTestGrantID("ent-A", "user", "alice")
		got := readBack(t, r, id)
		require.NotNil(t, got.GetDiscoveredAt(), "connector-observed grant must carry a stamped discovered_at")
		ts := got.GetDiscoveredAt().AsTime()
		require.Falsef(t, ts.Before(before) || ts.After(after),
			"connector-observed discovered_at %s must be the write-time stamp within [%s,%s]", ts, before, after)

		// Not re-derived at read: a second read returns the identical instant.
		got2 := readBack(t, r, id)
		require.True(t, got.GetDiscoveredAt().AsTime().Equal(got2.GetDiscoveredAt().AsTime()),
			"discovered_at must be stable across reads, never re-derived per call")
	})

	t.Run("synthesized_explicit_value_preserved", func(t *testing.T) {
		ctx := context.Background()
		e, r := newV3GrantReader(ctx, t)

		explicit := time.Date(2017, 5, 4, 3, 2, 1, 0, time.UTC)
		rec := v3.GrantRecord_builder{
			ExternalId: "g-synth-explicit",
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "app", ResourceId: "github", EntitlementId: canonicalTestEntID("ent-A"),
			}.Build(),
			Principal:    v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "alice"}.Build(),
			DiscoveredAt: timestamppb.New(explicit),
		}.Build()
		require.NoError(t, e.putSynthesizedGrantRecords(ctx, []*v3.GrantRecord{rec}))

		got := readBack(t, r, "g-synth-explicit")
		require.NotNil(t, got.GetDiscoveredAt())
		require.Equal(t, explicit, got.GetDiscoveredAt().AsTime(),
			"a synthesized grant carrying an explicit discovered_at must read back verbatim, not re-stamped")
	})

	t.Run("synthesized_nil_backfilled_at_write", func(t *testing.T) {
		ctx := context.Background()
		e, r := newV3GrantReader(ctx, t)

		before := time.Now().Add(-time.Second)
		rec := v3.GrantRecord_builder{
			ExternalId: "g-synth-nil",
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "app", ResourceId: "github", EntitlementId: canonicalTestEntID("ent-A"),
			}.Build(),
			Principal: v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: "bob"}.Build(),
		}.Build()
		require.Nil(t, rec.GetDiscoveredAt())
		require.NoError(t, e.putSynthesizedGrantRecords(ctx, []*v3.GrantRecord{rec}))
		after := time.Now().Add(time.Second)

		got := readBack(t, r, "g-synth-nil")
		require.NotNil(t, got.GetDiscoveredAt(), "synthesized-new grant must be backfilled at write")
		ts := got.GetDiscoveredAt().AsTime()
		require.Falsef(t, ts.Before(before) || ts.After(after),
			"synthesized-new discovered_at %s must be the write-time stamp within [%s,%s]", ts, before, after)
	})

	t.Run("expander_rewrite_preserves_prior", func(t *testing.T) {
		ctx := context.Background()
		e, r := newV3GrantReader(ctx, t)
		a := NewAdapter(e)

		// Seed a grant with a deterministic clearly-past discovered_at.
		prior := time.Date(2019, 2, 3, 4, 5, 6, 0, time.UTC)
		seedGrantWithDiscoveredAt(ctx, t, e, "g-exp", "ent-A", "alice", prior)

		// Expander rewrites the SAME grant identity (stripped annotation).
		app := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build()}.Build()
		rewrite := v2.Grant_builder{
			Id:          "g-exp",
			Entitlement: v2.Entitlement_builder{Id: canonicalTestEntID("ent-A"), Resource: app}.Build(),
			Principal:   v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build()}.Build(),
		}.Build()
		require.NoError(t, a.Grants().StoreExpandedGrants(ctx, rewrite))

		// StoreExpandedGrants defers the id/by_principal index build to
		// EndSync (the primary row is committed immediately), so read back
		// through the v3 reader via a STRUCTURED route (primary-keyspace
		// scan) rather than the bare-id GetGrant lookup. This still exercises
		// the single v3 read choke point.
		resp, err := r.ListGrantsForEntitlement(ctx, reader_v3.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: v3TestEntStub("ent-A"),
			PageSize:    100,
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 1, "expander rewrite must not duplicate the grant")
		got := resp.GetList()[0]
		require.NotNil(t, got.GetDiscoveredAt())
		require.Equal(t, prior, got.GetDiscoveredAt().AsTime(),
			"expander rewrite must PRESERVE the prior discovered_at through the v3 read path, never re-stamp to now")
	})
}
