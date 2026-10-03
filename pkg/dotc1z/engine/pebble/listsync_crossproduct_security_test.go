package pebble

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestSecurity_ListSyncsBoundedAgainstHostileSyncRunCardinality: a v3
// pebble c1z holds exactly one sync-run record, at the fixed key (keys.go).
// ListSyncs used to walk the whole typeSyncRun range and compute stats per
// row, so K planted rows (each missing the stats sidecar) turned one RPC
// into K full-keyspace stats scans; the c1 sync worker calls ListSyncs
// after every sync. Stats must be computed only for the canonical key: the
// canonical row carries exact stats, every planted row reads with nil
// stats, and paging past the canonical key does no stats work.
func TestSecurity_ListSyncsBoundedAgainstHostileSyncRunCardinality(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)

	// A real sync with real data rows (N grants): known counts.
	syncID, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	const grantsN = 200
	grants := make([]*v3.GrantRecord, 0, grantsN)
	for i := range grantsN {
		grants = append(grants, (&v3.GrantRecord_builder{
			Entitlement: v3.EntitlementRef_builder{
				ResourceTypeId: "app",
				ResourceId:     fmt.Sprintf("app-%04d", i%16),
				EntitlementId:  fmt.Sprintf("ent-%04d:member", i%64),
			}.Build(),
			Principal: v3.PrincipalRef_builder{
				ResourceTypeId: "user",
				ResourceId:     fmt.Sprintf("user-%06d", i),
			}.Build(),
			ExternalId: fmt.Sprintf("g-%06d", i),
		}).Build())
	}
	require.NoError(t, e.PutGrantRecords(ctx, grants...))

	db := e.db.UnsafeForTesting()
	// Plant K bogus sync-run rows under the range prefix but NOT at the
	// canonical fixed key (encodeSyncRunKey). These are the hostile
	// cardinality: pre-fix each one triggered a full-keyspace stats scan.
	const bogusK = 20
	plant := func(k int) {
		t.Helper()
		for i := range k {
			rec := (&v3.SyncRunRecord_builder{
				SyncId: fmt.Sprintf("bogus-%06d", i),
				Type:   v3.SyncType_SYNC_TYPE_FULL,
			}).Build()
			val, err := marshalRecord(rec)
			require.NoError(t, err)
			key := append([]byte{versionV3, typeSyncRun, 0x00}, []byte(fmt.Sprintf("%08d", i))...)
			require.NoError(t, db.Set(key, val, pebble.NoSync))
		}
	}
	plant(bogusK)

	// Deterministic assertions, no wall clock:
	resp, err := e.ListSyncs(ctx, (&reader_v2.SyncsReaderServiceListSyncsRequest_builder{}).Build())
	require.NoError(t, err)
	require.Len(t, resp.GetSyncs(), 1+bogusK,
		"all planted rows must surface (rows are data); the invariant is about their STATS, not their visibility")

	var canonical *reader_v2.SyncRun
	for _, s := range resp.GetSyncs() {
		if s.GetId() == syncID {
			canonical = s
		}
	}
	require.NotNil(t, canonical, "the canonical sync row must be present")
	require.NotNil(t, canonical.GetStats(),
		"the canonical fixed-key row must carry computed stats")
	require.EqualValues(t, grantsN, canonical.GetStats().GetGrants(),
		"canonical stats must reflect the exact seeded grant count")

	// Every noncanonical (bogus) row must read with NIL stats: the
	// pre-fix fallback computed them via full-keyspace scans, which is
	// the cross-product being bounded.
	for _, s := range resp.GetSyncs() {
		if s.GetId() == syncID {
			continue
		}
		require.Nil(t, s.GetStats(),
			"noncanonical sync-run row %q must not trigger stats computation (nil stats)", s.GetId())
	}

	// Page starting AFTER the canonical key: the cursor skips it, so the
	// page must contain only bogus rows, all with nil stats, and no
	// stats computation may run for them.
	paged, err := e.ListSyncs(ctx, (&reader_v2.SyncsReaderServiceListSyncsRequest_builder{
		PageSize:  10,
		PageToken: encodeCursor(append([]byte{versionV3, typeSyncRun, 0x00}, []byte("00000000")...)),
	}).Build())
	require.NoError(t, err)
	for _, s := range paged.GetSyncs() {
		require.NotEqual(t, syncID, s.GetId(), "the page after the canonical key must not re-serve the canonical row")
		require.Nil(t, s.GetStats(),
			"paged noncanonical row %q must not trigger stats computation", s.GetId())
	}
}
