package pebble

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

func pageTestV2Fixtures() (*v2.ResourceType, []*v2.Resource, *v2.Entitlement, *v2.Grant) {
	rt := v2.ResourceType_builder{Id: "app"}.Build()
	res := []*v2.Resource{
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build()}.Build(),
	}
	ent := v2.Entitlement_builder{
		Id:       canonicalTestEntID("ent-A"),
		Resource: res[0],
	}.Build()
	return rt, res, ent, mkV2Grant("g1", "ent-A", "user", "alice")
}

// A page committed through the v2 writer must land byte-identical to
// the same v2 records committed through the single-call adapters:
// translation, discovered_at defaulting and the source-scope stamp
// are the same shared code.
func TestPageWriterMatchesSingleCallAdapters(t *testing.T) {
	ctx := sourcecache.WithScope(context.Background(), "scope-1")
	rt, res, ent, grant := pageTestV2Fixtures()

	type snap struct {
		rts []*v3.ResourceTypeRecord
		rs  []*v3.ResourceRecord
		es  []*v3.EntitlementRecord
		gs  []*v3.GrantRecord
	}
	snapshot := func(t *testing.T, e *Engine) snap {
		var s snap
		require.NoError(t, e.IterateResourceTypes(ctx, func(r *v3.ResourceTypeRecord) bool { s.rts = append(s.rts, r); return true }))
		require.NoError(t, e.IterateResources(ctx, func(r *v3.ResourceRecord) bool { s.rs = append(s.rs, r); return true }))
		require.NoError(t, e.IterateEntitlements(ctx, func(r *v3.EntitlementRecord) bool { s.es = append(s.es, r); return true }))
		require.NoError(t, e.IterateGrants(ctx, func(r *v3.GrantRecord) bool { s.gs = append(s.gs, r); return true }))
		return s
	}

	single, _ := newTestEngine(t)
	_, err := single.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, single.PutResourceTypes(ctx, rt))
	require.NoError(t, single.PutResources(ctx, res...))
	require.NoError(t, single.PutEntitlements(ctx, ent))
	require.NoError(t, single.PutGrants(ctx, grant))

	paged, _ := newTestEngine(t)
	_, err = paged.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	var store c1zstore.PageLedgerStore = paged
	w := store.BeginPage()
	require.NoError(t, w.PutResourceTypes(ctx, rt))
	require.NoError(t, w.PutResources(ctx, res...))
	require.NoError(t, w.PutEntitlements(ctx, ent))
	require.NoError(t, w.PutGrants(ctx, grant))
	id := c1zstore.LedgerActionIdentity{Op: "SyncGrants", ResourceTypeID: "app", ResourceID: "github", PageToken: "p1"}
	require.NoError(t, w.Commit(ctx, id, &c1zstore.LedgerRow{NextPageToken: "p2", Attempt: "a1", Replayed: true}))

	s, p := snapshot(t, single), snapshot(t, paged)
	// discovered_at is stamped with "now" at translate time on both
	// sides; ignore it and compare everything else exactly (source
	// scope key included).
	ignoreTS := protocmp.IgnoreFields(&v3.ResourceTypeRecord{}, "discovered_at")
	ignoreTSr := protocmp.IgnoreFields(&v3.ResourceRecord{}, "discovered_at")
	ignoreTSe := protocmp.IgnoreFields(&v3.EntitlementRecord{}, "discovered_at")
	ignoreTSg := protocmp.IgnoreFields(&v3.GrantRecord{}, "discovered_at")
	require.Empty(t, cmp.Diff(s.rts, p.rts, protocmp.Transform(), ignoreTS))
	require.Empty(t, cmp.Diff(s.rs, p.rs, protocmp.Transform(), ignoreTSr))
	require.Empty(t, cmp.Diff(s.es, p.es, protocmp.Transform(), ignoreTSe))
	require.Empty(t, cmp.Diff(s.gs, p.gs, protocmp.Transform(), ignoreTSg))
	require.Equal(t, "scope-1", p.rs[0].GetSourceScopeKey(), "source scope stamped on the paged path")
	require.Equal(t, "scope-1", p.gs[0].GetSourceScopeKey())
	require.NotNil(t, p.rts[0].GetDiscoveredAt(), "discovered_at defaulted on the paged path")

	// The row, through the c1zstore view.
	row, found, err := store.GetLedgerRow(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, id, row.Identity)
	require.Equal(t, "p2", row.NextPageToken)
	require.Equal(t, "a1", row.Attempt)
	require.True(t, row.Replayed)
	require.False(t, row.CommittedAt.IsZero())
	require.EqualValues(t, 1, row.ResourceTypesWritten)
	require.EqualValues(t, 2, row.ResourcesWritten)
	require.EqualValues(t, 1, row.EntitlementsWritten)
	require.EqualValues(t, 1, row.GrantsWritten)

	// Absent and identity-mismatch both read as not found.
	_, found, err = store.GetLedgerRow(ctx, c1zstore.LedgerActionIdentity{Op: "SyncGrants", ResourceTypeID: "app", ResourceID: "github", PageToken: "p2"})
	require.NoError(t, err)
	require.False(t, found)
	other := id
	other.ParentResourceID = "acme"
	val, err := marshalRecord(v3.LedgerRow_builder{Identity: ledgerIdentityToProto(ledgerIdentityFromStore(id))}.Build())
	require.NoError(t, err)
	require.NoError(t, paged.db.UnsafeForTesting().Set(encodeLedgerKey(ledgerIdentityFromStore(other)), val, pebble.Sync))
	_, found, err = store.GetLedgerRow(ctx, other)
	require.NoError(t, err)
	require.False(t, found, "a colliding row is not this page's row")
	require.EqualValues(t, 1, paged.LedgerMismatches())

	// The single-call engine has no ledger at all.
	cnt, err := single.LedgerRowCount(ctx)
	require.NoError(t, err)
	require.Zero(t, cnt)
}

func TestPageWriterGetResourceAndDiscard(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	_, res, _, _ := pageTestV2Fixtures()

	w := e.BeginPage()
	_, err = w.GetResource(ctx, "user", "alice")
	require.ErrorIs(t, err, pebble.ErrNotFound)
	require.Equal(t, codes.NotFound, status.Code(err), "not-found adapts to the store's gRPC-shaped error")

	require.NoError(t, w.PutResources(ctx, res[1]))
	got, err := w.GetResource(ctx, "user", "alice")
	require.NoError(t, err)
	require.True(t, proto.Equal(res[1].GetId(), got.GetId()), "the page sees its own staged resource")

	w.Discard()
	require.ErrorIs(t, w.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "SyncResources", PageToken: ""}, nil), ErrPageUnitCommitted)
	_, err = e.GetResourceRecord(ctx, "user", "alice")
	require.ErrorIs(t, err, pebble.ErrNotFound, "a discarded page never ran")
}

// A writer begun with no sync bound refuses to stage or commit, the
// same way the single-call adapters do.
func TestPageWriterRequiresSync(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	w := e.BeginPage()
	_, res, _, _ := pageTestV2Fixtures()
	require.ErrorIs(t, w.PutResources(ctx, res...), ErrNoCurrentSync)
	require.ErrorIs(t, w.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "SyncResources"}, nil), ErrNoCurrentSync)
}
