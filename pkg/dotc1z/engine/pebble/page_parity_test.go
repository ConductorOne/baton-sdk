package pebble

import (
	"testing"

	"github.com/conductorone/baton-sdk/pkg/sourcecache"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

func TestPageEntitlementLookupMatchesDirectWrites(t *testing.T) {
	ent := func(resource, name string) *v3.EntitlementRecord {
		r := lookupTestEnt("group", resource, "shared-id")
		r.SetDisplayName(name)
		return r
	}
	for _, tc := range []struct {
		name           string
		stored, staged []*v3.EntitlementRecord
		wantErr        error
	}{
		{name: "missing", wantErr: pebble.ErrNotFound},
		{name: "stored", stored: []*v3.EntitlementRecord{ent("eng", "stored")}},
		{name: "staged", staged: []*v3.EntitlementRecord{ent("eng", "staged")}},
		{name: "staged overwrite", staged: []*v3.EntitlementRecord{ent("eng", "first"), ent("eng", "last")}},
		{name: "stored overwrite", stored: []*v3.EntitlementRecord{ent("eng", "stored")}, staged: []*v3.EntitlementRecord{ent("eng", "last")}},
		{name: "staged ambiguity", staged: []*v3.EntitlementRecord{ent("eng", "eng"), ent("sales", "sales")}, wantErr: ErrAmbiguousExternalID},
		{name: "mixed ambiguity", stored: []*v3.EntitlementRecord{ent("eng", "eng")}, staged: []*v3.EntitlementRecord{ent("sales", "sales")}, wantErr: ErrAmbiguousExternalID},
		{name: "stored ambiguity", stored: []*v3.EntitlementRecord{ent("eng", "eng"), ent("sales", "sales")}, wantErr: ErrAmbiguousExternalID},
		{
			name:   "overwrite does not hide ambiguity",
			stored: []*v3.EntitlementRecord{ent("eng", "eng"), ent("sales", "sales")},
			staged: []*v3.EntitlementRecord{ent("eng", "last")}, wantErr: ErrAmbiguousExternalID,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			direct, _ := newTestEngine(t)
			paged, _ := newTestEngine(t)
			for _, e := range []*Engine{direct, paged} {
				_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				require.NoError(t, e.PutEntitlementRecords(ctx, tc.stored...))
			}
			u := paged.ledger.newPageUnit()
			for _, r := range tc.staged {
				require.NoError(t, u.StageEntitlements(r))
				require.NoError(t, direct.PutEntitlementRecords(ctx, r))
			}
			want, directErr := direct.GetEntitlementRecord(ctx, "shared-id")
			got, pageErr := u.entitlementRecord(ctx, "shared-id")
			if tc.wantErr != nil {
				require.ErrorIs(t, directErr, tc.wantErr)
				require.ErrorIs(t, pageErr, tc.wantErr)
			} else {
				require.NoError(t, directErr)
				require.NoError(t, pageErr)
				require.True(t, proto.Equal(want, got))
			}
			require.NoError(t, u.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "SyncEntitlements"}, nil))
			committed, err := paged.GetEntitlementRecord(ctx, "shared-id")
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.True(t, proto.Equal(want, committed))
			}
		})
	}
}

// Two records sharing an external id are two identities; a structured ref
// names one. The direct writer and the page agree, whether the twin is
// stored or staged.
func TestPageCanonicalTombstonesDeleteExactlyTheNamedIdentity(t *testing.T) {
	for _, kind := range []sourcecache.RowKind{sourcecache.RowKindGrants, sourcecache.RowKindEntitlements} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := t.Context()
			direct, _ := newTestEngine(t)
			paged, _ := newTestEngine(t)
			for _, e := range []*Engine{direct, paged} {
				_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
			}
			u := paged.ledger.newPageUnit()
			eng := sourcecache.ResourceRef{ResourceTypeID: "group", ResourceID: "eng"}
			var tomb sourcecache.Tombstones
			switch kind {
			case sourcecache.RowKindGrants:
				ents := []*v3.EntitlementRecord{lookupTestEnt("group", "eng", "member"), lookupTestEnt("group", "sales", "member")}
				require.NoError(t, direct.PutEntitlementRecords(ctx, ents...))
				require.NoError(t, u.StageEntitlements(ents...))
				records := []*v3.GrantRecord{
					lookupTestGrant("member:user:alice", "group", "eng", "member", "user", "alice"),
					lookupTestGrant("member:user:alice", "group", "sales", "member", "user", "alice"),
				}
				require.NoError(t, direct.PutGrantRecords(ctx, records...))
				require.NoError(t, u.StageGrants(records...))
				tomb.Grants = []sourcecache.GrantRef{{
					Entitlement: sourcecache.EntitlementRef{Resource: eng, EntitlementID: "member"},
					Principal:   sourcecache.ResourceRef{ResourceTypeID: "user", ResourceID: "alice"},
				}}
				_, err := direct.DeleteGrantRecordsByRef(ctx, tomb.Grants, "scope")
				require.NoError(t, err)
			case sourcecache.RowKindEntitlements:
				records := []*v3.EntitlementRecord{lookupTestEnt("group", "eng", "shared"), lookupTestEnt("group", "sales", "shared")}
				require.NoError(t, direct.PutEntitlementRecords(ctx, records...))
				require.NoError(t, u.StageEntitlements(records...))
				tomb.Entitlements = []sourcecache.EntitlementRef{{Resource: eng, EntitlementID: "shared"}}
				_, err := direct.DeleteEntitlementRecordsByRef(ctx, tomb.Entitlements, "scope")
				require.NoError(t, err)
			default:
				t.Fatalf("resources have no same-id twins; kind %q not in this test", kind)
			}
			n, err := u.DropStagedRows(kind, "scope", tomb)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			require.NoError(t, u.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "Sync"}, nil))
			for _, e := range []*Engine{direct, paged} {
				var remaining []string
				if kind == sourcecache.RowKindGrants {
					require.NoError(t, e.IterateGrants(ctx, func(g *v3.GrantRecord) bool {
						remaining = append(remaining, g.GetEntitlement().GetResourceId())
						return true
					}))
				} else {
					require.NoError(t, e.IterateEntitlements(ctx, func(r *v3.EntitlementRecord) bool {
						remaining = append(remaining, r.GetResource().GetResourceId())
						return true
					}))
				}
				require.Equal(t, []string{"sales"}, remaining)
			}
		})
	}
}

// The twin is stored while the target is staged, and the other way round.
// A ref reaches exactly the row it names on either side.
func TestPageTombstoneNamesOneOfStoredAndStagedTwins(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	stored := lookupTestEnt("group", "eng", "shared")
	staged := lookupTestEnt("group", "sales", "shared")
	require.NoError(t, e.PutEntitlementRecords(ctx, stored))
	u := e.ledger.newPageUnit()
	require.NoError(t, u.StageEntitlements(staged))

	forStaged := sourcecache.Tombstones{Entitlements: []sourcecache.EntitlementRef{{
		Resource: sourcecache.ResourceRef{ResourceTypeID: "group", ResourceID: "sales"}, EntitlementID: "shared",
	}}}
	n, err := u.DropStagedRows(sourcecache.RowKindEntitlements, "scope", forStaged)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	_, err = e.DeleteEntitlementRecordsByRef(ctx, forStaged.Entitlements, "scope")
	require.NoError(t, err)
	require.NoError(t, u.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "Sync"}, nil))
	got, err := e.GetEntitlementRecord(ctx, "shared")
	require.NoError(t, err)
	require.Equal(t, "eng", got.GetResource().GetResourceId(), "the stored twin survives a delete naming the staged one")
}

func TestPageScopedTombstonesUseLatestRecord(t *testing.T) {
	for _, scope := range []string{"old", "new"} {
		t.Run(scope, func(t *testing.T) {
			ctx := t.Context()
			direct, _ := newTestEngine(t)
			paged, _ := newTestEngine(t)
			for _, e := range []*Engine{direct, paged} {
				_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
			}
			u := paged.ledger.newPageUnit()
			for _, version := range []string{"old", "new"} {
				r := lookupTestGrant("group:eng:member:user:alice", "group", "eng", "group:eng:member", "user", "alice")
				r.SetSourceScopeKey(version)
				require.NoError(t, direct.PutGrantRecords(ctx, r))
				require.NoError(t, u.StageGrants(r))
			}
			alice := []sourcecache.ResourceRef{{ResourceTypeID: "user", ResourceID: "alice"}}
			want, err := direct.DeleteGrantsByPrincipalsInScope(ctx, scope, alice)
			require.NoError(t, err)
			got, err := u.DropStagedRows(sourcecache.RowKindGrants, scope, sourcecache.Tombstones{Principals: alice})
			require.NoError(t, err)
			require.EqualValues(t, want, got)
			require.NoError(t, u.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "Sync"}, nil))
			var remaining []*v3.GrantRecord
			require.NoError(t, paged.IterateGrants(ctx, func(r *v3.GrantRecord) bool { remaining = append(remaining, r); return true }))
			require.Len(t, remaining, 1-int(want))
			if len(remaining) > 0 {
				require.Equal(t, "new", remaining[0].GetSourceScopeKey())
			}
		})
	}
}

// A principal tombstone matches (type, id), not the id alone.
func TestPagePrincipalTombstoneRequiresTheType(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	user := lookupTestGrant("group:eng:member:user:alice", "group", "eng", "group:eng:member", "user", "alice")
	group := lookupTestGrant("group:eng:member:group:alice", "group", "eng", "group:eng:member", "group", "alice")
	for _, g := range []*v3.GrantRecord{user, group} {
		g.SetSourceScopeKey("scope")
	}
	require.NoError(t, e.PutGrantRecords(ctx, user, group))
	n, err := e.DeleteGrantsByPrincipalsInScope(ctx, "scope", []sourcecache.ResourceRef{{ResourceTypeID: "group", ResourceID: "alice"}})
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	var remaining []string
	require.NoError(t, e.IterateGrants(ctx, func(g *v3.GrantRecord) bool {
		remaining = append(remaining, g.GetPrincipal().GetResourceTypeId())
		return true
	}))
	require.Equal(t, []string{"user"}, remaining)
}

func TestPageDeleteGrantsRejectsNilLikeDirectWriter(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	directErr := e.DeleteGrantsByRefs(ctx, (*v2.Grant)(nil))
	pageErr := e.ledger.BeginPage().DeleteGrants(ctx, (*v2.Grant)(nil))
	require.ErrorContains(t, directErr, "grant identity: nil record")
	require.ErrorContains(t, pageErr, "grant identity: nil record")
}

func TestPageCanonicalTombstoneOverwritesAndLookupRebuild(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	u := e.ledger.newPageUnit()
	removed := lookupTestEnt("group", "eng", "removed")
	kept := lookupTestEnt("group", "sales", "kept")
	require.NoError(t, u.StageEntitlements(removed, kept, removed, kept))
	n, err := u.DropStagedRows(sourcecache.RowKindEntitlements, "scope", sourcecache.Tombstones{Entitlements: []sourcecache.EntitlementRef{{
		Resource: sourcecache.ResourceRef{ResourceTypeID: "group", ResourceID: "eng"}, EntitlementID: "removed",
	}}})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	_, err = u.entitlementRecord(ctx, "removed")
	require.ErrorIs(t, err, pebble.ErrNotFound)
	got, err := u.entitlementRecord(ctx, "kept")
	require.NoError(t, err)
	require.True(t, proto.Equal(kept, got))
	g := lookupTestGrant("group:eng:member:user:alice", "group", "eng", "group:eng:member", "user", "alice")
	require.NoError(t, u.StageGrants(g, g))
	n, err = u.DropStagedRows(sourcecache.RowKindGrants, "scope", sourcecache.Tombstones{Grants: []sourcecache.GrantRef{{
		Entitlement: sourcecache.EntitlementRef{Resource: sourcecache.ResourceRef{ResourceTypeID: "group", ResourceID: "eng"}, EntitlementID: "group:eng:member"},
		Principal:   sourcecache.ResourceRef{ResourceTypeID: "user", ResourceID: "alice"},
	}}})
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, u.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "Sync"}, nil))
	got, err = e.GetEntitlementRecord(ctx, "kept")
	require.NoError(t, err)
	require.True(t, proto.Equal(kept, got))
}

func TestPageGrantDeleteValidationDoesNotStagePartialRequest(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	g := lookupTestGrant("group:eng:member:user:alice", "group", "eng", "group:eng:member", "user", "alice")
	require.NoError(t, e.PutGrantRecords(ctx, g))
	u := e.ledger.newPageUnit()
	require.Error(t, u.StageGrantDeletes(g, nil))
	require.NoError(t, u.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "Sync"}, nil))
	var count int
	require.NoError(t, e.IterateGrants(ctx, func(*v3.GrantRecord) bool { count++; return true }))
	require.Equal(t, 1, count)
}
