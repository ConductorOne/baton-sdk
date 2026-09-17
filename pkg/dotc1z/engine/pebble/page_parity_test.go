package pebble

import (
	"errors"
	"testing"

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

func TestPageCanonicalTombstonesRejectAmbiguousIDs(t *testing.T) {
	for _, kind := range []string{"grants", "entitlements"} {
		t.Run(kind, func(t *testing.T) {
			ctx := t.Context()
			direct, _ := newTestEngine(t)
			paged, _ := newTestEngine(t)
			for _, e := range []*Engine{direct, paged} {
				_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
			}
			u := paged.ledger.newPageUnit()
			var directErr error
			externalID := "shared"
			switch kind {
			case "grants":
				externalID = "member:user:alice"
				ents := []*v3.EntitlementRecord{lookupTestEnt("group", "eng", "member"), lookupTestEnt("group", "sales", "member")}
				require.NoError(t, direct.PutEntitlementRecords(ctx, ents...))
				require.NoError(t, u.StageEntitlements(ents...))
				records := []*v3.GrantRecord{
					lookupTestGrant(externalID, "group", "eng", "member", "user", "alice"),
					lookupTestGrant(externalID, "group", "sales", "member", "user", "alice"),
				}
				require.NoError(t, direct.PutGrantRecords(ctx, records...))
				require.NoError(t, u.StageGrants(records...))
				directErr = direct.DeleteGrantRecordsBounded(ctx, []string{externalID}, "scope")
			case "entitlements":
				records := []*v3.EntitlementRecord{lookupTestEnt("group", "eng", "shared"), lookupTestEnt("group", "sales", "shared")}
				require.NoError(t, direct.PutEntitlementRecords(ctx, records...))
				require.NoError(t, u.StageEntitlements(records...))
				directErr = direct.DeleteEntitlementRecords(ctx, []string{externalID}, "scope")
			}
			require.ErrorIs(t, directErr, ErrAmbiguousExternalID)
			n, err := u.DropStagedRows(ctx, kind, "scope", []string{externalID}, nil)
			require.ErrorIs(t, err, ErrAmbiguousExternalID)
			require.Zero(t, n)
			require.NoError(t, u.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "Sync"}, nil))
			var count int
			if kind == "grants" {
				require.NoError(t, paged.IterateGrants(ctx, func(*v3.GrantRecord) bool { count++; return true }))
			} else {
				require.NoError(t, paged.IterateEntitlements(ctx, func(*v3.EntitlementRecord) bool { count++; return true }))
			}
			require.Equal(t, 2, count)
		})
	}
}

func TestPageScopedTombstonesUseLatestRecord(t *testing.T) {
	for _, kind := range []string{"grants", "resources"} {
		for _, scope := range []string{"old", "new"} {
			t.Run(kind+"/"+scope, func(t *testing.T) {
				ctx := t.Context()
				direct, _ := newTestEngine(t)
				paged, _ := newTestEngine(t)
				for _, e := range []*Engine{direct, paged} {
					_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
					require.NoError(t, err)
				}
				u := paged.ledger.newPageUnit()
				for _, version := range []string{"old", "new"} {
					if kind == "grants" {
						r := lookupTestGrant("group:eng:member:user:alice", "group", "eng", "group:eng:member", "user", "alice")
						r.SetSourceScopeKey(version)
						require.NoError(t, direct.PutGrantRecords(ctx, r))
						require.NoError(t, u.StageGrants(r))
					} else {
						r := v3.ResourceRecord_builder{ResourceTypeId: "user", ResourceId: "alice", SourceScopeKey: version}.Build()
						require.NoError(t, direct.PutResourceRecords(ctx, r))
						require.NoError(t, u.StageResources(r))
					}
				}
				ids := map[string]struct{}{"alice": {}}
				var want int64
				var err error
				if kind == "grants" {
					want, err = direct.DeleteGrantsByPrincipalsInScope(ctx, scope, ids)
				} else {
					want, err = direct.DeleteResourcesByIDsInScope(ctx, scope, ids)
				}
				require.NoError(t, err)
				got, err := u.DropStagedRows(ctx, kind, scope, nil, []string{"alice"})
				require.NoError(t, err)
				require.EqualValues(t, want, got)
				require.NoError(t, u.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "Sync"}, nil))
				if kind == "grants" {
					var remaining []*v3.GrantRecord
					require.NoError(t, paged.IterateGrants(ctx, func(r *v3.GrantRecord) bool { remaining = append(remaining, r); return true }))
					require.Len(t, remaining, 1-int(want))
					if len(remaining) > 0 {
						require.Equal(t, "new", remaining[0].GetSourceScopeKey())
					}
				} else {
					r, err := paged.GetResourceRecord(ctx, "user", "alice")
					require.Equal(t, want == 1, errors.Is(err, pebble.ErrNotFound))
					if want == 0 {
						require.NoError(t, err)
						require.Equal(t, "new", r.GetSourceScopeKey())
					}
				}
			})
		}
	}
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
	n, err := u.DropStagedRows(ctx, "entitlements", "scope", []string{"removed"}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	_, err = u.entitlementRecord(ctx, "removed")
	require.ErrorIs(t, err, pebble.ErrNotFound)
	got, err := u.entitlementRecord(ctx, "kept")
	require.NoError(t, err)
	require.True(t, proto.Equal(kept, got))
	g := lookupTestGrant("group:eng:member:user:alice", "group", "eng", "group:eng:member", "user", "alice")
	require.NoError(t, u.StageGrants(g, g))
	n, err = u.DropStagedRows(ctx, "grants", "scope", []string{"group:eng:member:user:alice"}, nil)
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

func TestPageGrantTombstoneCandidateParity(t *testing.T) {
	for _, tc := range []struct {
		name, externalID, entitlementID, query string
		entitlementStored                      bool
		remaining                              int
	}{
		{"custom ID", "custom", "member", "custom", true, 1},
		{"opaque entitlement absent", "member:user:alice", "member", "member:user:alice", false, 1},
		{"opaque entitlement stored", "member:user:alice", "member", "member:user:alice", true, 0},
		{"stripped entitlement absent", "group:eng:member:user:alice", "group:eng:member", "group:eng:member:user:alice", false, 0},
		{"stored ID differs", "custom", "group:eng:member", "group:eng:member:user:alice", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			direct, _ := newTestEngine(t)
			paged, _ := newTestEngine(t)
			for _, e := range []*Engine{direct, paged} {
				_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
				if tc.entitlementStored {
					require.NoError(t, e.PutEntitlementRecords(ctx, lookupTestEnt("group", "eng", tc.entitlementID)))
				}
			}
			g := lookupTestGrant(tc.externalID, "group", "eng", tc.entitlementID, "user", "alice")
			require.NoError(t, direct.PutGrantRecords(ctx, g))
			u := paged.ledger.newPageUnit()
			require.NoError(t, u.StageGrants(g))
			require.NoError(t, direct.DeleteGrantRecordsBounded(ctx, []string{tc.query}, "scope"))
			n, err := u.DropStagedRows(ctx, "grants", "scope", []string{tc.query}, nil)
			require.NoError(t, err)
			require.Equal(t, 1-tc.remaining, n)
			require.NoError(t, u.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "Sync"}, nil))
			for _, e := range []*Engine{direct, paged} {
				count := 0
				require.NoError(t, e.IterateGrants(ctx, func(*v3.GrantRecord) bool { count++; return true }))
				require.Equal(t, tc.remaining, count)
			}
		})
	}
}
