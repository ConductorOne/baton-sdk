package pebble

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

func TestPageDeleteMatchesDirectIndexCleanup(t *testing.T) {
	for _, target := range []string{"stored", "staged", "both", "missing"} {
		t.Run(target, func(t *testing.T) {
			ctx := t.Context()
			engines := make([]*Engine, 2)
			for i := range engines {
				engines[i], _ = newTestEngine(t)
				_, err := engines[i].StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				require.NoError(t, err)
			}
			oldResource := scopeGateResource("target", "scope-old")
			oldResource.SetParent(v3.ResourceRef_builder{ResourceTypeId: "parent", ResourceId: "old"}.Build())
			newResource := scopeGateResource("target", "scope-new")
			newResource.SetParent(v3.ResourceRef_builder{ResourceTypeId: "parent", ResourceId: "new"}.Build())
			oldEntitlement := scopeGateEntitlement("shared", "scope-old")
			newEntitlement := scopeGateEntitlement("shared", "scope-new")
			for _, e := range engines {
				if target == "stored" || target == "both" {
					require.NoError(t, e.PutResourceRecords(ctx, oldResource))
					require.NoError(t, e.PutEntitlementRecords(ctx, oldEntitlement))
				}
			}
			direct, paged := engines[0], engines[1]
			u := paged.Ledger().newPageUnit()
			defer u.Discard()
			require.NoError(t, u.stageResourceDeletes([]resourceBufKey{{"user", "target"}, {"user", "target"}}))
			entID, err := entitlementIdentityFromRecord(oldEntitlement)
			require.NoError(t, err)
			require.NoError(t, u.stageEntitlementDeletes([]entitlementIdentity{entID, entID}))
			if target == "staged" || target == "both" {
				require.NoError(t, direct.PutResourceRecords(ctx, newResource))
				require.NoError(t, direct.PutEntitlementRecords(ctx, newEntitlement))
				require.NoError(t, u.StageResources(newResource))
				require.NoError(t, u.StageEntitlements(newEntitlement))
			}
			require.NoError(t, direct.DeleteResourceRecord(ctx, "user", "target"))
			require.NoError(t, direct.DeleteEntitlementRecordByIdentity(ctx, "group", "g1", "shared"))
			require.NoError(t, u.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "delete"}, nil))
			for _, e := range engines {
				_, err := e.GetResourceRecord(ctx, "user", "target")
				require.ErrorIs(t, err, pebble.ErrNotFound)
				_, err = e.GetEntitlementRecord(ctx, "shared")
				require.ErrorIs(t, err, pebble.ErrNotFound)
				require.Zero(t, countKeys(t, e, ResourceByParentLowerBound()))
				require.Zero(t, countAllSourceScopeKeys(t, e))
			}
			for _, kind := range []string{"resources", "entitlements"} {
				for _, scope := range []string{"scope-old", "scope-new"} {
					want, err := direct.SourceCachePoisoned(ctx, kind, scope)
					require.NoError(t, err)
					got, err := paged.SourceCachePoisoned(ctx, kind, scope)
					require.NoError(t, err)
					require.Equal(t, want, got, "%s %s", kind, scope)
				}
			}
			row, found, err := paged.Ledger().GetRow(ctx, c1zstore.LedgerActionIdentity{Op: "delete"})
			require.NoError(t, err)
			require.True(t, found)
			var puts uint64
			if target == "staged" || target == "both" {
				puts = 1
			}
			require.Equal(t, puts, row.ResourcesWritten)
			require.Equal(t, puts, row.EntitlementsWritten)
		})
	}
}

func TestPageDeleteFailureRetryInvalidatesEntitlementLookup(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	target := scopeGateEntitlement("shared", "scope-old")
	survivor := scopeGateEntitlement("shared", "scope-other")
	survivor.SetResource(v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "g2"}.Build())
	require.NoError(t, e.PutEntitlementRecords(ctx, target, survivor))
	require.NoError(t, e.PutResourceRecords(ctx, scopeGateResource("target", "scope-old")))
	_, err = e.GetEntitlementRecord(ctx, "shared")
	require.ErrorIs(t, err, ErrAmbiguousExternalID)
	w := e.Ledger().BeginPage()
	defer w.Discard()
	resource := V3ResourceToV2(scopeGateResource("target", ""))
	require.NoError(t, w.DeleteResources(ctx, resource))
	require.NoError(t, w.DeleteEntitlements(ctx, V3EntitlementToV2(target)))
	injected := errors.New("delete page commit failed")
	e.db.SetRecordCommitTestHook(func() error { return injected })
	id := c1zstore.LedgerActionIdentity{Op: "delete"}
	require.ErrorIs(t, w.Commit(ctx, id, nil), injected)
	e.db.SetRecordCommitTestHook(nil)
	_, err = e.GetResourceRecord(ctx, "user", "target")
	require.NoError(t, err)
	_, err = e.GetEntitlementRecord(ctx, "shared")
	require.ErrorIs(t, err, ErrAmbiguousExternalID)
	require.Equal(t, 3, countAllSourceScopeKeys(t, e))
	_, found, err := e.Ledger().GetRow(ctx, id)
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, w.Commit(ctx, id, nil))
	_, err = e.GetResourceRecord(ctx, "user", "target")
	require.ErrorIs(t, err, pebble.ErrNotFound)
	rec, err := e.GetEntitlementRecord(ctx, "shared")
	require.NoError(t, err)
	require.Equal(t, "g2", rec.GetResource().GetResourceId())
	require.Equal(t, 1, countAllSourceScopeKeys(t, e))
	require.ErrorIs(t, w.DeleteResources(ctx, resource), ErrPageUnitCommitted)
	require.ErrorIs(t, w.DeleteEntitlements(ctx, V3EntitlementToV2(target)), ErrPageUnitCommitted)
}

func TestPageDeleteValidatesWholeRequest(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	unbound := e.Ledger().BeginPage()
	require.ErrorIs(t, unbound.DeleteResources(ctx), ErrNoCurrentSync)
	require.ErrorIs(t, unbound.DeleteEntitlements(ctx), ErrNoCurrentSync)
	unbound.Discard()
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	resource := scopeGateResource("target", "")
	ent := scopeGateEntitlement("shared", "")
	require.NoError(t, e.PutResourceRecords(ctx, resource))
	require.NoError(t, e.PutEntitlementRecords(ctx, ent))
	w := e.Ledger().BeginPage()
	defer w.Discard()
	require.Error(t, w.DeleteResources(ctx, V3ResourceToV2(resource), nil))
	require.Error(t, w.DeleteResources(ctx, &v2.Resource{}))
	require.Error(t, w.DeleteEntitlements(ctx, V3EntitlementToV2(ent), nil))
	require.Error(t, w.DeleteEntitlements(ctx, &v2.Entitlement{}))
	require.NoError(t, w.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "invalid-deletes"}, nil))
	_, err = e.GetResourceRecord(ctx, "user", "target")
	require.NoError(t, err)
	_, err = e.GetEntitlementRecord(ctx, "shared")
	require.NoError(t, err)
}

func TestPageDeleteRefusesReplacementSync(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	resource := scopeGateResource("target", "")
	ent := scopeGateEntitlement("shared", "")
	w := e.Ledger().BeginPage()
	defer w.Discard()
	require.NoError(t, w.DeleteResources(ctx, V3ResourceToV2(resource)))
	require.NoError(t, w.DeleteEntitlements(ctx, V3EntitlementToV2(ent)))
	require.NoError(t, e.EndSync(ctx))
	_, err = e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, e.PutResourceRecords(ctx, resource))
	require.NoError(t, e.PutEntitlementRecords(ctx, ent))
	require.ErrorIs(t, w.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "delete"}, nil), ErrPageUnitForeignSync)
	_, err = e.GetResourceRecord(ctx, "user", "target")
	require.NoError(t, err)
	_, err = e.GetEntitlementRecord(ctx, "shared")
	require.NoError(t, err)
}

func TestPageDeleteDoesNotCascade(t *testing.T) {
	ctx := t.Context()
	e, _ := newTestEngine(t)
	_, err := e.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	grant := scGrant("member", "target", false)
	resource := grant.GetEntitlement().GetResource()
	entitlement := grant.GetEntitlement()
	require.NoError(t, e.PutResourceRecords(ctx, V2ResourceToV3(e.CurrentSyncID(), resource)))
	require.NoError(t, e.PutEntitlementRecords(ctx, V2EntitlementToV3(e.CurrentSyncID(), entitlement)))
	require.NoError(t, e.PutGrantRecords(ctx, V2GrantToV3(e.CurrentSyncID(), grant)))
	w := e.Ledger().BeginPage()
	require.NoError(t, w.DeleteResources(ctx, resource))
	require.NoError(t, w.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "delete-resource"}, nil))
	_, err = e.GetEntitlementRecord(ctx, entitlement.GetId())
	require.NoError(t, err)
	_, err = e.GetGrantRecord(ctx, grant.GetId())
	require.NoError(t, err)
	w = e.Ledger().BeginPage()
	require.NoError(t, w.DeleteEntitlements(ctx, entitlement))
	require.NoError(t, w.Commit(ctx, c1zstore.LedgerActionIdentity{Op: "delete-entitlement"}, nil))
	_, err = e.GetGrantRecord(ctx, grant.GetId())
	require.NoError(t, err)
	w = e.Ledger().BeginPage()
	w.Discard()
	require.ErrorIs(t, w.DeleteResources(ctx, resource), ErrPageUnitCommitted)
	require.ErrorIs(t, w.DeleteEntitlements(ctx, entitlement), ErrPageUnitCommitted)
}
