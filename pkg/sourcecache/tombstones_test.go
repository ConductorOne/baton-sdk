package sourcecache

import (
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func rid(rt, id string) *v2.ResourceId {
	return v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build()
}

func entRef(rt, id, ent string) *v2.SourceCacheEntitlementRef {
	return v2.SourceCacheEntitlementRef_builder{Resource: rid(rt, id), EntitlementId: ent}.Build()
}

func TestTombstonesFromProto(t *testing.T) {
	t.Run("nil is empty", func(t *testing.T) {
		got, err := TombstonesFromProto(RowKindGrants, nil)
		require.NoError(t, err)
		require.True(t, got.Empty())
	})

	t.Run("complete refs pass through", func(t *testing.T) {
		got, err := TombstonesFromProto(RowKindGrants, v2.SourceCacheTombstones_builder{
			Grants: []*v2.SourceCacheGrantRef{
				v2.SourceCacheGrantRef_builder{Entitlement: entRef("group", "eng", "member"), Principal: rid("user", "alice")}.Build(),
			},
			Principals: []*v2.ResourceId{rid("user", "bob")},
		}.Build())
		require.NoError(t, err)
		require.Equal(t, Tombstones{
			Grants: []GrantRef{{
				Entitlement: EntitlementRef{Resource: ResourceRef{ResourceTypeID: "group", ResourceID: "eng"}, EntitlementID: "member"},
				Principal:   ResourceRef{ResourceTypeID: "user", ResourceID: "alice"},
			}},
			Principals: []ResourceRef{{ResourceTypeID: "user", ResourceID: "bob"}},
		}, got)
	})

	incomplete := map[string]struct {
		kind RowKind
		p    *v2.SourceCacheTombstones
	}{
		"resource without type": {RowKindResources, v2.SourceCacheTombstones_builder{
			Resources: []*v2.ResourceId{rid("", "alice")}}.Build()},
		"resource without id": {RowKindResources, v2.SourceCacheTombstones_builder{
			Resources: []*v2.ResourceId{rid("user", "")}}.Build()},
		"entitlement without resource": {RowKindEntitlements, v2.SourceCacheTombstones_builder{
			Entitlements: []*v2.SourceCacheEntitlementRef{v2.SourceCacheEntitlementRef_builder{EntitlementId: "member"}.Build()}}.Build()},
		"entitlement without id": {RowKindEntitlements, v2.SourceCacheTombstones_builder{
			Entitlements: []*v2.SourceCacheEntitlementRef{entRef("group", "eng", "")}}.Build()},
		"grant without principal": {RowKindGrants, v2.SourceCacheTombstones_builder{
			Grants: []*v2.SourceCacheGrantRef{v2.SourceCacheGrantRef_builder{Entitlement: entRef("group", "eng", "member")}.Build()}}.Build()},
		"grant without entitlement": {RowKindGrants, v2.SourceCacheTombstones_builder{
			Grants: []*v2.SourceCacheGrantRef{v2.SourceCacheGrantRef_builder{Principal: rid("user", "alice")}.Build()}}.Build()},
		"principal without type": {RowKindGrants, v2.SourceCacheTombstones_builder{
			Principals: []*v2.ResourceId{rid("", "alice")}}.Build()},
	}
	for name, tc := range incomplete {
		t.Run(name, func(t *testing.T) {
			got, err := TombstonesFromProto(tc.kind, tc.p)
			require.ErrorIs(t, err, ErrIncompleteTombstone)
			require.True(t, got.Empty(), "a rejected page yields no partial tombstones")
		})
	}

	res := []*v2.ResourceId{rid("user", "alice")}
	ents := []*v2.SourceCacheEntitlementRef{entRef("group", "eng", "member")}
	grants := []*v2.SourceCacheGrantRef{v2.SourceCacheGrantRef_builder{Entitlement: entRef("group", "eng", "member"), Principal: rid("user", "alice")}.Build()}
	wrongKind := map[string]struct {
		kind RowKind
		p    *v2.SourceCacheTombstones
	}{
		"entitlements on resources":  {RowKindResources, v2.SourceCacheTombstones_builder{Entitlements: ents}.Build()},
		"grants on resources":        {RowKindResources, v2.SourceCacheTombstones_builder{Grants: grants}.Build()},
		"principals on resources":    {RowKindResources, v2.SourceCacheTombstones_builder{Principals: res}.Build()},
		"resources on entitlements":  {RowKindEntitlements, v2.SourceCacheTombstones_builder{Resources: res}.Build()},
		"grants on entitlements":     {RowKindEntitlements, v2.SourceCacheTombstones_builder{Grants: grants}.Build()},
		"principals on entitlements": {RowKindEntitlements, v2.SourceCacheTombstones_builder{Principals: res}.Build()},
		"resources on grants":        {RowKindGrants, v2.SourceCacheTombstones_builder{Resources: res}.Build()},
		"entitlements on grants":     {RowKindGrants, v2.SourceCacheTombstones_builder{Entitlements: ents}.Build()},
	}
	for name, tc := range wrongKind {
		t.Run(name, func(t *testing.T) {
			_, err := TombstonesFromProto(tc.kind, tc.p)
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrIncompleteTombstone)
		})
	}

	t.Run("unknown kind", func(t *testing.T) {
		_, err := TombstonesFromProto(RowKind("unknown"), &v2.SourceCacheTombstones{})
		require.Error(t, err)
		_, err = TombstonesFromProto(RowKind("unknown"), nil)
		require.Error(t, err, "a nil message does not excuse the kind")
	})
}

func TestTombstonesValidateRejectsIncompleteStructs(t *testing.T) {
	cases := map[string]struct {
		kind RowKind
		t    Tombstones
	}{
		"resource":  {RowKindResources, Tombstones{Resources: []ResourceRef{{ResourceID: "alice"}}}},
		"principal": {RowKindGrants, Tombstones{Principals: []ResourceRef{{ResourceTypeID: "user"}}}},
		"entitlement": {RowKindEntitlements, Tombstones{Entitlements: []EntitlementRef{{
			Resource: ResourceRef{ResourceTypeID: "group", ResourceID: "eng"}}}}},
		"grant": {RowKindGrants, Tombstones{Grants: []GrantRef{{
			Entitlement: EntitlementRef{Resource: ResourceRef{ResourceTypeID: "group", ResourceID: "eng"}, EntitlementID: "member"},
			Principal:   ResourceRef{ResourceID: "alice"}}}}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, tc.t.Validate(tc.kind), ErrIncompleteTombstone)
		})
	}
	require.NoError(t, Tombstones{}.Validate(RowKindGrants))
}
