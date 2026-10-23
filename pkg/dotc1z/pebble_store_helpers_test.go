package dotc1z

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// mkV2Grant builds a minimal v2.Grant for Pebble store tests. Mirrors the
// helper of the same name in pkg/dotc1z/engine/pebble's tests. Entitlement
// ids are SDK-shaped ("app:github:"+entID) so bare-id lookups resolve the
// way they do for real connector data.
func mkV2Grant(id, entID, principalRT, principalID string) *v2.Grant {
	ent := v2.Entitlement_builder{
		Id: "app:github:" + entID,
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "app",
				Resource:     "github",
			}.Build(),
		}.Build(),
	}.Build()
	principal := v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: principalRT,
			Resource:     principalID,
		}.Build(),
	}.Build()
	return v2.Grant_builder{
		Id:          batonGrant.NewGrantID(principal, ent),
		Entitlement: ent,
		Principal:   principal,
	}.Build()
}

func mkV2GrantID(entID, principalRT, principalID string) string {
	return batonGrant.NewGrantID(
		v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: principalRT,
				Resource:     principalID,
			}.Build(),
		}.Build(),
		v2.Entitlement_builder{
			Id: "app:github:" + entID,
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "app",
					Resource:     "github",
				}.Build(),
			}.Build(),
		}.Build(),
	)
}

func refOfResource(r *v2.Resource) sourcecache.ResourceRef {
	return sourcecache.ResourceRef{ResourceTypeID: r.GetId().GetResourceType(), ResourceID: r.GetId().GetResource()}
}

func refOfEntitlement(e *v2.Entitlement) sourcecache.EntitlementRef {
	return sourcecache.EntitlementRef{Resource: refOfResource(e.GetResource()), EntitlementID: e.GetId()}
}

func refOfGrant(g *v2.Grant) sourcecache.GrantRef {
	return sourcecache.GrantRef{Entitlement: refOfEntitlement(g.GetEntitlement()), Principal: refOfResource(g.GetPrincipal())}
}

func tombResources(refs ...sourcecache.ResourceRef) sourcecache.Tombstones {
	return sourcecache.Tombstones{Resources: refs}
}

func tombEntitlements(refs ...sourcecache.EntitlementRef) sourcecache.Tombstones {
	return sourcecache.Tombstones{Entitlements: refs}
}

func tombGrants(refs ...sourcecache.GrantRef) sourcecache.Tombstones {
	return sourcecache.Tombstones{Grants: refs}
}

func tombPrincipals(refs ...sourcecache.ResourceRef) sourcecache.Tombstones {
	return sourcecache.Tombstones{Principals: refs}
}

func userRef(id string) sourcecache.ResourceRef {
	return sourcecache.ResourceRef{ResourceTypeID: "user", ResourceID: id}
}

// The rows putSourceCacheVerificationRows writes, and refs to them.
func verifResource(prefix string, i int) *v2.Resource {
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("%s-%d", prefix, i)}.Build(),
	}.Build()
}

func verifEntitlement(prefix string, i int) *v2.Entitlement {
	return v2.Entitlement_builder{
		Id: fmt.Sprintf("%s-%d", prefix, i),
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: fmt.Sprintf("%s-group-%d", prefix, i)}.Build(),
		}.Build(),
	}.Build()
}

func verifGrant(prefix string, i int) *v2.Grant {
	return mkV2Grant("", fmt.Sprintf("%s-%d", prefix, i), "user", fmt.Sprintf("%s-principal-%d", prefix, i))
}

func verifResourceRef(prefix string, i int) sourcecache.ResourceRef {
	return refOfResource(verifResource(prefix, i))
}

func verifEntitlementRef(prefix string, i int) sourcecache.EntitlementRef {
	return refOfEntitlement(verifEntitlement(prefix, i))
}

func verifGrantRef(prefix string, i int) sourcecache.GrantRef {
	return refOfGrant(verifGrant(prefix, i))
}

func verifPrincipalRef(prefix string, i int) sourcecache.ResourceRef {
	return refOfResource(verifGrant(prefix, i).GetPrincipal())
}

func verifTombstone(kind sourcecache.RowKind, prefix string, i int) sourcecache.Tombstones {
	switch kind {
	case sourcecache.RowKindResources:
		return tombResources(verifResourceRef(prefix, i))
	case sourcecache.RowKindEntitlements:
		return tombEntitlements(verifEntitlementRef(prefix, i))
	case sourcecache.RowKindGrants:
		return tombGrants(verifGrantRef(prefix, i))
	}
	panic("unsupported row kind " + string(kind))
}
