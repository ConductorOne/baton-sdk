package dotc1z

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
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
