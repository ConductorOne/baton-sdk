package dotc1z

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// mkV2Grant builds a minimal v2.Grant for Pebble store tests. Mirrors the
// helper of the same name in pkg/dotc1z/engine/pebble's tests.
func mkV2Grant(id, entID, principalRT, principalID string) *v2.Grant {
	return v2.Grant_builder{
		Id: id,
		Entitlement: v2.Entitlement_builder{
			Id: entID,
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "app",
					Resource:     "github",
				}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: principalRT,
				Resource:     principalID,
			}.Build(),
		}.Build(),
	}.Build()
}
