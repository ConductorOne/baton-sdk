package grant

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// NewGrantsRevoked returns a GrantsRevoked annotation suitable for appending to
// the annotations slice returned from a Revoke provisioning call.
func NewGrantsRevoked(entitlementIDs ...string) *v2.GrantsRevoked {
	return &v2.GrantsRevoked{
		EntitlementIds: entitlementIDs,
	}
}

func AppendGrantsRevoked(annos annotations.Annotations, entitlementIDs ...string) annotations.Annotations {
	annos.Append(NewGrantsRevoked(entitlementIDs...))
	return annos
}
