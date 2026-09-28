package grant

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// NewGrantsRevoked returns a GrantsRevoked annotation suitable for appending to
// the annotations slice returned from a Revoke provisioning call.
func NewGrantsRevoked(grants ...*v2.Grant) *v2.GrantsRevoked {
	return &v2.GrantsRevoked{
		Grants: grants,
	}
}

// AppendGrantsRevoked adds grants to the GrantsRevoked annotation on annos and
// returns the updated slice. When annos already contains GrantsRevoked, the
// grants are appended to that message so a later Pick sees one annotation.
// Otherwise a new GrantsRevoked is appended.
func AppendGrantsRevoked(annos annotations.Annotations, grants ...*v2.Grant) annotations.Annotations {
	existing := &v2.GrantsRevoked{}
	found, err := annos.Pick(existing)
	if err != nil {
		panic(fmt.Errorf("failed to read GrantsRevoked annotation: %w", err))
	}
	if found {
		existing.Grants = append(existing.GetGrants(), grants...)
		annos.Update(existing)
		return annos
	}
	annos.Append(NewGrantsRevoked(grants...))
	return annos
}
