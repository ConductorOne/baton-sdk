package grant

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// NewGrantRejected returns a GrantRejected annotation suitable for appending to
// the annotations slice returned from a Grant provisioning call.
func NewGrantRejected(reason string) *v2.GrantRejected {
	return &v2.GrantRejected{
		Reason: reason,
	}
}

// AppendGrantRejected appends a GrantRejected annotation to the given
// annotations slice and returns the updated slice.
func AppendGrantRejected(annos annotations.Annotations, reason string) annotations.Annotations {
	annos.Append(NewGrantRejected(reason))
	return annos
}

// WithGrantRejected appends a GrantRejected annotation to a grant.
func WithGrantRejected(reason string) GrantOption {
	return WithAnnotation(NewGrantRejected(reason))
}
