package grant

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	eopt "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"google.golang.org/protobuf/proto"
)

type GrantOption func(*v2.Grant)

func WithAnnotation(msgs ...proto.Message) GrantOption {
	return func(g *v2.Grant) {
		annos := annotations.Annotations(g.Annotations)
		for _, msg := range msgs {
			annos.Append(msg)
		}
		g.Annotations = annos
	}
}

// NewGrant returns a new grant for the given entitlement on the resource for the provided principal resource ID.
func NewGrant(resource *v2.Resource, entitlementName string, principal *v2.ResourceId, grantOptions ...GrantOption) *v2.Grant {
	entitlement := &v2.Entitlement{
		Id:       eopt.NewEntitlementID(resource, entitlementName),
		Resource: resource,
	}

	grant := &v2.Grant{
		Entitlement: entitlement,
		Principal:   &v2.Resource{Id: principal},
		Id:          fmt.Sprintf("%s:%s:%s", entitlement.Id, principal.ResourceType, principal.Resource),
	}

	for _, grantOption := range grantOptions {
		grantOption(grant)
	}

	return grant
}
