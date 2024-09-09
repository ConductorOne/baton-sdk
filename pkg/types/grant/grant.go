package grant

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	eopt "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type GrantOption func(*v2.Grant) error

type GrantPrincipal interface {
	proto.Message
	GetBatonResource() bool
}

// Sometimes C1 doesn't have the grant ID, but does have the principal and entitlement.
const UnknownGrantId string = "🧸_UNKNOWN_GRANT_ID"

func WithGrantMetadata(metadata map[string]interface{}) GrantOption {
	return func(g *v2.Grant) error {
		md, err := structpb.NewStruct(metadata)
		if err != nil {
			return err
		}

		meta := &v2.GrantMetadata{Metadata: md}
		annos := annotations.Annotations(g.Annotations)
		annos.Update(meta)
		g.Annotations = annos

		return nil
	}
}

func WithExternalPrincipalID(externalID *v2.ExternalId) GrantOption {
	return func(g *v2.Grant) error {
		g.Principal.ExternalId = externalID
		return nil
	}
}

func WithAnnotation(msgs ...proto.Message) GrantOption {
	return func(g *v2.Grant) error {
		annos := annotations.Annotations(g.Annotations)
		for _, msg := range msgs {
			annos.Append(msg)
		}
		g.Annotations = annos

		return nil
	}
}

// NewGrant returns a new grant for the given entitlement on the resource for the provided principal resource ID.
func NewGrant(resource *v2.Resource, entitlementName string, principal GrantPrincipal, grantOptions ...GrantOption) *v2.Grant {
	entitlement := &v2.Entitlement{
		Id:       eopt.NewEntitlementID(resource, entitlementName),
		Resource: resource,
	}

	grant := &v2.Grant{
		Entitlement: entitlement,
	}

	var resourceID *v2.ResourceId
	switch p := principal.(type) {
	case *v2.ResourceId:
		resourceID = p
		grant.Principal = &v2.Resource{Id: p}
	case *v2.Resource:
		grant.Principal = p
		resourceID = p.Id
	default:
		panic("unexpected principal type")
	}

	if resourceID == nil {
		panic("principal resource must have a valid resource ID")
	}
	grant.Id = fmt.Sprintf("%s:%s:%s", entitlement.Id, resourceID.ResourceType, resourceID.Resource)

	for _, grantOption := range grantOptions {
		err := grantOption(grant)
		if err != nil {
			panic(err)
		}
	}

	return grant
}
