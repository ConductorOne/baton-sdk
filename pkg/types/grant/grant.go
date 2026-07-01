package grant

import (
	"errors"

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

var ErrInvalidGrantID = errors.New("invalid grant id")

type GrantIDParts struct {
	Entitlement     eopt.EntitlementIDParts
	PrincipalTypeID string
	PrincipalID     string
}

func WithGrantMetadata(metadata map[string]interface{}) GrantOption {
	return func(g *v2.Grant) error {
		md, err := structpb.NewStruct(metadata)
		if err != nil {
			return err
		}

		meta := v2.GrantMetadata_builder{Metadata: md}.Build()
		annos := annotations.Annotations(g.GetAnnotations())
		annos.Update(meta)
		g.SetAnnotations(annos)

		return nil
	}
}

// WithExternalPrincipalID: Deprecated. This field is no longer used.
func WithExternalPrincipalID(externalID *v2.ExternalId) GrantOption {
	return func(g *v2.Grant) error {
		g.GetPrincipal().SetExternalId(externalID) //nolint:staticcheck // Deprecated.
		return nil
	}
}

func WithAnnotation(msgs ...proto.Message) GrantOption {
	return func(g *v2.Grant) error {
		annos := annotations.Annotations(g.GetAnnotations())
		for _, msg := range msgs {
			annos.Append(msg)
		}
		g.SetAnnotations(annos)

		return nil
	}
}

// NewGrant returns a new grant for the given entitlement on the resource for the provided principal resource ID.
func NewGrant(resource *v2.Resource, entitlementName string, principal GrantPrincipal, grantOptions ...GrantOption) *v2.Grant {
	entitlement := v2.Entitlement_builder{
		Id:       eopt.NewEntitlementID(resource, entitlementName),
		Resource: resource,
	}.Build()

	grant := v2.Grant_builder{
		Entitlement: entitlement,
	}.Build()

	var resourceID *v2.ResourceId
	switch p := principal.(type) {
	case *v2.ResourceId:
		resourceID = p
		grant.SetPrincipal(v2.Resource_builder{Id: p}.Build())
	case *v2.Resource:
		grant.SetPrincipal(p)
		resourceID = p.GetId()
	default:
		panic("unexpected principal type")
	}

	if resourceID == nil {
		panic("principal resource must have a valid resource ID")
	}
	entParts := eopt.DeriveEntitlementIDParts(
		resource.GetId().GetResourceType(),
		resource.GetId().GetResource(),
		entitlement.GetId(),
	)
	grant.SetId(EncodeGrantID(entParts, resourceID.GetResourceType(), resourceID.GetResource()))

	for _, grantOption := range grantOptions {
		err := grantOption(grant)
		if err != nil {
			panic(err)
		}
	}

	return grant
}

// deprecated: use MustMakeBid instead (but you can't change ids for an existing connector).
func NewGrantID(principal GrantPrincipal, entitlement *v2.Entitlement) string {
	var resourceID *v2.ResourceId
	switch p := principal.(type) {
	case *v2.ResourceId:
		resourceID = p
	case *v2.Resource:
		resourceID = p.GetId()
	default:
		panic("unexpected principal type")
	}

	if resourceID == nil {
		panic("principal resource must have a valid resource ID")
	}
	entParts, ok := entitlementIDParts(entitlement)
	if !ok {
		return eopt.JoinEscapedID(entitlement.GetId(), resourceID.GetResourceType(), resourceID.GetResource())
	}
	return EncodeGrantID(entParts, resourceID.GetResourceType(), resourceID.GetResource())
}

func entitlementIDParts(entitlement *v2.Entitlement) (eopt.EntitlementIDParts, bool) {
	if entitlement == nil {
		return eopt.EntitlementIDParts{}, false
	}
	if res := entitlement.GetResource(); res != nil && res.GetId() != nil {
		return eopt.DeriveEntitlementIDParts(
			res.GetId().GetResourceType(),
			res.GetId().GetResource(),
			entitlement.GetId(),
		), true
	}
	parts, err := eopt.DecodeEntitlementID(entitlement.GetId())
	if err != nil {
		return eopt.EntitlementIDParts{}, false
	}
	return parts, true
}

func EncodeGrantID(entitlement eopt.EntitlementIDParts, principalTypeID, principalID string) string {
	if entitlement.Kind == eopt.EntitlementKindCustom {
		return eopt.JoinEscapedID(
			entitlement.ResourceTypeID,
			entitlement.ResourceID,
			"custom",
			entitlement.Name,
			principalTypeID,
			principalID,
		)
	}
	return eopt.JoinEscapedID(
		entitlement.ResourceTypeID,
		entitlement.ResourceID,
		entitlement.Name,
		principalTypeID,
		principalID,
	)
}

func DecodeGrantID(id string) (GrantIDParts, error) {
	parts, err := eopt.SplitEscapedID(id)
	if err != nil {
		return GrantIDParts{}, err
	}
	switch {
	case len(parts) == 5:
		return GrantIDParts{
			Entitlement: eopt.EntitlementIDParts{
				ResourceTypeID: parts[0],
				ResourceID:     parts[1],
				Kind:           eopt.EntitlementKindSDK,
				Name:           parts[2],
			},
			PrincipalTypeID: parts[3],
			PrincipalID:     parts[4],
		}, nil
	case len(parts) == 6 && parts[2] == "custom":
		return GrantIDParts{
			Entitlement: eopt.EntitlementIDParts{
				ResourceTypeID: parts[0],
				ResourceID:     parts[1],
				Kind:           eopt.EntitlementKindCustom,
				Name:           parts[3],
			},
			PrincipalTypeID: parts[4],
			PrincipalID:     parts[5],
		}, nil
	default:
		return GrantIDParts{}, ErrInvalidGrantID
	}
}
