package chaosconnector

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/bid"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/protobuf/proto"
)

const (
	ExternalUserTypeID    = "external-user"
	ExternalGroupTypeID   = "external-group"
	InternalGroupTypeID   = "internal-group"
	ExternalPlaceholderID = "external-placeholder"
)

type ExternalMatchKind string

const (
	ExternalMatchAll       ExternalMatchKind = "all"
	ExternalMatchID        ExternalMatchKind = "id"
	ExternalMatchAttribute ExternalMatchKind = "attribute"
)

// ExternalPrincipalCase describes one grant-carrier matching policy. The
// scenarios deliberately remain independent: one produces the external c1z,
// while the other consumes it during an internal sync.
type ExternalPrincipalCase struct {
	Name                 string
	Match                ExternalMatchKind
	Trait                v2.ResourceType_Trait
	Key                  string
	Value                string
	ExpectedPrincipalIDs []string
	Expandable           bool
}

func ExternalPrincipalCorpus() []ExternalPrincipalCase {
	return []ExternalPrincipalCase{
		{
			Name:                 "external-principal/id-match",
			Match:                ExternalMatchID,
			Value:                "external-user-1",
			ExpectedPrincipalIDs: []string{"external-user-1"},
		},
		{
			Name:                 "external-principal/id-expandable-remap",
			Match:                ExternalMatchID,
			Value:                "external-group-1",
			ExpectedPrincipalIDs: []string{"external-group-1"},
			Expandable:           true,
		},
		{
			Name:  "external-principal/id-miss",
			Match: ExternalMatchID,
			Value: "missing-user",
		},
		{
			Name:                 "external-principal/email-case-fold",
			Match:                ExternalMatchAttribute,
			Trait:                v2.ResourceType_TRAIT_USER,
			Key:                  "email",
			Value:                "TARGET@EXAMPLE.COM",
			ExpectedPrincipalIDs: []string{"external-user-1"},
		},
		{
			Name:                 "external-principal/user-profile-case-fold",
			Match:                ExternalMatchAttribute,
			Trait:                v2.ResourceType_TRAIT_USER,
			Key:                  "department",
			Value:                "ENGINEERING",
			ExpectedPrincipalIDs: []string{"external-user-1"},
		},
		{
			Name:                 "external-principal/group-profile-case-fold",
			Match:                ExternalMatchAttribute,
			Trait:                v2.ResourceType_TRAIT_GROUP,
			Key:                  "external_id",
			Value:                "group-123",
			ExpectedPrincipalIDs: []string{"external-group-1"},
		},
		{
			Name:                 "external-principal/all-users",
			Match:                ExternalMatchAll,
			Trait:                v2.ResourceType_TRAIT_USER,
			ExpectedPrincipalIDs: []string{"external-user-1", "external-user-2"},
		},
		{
			Name:                 "external-principal/all-groups",
			Match:                ExternalMatchAll,
			Trait:                v2.ResourceType_TRAIT_GROUP,
			ExpectedPrincipalIDs: []string{"external-group-1"},
		},
		{
			Name:  "external-principal/attribute-miss",
			Match: ExternalMatchAttribute,
			Trait: v2.ResourceType_TRAIT_USER,
			Key:   "department",
			Value: "finance",
		},
	}
}

func (c ExternalPrincipalCase) Build() (*Scenario, *Scenario, error) {
	external, err := externalPrincipalSourceScenario()
	if err != nil {
		return nil, nil, err
	}
	internal, err := c.internalCarrierScenario()
	if err != nil {
		return nil, nil, err
	}
	return external, internal, nil
}

func externalPrincipalSourceScenario() (*Scenario, error) {
	userType := v2.ResourceType_builder{
		Id:          ExternalUserTypeID,
		DisplayName: "External User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		Annotations: annotations.New(&v2.TypeScopedEntitlements{}, &v2.TypeScopedGrants{}),
	}.Build()
	groupType := v2.ResourceType_builder{
		Id:          ExternalGroupTypeID,
		DisplayName: "External Group",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
		Annotations: annotations.New(&v2.TypeScopedEntitlements{}, &v2.TypeScopedGrants{}),
	}.Build()
	user1, err := rs.NewUserResource(
		"External User 1",
		userType,
		"external-user-1",
		[]rs.UserTraitOption{rs.WithEmail("target@example.com", true)},
		rs.WithResourceProfile(map[string]any{"department": "engineering"}),
	)
	if err != nil {
		return nil, err
	}
	user2, err := rs.NewUserResource("External User 2", userType, "external-user-2", nil)
	if err != nil {
		return nil, err
	}
	group, err := rs.NewGroupResource(
		"External Group 1",
		groupType,
		"external-group-1",
		nil,
		rs.WithResourceProfile(map[string]any{"external_id": "group-123"}),
	)
	if err != nil {
		return nil, err
	}
	groupEntitlement := et.NewEntitlement(group, "member", "membership")

	return &Scenario{
		Name:         "external-principal-source",
		Seed:         1,
		InitialEpoch: "initial",
		Epochs: map[string]*Dataset{"initial": {
			ResourceTypes: []*v2.ResourceType{userType, groupType},
			Resources: map[string]Pages[*v2.Resource]{
				ExternalUserTypeID:  {"": {List: []*v2.Resource{user1, user2}}},
				ExternalGroupTypeID: {"": {List: []*v2.Resource{group}}},
			},
			StaticEntitlements: map[string]Pages[*v2.Entitlement]{
				ExternalUserTypeID:  {"": {}},
				ExternalGroupTypeID: {"": {}},
			},
			Entitlements: map[string]Pages[*v2.Entitlement]{
				ExternalUserTypeID:  {"": {}},
				ExternalGroupTypeID: {"": {List: []*v2.Entitlement{groupEntitlement}}},
			},
			Grants: map[string]Pages[*v2.Grant]{
				ExternalUserTypeID:  {"": {}},
				ExternalGroupTypeID: {"": {}},
			},
		}},
	}, nil
}

func (c ExternalPrincipalCase) internalCarrierScenario() (*Scenario, error) {
	groupType := v2.ResourceType_builder{
		Id:          InternalGroupTypeID,
		DisplayName: "Internal Group",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
		Annotations: annotations.New(&v2.TypeScopedEntitlements{}, &v2.TypeScopedGrants{}),
	}.Build()
	placeholderType := v2.ResourceType_builder{
		Id:          ExternalPlaceholderID,
		DisplayName: "External Placeholder",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
		Annotations: annotations.New(&v2.TypeScopedEntitlements{}, &v2.TypeScopedGrants{}),
	}.Build()
	group, err := rs.NewGroupResource("Internal Group 1", groupType, "internal-group-1", nil)
	if err != nil {
		return nil, err
	}
	entitlement := et.NewEntitlement(group, "member", "membership")
	placeholder := v2.ResourceId_builder{
		ResourceType: ExternalPlaceholderID,
		Resource:     "placeholder",
	}.Build()

	var matchAnnotation proto.Message
	switch c.Match {
	case ExternalMatchAll:
		matchAnnotation = v2.ExternalResourceMatchAll_builder{ResourceType: c.Trait}.Build()
	case ExternalMatchID:
		matchAnnotation = v2.ExternalResourceMatchID_builder{Id: c.Value}.Build()
	case ExternalMatchAttribute:
		matchAnnotation = v2.ExternalResourceMatch_builder{
			Key:          c.Key,
			Value:        c.Value,
			ResourceType: c.Trait,
		}.Build()
	default:
		panic("chaosconnector: unknown external principal match kind")
	}
	grantOptions := []gt.GrantOption{gt.WithAnnotation(matchAnnotation)}
	if c.Expandable {
		placeholderEntitlementID, makeErr := bid.MakeBid(v2.Entitlement_builder{
			Resource: v2.Resource_builder{Id: placeholder}.Build(),
			Slug:     "member",
		}.Build())
		if makeErr != nil {
			return nil, makeErr
		}
		grantOptions = append(grantOptions, gt.WithAnnotation(v2.GrantExpandable_builder{
			EntitlementIds:  []string{placeholderEntitlementID},
			Shallow:         true,
			ResourceTypeIds: []string{InternalGroupTypeID},
		}.Build()))
	}
	grant := gt.NewGrant(
		group,
		"member",
		placeholder,
		grantOptions...,
	)

	return &Scenario{
		Name:         c.Name,
		Seed:         1,
		InitialEpoch: "initial",
		Epochs: map[string]*Dataset{"initial": {
			ResourceTypes: []*v2.ResourceType{groupType, placeholderType},
			Resources: map[string]Pages[*v2.Resource]{
				InternalGroupTypeID:   {"": {List: []*v2.Resource{group}}},
				ExternalPlaceholderID: {"": {}},
			},
			StaticEntitlements: map[string]Pages[*v2.Entitlement]{
				InternalGroupTypeID:   {"": {}},
				ExternalPlaceholderID: {"": {}},
			},
			Entitlements: map[string]Pages[*v2.Entitlement]{
				InternalGroupTypeID:   {"": {List: []*v2.Entitlement{entitlement}}},
				ExternalPlaceholderID: {"": {}},
			},
			Grants: map[string]Pages[*v2.Grant]{
				InternalGroupTypeID:   {"": {List: []*v2.Grant{grant}}},
				ExternalPlaceholderID: {"": {}},
			},
		}},
	}, nil
}
