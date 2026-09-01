package connector

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

var (
	// resourceTypeUser covers both accepted team members and outstanding invitations. Invitations
	// are surfaced as pending users because that is what they are: a person who has been granted
	// access but has not yet claimed it with an Apple ID.
	resourceTypeUser = &v2.ResourceType{
		Id:          "user",
		DisplayName: "User",
		Traits: []v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_USER,
		},
		Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
	}

	// resourceTypeRole is the fixed App Store Connect role enum. Apple exposes no discovery
	// endpoint for it, so the list is static.
	resourceTypeRole = &v2.ResourceType{
		Id:          "role",
		DisplayName: "Role",
		Traits: []v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_ROLE,
		},
	}

	// resourceTypeApp models an app in the team's account, so per-app access (visibleApps) can be
	// reviewed and provisioned like any other entitlement.
	resourceTypeApp = &v2.ResourceType{
		Id:          "app",
		DisplayName: "App",
		Traits: []v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_APP,
		},
	}
)
