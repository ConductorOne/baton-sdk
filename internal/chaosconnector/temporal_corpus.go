package chaosconnector

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

const retryDriftEpoch = "retry-drift"

// TemporalCase changes the connector world after a response is produced but
// lost, then asserts the answer accepted from the retry.
type TemporalCase struct {
	Name                    string
	Build                   func() (*Scenario, error)
	Schedule                Schedule
	Expectation             SemanticExpectation
	AbsentCanonicalIdentity string
}

// TemporalCorpus returns deterministic changed-answer retry cases.
func TemporalCorpus() []TemporalCase {
	return []TemporalCase{
		{
			Name:     "retry-drift/resource-content",
			Build:    resourceContentDriftScenario,
			Schedule: retryDriftSchedule("ListResources"),
			Expectation: SemanticExpectation{
				Entity:            ReferentialResource,
				CanonicalIdentity: resourceIdentity("user-1"),
				Multiplicity:      1,
				DisplayName:       "Resource From Retry",
			},
		},
		{
			Name:     "retry-drift/resource-identity",
			Build:    resourceIdentityDriftScenario,
			Schedule: retryDriftSchedule("ListResources"),
			Expectation: SemanticExpectation{
				Entity:            ReferentialResource,
				CanonicalIdentity: resourceIdentity("user-2"),
				Multiplicity:      1,
				DisplayName:       "Chaos User 2",
			},
			AbsentCanonicalIdentity: resourceIdentity("user-1"),
		},
		{
			Name:     "retry-drift/entitlement-content",
			Build:    entitlementContentDriftScenario,
			Schedule: retryDriftSchedule("ListEntitlements"),
			Expectation: SemanticExpectation{
				Entity:            ReferentialEntitlement,
				CanonicalIdentity: "chaos-user:user-1:member",
				Multiplicity:      1,
				DisplayName:       "Entitlement From Retry",
			},
		},
		{
			Name:     "retry-drift/grant-content",
			Build:    grantContentDriftScenario,
			Schedule: retryDriftSchedule("ListGrants"),
			Expectation: SemanticExpectation{
				Entity:            ReferentialGrant,
				CanonicalIdentity: "chaos-user:user-1:member\x00chaos-user\x00user-1",
				Multiplicity:      1,
				ExternalID:        "grant-from-retry",
			},
		},
	}
}

func retryDriftSchedule(method string) Schedule {
	return NewSchedule(Rule{
		ID: "change-answer-after-lost-response",
		Match: Matcher{
			Domain:       DomainConnector,
			Method:       method,
			ResourceType: FullCapabilityResourceTypeID,
			Attempt:      1,
			Phase:        PhaseAfterDelegate,
		},
		Effects: []Effect{
			{Kind: EffectSetEpoch, Epoch: retryDriftEpoch},
			{Kind: EffectLoseResponse, Code: codes.Unavailable, Message: "first answer lost"},
		},
		MinFires: 1,
		MaxFires: 1,
	})
}

func resourceContentDriftScenario() (*Scenario, error) {
	return scenarioWithChangedEpoch(func(dataset *Dataset) {
		resource := dataset.Resources[FullCapabilityResourceTypeID][""].List[0]
		resource.SetDisplayName("Resource From Retry")
	})
}

func resourceIdentityDriftScenario() (*Scenario, error) {
	return scenarioWithChangedEpoch(func(dataset *Dataset) {
		resource := dataset.Resources[FullCapabilityResourceTypeID][""].List[0]
		resource.SetId(v2.ResourceId_builder{
			ResourceType: FullCapabilityResourceTypeID,
			Resource:     "user-2",
		}.Build())
		resource.SetDisplayName("Chaos User 2")

		entitlement := dataset.Entitlements[FullCapabilityResourceTypeID][""].List[0]
		entitlement.SetId("chaos-user:user-2:member")
		entitlement.SetResource(resource)

		grant := dataset.Grants[FullCapabilityResourceTypeID][""].List[0]
		grant.SetEntitlement(entitlement)
		grant.SetPrincipal(resource)
	})
}

func entitlementContentDriftScenario() (*Scenario, error) {
	return scenarioWithChangedEpoch(func(dataset *Dataset) {
		entitlement := dataset.Entitlements[FullCapabilityResourceTypeID][""].List[0]
		entitlement.SetDisplayName("Entitlement From Retry")
	})
}

func grantContentDriftScenario() (*Scenario, error) {
	return scenarioWithChangedEpoch(func(dataset *Dataset) {
		grant := dataset.Grants[FullCapabilityResourceTypeID][""].List[0]
		grant.SetId("grant-from-retry")
	})
}

func scenarioWithChangedEpoch(change func(*Dataset)) (*Scenario, error) {
	scenario, err := NewFullScenario()
	if err != nil {
		return nil, err
	}
	changedScenario, err := NewFullScenario()
	if err != nil {
		return nil, err
	}
	changed := cloneDataset(changedScenario.Epochs[changedScenario.InitialEpoch])
	change(changed)
	scenario.Epochs[retryDriftEpoch] = changed
	return scenario, nil
}

func cloneDataset(dataset *Dataset) *Dataset {
	return &Dataset{
		ResourceTypes:      cloneMessages(dataset.ResourceTypes),
		Resources:          clonePageMap(dataset.Resources),
		StaticEntitlements: clonePageMap(dataset.StaticEntitlements),
		Entitlements:       clonePageMap(dataset.Entitlements),
		Grants:             clonePageMap(dataset.Grants),
	}
}

func clonePageMap[T proto.Message](in map[string]Pages[T]) map[string]Pages[T] {
	out := make(map[string]Pages[T], len(in))
	for scope, pages := range in {
		clonedPages := make(Pages[T], len(pages))
		for token, page := range pages {
			page.List = cloneMessages(page.List)
			page.Spawn = append([]string(nil), page.Spawn...)
			page.Annotations = cloneMessages(page.Annotations)
			clonedPages[token] = page
		}
		out[scope] = clonedPages
	}
	return out
}
