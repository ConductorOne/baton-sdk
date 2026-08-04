package chaosconnector

import (
	"fmt"
	"math/rand"
	"slices"

	"google.golang.org/grpc/codes"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

const (
	FullCapabilityResourceTypeID = "chaos-user"
	IssuedSecretResourceTypeID   = "chaos-secret"
)

// NewFullScenario returns the small deterministic estate used to verify the
// connector's complete capability skeleton.
func NewFullScenario() (*Scenario, error) {
	userType := v2.ResourceType_builder{
		Id:          FullCapabilityResourceTypeID,
		DisplayName: "Chaos User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		Annotations: annotations.New(&v2.TypeScopedEntitlements{}, &v2.TypeScopedGrants{}),
	}.Build()
	secretType := v2.ResourceType_builder{
		Id:          IssuedSecretResourceTypeID,
		DisplayName: "Chaos Secret",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_SECRET},
		Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
	}.Build()

	user, err := rs.NewUserResource("Chaos User 1", userType, "user-1", nil)
	if err != nil {
		return nil, err
	}
	entitlement := et.NewEntitlement(user, "member", "assignment")
	grant := gt.NewGrant(user, "member", user)

	return &Scenario{
		Name:         "full-capability",
		Seed:         1,
		InitialEpoch: "initial",
		Epochs: map[string]*Dataset{
			"initial": {
				ResourceTypes: []*v2.ResourceType{userType, secretType},
				Resources: map[string]Pages[*v2.Resource]{
					FullCapabilityResourceTypeID: {
						"": {List: []*v2.Resource{user}},
					},
					IssuedSecretResourceTypeID: {
						"": {},
					},
				},
				StaticEntitlements: map[string]Pages[*v2.Entitlement]{
					FullCapabilityResourceTypeID: {
						"": {},
					},
				},
				Entitlements: map[string]Pages[*v2.Entitlement]{
					FullCapabilityResourceTypeID: {
						"": {List: []*v2.Entitlement{entitlement}},
					},
				},
				Grants: map[string]Pages[*v2.Grant]{
					FullCapabilityResourceTypeID: {
						"": {List: []*v2.Grant{grant}},
					},
				},
			},
		},
	}, nil
}

// NewGeneratedSyncScenario builds a deterministic type-scoped fan-out graph.
// Every non-root token carries one entitlement and matching grant.
func NewGeneratedSyncScenario(seed int64) (*Scenario, error) {
	scenario, err := NewFullScenario()
	if err != nil {
		return nil, err
	}
	scenario.Name = fmt.Sprintf("generated-sync-%d", seed)
	scenario.Seed = seed
	dataset := scenario.Epochs[scenario.InitialEpoch]
	user := dataset.Resources[FullCapabilityResourceTypeID][""].List[0]

	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test generator
	count := 8 + rng.Intn(9)
	tokens := make([]string, count)
	for i := range count {
		tokens[i] = fmt.Sprintf("p%02d", i)
	}
	rng.Shuffle(len(tokens), func(i, j int) {
		tokens[i], tokens[j] = tokens[j], tokens[i]
	})

	entitlementPages := make(Pages[*v2.Entitlement], count+1)
	grantPages := make(Pages[*v2.Grant], count+1)
	rootCount := min(3, len(tokens))
	entitlementPages[""] = Page[*v2.Entitlement]{Spawn: append([]string(nil), tokens[:rootCount]...)}
	grantPages[""] = Page[*v2.Grant]{Spawn: append([]string(nil), tokens[:rootCount]...)}

	for i, token := range tokens {
		name := fmt.Sprintf("generated-%02d", i)
		entitlement := et.NewEntitlement(user, name, "assignment")
		grant := gt.NewGrant(user, name, user)
		entitlementPage := Page[*v2.Entitlement]{List: []*v2.Entitlement{entitlement}}
		grantPage := Page[*v2.Grant]{List: []*v2.Grant{grant}}
		childIndex := i + rootCount
		if childIndex < len(tokens) {
			if rng.Intn(2) == 0 {
				entitlementPage.Next = tokens[childIndex]
				grantPage.Next = tokens[childIndex]
			} else {
				entitlementPage.Spawn = []string{tokens[childIndex]}
				grantPage.Spawn = []string{tokens[childIndex]}
			}
		}
		entitlementPages[token] = entitlementPage
		grantPages[token] = grantPage
	}
	dataset.Entitlements[FullCapabilityResourceTypeID] = entitlementPages
	dataset.Grants[FullCapabilityResourceTypeID] = grantPages
	return scenario, nil
}

// GeneratedRetrySchedule injects deterministic retryable failures into a
// generated scenario. At least one rule is always armed.
func GeneratedRetrySchedule(scenario *Scenario) Schedule {
	dataset := scenario.Epochs[scenario.InitialEpoch]
	pages := dataset.Entitlements[FullCapabilityResourceTypeID]
	rules := make([]Rule, 0)
	tokens := make([]string, 0, len(pages))
	for token := range pages {
		if token != "" {
			tokens = append(tokens, token)
		}
	}
	slices.Sort(tokens)
	index := 0
	for _, token := range tokens {
		if index%4 != 0 {
			index++
			continue
		}
		rules = append(rules, Rule{
			ID: "generated-entitlement-retry-" + token,
			Match: Matcher{
				Service:   ExactString("EntitlementsService"),
				Method:    ExactString("ListEntitlements"),
				PageToken: ExactString(token),
				Attempt:   1,
				Phase:     PhaseBeforeCall,
			},
			Effects: []Effect{{
				Kind:    EffectError,
				Code:    codes.Unavailable,
				Message: "generated retryable fault",
			}},
			MinFires: 1,
			MaxFires: 1,
		})
		index++
	}
	if len(rules) == 0 {
		for _, token := range tokens {
			rules = append(rules, Rule{
				ID: "generated-entitlement-retry-" + token,
				Match: Matcher{
					Service:   ExactString("EntitlementsService"),
					Method:    ExactString("ListEntitlements"),
					PageToken: ExactString(token),
					Attempt:   1,
					Phase:     PhaseBeforeCall,
				},
				Effects:  []Effect{{Kind: EffectError, Code: codes.Unavailable, Message: "generated retryable fault"}},
				MinFires: 1,
				MaxFires: 1,
			})
			break
		}
	}
	return NewSchedule(rules...)
}
