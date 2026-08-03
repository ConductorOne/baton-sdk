package chaosconnector

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/proto"
)

// ConcurrentDuplicateCase forces one conflicting sibling response to arrive
// last. The blocked token's value must be the final stored value.
type ConcurrentDuplicateCase struct {
	Name          string
	Entity        ReferentialEntity
	BlockedToken  string
	FirstToken    string
	Schedule      Schedule
	CrashSchedule Schedule
}

// ConcurrentDuplicateCorpus covers both completion orders for every canonical
// row family written concurrently by the syncer.
func ConcurrentDuplicateCorpus() []ConcurrentDuplicateCase {
	var out []ConcurrentDuplicateCase
	for _, entity := range []ReferentialEntity{
		ReferentialResource,
		ReferentialEntitlement,
		ReferentialGrant,
	} {
		out = append(out,
			newConcurrentDuplicateCase(entity, "left", "right"),
			newConcurrentDuplicateCase(entity, "right", "left"),
		)
	}
	return out
}

// NewConcurrentDuplicateScenario returns two spawned cursor pages carrying
// conflicting observations of the same canonical row.
func NewConcurrentDuplicateScenario(entity ReferentialEntity) (*Scenario, error) {
	scenario, err := NewFullScenario()
	if err != nil {
		return nil, err
	}
	dataset := scenario.Epochs[scenario.InitialEpoch]
	switch entity {
	case ReferentialResource:
		baseline := dataset.Resources[FullCapabilityResourceTypeID][""].List[0]
		left := proto.Clone(baseline).(*v2.Resource)
		left.SetDisplayName(concurrentDuplicateValue(entity, "left"))
		right := proto.Clone(baseline).(*v2.Resource)
		right.SetDisplayName(concurrentDuplicateValue(entity, "right"))
		// Resource pagination is sequential. Use the two independently
		// scheduled resource-type roots to create concurrent writers for the
		// same canonical resource instead.
		dataset.Resources[FullCapabilityResourceTypeID] = Pages[*v2.Resource]{
			"": {List: []*v2.Resource{left}},
		}
		dataset.Resources[IssuedSecretResourceTypeID] = Pages[*v2.Resource]{
			"": {List: []*v2.Resource{right}},
		}
	case ReferentialEntitlement:
		baseline := dataset.Entitlements[FullCapabilityResourceTypeID][""].List[0]
		left := proto.Clone(baseline).(*v2.Entitlement)
		left.SetDisplayName(concurrentDuplicateValue(entity, "left"))
		right := proto.Clone(baseline).(*v2.Entitlement)
		right.SetDisplayName(concurrentDuplicateValue(entity, "right"))
		dataset.Entitlements[FullCapabilityResourceTypeID] = Pages[*v2.Entitlement]{
			"":      {Spawn: []string{"left", "right"}},
			"left":  {List: []*v2.Entitlement{left}},
			"right": {List: []*v2.Entitlement{right}},
		}
	case ReferentialGrant:
		baseline := dataset.Grants[FullCapabilityResourceTypeID][""].List[0]
		left := proto.Clone(baseline).(*v2.Grant)
		left.SetId(concurrentDuplicateValue(entity, "left"))
		right := proto.Clone(baseline).(*v2.Grant)
		right.SetId(concurrentDuplicateValue(entity, "right"))
		dataset.Grants[FullCapabilityResourceTypeID] = Pages[*v2.Grant]{
			"":      {Spawn: []string{"left", "right"}},
			"left":  {List: []*v2.Grant{left}},
			"right": {List: []*v2.Grant{right}},
		}
	default:
		return nil, fmt.Errorf("chaosconnector: unsupported concurrent duplicate entity %q", entity)
	}
	return scenario, nil
}

func newConcurrentDuplicateCase(entity ReferentialEntity, blocked, first string) ConcurrentDuplicateCase {
	barrier := "release-" + blocked
	method := concurrentDuplicateMethod(entity)
	match := Matcher{
		Domain:       DomainConnector,
		Method:       ExactString(method),
		ResourceType: ExactString(FullCapabilityResourceTypeID),
		PageToken:    ExactString(blocked),
		Phase:        PhaseAfterDelegate,
	}
	if entity == ReferentialResource {
		match.ResourceType = ExactString(concurrentResourceScope(blocked))
		match.PageToken = ExactString("")
	}
	return ConcurrentDuplicateCase{
		Name:         fmt.Sprintf("concurrent-duplicate/%s/%s-completes-last", entity, blocked),
		Entity:       entity,
		BlockedToken: blocked,
		FirstToken:   first,
		Schedule: NewSchedule(Rule{
			ID:       "block-" + blocked,
			Match:    match,
			Effects:  []Effect{{Kind: EffectBlock, Barrier: barrier}},
			MinFires: 1,
			MaxFires: 1,
		}),
		CrashSchedule: NewSchedule(Rule{
			ID:    "crash-" + blocked,
			Match: match,
			Effects: []Effect{
				{Kind: EffectBlock, Barrier: barrier},
				{Kind: EffectCrash},
			},
			MinFires: 1,
			MaxFires: 1,
		}),
	}
}

func (c ConcurrentDuplicateCase) Method() string {
	return concurrentDuplicateMethod(c.Entity)
}

func (c ConcurrentDuplicateCase) OperationMatchesToken(operation Operation, token string) bool {
	if operation.Method != c.Method() {
		return false
	}
	if c.Entity == ReferentialResource {
		return operation.ResourceType == concurrentResourceScope(token)
	}
	return operation.PageToken == token
}

func (c ConcurrentDuplicateCase) Expectation(token string) SemanticExpectation {
	expectation := SemanticExpectation{
		Entity:       c.Entity,
		Multiplicity: 1,
	}
	switch c.Entity {
	case ReferentialResource:
		expectation.CanonicalIdentity = resourceIdentity("user-1")
		expectation.DisplayName = concurrentDuplicateValue(c.Entity, token)
	case ReferentialEntitlement:
		expectation.CanonicalIdentity = "chaos-user:user-1:member"
		expectation.DisplayName = concurrentDuplicateValue(c.Entity, token)
	case ReferentialGrant:
		expectation.CanonicalIdentity = "chaos-user:user-1:member\x00chaos-user\x00user-1"
		expectation.ExternalID = concurrentDuplicateValue(c.Entity, token)
	}
	return expectation
}

func concurrentDuplicateMethod(entity ReferentialEntity) string {
	switch entity {
	case ReferentialResource:
		return "ListResources"
	case ReferentialEntitlement:
		return "ListEntitlements"
	case ReferentialGrant:
		return "ListGrants"
	default:
		return ""
	}
}

func concurrentDuplicateValue(entity ReferentialEntity, token string) string {
	return fmt.Sprintf("concurrent-%s-observation-from-%s", entity, token)
}

func concurrentResourceScope(token string) string {
	if token == "left" {
		return FullCapabilityResourceTypeID
	}
	return IssuedSecretResourceTypeID
}
