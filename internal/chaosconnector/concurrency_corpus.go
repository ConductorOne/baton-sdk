package chaosconnector

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/proto"
)

// ConcurrentDuplicateCase forces one conflicting entitlement sibling response
// to arrive last. The blocked token's value must be the final stored value.
type ConcurrentDuplicateCase struct {
	Name          string
	BlockedToken  string
	FirstToken    string
	ExpectedName  string
	Schedule      Schedule
	CrashSchedule Schedule
}

// ConcurrentDuplicateCorpus covers both completion orders.
func ConcurrentDuplicateCorpus() []ConcurrentDuplicateCase {
	return []ConcurrentDuplicateCase{
		newConcurrentDuplicateCase("left", "right"),
		newConcurrentDuplicateCase("right", "left"),
	}
}

// NewConcurrentDuplicateScenario returns two spawned cursor pages carrying
// conflicting observations of the same canonical entitlement.
func NewConcurrentDuplicateScenario() (*Scenario, error) {
	scenario, err := NewFullScenario()
	if err != nil {
		return nil, err
	}
	dataset := scenario.Epochs[scenario.InitialEpoch]
	baseline := dataset.Entitlements[FullCapabilityResourceTypeID][""].List[0]
	left := proto.Clone(baseline).(*v2.Entitlement)
	left.SetDisplayName(concurrentDuplicateDisplayName("left"))
	right := proto.Clone(baseline).(*v2.Entitlement)
	right.SetDisplayName(concurrentDuplicateDisplayName("right"))
	dataset.Entitlements[FullCapabilityResourceTypeID] = Pages[*v2.Entitlement]{
		"": {
			Spawn: []string{"left", "right"},
		},
		"left": {
			List: []*v2.Entitlement{left},
		},
		"right": {
			List: []*v2.Entitlement{right},
		},
	}
	return scenario, nil
}

func newConcurrentDuplicateCase(blocked, first string) ConcurrentDuplicateCase {
	barrier := "release-" + blocked
	return ConcurrentDuplicateCase{
		Name:         fmt.Sprintf("concurrent-duplicate/%s-completes-last", blocked),
		BlockedToken: blocked,
		FirstToken:   first,
		ExpectedName: concurrentDuplicateDisplayName(blocked),
		Schedule: NewSchedule(Rule{
			ID: "block-" + blocked,
			Match: Matcher{
				Domain:       DomainConnector,
				Method:       ExactString("ListEntitlements"),
				ResourceType: ExactString(FullCapabilityResourceTypeID),
				PageToken:    ExactString(blocked),
				Phase:        PhaseAfterDelegate,
			},
			Effects:  []Effect{{Kind: EffectBlock, Barrier: barrier}},
			MinFires: 1,
			MaxFires: 1,
		}),
		CrashSchedule: NewSchedule(Rule{
			ID: "crash-" + blocked,
			Match: Matcher{
				Domain:       DomainConnector,
				Method:       ExactString("ListEntitlements"),
				ResourceType: ExactString(FullCapabilityResourceTypeID),
				PageToken:    ExactString(blocked),
				Phase:        PhaseAfterDelegate,
			},
			Effects: []Effect{
				{Kind: EffectBlock, Barrier: barrier},
				{Kind: EffectCrash},
			},
			MinFires: 1,
			MaxFires: 1,
		}),
	}
}

func concurrentDuplicateDisplayName(token string) string {
	return "Concurrent observation from " + token
}
