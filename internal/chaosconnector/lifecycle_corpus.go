package chaosconnector

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const (
	LifecycleDropCaseName   = "lifecycle/drop-does-not-resurrect"
	LifecycleFailCaseName   = "lifecycle/hard-invalid-never-seals"
	LifecycleRetainCaseName = "lifecycle/warn-retain-survives-resume"
	LifecycleDriftCaseName  = "lifecycle/resume-uses-current-answer"
)

// LifecycleCase represents one data-policy equivalence class crossing an
// interrupted attempt and a persisted resume.
type LifecycleAttemptExpectation struct {
	Sealed              bool
	Present             bool
	DisplayName         string
	EntitlementsDropped int64
	MustFail            bool
	ErrorContains       string
}

type LifecycleCase struct {
	Name                 string
	Policy               DataPolicy
	BuildInitial         func() (*Scenario, error)
	BuildResume          func() (*Scenario, error)
	InterruptSchedule    Schedule
	InterruptedPageToken string
	Identity             string
	Initial              LifecycleAttemptExpectation
	Resume               LifecycleAttemptExpectation
}

// LifecycleCorpus covers each materially different data-policy outcome
// without multiplying every referential shape by every cut point.
func LifecycleCorpus() []LifecycleCase {
	return []LifecycleCase{
		{
			Name:                 LifecycleDropCaseName,
			Policy:               DataPolicySkipReport,
			BuildInitial:         dropLifecycleScenario,
			BuildResume:          dropLifecycleScenario,
			InterruptSchedule:    lifecycleCrashSchedule("cut"),
			InterruptedPageToken: "cut",
			Identity:             "lifecycle-unknown-type",
			Initial: LifecycleAttemptExpectation{
				EntitlementsDropped: 1,
			},
			Resume: LifecycleAttemptExpectation{
				Sealed:              true,
				EntitlementsDropped: 1,
			},
		},
		{
			Name:                 LifecycleFailCaseName,
			Policy:               DataPolicyFail,
			BuildInitial:         hardInvalidLifecycleScenario,
			BuildResume:          hardInvalidLifecycleScenario,
			InterruptSchedule:    lifecycleCrashSchedule("bad"),
			InterruptedPageToken: "bad",
			Identity:             "",
			Resume: LifecycleAttemptExpectation{
				MustFail:      true,
				ErrorContains: "entitlement with missing identity",
			},
		},
		{
			Name:                 LifecycleRetainCaseName,
			Policy:               DataPolicyWarnRetain,
			BuildInitial:         retainLifecycleScenario,
			BuildResume:          retainLifecycleScenario,
			InterruptSchedule:    lifecycleCrashSchedule("cut"),
			InterruptedPageToken: "cut",
			Identity:             "lifecycle-dangling",
			Initial: LifecycleAttemptExpectation{
				Present:     true,
				DisplayName: "Lifecycle dangling entitlement",
			},
			Resume: LifecycleAttemptExpectation{
				Sealed:      true,
				Present:     true,
				DisplayName: "Lifecycle dangling entitlement",
			},
		},
		{
			Name:                 LifecycleDriftCaseName,
			Policy:               DataPolicyAccept,
			BuildInitial:         func() (*Scenario, error) { return changedLifecycleScenario("Old interrupted answer") },
			BuildResume:          func() (*Scenario, error) { return changedLifecycleScenario("Answer from resume") },
			InterruptSchedule:    lifecycleCrashSchedule("changed"),
			InterruptedPageToken: "changed",
			Identity:             "lifecycle-changed-answer",
			Resume: LifecycleAttemptExpectation{
				Sealed:      true,
				Present:     true,
				DisplayName: "Answer from resume",
			},
		},
	}
}

// LifecycleCaseByName resolves the same deterministic case registry for
// in-process tests and real-process harnesses.
func LifecycleCaseByName(name string) (LifecycleCase, bool) {
	for _, corpusCase := range LifecycleCorpus() {
		if corpusCase.Name == name {
			return corpusCase, true
		}
	}
	return LifecycleCase{}, false
}

func lifecycleCrashSchedule(pageToken string) Schedule {
	return NewSchedule(Rule{
		ID: "interrupt-" + pageToken,
		Match: Matcher{
			Domain:       DomainConnector,
			Method:       ExactString("ListEntitlements"),
			ResourceType: ExactString(FullCapabilityResourceTypeID),
			PageToken:    ExactString(pageToken),
			Phase:        PhaseAfterDelegate,
		},
		Effects:  []Effect{{Kind: EffectCrash}},
		MinFires: 1,
		MaxFires: 1,
	})
}

func dropLifecycleScenario() (*Scenario, error) {
	scenario, dataset, err := newLifecycleScenario()
	if err != nil {
		return nil, err
	}
	resource := resourceForShape(ReferenceTypeUnknown, "lifecycle-unknown-resource")
	entitlement := v2.Entitlement_builder{
		Id:          "lifecycle-unknown-type",
		DisplayName: "Lifecycle unknown type",
		Resource:    resource,
	}.Build()
	setLifecycleEntitlementPages(dataset, []*v2.Entitlement{entitlement}, "cut", nil)
	return scenario, nil
}

func hardInvalidLifecycleScenario() (*Scenario, error) {
	scenario, dataset, err := newLifecycleScenario()
	if err != nil {
		return nil, err
	}
	malformed := v2.Entitlement_builder{
		DisplayName: "Lifecycle missing identity",
		Resource:    baselineResource(dataset),
	}.Build()
	setLifecycleEntitlementPages(dataset, nil, "bad", []*v2.Entitlement{malformed})
	return scenario, nil
}

func retainLifecycleScenario() (*Scenario, error) {
	scenario, dataset, err := newLifecycleScenario()
	if err != nil {
		return nil, err
	}
	dangling := v2.Entitlement_builder{
		Id:          "lifecycle-dangling",
		DisplayName: "Lifecycle dangling entitlement",
		Resource:    resourceForShape(ReferenceRowMissing, "lifecycle-dangling"),
	}.Build()
	setLifecycleEntitlementPages(dataset, []*v2.Entitlement{dangling}, "cut", nil)
	return scenario, nil
}

func changedLifecycleScenario(displayName string) (*Scenario, error) {
	scenario, dataset, err := newLifecycleScenario()
	if err != nil {
		return nil, err
	}
	entitlement := v2.Entitlement_builder{
		Id:          "lifecycle-changed-answer",
		DisplayName: displayName,
		Resource:    baselineResource(dataset),
	}.Build()
	setLifecycleEntitlementPages(dataset, nil, "changed", []*v2.Entitlement{entitlement})
	return scenario, nil
}

func newLifecycleScenario() (*Scenario, *Dataset, error) {
	scenario, err := NewFullScenario()
	if err != nil {
		return nil, nil, err
	}
	dataset := scenario.Epochs[scenario.InitialEpoch]
	// Grants are irrelevant to these entitlement-policy cases and retaining a
	// grant to the removed baseline entitlement would add a second adversary.
	dataset.Grants[FullCapabilityResourceTypeID] = Pages[*v2.Grant]{"": {}}
	return scenario, dataset, nil
}

func setLifecycleEntitlementPages(
	dataset *Dataset,
	rootList []*v2.Entitlement,
	next string,
	nextList []*v2.Entitlement,
) {
	pages := Pages[*v2.Entitlement]{
		"": {
			List: cloneMessages(rootList),
			Next: next,
		},
	}
	if next != "" {
		pages[next] = Page[*v2.Entitlement]{List: cloneMessages(nextList)}
	}
	dataset.Entitlements[FullCapabilityResourceTypeID] = pages
}
