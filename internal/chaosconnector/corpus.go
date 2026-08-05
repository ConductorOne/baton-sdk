package chaosconnector

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// DataClass distinguishes representation, relational, temporal, and legal
// adversaries because they do not share one rejection policy.
type DataClass string

const (
	DataRepresentationInvalid  DataClass = "representation-invalid"
	DataRelationallyInvalid    DataClass = "relationally-inconsistent"
	DataTemporallyInconsistent DataClass = "temporally-inconsistent"
	DataLegalHostile           DataClass = "legal-hostile"
)

// DataPolicy is the expected SDK treatment for one corpus case.
type DataPolicy string

const (
	DataPolicyAccept     DataPolicy = "accept"
	DataPolicyNormalize  DataPolicy = "normalize"
	DataPolicySkipReport DataPolicy = "skip-and-report"
	DataPolicyWarnRetain DataPolicy = "warn-and-retain"
	DataPolicyRejectRPC  DataPolicy = "reject-rpc"
	DataPolicyFail       DataPolicy = "fail-operation"
	DataPolicyUnresolved DataPolicy = "unresolved"
)

// CorpusCase is one named, policy-bearing data adversary.
type CorpusCase struct {
	Name   string
	Class  DataClass
	Policy DataPolicy
	Apply  func(*Scenario) error
}

// InitialDataCorpus contains named representation cases that have not moved
// into a generated relational or temporal corpus.
func InitialDataCorpus() []CorpusCase {
	return []CorpusCase{
		{
			Name:   "entitlement-missing-resource",
			Class:  DataRepresentationInvalid,
			Policy: DataPolicySkipReport,
			Apply: func(scenario *Scenario) error {
				dataset, err := initialDataset(scenario)
				if err != nil {
					return err
				}
				entitlementPages := dataset.Entitlements[FullCapabilityResourceTypeID]
				entitlementPage := entitlementPages[""]
				entitlementPage.List = append(entitlementPage.List,
					v2.Entitlement_builder{Id: "chaos:malformed:no-resource"}.Build())
				entitlementPages[""] = entitlementPage
				return nil
			},
		},
	}
}

func initialDataset(scenario *Scenario) (*Dataset, error) {
	if err := scenario.Validate(); err != nil {
		return nil, err
	}
	dataset := scenario.Epochs[scenario.InitialEpoch]
	if dataset == nil {
		return nil, fmt.Errorf("chaosconnector: initial dataset is nil")
	}
	return dataset, nil
}
