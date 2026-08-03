package chaosconnector

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/proto"
)

// SemanticCategory identifies adversaries whose rows are individually
// well-formed but conflict across identity, relationship, or time.
type SemanticCategory string

const (
	SemanticDuplicate SemanticCategory = "duplicate"
	SemanticParent    SemanticCategory = "parent-reference"
)

// SemanticExpectation describes the canonical row that must survive a case.
type SemanticExpectation struct {
	Entity            ReferentialEntity
	CanonicalIdentity string
	Multiplicity      int
	DisplayName       string
	ExternalID        string
	ParentIdentity    string
}

// SemanticCase is one named relational adversary and its store oracle.
type SemanticCase struct {
	Name        string
	Category    SemanticCategory
	Policy      DataPolicy
	Apply       func(*Scenario) error
	Expectation SemanticExpectation
}

// SemanticCorpus returns identity-conflict and parent-reference cases. New
// categories can be appended without changing the lifecycle runner.
func SemanticCorpus() []SemanticCase {
	var out []SemanticCase
	out = append(out, duplicateIdentityCases()...)
	out = append(out, parentReferenceCases()...)
	return out
}

func duplicateIdentityCases() []SemanticCase {
	const (
		lastResourceName    = "Last Resource Observation"
		lastEntitlementName = "Last Entitlement Observation"
		lastGrantID         = "last-grant-observation"
	)
	return []SemanticCase{
		{
			Name:     "duplicate/resource-identical-same-page",
			Category: SemanticDuplicate,
			Policy:   DataPolicyAccept,
			Apply: func(scenario *Scenario) error {
				dataset, err := initialDataset(scenario)
				if err != nil {
					return err
				}
				page := dataset.Resources[FullCapabilityResourceTypeID][""]
				page.List = append(page.List, proto.Clone(page.List[0]).(*v2.Resource))
				dataset.Resources[FullCapabilityResourceTypeID][""] = page
				return nil
			},
			Expectation: resourceExpectation("Chaos User 1"),
		},
		{
			Name:     "duplicate/resource-conflict-same-page",
			Category: SemanticDuplicate,
			Policy:   DataPolicyWarnRetain,
			Apply: func(scenario *Scenario) error {
				dataset, err := initialDataset(scenario)
				if err != nil {
					return err
				}
				last := proto.Clone(baselineResource(dataset)).(*v2.Resource)
				last.SetDisplayName(lastResourceName)
				page := dataset.Resources[FullCapabilityResourceTypeID][""]
				page.List = append(page.List, last)
				dataset.Resources[FullCapabilityResourceTypeID][""] = page
				return nil
			},
			Expectation: resourceExpectation(lastResourceName),
		},
		{
			Name:     "duplicate/resource-conflict-cross-page",
			Category: SemanticDuplicate,
			Policy:   DataPolicyWarnRetain,
			Apply: func(scenario *Scenario) error {
				dataset, err := initialDataset(scenario)
				if err != nil {
					return err
				}
				last := proto.Clone(baselineResource(dataset)).(*v2.Resource)
				last.SetDisplayName(lastResourceName)
				root := dataset.Resources[FullCapabilityResourceTypeID][""]
				root.Next = "duplicate"
				dataset.Resources[FullCapabilityResourceTypeID][""] = root
				dataset.Resources[FullCapabilityResourceTypeID]["duplicate"] = Page[*v2.Resource]{
					List: []*v2.Resource{last},
				}
				return nil
			},
			Expectation: resourceExpectation(lastResourceName),
		},
		{
			Name:     "duplicate/entitlement-conflict-same-page",
			Category: SemanticDuplicate,
			Policy:   DataPolicyWarnRetain,
			Apply: func(scenario *Scenario) error {
				dataset, err := initialDataset(scenario)
				if err != nil {
					return err
				}
				page := dataset.Entitlements[FullCapabilityResourceTypeID][""]
				last := proto.Clone(page.List[0]).(*v2.Entitlement)
				last.SetDisplayName(lastEntitlementName)
				page.List = append(page.List, last)
				dataset.Entitlements[FullCapabilityResourceTypeID][""] = page
				return nil
			},
			Expectation: SemanticExpectation{
				Entity:            ReferentialEntitlement,
				CanonicalIdentity: "chaos-user:user-1:member",
				Multiplicity:      1,
				DisplayName:       lastEntitlementName,
			},
		},
		{
			Name:     "duplicate/grant-conflict-same-page",
			Category: SemanticDuplicate,
			Policy:   DataPolicyWarnRetain,
			Apply: func(scenario *Scenario) error {
				dataset, err := initialDataset(scenario)
				if err != nil {
					return err
				}
				page := dataset.Grants[FullCapabilityResourceTypeID][""]
				last := proto.Clone(page.List[0]).(*v2.Grant)
				last.SetId(lastGrantID)
				page.List = append(page.List, last)
				dataset.Grants[FullCapabilityResourceTypeID][""] = page
				return nil
			},
			Expectation: SemanticExpectation{
				Entity:            ReferentialGrant,
				CanonicalIdentity: "chaos-user\x00user-1\x00chaos-user:user-1:member\x00chaos-user\x00user-1",
				Multiplicity:      1,
				ExternalID:        lastGrantID,
			},
		},
	}
}

func parentReferenceCases() []SemanticCase {
	return []SemanticCase{
		parentCase("parent/missing-row", "parent-child-missing",
			v2.ResourceId_builder{
				ResourceType: FullCapabilityResourceTypeID,
				Resource:     "absent-parent",
			}.Build()),
		parentCase("parent/unknown-type", "parent-child-unknown",
			v2.ResourceId_builder{
				ResourceType: "chaos-unknown-type",
				Resource:     "unknown-parent",
			}.Build()),
		parentCase("parent/self-cycle", "parent-self",
			v2.ResourceId_builder{
				ResourceType: FullCapabilityResourceTypeID,
				Resource:     "parent-self",
			}.Build()),
		{
			Name:     "parent/two-node-cycle",
			Category: SemanticParent,
			Policy:   DataPolicyWarnRetain,
			Apply: func(scenario *Scenario) error {
				dataset, err := initialDataset(scenario)
				if err != nil {
					return err
				}
				first := resourceWithParent(dataset, "parent-cycle-a", "parent-cycle-b")
				second := resourceWithParent(dataset, "parent-cycle-b", "parent-cycle-a")
				page := dataset.Resources[FullCapabilityResourceTypeID][""]
				page.List = append(page.List, first, second)
				dataset.Resources[FullCapabilityResourceTypeID][""] = page
				return nil
			},
			Expectation: SemanticExpectation{
				Entity:            ReferentialResource,
				CanonicalIdentity: resourceIdentity("parent-cycle-b"),
				Multiplicity:      1,
				DisplayName:       "parent-cycle-b",
				ParentIdentity:    resourceIdentity("parent-cycle-a"),
			},
		},
	}
}

func parentCase(name, childID string, parent *v2.ResourceId) SemanticCase {
	return SemanticCase{
		Name:     name,
		Category: SemanticParent,
		Policy:   DataPolicyWarnRetain,
		Apply: func(scenario *Scenario) error {
			dataset, err := initialDataset(scenario)
			if err != nil {
				return err
			}
			child := proto.Clone(baselineResource(dataset)).(*v2.Resource)
			child.SetId(v2.ResourceId_builder{
				ResourceType: FullCapabilityResourceTypeID,
				Resource:     childID,
			}.Build())
			child.SetDisplayName(childID)
			child.SetParentResourceId(parent)
			page := dataset.Resources[FullCapabilityResourceTypeID][""]
			page.List = append(page.List, child)
			dataset.Resources[FullCapabilityResourceTypeID][""] = page
			return nil
		},
		Expectation: SemanticExpectation{
			Entity:            ReferentialResource,
			CanonicalIdentity: resourceIdentity(childID),
			Multiplicity:      1,
			DisplayName:       childID,
			ParentIdentity:    resourceIDKey(parent),
		},
	}
}

func resourceWithParent(dataset *Dataset, id, parentID string) *v2.Resource {
	resource := proto.Clone(baselineResource(dataset)).(*v2.Resource)
	resource.SetId(v2.ResourceId_builder{
		ResourceType: FullCapabilityResourceTypeID,
		Resource:     id,
	}.Build())
	resource.SetDisplayName(id)
	resource.SetParentResourceId(v2.ResourceId_builder{
		ResourceType: FullCapabilityResourceTypeID,
		Resource:     parentID,
	}.Build())
	return resource
}

func resourceExpectation(displayName string) SemanticExpectation {
	return SemanticExpectation{
		Entity:            ReferentialResource,
		CanonicalIdentity: resourceIdentity("user-1"),
		Multiplicity:      1,
		DisplayName:       displayName,
	}
}

func resourceIdentity(id string) string {
	return FullCapabilityResourceTypeID + "\x00" + id
}

func resourceIDKey(id *v2.ResourceId) string {
	if id == nil {
		return ""
	}
	return fmt.Sprintf("%s\x00%s", id.GetResourceType(), id.GetResource())
}
