package chaosconnector

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// ReferentialEntity identifies the row family under test.
type ReferentialEntity string

const (
	ReferentialResource    ReferentialEntity = "resource"
	ReferentialEntitlement ReferentialEntity = "entitlement"
	ReferentialGrant       ReferentialEntity = "grant"
)

// ReferenceShape is the finite vocabulary used to generate referential
// adversaries. Paths use the applicable subset; unsupported combinations are
// omitted rather than normalized into duplicate test cases.
type ReferenceShape string

const (
	ReferenceValid           ReferenceShape = "valid"
	ReferenceCarrierNil      ReferenceShape = "carrier-nil"
	ReferenceExternalIDEmpty ReferenceShape = "external-id-empty"
	ReferenceResourceNil     ReferenceShape = "resource-nil"
	ReferenceIdentityNil     ReferenceShape = "identity-nil"
	ReferenceTypeEmpty       ReferenceShape = "resource-type-empty"
	ReferenceObjectIDEmpty   ReferenceShape = "resource-id-empty"
	ReferenceTypeUnknown     ReferenceShape = "resource-type-unknown"
	ReferenceRowMissing      ReferenceShape = "referenced-row-missing"
)

// ReferentialCase is one generated, policy-bearing corpus cell.
type ReferentialCase struct {
	Name                 string
	Entity               ReferentialEntity
	Reference            ReferenceShape
	EntitlementReference ReferenceShape
	PrincipalReference   ReferenceShape
	Policy               DataPolicy
	Identity             string
	Apply                func(*Scenario) error
}

// ReferentialCorpus returns every currently modeled resource identity,
// entitlement-resource, and grant entitlement×principal cell.
func ReferentialCorpus() []ReferentialCase {
	var out []ReferentialCase
	out = append(out, resourceReferentialCases()...)
	out = append(out, entitlementReferentialCases()...)
	out = append(out, grantReferentialCases()...)
	return out
}

func resourceReferentialCases() []ReferentialCase {
	states := []ReferenceShape{
		ReferenceCarrierNil,
		ReferenceIdentityNil,
		ReferenceTypeEmpty,
		ReferenceObjectIDEmpty,
	}
	out := make([]ReferentialCase, 0, len(states))
	for _, state := range states {
		state := state
		out = append(out, ReferentialCase{
			Name:      "resource/" + string(state),
			Entity:    ReferentialResource,
			Reference: state,
			Policy:    DataPolicySkipReport,
			Apply: func(scenario *Scenario) error {
				dataset, err := initialDataset(scenario)
				if err != nil {
					return err
				}
				page := dataset.Resources[FullCapabilityResourceTypeID][""]
				page.List = append(page.List, resourceForShape(state, "resource-adversary"))
				dataset.Resources[FullCapabilityResourceTypeID][""] = page
				return nil
			},
		})
	}
	return out
}

func entitlementReferentialCases() []ReferentialCase {
	states := []ReferenceShape{
		ReferenceCarrierNil,
		ReferenceExternalIDEmpty,
		ReferenceResourceNil,
		ReferenceIdentityNil,
		ReferenceTypeEmpty,
		ReferenceObjectIDEmpty,
		ReferenceTypeUnknown,
		ReferenceRowMissing,
		ReferenceValid,
	}
	out := make([]ReferentialCase, 0, len(states))
	for _, state := range states {
		state := state
		id := "chaos-entitlement-" + string(state)
		out = append(out, ReferentialCase{
			Name:      "entitlement/" + string(state),
			Entity:    ReferentialEntitlement,
			Reference: state,
			Policy:    entitlementPolicy(state),
			Identity:  id,
			Apply: func(scenario *Scenario) error {
				dataset, err := initialDataset(scenario)
				if err != nil {
					return err
				}
				resource := baselineResource(dataset)
				entitlement := entitlementForShape(state, id, resource)
				page := dataset.Entitlements[FullCapabilityResourceTypeID][""]
				page.List = append(page.List, entitlement)
				dataset.Entitlements[FullCapabilityResourceTypeID][""] = page
				return nil
			},
		})
	}
	return out
}

func grantReferentialCases() []ReferentialCase {
	entitlementStates := []ReferenceShape{
		ReferenceCarrierNil,
		ReferenceExternalIDEmpty,
		ReferenceResourceNil,
		ReferenceIdentityNil,
		ReferenceTypeEmpty,
		ReferenceObjectIDEmpty,
		ReferenceTypeUnknown,
		ReferenceRowMissing,
		ReferenceValid,
	}
	principalStates := []ReferenceShape{
		ReferenceCarrierNil,
		ReferenceIdentityNil,
		ReferenceTypeEmpty,
		ReferenceObjectIDEmpty,
		ReferenceTypeUnknown,
		ReferenceRowMissing,
		ReferenceValid,
	}
	out := make([]ReferentialCase, 0, len(entitlementStates)*len(principalStates)+1)
	out = append(out, ReferentialCase{
		Name:   "grant/carrier-nil",
		Entity: ReferentialGrant,
		Policy: DataPolicyFail,
		Apply: func(scenario *Scenario) error {
			dataset, err := initialDataset(scenario)
			if err != nil {
				return err
			}
			page := dataset.Grants[FullCapabilityResourceTypeID][""]
			page.List = append(page.List, nil)
			dataset.Grants[FullCapabilityResourceTypeID][""] = page
			return nil
		},
	})
	for _, entitlementState := range entitlementStates {
		for _, principalState := range principalStates {
			entitlementState := entitlementState
			principalState := principalState
			name := fmt.Sprintf("grant/entitlement-%s/principal-%s", entitlementState, principalState)
			out = append(out, ReferentialCase{
				Name:                 name,
				Entity:               ReferentialGrant,
				EntitlementReference: entitlementState,
				PrincipalReference:   principalState,
				Policy:               grantPolicy(entitlementState, principalState),
				Identity:             name,
				Apply: func(scenario *Scenario) error {
					dataset, err := initialDataset(scenario)
					if err != nil {
						return err
					}
					resource := baselineResource(dataset)
					entitlementID := "chaos-grant-entitlement-" + string(entitlementState)
					entitlement := entitlementForShape(entitlementState, entitlementID, resource)
					principal := resourceForShape(principalState, "grant-principal")
					grant := v2.Grant_builder{
						Id:          name,
						Entitlement: entitlement,
						Principal:   principal,
					}.Build()
					page := dataset.Grants[FullCapabilityResourceTypeID][""]
					page.List = append(page.List, grant)
					dataset.Grants[FullCapabilityResourceTypeID][""] = page
					return nil
				},
			})
		}
	}
	return out
}

func entitlementPolicy(state ReferenceShape) DataPolicy {
	switch state {
	case ReferenceCarrierNil:
		return DataPolicySkipReport
	case ReferenceTypeUnknown:
		return DataPolicySkipReport
	case ReferenceRowMissing:
		return DataPolicyWarnRetain
	case ReferenceValid:
		return DataPolicyAccept
	case ReferenceExternalIDEmpty, ReferenceResourceNil, ReferenceIdentityNil,
		ReferenceTypeEmpty, ReferenceObjectIDEmpty:
		return DataPolicySkipReport
	default:
		return DataPolicyUnresolved
	}
}

func grantPolicy(entitlementState, principalState ReferenceShape) DataPolicy {
	// The filter can only apply the out-of-scope drop rule when both sides
	// expose enough identity to inspect their resource types.
	if uninspectableEntitlementReference(entitlementState) ||
		uninspectablePrincipalReference(principalState) {
		return DataPolicyFail
	}
	if entitlementState == ReferenceTypeUnknown || principalState == ReferenceTypeUnknown {
		return DataPolicySkipReport
	}
	if structurallyInvalidEntitlementReference(entitlementState) ||
		structurallyInvalidPrincipalReference(principalState) {
		return DataPolicyFail
	}
	if entitlementState == ReferenceRowMissing || principalState == ReferenceRowMissing {
		return DataPolicyWarnRetain
	}
	if entitlementState == ReferenceValid && principalState == ReferenceValid {
		return DataPolicyAccept
	}
	return DataPolicyUnresolved
}

func uninspectableEntitlementReference(state ReferenceShape) bool {
	switch state {
	case ReferenceCarrierNil, ReferenceResourceNil, ReferenceIdentityNil:
		return true
	default:
		return false
	}
}

func uninspectablePrincipalReference(state ReferenceShape) bool {
	switch state {
	case ReferenceCarrierNil, ReferenceIdentityNil:
		return true
	default:
		return false
	}
}

func structurallyInvalidEntitlementReference(state ReferenceShape) bool {
	switch state {
	case ReferenceCarrierNil, ReferenceExternalIDEmpty, ReferenceResourceNil,
		ReferenceIdentityNil, ReferenceTypeEmpty, ReferenceObjectIDEmpty:
		return true
	default:
		return false
	}
}

func structurallyInvalidPrincipalReference(state ReferenceShape) bool {
	switch state {
	case ReferenceCarrierNil, ReferenceIdentityNil, ReferenceTypeEmpty, ReferenceObjectIDEmpty:
		return true
	default:
		return false
	}
}

func baselineResource(dataset *Dataset) *v2.Resource {
	return dataset.Resources[FullCapabilityResourceTypeID][""].List[0]
}

func entitlementForShape(state ReferenceShape, id string, validResource *v2.Resource) *v2.Entitlement {
	switch state {
	case ReferenceCarrierNil:
		return nil
	case ReferenceExternalIDEmpty:
		return v2.Entitlement_builder{Resource: validResource}.Build()
	case ReferenceResourceNil:
		return v2.Entitlement_builder{Id: id}.Build()
	default:
		return v2.Entitlement_builder{
			Id:       id,
			Resource: resourceForShape(state, "entitlement-resource"),
		}.Build()
	}
}

func resourceForShape(state ReferenceShape, id string) *v2.Resource {
	switch state {
	case ReferenceCarrierNil:
		return nil
	case ReferenceIdentityNil:
		return v2.Resource_builder{DisplayName: id}.Build()
	case ReferenceTypeEmpty:
		return v2.Resource_builder{
			Id:          v2.ResourceId_builder{Resource: id}.Build(),
			DisplayName: id,
		}.Build()
	case ReferenceObjectIDEmpty:
		return v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: FullCapabilityResourceTypeID}.Build(),
			DisplayName: id,
		}.Build()
	case ReferenceTypeUnknown:
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "chaos-unknown-type",
				Resource:     id,
			}.Build(),
			DisplayName: id,
		}.Build()
	case ReferenceRowMissing:
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: FullCapabilityResourceTypeID,
				Resource:     "chaos-missing-row-" + id,
			}.Build(),
			DisplayName: id,
		}.Build()
	case ReferenceValid:
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: FullCapabilityResourceTypeID,
				Resource:     "user-1",
			}.Build(),
			DisplayName: "Chaos User 1",
		}.Build()
	default:
		panic("chaosconnector: unsupported reference shape " + state)
	}
}
