package oracle

import (
	"context"
	"fmt"
	"slices"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

type ExternalPrincipalExpectation struct {
	PrincipalIDs             []string
	ExpandableEntitlementIDs []string
	RequireSealed            bool
}

type ExternalPrincipalObservation struct {
	PrincipalIDs             []string
	ExpandableEntitlementIDs []string
	CarrierCount             int
	Sealed                   bool
}

type ExternalPrincipalStoreReader interface {
	LifecycleStoreReader
	Grants() c1zstore.GrantStore
}

// ReadExternalPrincipal observes the rewritten grant relation exhaustively.
// It retains multiplicity so duplicate expansions cannot pass as set equality.
func ReadExternalPrincipal(
	ctx context.Context,
	reader ExternalPrincipalStoreReader,
	entitlementID string,
	carrierTypeID string,
) (ExternalPrincipalObservation, error) {
	latest, err := reader.SyncMeta().LatestFullSync(ctx)
	if err != nil {
		return ExternalPrincipalObservation{}, fmt.Errorf("chaos oracle: latest full sync: %w", err)
	}
	out := ExternalPrincipalObservation{Sealed: latest != nil}
	for annotated, listErr := range reader.Grants().ListWithAnnotations(ctx) {
		if listErr != nil {
			return ExternalPrincipalObservation{}, fmt.Errorf(
				"chaos oracle: list external-principal grants: %w",
				listErr,
			)
		}
		grant := annotated.Grant
		if grant.GetEntitlement().GetId() != entitlementID {
			continue
		}
		principal := grant.GetPrincipal().GetId()
		if principal.GetResourceType() == carrierTypeID {
			out.CarrierCount++
			continue
		}
		out.PrincipalIDs = append(out.PrincipalIDs, principal.GetResource())
	}
	for pending, listErr := range reader.Grants().PendingExpansion(ctx) {
		if listErr != nil {
			return ExternalPrincipalObservation{}, fmt.Errorf(
				"chaos oracle: list pending external-principal expansions: %w",
				listErr,
			)
		}
		if pending.TargetEntitlementID == entitlementID && pending.Annotation != nil {
			out.ExpandableEntitlementIDs = append(
				out.ExpandableEntitlementIDs,
				pending.Annotation.GetEntitlementIds()...,
			)
		}
	}
	slices.Sort(out.PrincipalIDs)
	slices.Sort(out.ExpandableEntitlementIDs)
	return out, nil
}

func CompareExternalPrincipal(
	expected ExternalPrincipalExpectation,
	actual ExternalPrincipalObservation,
) error {
	principalIDs := append([]string(nil), expected.PrincipalIDs...)
	slices.Sort(principalIDs)
	actualPrincipalIDs := append([]string(nil), actual.PrincipalIDs...)
	slices.Sort(actualPrincipalIDs)
	if !slices.Equal(principalIDs, actualPrincipalIDs) {
		return fmt.Errorf(
			"chaos oracle: external principal mismatch: expected %v, actual %v",
			principalIDs,
			actualPrincipalIDs,
		)
	}
	if actual.CarrierCount != 0 {
		return fmt.Errorf("chaos oracle: %d unresolved external-principal carriers survived", actual.CarrierCount)
	}
	expectedExpandableIDs := append([]string(nil), expected.ExpandableEntitlementIDs...)
	actualExpandableIDs := append([]string(nil), actual.ExpandableEntitlementIDs...)
	slices.Sort(expectedExpandableIDs)
	slices.Sort(actualExpandableIDs)
	if !slices.Equal(expectedExpandableIDs, actualExpandableIDs) {
		return fmt.Errorf(
			"chaos oracle: expandable entitlement mismatch: expected %v, actual %v",
			expectedExpandableIDs,
			actualExpandableIDs,
		)
	}
	if expected.RequireSealed != actual.Sealed {
		return fmt.Errorf(
			"chaos oracle: sealed mismatch: expected %t, actual %t",
			expected.RequireSealed,
			actual.Sealed,
		)
	}
	return nil
}
