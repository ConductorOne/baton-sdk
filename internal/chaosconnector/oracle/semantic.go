package oracle

import (
	"context"
	"errors"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// SemanticEntity is oracle-owned so corpus and oracle cannot confirm each
// other's entity switching by sharing one enum implementation.
type SemanticEntity string

const (
	SemanticResource    SemanticEntity = "resource"
	SemanticEntitlement SemanticEntity = "entitlement"
	SemanticGrant       SemanticEntity = "grant"
)

// SemanticTarget selects one canonical identity from the store.
type SemanticTarget struct {
	Entity            SemanticEntity
	CanonicalIdentity string
}

// SemanticExpectation uses pointers to distinguish "assert empty" from
// "field is outside this oracle".
type SemanticExpectation struct {
	Multiplicity   int
	DisplayName    *string
	ExternalID     *string
	ParentIdentity *string
}

// SemanticObservation is the independently read projection of one canonical
// identity after a semantic adversary.
type SemanticObservation struct {
	Multiplicity   int
	DisplayName    string
	ExternalID     string
	ParentIdentity string
}

// CompareSemantic verifies multiplicity and the content fields selected by
// the expectation. It returns errors so planted-violation tests can calibrate
// the oracle without relying on a test assertion helper.
func CompareSemantic(
	expected SemanticExpectation,
	actual SemanticObservation,
) error {
	var errs []error
	if actual.Multiplicity != expected.Multiplicity {
		errs = append(errs, fmt.Errorf(
			"semantic multiplicity mismatch: expected %d, actual %d",
			expected.Multiplicity,
			actual.Multiplicity,
		))
	}
	if expected.DisplayName != nil && actual.DisplayName != *expected.DisplayName {
		errs = append(errs, fmt.Errorf(
			"semantic display name mismatch: expected %q, actual %q",
			*expected.DisplayName,
			actual.DisplayName,
		))
	}
	if expected.ExternalID != nil && actual.ExternalID != *expected.ExternalID {
		errs = append(errs, fmt.Errorf(
			"semantic external id mismatch: expected %q, actual %q",
			*expected.ExternalID,
			actual.ExternalID,
		))
	}
	if expected.ParentIdentity != nil && actual.ParentIdentity != *expected.ParentIdentity {
		errs = append(errs, fmt.Errorf(
			"semantic parent mismatch: expected %q, actual %q",
			*expected.ParentIdentity,
			actual.ParentIdentity,
		))
	}
	return errors.Join(errs...)
}

// ReadSemantic exhaustively pages the public store surface and projects every
// row matching target. Unknown entities fail closed.
func ReadSemantic(
	ctx context.Context,
	reader StoreReader,
	target SemanticTarget,
) (SemanticObservation, error) {
	var out SemanticObservation
	switch target.Entity {
	case SemanticResource:
		err := pageRows(
			func(token string) ([]*v2.Resource, string, error) {
				response, listErr := reader.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
					PageToken: token,
				}.Build())
				if listErr != nil {
					return nil, "", listErr
				}
				return response.GetList(), response.GetNextPageToken(), nil
			},
			func(item *v2.Resource) {
				if resourceKey(item.GetId()) != target.CanonicalIdentity {
					return
				}
				out.Multiplicity++
				out.DisplayName = item.GetDisplayName()
				out.ParentIdentity = resourceKey(item.GetParentResourceId())
			},
		)
		return out, err
	case SemanticEntitlement:
		err := pageRows(
			func(token string) ([]*v2.Entitlement, string, error) {
				response, listErr := reader.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
					PageToken: token,
				}.Build())
				if listErr != nil {
					return nil, "", listErr
				}
				return response.GetList(), response.GetNextPageToken(), nil
			},
			func(item *v2.Entitlement) {
				if item.GetId() != target.CanonicalIdentity {
					return
				}
				out.Multiplicity++
				out.DisplayName = item.GetDisplayName()
			},
		)
		return out, err
	case SemanticGrant:
		err := pageRows(
			func(token string) ([]*v2.Grant, string, error) {
				response, listErr := reader.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
					PageToken: token,
				}.Build())
				if listErr != nil {
					return nil, "", listErr
				}
				return response.GetList(), response.GetNextPageToken(), nil
			},
			func(item *v2.Grant) {
				if grantKey(item) != target.CanonicalIdentity {
					return
				}
				out.Multiplicity++
				out.ExternalID = item.GetId()
			},
		)
		return out, err
	default:
		return out, fmt.Errorf("chaos oracle: unknown semantic entity %q", target.Entity)
	}
}

// LifecycleExpectation declares the durable result of one attempt.
type LifecycleExpectation struct {
	Sealed      bool
	Present     bool
	DisplayName *string
	Dropped     int64
}

// LifecycleObservation is measured independently from the c1z and syncer
// counters.
type LifecycleObservation struct {
	Sealed      bool
	Present     bool
	DisplayName string
	Dropped     int64
}

type LifecycleStoreReader interface {
	StoreReader
	SyncMeta() c1zstore.SyncMeta
}

// ReadLifecycle observes sealing and one entitlement identity through public
// store interfaces. Runtime-only evidence such as drop counters is supplied
// separately by the caller.
func ReadLifecycle(
	ctx context.Context,
	reader LifecycleStoreReader,
	entitlementID string,
) (LifecycleObservation, error) {
	latest, err := reader.SyncMeta().LatestFullSync(ctx)
	if err != nil {
		return LifecycleObservation{}, fmt.Errorf("chaos oracle: latest full sync: %w", err)
	}
	out := LifecycleObservation{Sealed: latest != nil}
	if entitlementID == "" {
		return out, nil
	}
	semantic, err := ReadSemantic(ctx, reader, SemanticTarget{
		Entity:            SemanticEntitlement,
		CanonicalIdentity: entitlementID,
	})
	if err != nil {
		return LifecycleObservation{}, err
	}
	out.Present = semantic.Multiplicity > 0
	out.DisplayName = semantic.DisplayName
	return out, nil
}

// CompareLifecycle calibrates sealing, retention, content, and evidence as
// separate claims so one correct field cannot mask another violation.
func CompareLifecycle(expected LifecycleExpectation, actual LifecycleObservation) error {
	var errs []error
	if actual.Sealed != expected.Sealed {
		errs = append(errs, fmt.Errorf(
			"lifecycle sealing mismatch: expected %t, actual %t",
			expected.Sealed,
			actual.Sealed,
		))
	}
	if actual.Present != expected.Present {
		errs = append(errs, fmt.Errorf(
			"lifecycle presence mismatch: expected %t, actual %t",
			expected.Present,
			actual.Present,
		))
	}
	if expected.DisplayName != nil && actual.DisplayName != *expected.DisplayName {
		errs = append(errs, fmt.Errorf(
			"lifecycle display name mismatch: expected %q, actual %q",
			*expected.DisplayName,
			actual.DisplayName,
		))
	}
	if actual.Dropped != expected.Dropped {
		errs = append(errs, fmt.Errorf(
			"lifecycle drop count mismatch: expected %d, actual %d",
			expected.Dropped,
			actual.Dropped,
		))
	}
	return errors.Join(errs...)
}
