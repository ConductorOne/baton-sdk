// Package oracle contains independent auditors for chaos connector runs.
package oracle

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// StoreReader is the raw read surface needed by the identity oracle.
type StoreReader interface {
	ListResourceTypes(context.Context, *v2.ResourceTypesServiceListResourceTypesRequest) (*v2.ResourceTypesServiceListResourceTypesResponse, error)
	ListResources(context.Context, *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error)
	ListEntitlements(context.Context, *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error)
	ListGrants(context.Context, *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error)
}

// IdentitySnapshot is the canonical identity and relationship projection of a
// sealed store. Content/provenance auditors remain separate so identity
// equality is never misrepresented as full proto equality.
type IdentitySnapshot struct {
	ResourceTypes []string
	Resources     []string
	Entitlements  []string
	Grants        []string
}

// ExpectedIdentities derives the identity oracle directly from a manifest.
func ExpectedIdentities(manifest *chaosconnector.Manifest) IdentitySnapshot {
	var out IdentitySnapshot
	for _, resourceType := range manifest.ResourceTypes {
		if resourceType != nil {
			out.ResourceTypes = append(out.ResourceTypes, resourceType.GetId())
		}
	}
	for _, item := range manifest.Resources {
		if item != nil {
			out.Resources = append(out.Resources, resourceKey(item.GetId()))
		}
	}
	for _, item := range append(
		append([]*v2.Entitlement(nil), manifest.StaticEntitlements...),
		manifest.Entitlements...,
	) {
		if item != nil {
			out.Entitlements = append(out.Entitlements, item.GetId())
		}
	}
	for _, item := range manifest.Grants {
		if item != nil {
			out.Grants = append(out.Grants, grantKey(item))
		}
	}
	out.sort()
	return out
}

// ReadIdentities exhaustively pages the store and checks token progress.
func ReadIdentities(ctx context.Context, reader StoreReader) (IdentitySnapshot, error) {
	var out IdentitySnapshot

	resourceTypes, err := reader.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{}.Build())
	if err != nil {
		return out, fmt.Errorf("chaos oracle: list resource types: %w", err)
	}
	for _, resourceType := range resourceTypes.GetList() {
		out.ResourceTypes = append(out.ResourceTypes, resourceType.GetId())
	}

	if err := pageRows(
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
			out.Resources = append(out.Resources, resourceKey(item.GetId()))
		},
	); err != nil {
		return out, fmt.Errorf("chaos oracle: list resources: %w", err)
	}

	if err := pageRows(
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
			out.Entitlements = append(out.Entitlements, item.GetId())
		},
	); err != nil {
		return out, fmt.Errorf("chaos oracle: list entitlements: %w", err)
	}

	if err := pageRows(
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
			out.Grants = append(out.Grants, grantKey(item))
		},
	); err != nil {
		return out, fmt.Errorf("chaos oracle: list grants: %w", err)
	}

	out.sort()
	return out, nil
}

func pageRows[T any](
	list func(string) ([]T, string, error),
	consume func(T),
) error {
	token := ""
	seen := make(map[string]struct{})
	for {
		rows, next, err := list(token)
		if err != nil {
			return err
		}
		for _, row := range rows {
			consume(row)
		}
		if next == "" {
			return nil
		}
		if _, duplicate := seen[next]; duplicate || next == token {
			return fmt.Errorf("pagination made no progress at token %q", next)
		}
		seen[next] = struct{}{}
		token = next
	}
}

// CompareIdentities asserts exact set-and-multiplicity equality. Sorting rather
// than mapping preserves duplicate detection.
func CompareIdentities(expected, actual IdentitySnapshot) error {
	return errors.Join(
		compareSlice("resource types", expected.ResourceTypes, actual.ResourceTypes),
		compareSlice("resources", expected.Resources, actual.Resources),
		compareSlice("entitlements", expected.Entitlements, actual.Entitlements),
		compareSlice("grants", expected.Grants, actual.Grants),
	)
}

func compareSlice(name string, expected, actual []string) error {
	if slices.Equal(expected, actual) {
		return nil
	}
	return fmt.Errorf("chaos oracle: %s mismatch: expected %v, actual %v", name, expected, actual)
}

func (s *IdentitySnapshot) sort() {
	slices.Sort(s.ResourceTypes)
	slices.Sort(s.Resources)
	slices.Sort(s.Entitlements)
	slices.Sort(s.Grants)
}

func resourceKey(id *v2.ResourceId) string {
	if id == nil {
		return "<nil>"
	}
	return id.GetResourceType() + "\x00" + id.GetResource()
}

func grantKey(grant *v2.Grant) string {
	if grant == nil {
		return "<nil>"
	}
	return grant.GetEntitlement().GetId() + "\x00" + resourceKey(grant.GetPrincipal().GetId())
}
