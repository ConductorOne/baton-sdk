package test

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

// exhaustPagination - some integration tests don't care about how pagination
// works and just want to get the full list of resources.
func exhaustPagination[T any, R any](
	ctx context.Context,
	resource *R,
	f func(
		ctx context.Context,
		resource *R,
		pToken *pagination.Token,
	) (
		[]*T,
		string,
		annotations.Annotations,
		error,
	),
) (
	[]*T,
	annotations.Annotations,
	error,
) {
	var outputAnnotations annotations.Annotations
	pToken := pagination.Token{}
	objects := make([]*T, 0)
	for {
		nextObjects, nextToken, listAnnotations, err := f(ctx, resource, &pToken)
		if err != nil {
			return nil, nil, err
		}
		objects = append(objects, nextObjects...)

		if IsRatelimited(listAnnotations) {
			return nil,
				listAnnotations,
				fmt.Errorf("request was ratelimited, expected not to be ratelimited")
		}
		if nextToken == "" {
			// Just return the final set of annotations.
			outputAnnotations = listAnnotations
			break
		}
		pToken.Token = nextToken
	}
	return objects, outputAnnotations, nil
}

func ExhaustResourcePagination(
	ctx context.Context,
	c connectorbuilder.ResourceSyncer,
) (
	[]*v2.Resource,
	annotations.Annotations,
	error,
) {
	return exhaustPagination[v2.Resource, v2.ResourceId](ctx, nil, c.List)
}

func ExhaustEntitlementPagination(
	ctx context.Context,
	c connectorbuilder.ResourceSyncer,
	resource *v2.Resource,
) (
	[]*v2.Entitlement,
	annotations.Annotations,
	error,
) {
	return exhaustPagination[v2.Entitlement, v2.Resource](ctx, resource, c.Entitlements)
}

func ExhaustGrantPagination(
	ctx context.Context,
	c connectorbuilder.ResourceSyncer,
	resource *v2.Resource,
) (
	[]*v2.Grant,
	annotations.Annotations,
	error,
) {
	return exhaustPagination[v2.Grant, v2.Resource](ctx, resource, c.Grants)
}
