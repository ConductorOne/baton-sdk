package resource

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ScopeBindingTraitOption func(rs *v2.ScopeBindingTrait) error

// WithRoleScopeRoleId sets the role of role scope.
func WithRoleScopeRoleId(resourceId *v2.ResourceId) ScopeBindingTraitOption {
	return func(rs *v2.ScopeBindingTrait) error {
		rs.RoleId = resourceId
		return nil
	}
}

// WithRoleScopeResourceId sets the resource scope of role scope.
func WithRoleScopeResourceId(resourceId *v2.ResourceId) ScopeBindingTraitOption {
	return func(rs *v2.ScopeBindingTrait) error {
		rs.ScopeResourceId = resourceId
		return nil
	}
}

// GetScopeBindingTrait attempts to return the ScopeBindingTrait instance on a resource.
func GetScopeBindingTrait(resource *v2.Resource) (*v2.ScopeBindingTrait, error) {
	ret := &v2.ScopeBindingTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "scope binding trait was not found on resource")
	}

	return ret, nil
}
