package resource

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

type RoleScopeTraitOption func(rs *v2.RoleScopeTrait) error

// WithRoleScopeRoleId sets the role of role scope.
func WithRoleScopeRoleId(resourceId *v2.ResourceId) RoleScopeTraitOption {
	return func(rs *v2.RoleScopeTrait) error {
		rs.RoleId = resourceId
		return nil
	}
}

// WithRoleScopeResourceId sets the resource scope of role scope.
func WithRoleScopeResourceId(resourceId *v2.ResourceId) RoleScopeTraitOption {
	return func(rs *v2.RoleScopeTrait) error {
		rs.ScopeResourceId = resourceId
		return nil
	}
}

// GetRoleScopeTrait attempts to return the RoleScopeTrait instance on a resource.
func GetRoleScopeTrait(resource *v2.Resource) (*v2.RoleScopeTrait, error) {
	ret := &v2.RoleScopeTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("role scope trait was not found on resource")
	}

	return ret, nil
}
