package resource

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/types/known/structpb"
)

type RoleTraitOption func(gt *v2.RoleTrait) error

func WithRoleProfile(profile map[string]interface{}) RoleTraitOption {
	return func(rt *v2.RoleTrait) error {
		p, err := structpb.NewStruct(profile)
		if err != nil {
			return err
		}

		rt.Profile = p

		return nil
	}
}

// NewRoleTrait creates a new `RoleTrait` with the provided profile.
func NewRoleTrait(opts ...RoleTraitOption) (*v2.RoleTrait, error) {
	groupTrait := &v2.RoleTrait{}

	for _, opt := range opts {
		err := opt(groupTrait)
		if err != nil {
			return nil, err
		}
	}

	return groupTrait, nil
}

// GetRoleTrait attempts to return the RoleTrait instance on a resource.
func GetRoleTrait(resource *v2.Resource) (*v2.RoleTrait, error) {
	ret := &v2.RoleTrait{}
	annos := annotations.Annotations(resource.Annotations)
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("group trait was not found on resource")
	}

	return ret, nil
}
