package resource

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/types/known/structpb"
)

type GroupSourceType string

const (
	GroupSourceTypeNative          GroupSourceType = "native"
	GroupSourceTypeAppImported     GroupSourceType = "app_imported"
	GroupSourceTypeBuiltIn         GroupSourceType = "built_in"
	GroupSourceTypeDirectorySynced GroupSourceType = "directory_synced"
	GroupSourceTypeDynamic         GroupSourceType = "dynamic"
	GroupSourceTypeDistribution    GroupSourceType = "distribution"
)

type GroupTraitOption func(gt *v2.GroupTrait) error

func WithGroupProfile(profile map[string]interface{}) GroupTraitOption {
	return func(gt *v2.GroupTrait) error {
		p, err := structpb.NewStruct(profile)
		if err != nil {
			return err
		}

		gt.SetProfile(p)

		return nil
	}
}

func WithGroupIcon(assetRef *v2.AssetRef) GroupTraitOption {
	return func(gt *v2.GroupTrait) error {
		gt.SetIcon(assetRef)
		return nil
	}
}

func WithGroupSourceType(sourceType GroupSourceType) GroupTraitOption {
	return func(gt *v2.GroupTrait) error {
		gt.SetGroupSourceType(string(sourceType))
		return nil
	}
}

func WithRawGroupSourceType(raw string) GroupTraitOption {
	return func(gt *v2.GroupTrait) error {
		gt.SetRawGroupSourceType(raw)
		return nil
	}
}

// NewGroupTrait creates a new `GroupTrait` with the provided profile.
func NewGroupTrait(opts ...GroupTraitOption) (*v2.GroupTrait, error) {
	groupTrait := &v2.GroupTrait{}

	for _, opt := range opts {
		err := opt(groupTrait)
		if err != nil {
			return nil, err
		}
	}

	return groupTrait, nil
}

// GetGroupTrait attempts to return the GroupTrait instance on a resource.
func GetGroupTrait(resource *v2.Resource) (*v2.GroupTrait, error) {
	ret := &v2.GroupTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("group trait was not found on resource")
	}

	return ret, nil
}
