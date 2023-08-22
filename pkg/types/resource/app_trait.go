package resource

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/types/known/structpb"
)

type AppTraitOption func(gt *v2.AppTrait) error

func WithAppIcon(assetRef *v2.AssetRef) AppTraitOption {
	return func(at *v2.AppTrait) error {
		at.Icon = assetRef

		return nil
	}
}

func WithAppLogo(assetRef *v2.AssetRef) AppTraitOption {
	return func(at *v2.AppTrait) error {
		at.Logo = assetRef

		return nil
	}
}

func WithAppFlags(flags ...v2.AppTrait_AppFlag) AppTraitOption {
	return func(at *v2.AppTrait) error {
		at.Flags = flags
		return nil
	}
}

func WithAppProfile(profile map[string]interface{}) AppTraitOption {
	return func(at *v2.AppTrait) error {
		p, err := structpb.NewStruct(profile)
		if err != nil {
			return err
		}

		at.Profile = p

		return nil
	}
}

func WithAppHelpURL(helpURL string) AppTraitOption {
	return func(at *v2.AppTrait) error {
		at.HelpUrl = helpURL
		return nil
	}
}

// NewAppTrait creates a new `AppTrait` with the given help URL, and profile.
func NewAppTrait(opts ...AppTraitOption) (*v2.AppTrait, error) {
	at := &v2.AppTrait{}

	for _, opt := range opts {
		err := opt(at)
		if err != nil {
			return nil, err
		}
	}

	return at, nil
}

// GetAppTrait attempts to return the AppTrait instance on a resource.
func GetAppTrait(resource *v2.Resource) (*v2.AppTrait, error) {
	ret := &v2.AppTrait{}
	annos := annotations.Annotations(resource.Annotations)
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("app trait was not found on resource")
	}

	return ret, nil
}
