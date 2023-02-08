package resource

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/types/known/structpb"
)

type UserTraitOption func(ut *v2.UserTrait) error

func WithStatus(status v2.UserTrait_Status_Status) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.Status = &v2.UserTrait_Status{Status: status}

		return nil
	}
}

func WithEmail(email string, primary bool) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		if email == "" {
			return nil
		}

		traitEmail := &v2.UserTrait_Email{
			Address:   email,
			IsPrimary: primary,
		}

		ut.Emails = append(ut.Emails, traitEmail)

		return nil
	}
}

func WithUserIcon(assetRef *v2.AssetRef) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.Icon = assetRef

		return nil
	}
}

func WithUserProfile(profile map[string]interface{}) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		p, err := structpb.NewStruct(profile)
		if err != nil {
			return err
		}

		ut.Profile = p

		return nil
	}
}

func WithAccountType(accountType v2.UserTrait_AccountType) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.AccountType = accountType
		return nil
	}
}

// NewUserTrait creates a new `UserTrait`.
func NewUserTrait(opts ...UserTraitOption) (*v2.UserTrait, error) {
	userTrait := &v2.UserTrait{}

	for _, opt := range opts {
		err := opt(userTrait)
		if err != nil {
			return nil, err
		}
	}

	// If no status was set, default to be enabled.
	if userTrait.Status == nil {
		userTrait.Status = &v2.UserTrait_Status{Status: v2.UserTrait_Status_STATUS_ENABLED}
	}

	// If account type isn't specified, default to a human user.
	if userTrait.AccountType == v2.UserTrait_ACCOUNT_TYPE_UNSPECIFIED {
		userTrait.AccountType = v2.UserTrait_ACCOUNT_TYPE_HUMAN
	}

	return userTrait, nil
}

// GetUserTrait attempts to return the UserTrait instance on a resource.
func GetUserTrait(resource *v2.Resource) (*v2.UserTrait, error) {
	ret := &v2.UserTrait{}
	annos := annotations.Annotations(resource.Annotations)
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("user trait was not found on resource")
	}

	return ret, nil
}
