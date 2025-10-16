package resource

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

type UserTraitOption func(ut *v2.UserTrait) error

func WithStatus(status v2.UserTrait_Status_Status) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.SetStatus(v2.UserTrait_Status_builder{Status: status}.Build())

		return nil
	}
}

func WithDetailedStatus(status v2.UserTrait_Status_Status, details string) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.SetStatus(v2.UserTrait_Status_builder{Status: status, Details: details}.Build())

		return nil
	}
}

func WithEmail(email string, primary bool) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		if email == "" {
			return nil
		}

		traitEmail := v2.UserTrait_Email_builder{
			Address:   email,
			IsPrimary: primary,
		}.Build()

		ut.SetEmails(append(ut.GetEmails(), traitEmail))

		return nil
	}
}

func WithUserLogin(login string, aliases ...string) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		if login == "" {
			// If login is empty do nothing
			return nil
		}
		ut.SetLogin(login)
		ut.SetLoginAliases(aliases)
		return nil
	}
}

func WithEmployeeID(employeeIDs ...string) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.SetEmployeeIds(employeeIDs)
		return nil
	}
}

func WithUserIcon(assetRef *v2.AssetRef) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.SetIcon(assetRef)

		return nil
	}
}

func WithUserProfile(profile map[string]interface{}) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		p, err := structpb.NewStruct(profile)
		if err != nil {
			return err
		}

		ut.SetProfile(p)

		return nil
	}
}

func WithAccountType(accountType v2.UserTrait_AccountType) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.SetAccountType(accountType)
		return nil
	}
}

func WithCreatedAt(createdAt time.Time) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.SetCreatedAt(timestamppb.New(createdAt))
		return nil
	}
}

func WithLastLogin(lastLogin time.Time) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.SetLastLogin(timestamppb.New(lastLogin))
		return nil
	}
}

func WithMFAStatus(mfaStatus *v2.UserTrait_MFAStatus) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.SetMfaStatus(mfaStatus)
		return nil
	}
}

func WithSSOStatus(ssoStatus *v2.UserTrait_SSOStatus) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.SetSsoStatus(ssoStatus)
		return nil
	}
}

func WithStructuredName(structuredName *v2.UserTrait_StructuredName) UserTraitOption {
	return func(ut *v2.UserTrait) error {
		ut.SetStructuredName(structuredName)
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
	if !userTrait.HasStatus() {
		userTrait.SetStatus(v2.UserTrait_Status_builder{Status: v2.UserTrait_Status_STATUS_ENABLED}.Build())
	}

	// If account type isn't specified, default to a human user.
	if userTrait.GetAccountType() == v2.UserTrait_ACCOUNT_TYPE_UNSPECIFIED {
		userTrait.SetAccountType(v2.UserTrait_ACCOUNT_TYPE_HUMAN)
	}

	return userTrait, nil
}

// GetUserTrait attempts to return the UserTrait instance on a resource.
func GetUserTrait(resource *v2.Resource) (*v2.UserTrait, error) {
	ret := &v2.UserTrait{}
	annos := annotations.Annotations(resource.GetAnnotations())
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("user trait was not found on resource")
	}

	return ret, nil
}
