package sdk

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/types/known/structpb"
)

// NewUserTrait creates a new `UserTrait` with the given primary email, status, asset reference, and profile.
func NewUserTrait(primaryEmail string, status v2.UserTrait_Status_Status, assetRef *v2.AssetRef, profile map[string]interface{}) (*v2.UserTrait, error) {
	emails := make([]*v2.UserTrait_Email, 0)
	if primaryEmail != "" {
		emails = append(emails, &v2.UserTrait_Email{
			Address:   primaryEmail,
			IsPrimary: true,
		})
	}

	p, err := structpb.NewStruct(profile)
	if err != nil {
		return nil, err
	}

	ut := &v2.UserTrait{
		Emails:  emails,
		Status:  &v2.UserTrait_Status{Status: status},
		Profile: p,
		Icon:    assetRef,
	}

	return ut, nil
}

// NewGroupTrait creates a new `GroupTrait` with the fiven icon, and profile.
func NewGroupTrait(assetRef *v2.AssetRef, profile map[string]interface{}) (*v2.GroupTrait, error) {
	p, err := structpb.NewStruct(profile)
	if err != nil {
		return nil, err
	}

	ut := &v2.GroupTrait{
		Profile: p,
		Icon:    assetRef,
	}

	return ut, nil
}

// NewAppTrait creates a new `AppTrait` with the given icon, logo, help URL, and profile.
func NewAppTrait(icon *v2.AssetRef, logo *v2.AssetRef, helpURL string, profile map[string]interface{}) (*v2.AppTrait, error) {
	p, err := structpb.NewStruct(profile)
	if err != nil {
		return nil, err
	}

	ut := &v2.AppTrait{
		Profile: p,
		Icon:    icon,
		Logo:    logo,
		HelpUrl: helpURL,
	}

	return ut, nil
}
