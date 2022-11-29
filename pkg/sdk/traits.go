package sdk

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/types/known/structpb"
)

// NewUserTrait creates a new `UserTrait` with the given primary email, status, asset reference, and profile.
func NewUserTrait(primaryEmail string, status v2.UserTrait_Status_Status, profile map[string]interface{}) (*v2.UserTrait, error) {
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

	userTrait := &v2.UserTrait{
		Emails:  emails,
		Status:  &v2.UserTrait_Status{Status: status},
		Profile: p,
	}

	return userTrait, nil
}

// NewGroupTrait creates a new `GroupTrait` with the fiven icon, and profile.
func NewGroupTrait(profile map[string]interface{}) (*v2.GroupTrait, error) {
	p, err := structpb.NewStruct(profile)
	if err != nil {
		return nil, err
	}

	groupTrait := &v2.GroupTrait{
		Profile: p,
	}

	return groupTrait, nil
}

// NewAppTrait creates a new `AppTrait` with the given icon, logo, help URL, and profile.
func NewAppTrait(helpURL string, profile map[string]interface{}) (*v2.AppTrait, error) {
	p, err := structpb.NewStruct(profile)
	if err != nil {
		return nil, err
	}

	ut := &v2.AppTrait{
		Profile: p,
		HelpUrl: helpURL,
	}

	return ut, nil
}

// NewRoleTrait creates a new `RoleTrait` with the given profile.
func NewRoleTrait(profile map[string]interface{}) (*v2.RoleTrait, error) {
	p, err := structpb.NewStruct(profile)
	if err != nil {
		return nil, err
	}

	rt := &v2.RoleTrait{
		Profile: p,
	}

	return rt, nil
}
