package sdk

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/types/known/structpb"
)

// NewUserTrait creates a new `UserTrait` with the given primary email, status, and profile.
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

// NewGroupTrait creates a new `GroupTrait` with the provided profile.
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

// GetGroupTrait attempts to return the GroupTrait instance on a resource.
func GetGroupTrait(resource *v2.Resource) (*v2.GroupTrait, error) {
	ret := &v2.GroupTrait{}
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

// NewAppTrait creates a new `AppTrait` with the given help URL, and profile.
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

// GetRoleTrait attempts to return the RoleTrait instance on a resource.
func GetRoleTrait(resource *v2.Resource) (*v2.RoleTrait, error) {
	ret := &v2.RoleTrait{}
	annos := annotations.Annotations(resource.Annotations)
	ok, err := annos.Pick(ret)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("role trait was not found on resource")
	}

	return ret, nil
}

// GetProfileStringValue returns a string and true if the value is found.
func GetProfileStringValue(profile *structpb.Struct, k string) (string, bool) {
	if profile == nil {
		return "", false
	}

	v, ok := profile.Fields[k]
	if !ok {
		return "", false
	}

	s, ok := v.Kind.(*structpb.Value_StringValue)
	if !ok {
		return "", false
	}

	return s.StringValue, true
}

// GetProfileInt64Value returns an int64 and true if the value is found.
func GetProfileInt64Value(profile *structpb.Struct, k string) (int64, bool) {
	if profile == nil {
		return 0, false
	}

	v, ok := profile.Fields[k]
	if !ok {
		return 0, false
	}

	s, ok := v.Kind.(*structpb.Value_NumberValue)
	if !ok {
		return 0, false
	}

	return int64(s.NumberValue), true
}
