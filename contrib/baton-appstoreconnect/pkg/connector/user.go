package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-appstoreconnect/pkg/appstoreconnect"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type userResourceType struct {
	resourceType *v2.ResourceType
	client       *appstoreconnect.Client
}

func (u *userResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return u.resourceType
}

// userResource builds a resource for an accepted team member.
func userResource(user *appstoreconnect.User) (*v2.Resource, error) {
	profile := map[string]interface{}{
		profileKeyLogin:               user.Attributes.Username,
		profileKeyUserID:              user.ID,
		profileKeyAllAppsVisible:      user.Attributes.AllAppsVisible,
		profileKeyProvisioningAllowed: user.Attributes.ProvisioningAllowed,
	}
	if len(user.Attributes.Roles) > 0 {
		profile[profileKeyRoles] = toAnySlice(user.Attributes.Roles)
	}

	traitOptions := []rs.UserTraitOption{
		rs.WithEmail(user.Attributes.Username, true),
		rs.WithUserLogin(user.Attributes.Username),
		rs.WithAccountType(v2.UserTrait_ACCOUNT_TYPE_HUMAN),
	}

	if firstName := user.Attributes.FirstName; firstName != "" || user.Attributes.LastName != "" {
		traitOptions = append(traitOptions, rs.WithStructuredName(v2.UserTrait_StructuredName_builder{
			GivenName:  firstName,
			FamilyName: user.Attributes.LastName,
		}.Build()))
	}

	return rs.NewUserResource(
		user.DisplayName(),
		resourceTypeUser,
		user.ID,
		traitOptions,
		rs.WithResourceProfile(profile),
		rs.WithResourceStatus(v2.Status_RESOURCE_STATUS_ENABLED, ""),
	)
}

// invitationResource builds a pending user resource for an outstanding invitation. An invitee has
// no Apple ID on the team yet, so the resource carries the invitation id rather than a user id;
// once the invitation is accepted the invitation disappears and a distinct user record takes its
// place, correlated by email address.
func invitationResource(invitation *appstoreconnect.UserInvitation) (*v2.Resource, error) {
	profile := map[string]interface{}{
		profileKeyLogin:               invitation.Attributes.Email,
		profileKeyUserID:              invitation.ID,
		profileKeyAllAppsVisible:      invitation.Attributes.AllAppsVisible,
		profileKeyProvisioningAllowed: invitation.Attributes.ProvisioningAllowed,
		profileKeyPending:             true,
	}
	if len(invitation.Attributes.Roles) > 0 {
		profile[profileKeyRoles] = toAnySlice(invitation.Attributes.Roles)
	}

	traitOptions := []rs.UserTraitOption{
		rs.WithEmail(invitation.Attributes.Email, true),
		rs.WithUserLogin(invitation.Attributes.Email),
		rs.WithAccountType(v2.UserTrait_ACCOUNT_TYPE_HUMAN),
	}

	if invitation.Attributes.FirstName != "" || invitation.Attributes.LastName != "" {
		traitOptions = append(traitOptions, rs.WithStructuredName(v2.UserTrait_StructuredName_builder{
			GivenName:  invitation.Attributes.FirstName,
			FamilyName: invitation.Attributes.LastName,
		}.Build()))
	}

	return rs.NewUserResource(
		invitation.DisplayName(),
		resourceTypeUser,
		invitation.ID,
		traitOptions,
		rs.WithResourceProfile(profile),
		rs.WithResourceStatus(v2.Status_RESOURCE_STATUS_PENDING, "App Store Connect invitation has not been accepted yet"),
	)
}

// List walks team members and then outstanding invitations. Roles arrive inline on each user, so
// this single pass also feeds the role grants without any per-user fan-out.
func (u *userResourceType) List(ctx context.Context, _ *v2.ResourceId, opts rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	bag, err := newPaginationBag(opts.PageToken.Token)
	if err != nil {
		return nil, nil, err
	}

	switch bag.Current().ResourceTypeID {
	case pageStateUsers:
		users, nextURL, annos, err := u.client.ListUsers(ctx, bag.PageToken())
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("baton-appstoreconnect: failed to list users: %w", err)
		}

		resources := make([]*v2.Resource, 0, len(users))
		for i := range users {
			resource, err := userResource(&users[i])
			if err != nil {
				return nil, &rs.SyncOpResults{Annotations: annos}, err
			}
			resources = append(resources, resource)
		}

		nextToken, err := advance(bag, nextURL)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}

		return resources, &rs.SyncOpResults{NextPageToken: nextToken, Annotations: annos}, nil

	case pageStateInvitations:
		invitations, nextURL, annos, err := u.client.ListUserInvitations(ctx, bag.PageToken())
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("baton-appstoreconnect: failed to list user invitations: %w", err)
		}

		resources := make([]*v2.Resource, 0, len(invitations))
		for i := range invitations {
			resource, err := invitationResource(&invitations[i])
			if err != nil {
				return nil, &rs.SyncOpResults{Annotations: annos}, err
			}
			resources = append(resources, resource)
		}

		nextToken, err := advance(bag, nextURL)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}

		return resources, &rs.SyncOpResults{NextPageToken: nextToken, Annotations: annos}, nil

	default:
		return nil, nil, fmt.Errorf("baton-appstoreconnect: unexpected page state %q", bag.Current().ResourceTypeID)
	}
}

func (u *userResourceType) Entitlements(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

func (u *userResourceType) Grants(_ context.Context, _ *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return nil, nil, nil
}

// CreateAccountCapabilityDetails reports that App Store Connect accounts are invitation-based: the
// invitee signs in with their own Apple ID, so the connector never handles a password.
func (u *userResourceType) CreateAccountCapabilityDetails(_ context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return v2.CredentialDetailsAccountProvisioning_builder{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{
			v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_NO_PASSWORD,
		},
		PreferredCredentialOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_NO_PASSWORD,
	}.Build(), nil, nil
}

// CreateAccount issues an App Store Connect invitation. Apple has no direct account creation: the
// returned resource is the pending invitation, and it is replaced by a real user record with a
// different id once the invitee accepts.
func (u *userResourceType) CreateAccount(
	ctx context.Context,
	accountInfo *v2.AccountInfo,
	_ *v2.LocalCredentialOptions,
) (connectorbuilder.CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error) {
	profile := accountInfo.GetProfile()
	if profile == nil {
		return nil, nil, nil, status.Error(codes.InvalidArgument, "baton-appstoreconnect: account info has no profile")
	}
	fields := profile.GetFields()

	email := stringFromProfileField(fields, profileFieldEmail)
	if email == "" {
		return nil, nil, nil, status.Error(codes.InvalidArgument, "baton-appstoreconnect: email is required")
	}

	firstName := stringFromProfileField(fields, profileFieldFirstName)
	lastName := stringFromProfileField(fields, profileFieldLastName)
	if firstName == "" || lastName == "" {
		return nil, nil, nil, status.Error(codes.InvalidArgument, "baton-appstoreconnect: first name and last name are required by App Store Connect invitations")
	}

	roles := stringListFromProfileField(fields, profileFieldRoles)
	for _, role := range roles {
		if role == appstoreconnect.RoleAccountHolder {
			return nil, nil, nil, status.Error(codes.InvalidArgument, "baton-appstoreconnect: the Account Holder role cannot be assigned through the API")
		}
	}

	request := appstoreconnect.UserInvitationRequest{
		Email:               email,
		FirstName:           firstName,
		LastName:            lastName,
		Roles:               roles,
		AllAppsVisible:      boolFromProfileField(fields, profileFieldAllAppsVisible, true),
		ProvisioningAllowed: boolFromProfileField(fields, profileFieldProvisioningAllowed, false),
		VisibleAppIDs:       stringListFromProfileField(fields, profileFieldVisibleAppIDs),
	}

	invitation, annos, err := u.client.CreateUserInvitation(ctx, request)
	if err != nil {
		if appstoreconnect.IsConflict(err) {
			return v2.CreateAccountResponse_AlreadyExistsResult_builder{
				IsCreateAccountResult: true,
			}.Build(), nil, annos, nil
		}
		return nil, nil, annos, fmt.Errorf("baton-appstoreconnect: failed to invite user: %w", err)
	}

	resource, err := invitationResource(invitation)
	if err != nil {
		return nil, nil, annos, err
	}

	return v2.CreateAccountResponse_SuccessResult_builder{
		IsCreateAccountResult: true,
		Resource:              resource,
	}.Build(), nil, annos, nil
}

// Delete removes a team member. The same resource type carries accepted users and pending
// invitations, and the two live behind different endpoints, so a user delete that comes back "not
// found" is retried as an invitation cancellation before giving up.
func (u *userResourceType) Delete(ctx context.Context, resourceID *v2.ResourceId, _ *v2.ResourceId) (annotations.Annotations, error) {
	annos, err := u.client.DeleteUser(ctx, resourceID.Resource)
	if err == nil {
		return annos, nil
	}
	if !appstoreconnect.IsNotFound(err) {
		return annos, fmt.Errorf("baton-appstoreconnect: failed to delete user: %w", err)
	}

	invitationAnnos, invitationErr := u.client.DeleteUserInvitation(ctx, resourceID.Resource)
	annos = append(annos, invitationAnnos...)
	if invitationErr == nil {
		return annos, nil
	}
	if appstoreconnect.IsNotFound(invitationErr) {
		return append(annos, annotations.New(&v2.ResourceDoesNotExist{})...), nil
	}

	return annos, fmt.Errorf("baton-appstoreconnect: failed to cancel user invitation: %w", invitationErr)
}

func userBuilder(client *appstoreconnect.Client) *userResourceType {
	return &userResourceType{
		resourceType: resourceTypeUser,
		client:       client,
	}
}
