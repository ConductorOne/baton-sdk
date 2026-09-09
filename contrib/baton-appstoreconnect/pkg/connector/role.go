package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-appstoreconnect/pkg/appstoreconnect"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// roleAssignment is the entitlement slug for holding an App Store Connect role.
const roleAssignment = "assigned"

type roleResourceType struct {
	resourceType *v2.ResourceType
	client       *appstoreconnect.Client
}

func (r *roleResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return r.resourceType
}

// List returns the fixed App Store Connect role enum. There is no roles endpoint to page through.
func (r *roleResourceType) List(_ context.Context, _ *v2.ResourceId, _ rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	resources := make([]*v2.Resource, 0, len(appstoreconnect.AllRoles))
	for _, role := range appstoreconnect.AllRoles {
		resource, err := roleResource(role)
		if err != nil {
			return nil, nil, err
		}
		resources = append(resources, resource)
	}

	return resources, nil, nil
}

func (r *roleResourceType) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return []*v2.Entitlement{
		ent.NewAssignmentEntitlement(
			resource,
			roleAssignment,
			ent.WithGrantableTo(resourceTypeUser),
			ent.WithDisplayName(fmt.Sprintf("%s role", resource.GetDisplayName())),
			ent.WithDescription(fmt.Sprintf("Holds the %s role in App Store Connect", resource.GetDisplayName())),
		),
	}, nil, nil
}

// Grants walks team members and then outstanding invitations, emitting a grant for everyone who
// holds this role. Roles are inline on both records, so no per-principal lookup is needed.
func (r *roleResourceType) Grants(ctx context.Context, resource *v2.Resource, opts rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	bag, err := newPaginationBag(opts.PageToken.Token)
	if err != nil {
		return nil, nil, err
	}

	role := resource.GetId().GetResource()

	switch bag.Current().ResourceTypeID {
	case pageStateUsers:
		users, nextURL, annos, err := r.client.ListUsers(ctx, bag.PageToken())
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("baton-appstoreconnect: failed to list users: %w", err)
		}

		var grants []*v2.Grant
		for i := range users {
			if !contains(users[i].Attributes.Roles, role) {
				continue
			}
			grants = append(grants, grant.NewGrant(resource, roleAssignment, userResourceID(users[i].ID)))
		}

		nextToken, err := advance(bag, nextURL)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}

		return grants, &rs.SyncOpResults{NextPageToken: nextToken, Annotations: annos}, nil

	case pageStateInvitations:
		invitations, nextURL, annos, err := r.client.ListUserInvitations(ctx, bag.PageToken())
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("baton-appstoreconnect: failed to list user invitations: %w", err)
		}

		var grants []*v2.Grant
		for i := range invitations {
			if !contains(invitations[i].Attributes.Roles, role) {
				continue
			}
			grants = append(grants, grant.NewGrant(resource, roleAssignment, userResourceID(invitations[i].ID)))
		}

		nextToken, err := advance(bag, nextURL)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}

		return grants, &rs.SyncOpResults{NextPageToken: nextToken, Annotations: annos}, nil

	default:
		return nil, nil, fmt.Errorf("baton-appstoreconnect: unexpected page state %q", bag.Current().ResourceTypeID)
	}
}

// Grant adds a role to a user. Apple replaces the whole roles array on PATCH, so the current roles
// are read first and the new one appended. That read-modify-write is not atomic: a role assignment
// made elsewhere between the read and the write is overwritten. Grants are serialized per user by
// ConductorOne, so the realistic exposure is a concurrent change in the App Store Connect UI.
func (r *roleResourceType) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	if principal.GetId().GetResourceType() != resourceTypeUser.Id {
		return nil, nil, status.Error(codes.InvalidArgument, "baton-appstoreconnect: only users can be granted a role")
	}

	role := entitlement.GetResource().GetId().GetResource()
	if role == appstoreconnect.RoleAccountHolder {
		return nil, nil, status.Error(codes.InvalidArgument, "baton-appstoreconnect: the Account Holder role cannot be changed through the API")
	}
	if !appstoreconnect.IsKnownRole(role) {
		return nil, nil, status.Errorf(codes.InvalidArgument, "baton-appstoreconnect: unknown role %q", role)
	}

	userID := principal.GetId().GetResource()

	user, annos, err := r.client.GetUser(ctx, userID)
	if err != nil {
		return nil, annos, fmt.Errorf("baton-appstoreconnect: failed to read user before granting role: %w", err)
	}

	if contains(user.Attributes.Roles, role) {
		return nil, append(annos, annotations.New(&v2.GrantAlreadyExists{})...), nil
	}

	roles := append(append([]string{}, user.Attributes.Roles...), role)

	_, updateAnnos, err := r.client.UpdateUser(ctx, userID, appstoreconnect.UserUpdate{Roles: roles})
	annos = append(annos, updateAnnos...)
	if err != nil {
		return nil, annos, fmt.Errorf("baton-appstoreconnect: failed to grant role %q: %w", role, err)
	}

	return []*v2.Grant{grant.NewGrant(entitlement.GetResource(), roleAssignment, principal.GetId())}, annos, nil
}

// Revoke removes a role from a user, again by rewriting the full roles array.
func (r *roleResourceType) Revoke(ctx context.Context, revokeGrant *v2.Grant) (annotations.Annotations, error) {
	principal := revokeGrant.GetPrincipal()
	if principal.GetId().GetResourceType() != resourceTypeUser.Id {
		return nil, status.Error(codes.InvalidArgument, "baton-appstoreconnect: only users can have a role revoked")
	}

	role := revokeGrant.GetEntitlement().GetResource().GetId().GetResource()
	if role == appstoreconnect.RoleAccountHolder {
		return nil, status.Error(codes.InvalidArgument, "baton-appstoreconnect: the Account Holder role cannot be changed through the API")
	}

	userID := principal.GetId().GetResource()

	user, annos, err := r.client.GetUser(ctx, userID)
	if err != nil {
		if appstoreconnect.IsNotFound(err) {
			return append(annos, annotations.New(&v2.ResourceDoesNotExist{})...), nil
		}
		return annos, fmt.Errorf("baton-appstoreconnect: failed to read user before revoking role: %w", err)
	}

	if !contains(user.Attributes.Roles, role) {
		// Already revoked. Reporting success keeps a retried revoke idempotent.
		return annos, nil
	}

	roles := removeValue(user.Attributes.Roles, role)

	_, updateAnnos, err := r.client.UpdateUser(ctx, userID, appstoreconnect.UserUpdate{Roles: roles})
	annos = append(annos, updateAnnos...)
	if err != nil {
		return annos, fmt.Errorf("baton-appstoreconnect: failed to revoke role %q: %w", role, err)
	}

	return annos, nil
}

func roleBuilder(client *appstoreconnect.Client) *roleResourceType {
	return &roleResourceType{
		resourceType: resourceTypeRole,
		client:       client,
	}
}
