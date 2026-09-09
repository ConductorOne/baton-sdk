package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-appstoreconnect/pkg/appstoreconnect"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// appVisible is the entitlement slug for being able to see an app in App Store Connect.
const appVisible = "visible"

type appResourceType struct {
	resourceType *v2.ResourceType
	client       *appstoreconnect.Client
}

func (a *appResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return a.resourceType
}

// appResource builds the resource for one app in the team's account.
func appResource(app *appstoreconnect.App) (*v2.Resource, error) {
	profile := map[string]interface{}{
		profileKeyBundleID: app.Attributes.BundleID,
	}
	if app.Attributes.SKU != "" {
		profile[profileKeySKU] = app.Attributes.SKU
	}

	name := app.Attributes.Name
	if name == "" {
		name = app.Attributes.BundleID
	}

	return rs.NewAppResource(
		name,
		resourceTypeApp,
		app.ID,
		nil,
		rs.WithResourceProfile(profile),
	)
}

func (a *appResourceType) List(ctx context.Context, _ *v2.ResourceId, opts rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	bag := &pagination.Bag{}
	if err := bag.Unmarshal(opts.PageToken.Token); err != nil {
		return nil, nil, fmt.Errorf("baton-appstoreconnect: page token corrupt: %w", err)
	}
	if bag.Current() == nil {
		bag.Push(pagination.PageState{ResourceTypeID: resourceTypeApp.Id})
	}

	apps, nextURL, annos, err := a.client.ListApps(ctx, bag.PageToken())
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("baton-appstoreconnect: failed to list apps: %w", err)
	}

	resources := make([]*v2.Resource, 0, len(apps))
	for i := range apps {
		resource, err := appResource(&apps[i])
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}
		resources = append(resources, resource)
	}

	nextToken, err := bag.NextToken(nextURL)
	if err != nil {
		return nil, &rs.SyncOpResults{Annotations: annos}, err
	}

	return resources, &rs.SyncOpResults{NextPageToken: nextToken, Annotations: annos}, nil
}

func (a *appResourceType) Entitlements(_ context.Context, resource *v2.Resource, _ rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return []*v2.Entitlement{
		ent.NewAssignmentEntitlement(
			resource,
			appVisible,
			ent.WithGrantableTo(resourceTypeUser),
			ent.WithDisplayName(fmt.Sprintf("%s access", resource.GetDisplayName())),
			ent.WithDescription(fmt.Sprintf("Can see the %s app in App Store Connect", resource.GetDisplayName())),
		),
	}, nil, nil
}

// Grants emits a grant for everyone who can see this app: users limited to a set of apps that
// includes it, and users whose access is not app-limited at all (allAppsVisible). The latter really
// do have access to the app, so leaving them out would understate access at review time.
func (a *appResourceType) Grants(ctx context.Context, resource *v2.Resource, opts rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	bag, err := newPaginationBag(opts.PageToken.Token)
	if err != nil {
		return nil, nil, err
	}

	appID := resource.GetId().GetResource()

	switch bag.Current().ResourceTypeID {
	case pageStateUsers:
		users, nextURL, annos, err := a.client.ListUsers(ctx, bag.PageToken())
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("baton-appstoreconnect: failed to list users: %w", err)
		}

		var grants []*v2.Grant
		for i := range users {
			visible, visibleAnnos, err := a.userSeesApp(ctx, &users[i], appID)
			annos = append(annos, visibleAnnos...)
			if err != nil {
				return nil, &rs.SyncOpResults{Annotations: annos}, err
			}
			if !visible {
				continue
			}
			grants = append(grants, grant.NewGrant(resource, appVisible, userResourceID(users[i].ID)))
		}

		nextToken, err := advance(bag, nextURL)
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, err
		}

		return grants, &rs.SyncOpResults{NextPageToken: nextToken, Annotations: annos}, nil

	case pageStateInvitations:
		invitations, nextURL, annos, err := a.client.ListUserInvitations(ctx, bag.PageToken())
		if err != nil {
			return nil, &rs.SyncOpResults{Annotations: annos}, fmt.Errorf("baton-appstoreconnect: failed to list user invitations: %w", err)
		}

		var grants []*v2.Grant
		for i := range invitations {
			if !invitations[i].Attributes.AllAppsVisible && !contains(invitations[i].VisibleAppIDs(), appID) {
				continue
			}
			grants = append(grants, grant.NewGrant(resource, appVisible, userResourceID(invitations[i].ID)))
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

// userSeesApp reports whether the user can see the app. The visibleApps relationship is inlined on
// the user list, but Apple truncates it, so a user with more visible apps than fit needs a
// follow-up request to the relationship endpoint before the answer can be trusted.
func (a *appResourceType) userSeesApp(ctx context.Context, user *appstoreconnect.User, appID string) (bool, annotations.Annotations, error) {
	if user.Attributes.AllAppsVisible {
		return true, nil, nil
	}

	if user.VisibleAppsComplete() {
		return contains(user.VisibleAppIDs(), appID), nil, nil
	}

	apps, annos, err := a.client.ListUserVisibleApps(ctx, user.ID)
	if err != nil {
		return false, annos, fmt.Errorf("baton-appstoreconnect: failed to list visible apps for user %s: %w", user.ID, err)
	}

	for i := range apps {
		if apps[i].ID == appID {
			return true, annos, nil
		}
	}

	return false, annos, nil
}

// Grant makes an app visible to an app-limited user. Apple replaces the whole visibleApps
// relationship on PATCH, so the user's current set is read first and the new app added to it.
func (a *appResourceType) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	if principal.GetId().GetResourceType() != resourceTypeUser.Id {
		return nil, nil, status.Error(codes.InvalidArgument, "baton-appstoreconnect: only users can be granted app access")
	}

	appID := entitlement.GetResource().GetId().GetResource()
	userID := principal.GetId().GetResource()

	user, annos, err := a.client.GetUser(ctx, userID)
	if err != nil {
		return nil, annos, fmt.Errorf("baton-appstoreconnect: failed to read user before granting app access: %w", err)
	}

	// A user with allAppsVisible already sees this app. Narrowing them to a single app to record
	// the grant would remove access to everything else, so report the grant as already held.
	if user.Attributes.AllAppsVisible {
		return nil, append(annos, annotations.New(&v2.GrantAlreadyExists{})...), nil
	}

	currentIDs, currentAnnos, err := a.currentVisibleAppIDs(ctx, user)
	annos = append(annos, currentAnnos...)
	if err != nil {
		return nil, annos, err
	}

	if contains(currentIDs, appID) {
		return nil, append(annos, annotations.New(&v2.GrantAlreadyExists{})...), nil
	}

	allAppsVisible := false
	_, updateAnnos, err := a.client.UpdateUser(ctx, userID, appstoreconnect.UserUpdate{
		AllAppsVisible: &allAppsVisible,
		VisibleAppIDs:  append(currentIDs, appID),
		SetVisibleApps: true,
	})
	annos = append(annos, updateAnnos...)
	if err != nil {
		return nil, annos, fmt.Errorf("baton-appstoreconnect: failed to grant access to app %s: %w", appID, err)
	}

	return []*v2.Grant{grant.NewGrant(entitlement.GetResource(), appVisible, principal.GetId())}, annos, nil
}

// Revoke hides an app from an app-limited user.
func (a *appResourceType) Revoke(ctx context.Context, revokeGrant *v2.Grant) (annotations.Annotations, error) {
	principal := revokeGrant.GetPrincipal()
	if principal.GetId().GetResourceType() != resourceTypeUser.Id {
		return nil, status.Error(codes.InvalidArgument, "baton-appstoreconnect: only users can have app access revoked")
	}

	appID := revokeGrant.GetEntitlement().GetResource().GetId().GetResource()
	userID := principal.GetId().GetResource()

	user, annos, err := a.client.GetUser(ctx, userID)
	if err != nil {
		if appstoreconnect.IsNotFound(err) {
			return append(annos, annotations.New(&v2.ResourceDoesNotExist{})...), nil
		}
		return annos, fmt.Errorf("baton-appstoreconnect: failed to read user before revoking app access: %w", err)
	}

	// The grant exists because the user sees every app, not because of a per-app assignment.
	// Revoking it would mean turning off allAppsVisible and enumerating everything else they should
	// keep, which is a much larger change than the request asked for. Refuse instead of guessing.
	if user.Attributes.AllAppsVisible {
		return annos, status.Errorf(
			codes.FailedPrecondition,
			"baton-appstoreconnect: user %s has access to all apps; revoke the role or turn off all-apps access in App Store Connect before removing a single app",
			userID,
		)
	}

	currentIDs, currentAnnos, err := a.currentVisibleAppIDs(ctx, user)
	annos = append(annos, currentAnnos...)
	if err != nil {
		return annos, err
	}

	if !contains(currentIDs, appID) {
		return append(annos, annotations.New(&v2.GrantAlreadyRevoked{})...), nil
	}

	allAppsVisible := false
	_, updateAnnos, err := a.client.UpdateUser(ctx, userID, appstoreconnect.UserUpdate{
		AllAppsVisible: &allAppsVisible,
		VisibleAppIDs:  removeValue(currentIDs, appID),
		SetVisibleApps: true,
	})
	annos = append(annos, updateAnnos...)
	if err != nil {
		return annos, fmt.Errorf("baton-appstoreconnect: failed to revoke access to app %s: %w", appID, err)
	}

	return annos, nil
}

// currentVisibleAppIDs resolves the user's complete visibleApps set, falling back to the
// relationship endpoint when the inlined list was truncated. Provisioning is full-replace, so an
// incomplete read here would silently drop the apps that were left out.
func (a *appResourceType) currentVisibleAppIDs(ctx context.Context, user *appstoreconnect.User) ([]string, annotations.Annotations, error) {
	if user.VisibleAppsComplete() {
		return user.VisibleAppIDs(), nil, nil
	}

	apps, annos, err := a.client.ListUserVisibleApps(ctx, user.ID)
	if err != nil {
		return nil, annos, fmt.Errorf("baton-appstoreconnect: failed to list visible apps for user %s: %w", user.ID, err)
	}

	ids := make([]string, 0, len(apps))
	for i := range apps {
		ids = append(ids, apps[i].ID)
	}

	return ids, annos, nil
}

func appBuilder(client *appstoreconnect.Client) *appResourceType {
	return &appResourceType{
		resourceType: resourceTypeApp,
		client:       client,
	}
}
