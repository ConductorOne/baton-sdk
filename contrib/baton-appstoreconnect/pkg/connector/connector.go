package connector

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-appstoreconnect/pkg/appstoreconnect"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AppStoreConnect is the Apple App Store Connect connector.
type AppStoreConnect struct {
	client *appstoreconnect.Client
}

// Config carries everything New needs to build a client.
type Config struct {
	KeyID         string
	IssuerID      string
	PrivateKeyPEM string
	BaseURL       string
}

// ResourceSyncers returns the syncers this connector runs.
func (a *AppStoreConnect) ResourceSyncers(_ context.Context) []connectorbuilder.ResourceSyncerV2 {
	return []connectorbuilder.ResourceSyncerV2{
		userBuilder(a.client),
		roleBuilder(a.client),
		appBuilder(a.client),
	}
}

// Metadata returns metadata about the connector.
func (a *AppStoreConnect) Metadata(_ context.Context) (*v2.ConnectorMetadata, error) {
	return v2.ConnectorMetadata_builder{
		DisplayName:           "Apple App Store Connect",
		Description:           "Syncs users, roles and per-app access from App Store Connect, the developer side of the App Store.",
		AccountCreationSchema: accountCreationSchema(),
	}.Build(), nil
}

// accountCreationSchema describes the fields an App Store Connect invitation accepts. Apple
// requires a name as well as an email, and account creation only ever produces an invitation.
func accountCreationSchema() *v2.ConnectorAccountCreationSchema {
	defaultAllAppsVisible := true
	defaultProvisioningAllowed := false

	return v2.ConnectorAccountCreationSchema_builder{
		FieldMap: map[string]*v2.ConnectorAccountCreationSchema_Field{
			profileFieldEmail: v2.ConnectorAccountCreationSchema_Field_builder{
				DisplayName: "Email",
				Description: "The Apple ID email address to invite to the team.",
				Placeholder: "jane@example.com",
				Required:    true,
				Order:       1,
				StringField: &v2.ConnectorAccountCreationSchema_StringField{},
			}.Build(),
			profileFieldFirstName: v2.ConnectorAccountCreationSchema_Field_builder{
				DisplayName: "First Name",
				Description: "The invitee's first name. Required by App Store Connect.",
				Placeholder: "Jane",
				Required:    true,
				Order:       2,
				StringField: &v2.ConnectorAccountCreationSchema_StringField{},
			}.Build(),
			profileFieldLastName: v2.ConnectorAccountCreationSchema_Field_builder{
				DisplayName: "Last Name",
				Description: "The invitee's last name. Required by App Store Connect.",
				Placeholder: "Doe",
				Required:    true,
				Order:       3,
				StringField: &v2.ConnectorAccountCreationSchema_StringField{},
			}.Build(),
			profileFieldRoles: v2.ConnectorAccountCreationSchema_Field_builder{
				DisplayName:     "Roles",
				Description:     "App Store Connect roles to grant, e.g. DEVELOPER. The ACCOUNT_HOLDER role cannot be assigned through the API.",
				Required:        false,
				Order:           4,
				StringListField: &v2.ConnectorAccountCreationSchema_StringListField{},
			}.Build(),
			profileFieldAllAppsVisible: v2.ConnectorAccountCreationSchema_Field_builder{
				DisplayName: "All Apps Visible",
				Description: "Give the invitee access to every app in the team. Turn this off to limit them to specific apps.",
				Required:    false,
				Order:       5,
				BoolField: v2.ConnectorAccountCreationSchema_BoolField_builder{
					DefaultValue: &defaultAllAppsVisible,
				}.Build(),
			}.Build(),
			profileFieldVisibleAppIDs: v2.ConnectorAccountCreationSchema_Field_builder{
				DisplayName:     "Visible App IDs",
				Description:     "App Store Connect app IDs the invitee may see. Only used when All Apps Visible is off.",
				Required:        false,
				Order:           6,
				StringListField: &v2.ConnectorAccountCreationSchema_StringListField{},
			}.Build(),
			profileFieldProvisioningAllowed: v2.ConnectorAccountCreationSchema_Field_builder{
				DisplayName: "Provisioning Allowed",
				Description: "Allow the invitee to manage certificates, identifiers and provisioning profiles.",
				Required:    false,
				Order:       7,
				BoolField: v2.ConnectorAccountCreationSchema_BoolField_builder{
					DefaultValue: &defaultProvisioningAllowed,
				}.Build(),
			}.Build(),
		},
	}.Build()
}

// Validate confirms the API key works and has the access the connector needs. Listing users is the
// narrowest call that proves both: it needs an Admin-scoped key, which is exactly what user
// management requires.
func (a *AppStoreConnect) Validate(ctx context.Context) (annotations.Annotations, error) {
	_, _, annos, err := a.client.ListUsers(ctx, "")
	if err == nil {
		return annos, nil
	}

	if appstoreconnect.IsForbidden(err) {
		return annos, status.Error(
			codes.PermissionDenied,
			"baton-appstoreconnect: the API key was accepted but is not allowed to read users; App Store Connect user management requires a key with the Admin role",
		)
	}

	ctxzap.Extract(ctx).Error("baton-appstoreconnect: credential validation failed")

	return annos, status.Errorf(codes.Unauthenticated, "baton-appstoreconnect: could not authenticate to App Store Connect: %s", err.Error())
}

// New builds the connector.
func New(ctx context.Context, cfg Config) (*AppStoreConnect, error) {
	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, ctxzap.Extract(ctx)))
	if err != nil {
		return nil, err
	}

	client, err := appstoreconnect.NewClient(httpClient, cfg.KeyID, cfg.IssuerID, cfg.PrivateKeyPEM, cfg.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("baton-appstoreconnect: failed to build API client: %w", err)
	}

	return &AppStoreConnect{client: client}, nil
}

// Compile-time proof that every capability this connector advertises is actually wired up. Losing
// one of these is otherwise invisible: the builder simply stops offering the capability at runtime.
var (
	_ connectorbuilder.ConnectorBuilderV2    = (*AppStoreConnect)(nil)
	_ connectorbuilder.ResourceSyncerV2      = (*userResourceType)(nil)
	_ connectorbuilder.AccountManagerV2      = (*userResourceType)(nil)
	_ connectorbuilder.ResourceDeleterV2     = (*userResourceType)(nil)
	_ connectorbuilder.ResourceProvisionerV2 = (*roleResourceType)(nil)
	_ connectorbuilder.ResourceProvisionerV2 = (*appResourceType)(nil)
)
