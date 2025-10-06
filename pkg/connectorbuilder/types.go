package connectorbuilder

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ResourceType interface {
	ResourceType(ctx context.Context) *v2.ResourceType
}

// ResourceSyncer is the primary interface for connector developers to implement.
//
// It defines the core functionality for synchronizing resources, entitlements, and grants
// from external systems into Baton. Every connector must implement at least this interface
// for each resource type it supports.
//
// Extensions to this interface include:
// - ResourceProvisioner/ResourceProvisionerV2: For adding/removing access
// - ResourceManager: For creating and managing resources
// - ResourceDeleter: For deleting resources
// - AccountManager: For account provisioning operations
// - CredentialManager: For credential rotation operations.
// - ResourceTargetedSyncer: For directly getting a resource supporting targeted sync.
type ResourceSyncer interface {
	ResourceType
	List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error)
	Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error)
	Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error)
}

type ResourceSyncer2 interface {
	ResourceType
	List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token, opts resource.Options) ([]*v2.Resource, string, annotations.Annotations, error)
	Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token, opts resource.Options) ([]*v2.Entitlement, string, annotations.Annotations, error)
	Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token, opts resource.Options) ([]*v2.Grant, string, annotations.Annotations, error)
}

// ResourceProvisioner extends ResourceSyncer to add capabilities for granting and revoking access.
//
// Note: ResourceProvisionerV2 is preferred for new connectors as it provides
// enhanced grant capabilities.
//
// Implementing this interface indicates the connector supports provisioning operations
// for the associated resource type.
type ResourceProvisioner interface {
	ResourceType
	Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error)
	Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error)
}

// ResourceProvisionerV2 extends ResourceSyncer to add capabilities for granting and revoking access
// with enhanced functionality compared to ResourceProvisioner.
//
// This is the recommended interface for implementing provisioning operations in new connectors.
// It differs from ResourceProvisioner by returning a list of grants from the Grant method.
type ResourceProvisionerV2 interface {
	ResourceType
	Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error)
	Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error)
}

// ResourceManager extends ResourceSyncer to add capabilities for creating resources.
//
// Implementing this interface indicates the connector supports creating and deleting resources
// of the associated resource type. A ResourceManager automatically provides ResourceDeleter
// functionality.
type ResourceManager interface {
	Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error)
	ResourceDeleter
}

// ResourceManagerV2 extends ResourceSyncer to add capabilities for creating resources.
//
// This is the recommended interface for implementing resource creation operations in new connectors.
type ResourceManagerV2 interface {
	Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error)
	ResourceDeleterV2
}

// ResourceDeleter extends ResourceSyncer to add capabilities for deleting resources.
//
// Implementing this interface indicates the connector supports deleting resources
// of the associated resource type.
type ResourceDeleter interface {
	Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error)
}

// ResourceDeleterV2 extends ResourceSyncer to add capabilities for deleting resources.
//
// This is the recommended interface for implementing resource deletion operations in new connectors.
// It differs from ResourceDeleter by having the resource, not just the id.
type ResourceDeleterV2 interface {
	Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error)
}

// ResourceTargetedSyncer extends ResourceSyncer to add capabilities for directly syncing an individual resource
//
// Implementing this interface indicates the connector supports calling "get" on a resource
// of the associated resource type.
type ResourceTargetedSyncer interface {
	Get(ctx context.Context, resourceId *v2.ResourceId, parentResourceId *v2.ResourceId) (*v2.Resource, annotations.Annotations, error)
}

// CreateAccountResponse is a semi-opaque type returned from CreateAccount operations.
//
// This is used to communicate the result of account creation back to Baton.
type CreateAccountResponse interface {
	proto.Message
	GetIsCreateAccountResult() bool
}

// AccountManager extends ResourceSyncer to add capabilities for managing user accounts.
//
// Implementing this interface indicates the connector supports creating accounts
// in the external system. A resource type should implement this interface if it
// represents users or accounts that can be provisioned.
type AccountManager interface {
	CreateAccount(ctx context.Context,
		accountInfo *v2.AccountInfo,
		credentialOptions *v2.LocalCredentialOptions) (CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error)
	CreateAccountCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error)
}

type OldAccountManager interface {
	CreateAccount(ctx context.Context,
		accountInfo *v2.AccountInfo,
		credentialOptions *v2.CredentialOptions) (CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error)
}

// CredentialManager extends ResourceSyncer to add capabilities for managing credentials.
//
// Implementing this interface indicates the connector supports rotating credentials
// for resources of the associated type. This is commonly used for user accounts
// or service accounts that have rotatable credentials.
type CredentialManager interface {
	Rotate(ctx context.Context,
		resourceId *v2.ResourceId,
		credentialOptions *v2.LocalCredentialOptions) ([]*v2.PlaintextData, annotations.Annotations, error)
	RotateCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsCredentialRotation, annotations.Annotations, error)
}

type OldCredentialManager interface {
	Rotate(ctx context.Context,
		resourceId *v2.ResourceId,
		credentialOptions *v2.CredentialOptions) ([]*v2.PlaintextData, annotations.Annotations, error)
}

// Compatibility interface lets us handle both EventFeed and EventProvider the same.
type EventLister interface {
	ListEvents(ctx context.Context, earliestEvent *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error)
}

// Deprecated: This interface is deprecated in favor of EventProviderV2 which supports
// multiple event feeds. Implementing this interface indicates the connector can provide
// a single stream of events from the external system, enabling near real-time updates
// in Baton. New connectors should implement EventProviderV2 instead.
type EventProvider interface {
	ConnectorBuilder
	EventLister
}

// NewEventProviderV2 is a new interface that allows connectors to provide multiple event feeds.
//
// This is the recommended interface for implementing event feed support in new connectors.
type EventProviderV2 interface {
	ConnectorBuilder
	EventFeeds(ctx context.Context) []EventFeed
}

// EventFeed is a single stream of events from the external system.
//
// EventFeedMetadata describes this feed, and a connector can have multiple feeds.
type EventFeed interface {
	EventLister
	EventFeedMetadata(ctx context.Context) *v2.EventFeedMetadata
}

// TicketManager extends ConnectorBuilder to add capabilities for ticket management.
//
// Implementing this interface indicates the connector can integrate with an external
// ticketing system, allowing Baton to create and track tickets in that system.
type TicketManager interface {
	GetTicket(ctx context.Context, ticketId string) (*v2.Ticket, annotations.Annotations, error)
	CreateTicket(ctx context.Context, ticket *v2.Ticket, schema *v2.TicketSchema) (*v2.Ticket, annotations.Annotations, error)
	GetTicketSchema(ctx context.Context, schemaID string) (*v2.TicketSchema, annotations.Annotations, error)
	ListTicketSchemas(ctx context.Context, pToken *pagination.Token) ([]*v2.TicketSchema, string, annotations.Annotations, error)
	BulkCreateTickets(context.Context, *v2.TicketsServiceBulkCreateTicketsRequest) (*v2.TicketsServiceBulkCreateTicketsResponse, error)
	BulkGetTickets(context.Context, *v2.TicketsServiceBulkGetTicketsRequest) (*v2.TicketsServiceBulkGetTicketsResponse, error)
}

// CustomActionManager defines capabilities for handling custom actions.
//
// Note: RegisterActionManager is preferred for new connectors.
//
// This interface allows connectors to define and execute custom actions
// that can be triggered from Baton.
type CustomActionManager interface {
	ListActionSchemas(ctx context.Context) ([]*v2.BatonActionSchema, annotations.Annotations, error)
	GetActionSchema(ctx context.Context, name string) (*v2.BatonActionSchema, annotations.Annotations, error)
	InvokeAction(ctx context.Context, name string, args *structpb.Struct) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error)
	GetActionStatus(ctx context.Context, id string) (v2.BatonActionStatus, string, *structpb.Struct, annotations.Annotations, error)
}

// RegisterActionManager extends ConnectorBuilder to add capabilities for registering custom actions.
//
// This is the recommended interface for implementing custom action support in new connectors.
// It provides a mechanism to register a CustomActionManager with the connector.
type RegisterActionManager interface {
	RegisterActionManager(ctx context.Context) (CustomActionManager, error)
}

// ConnectorBuilder is the foundational interface for creating Baton connectors.
//
// This interface defines the core capabilities required by all connectors, including
// metadata, validation, and registering resource syncers. Additional functionality
// can be added by implementing extension interfaces such as:
// - RegisterActionManager: For custom action support
// - EventProvider: For event stream support
// - TicketManager: For ticket management integration.
type MetadataProvider interface {
	Metadata(ctx context.Context) (*v2.ConnectorMetadata, error)
}

type ValidateProvider interface {
	Validate(ctx context.Context) (annotations.Annotations, error)
}

type ConnectorBuilder interface {
	MetadataProvider
	ValidateProvider
	ResourceSyncers(ctx context.Context) []ResourceSyncer
}

type ConnectorBuilder2 interface {
	MetadataProvider
	ValidateProvider
	ResourceSyncers(ctx context.Context) []ResourceSyncer2
}
