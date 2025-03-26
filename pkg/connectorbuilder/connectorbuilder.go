package connectorbuilder

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/metrics"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

var tracer = otel.Tracer("baton-sdk/pkg.connectorbuilder")

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
type ResourceSyncer interface {
	ResourceType(ctx context.Context) *v2.ResourceType
	List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error)
	Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error)
	Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error)
}

// ResourceProvisioner extends ResourceSyncer to add capabilities for granting and revoking access.
//
// Note: ResourceProvisionerV2 is preferred for new connectors as it provides
// enhanced grant capabilities.
//
// Implementing this interface indicates the connector supports provisioning operations
// for the associated resource type.
type ResourceProvisioner interface {
	ResourceSyncer
	ResourceType(ctx context.Context) *v2.ResourceType
	Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error)
	Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error)
}

// ResourceProvisionerV2 extends ResourceSyncer to add capabilities for granting and revoking access
// with enhanced functionality compared to ResourceProvisioner.
//
// This is the recommended interface for implementing provisioning operations in new connectors.
// It differs from ResourceProvisioner by returning a list of grants from the Grant method.
type ResourceProvisionerV2 interface {
	ResourceSyncer
	ResourceType(ctx context.Context) *v2.ResourceType
	Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error)
	Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error)
}

// ResourceManager extends ResourceSyncer to add capabilities for creating resources.
//
// Implementing this interface indicates the connector supports creating and deleting resources
// of the associated resource type. A ResourceManager automatically provides ResourceDeleter
// functionality.
type ResourceManager interface {
	ResourceSyncer
	Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error)
	ResourceDeleter
}

// ResourceDeleter extends ResourceSyncer to add capabilities for deleting resources.
//
// Implementing this interface indicates the connector supports deleting resources
// of the associated resource type.
type ResourceDeleter interface {
	ResourceSyncer
	Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error)
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
	ResourceSyncer
	CreateAccount(ctx context.Context, accountInfo *v2.AccountInfo, credentialOptions *v2.CredentialOptions) (CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error)
	CreateAccountCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error)
}

// CredentialManager extends ResourceSyncer to add capabilities for managing credentials.
//
// Implementing this interface indicates the connector supports rotating credentials
// for resources of the associated type. This is commonly used for user accounts
// or service accounts that have rotatable credentials.
type CredentialManager interface {
	ResourceSyncer
	Rotate(ctx context.Context, resourceId *v2.ResourceId, credentialOptions *v2.CredentialOptions) ([]*v2.PlaintextData, annotations.Annotations, error)
	RotateCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsCredentialRotation, annotations.Annotations, error)
}

// EventProvider extends ConnectorBuilder to add capabilities for providing event streams.
//
// Implementing this interface indicates the connector can provide a stream of events
// from the external system, enabling near real-time updates in Baton.
type EventProvider interface {
	ConnectorBuilder
	ListEvents(ctx context.Context, earliestEvent *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error)
}

// TicketManager extends ConnectorBuilder to add capabilities for ticket management.
//
// Implementing this interface indicates the connector can integrate with an external
// ticketing system, allowing Baton to create and track tickets in that system.
type TicketManager interface {
	ConnectorBuilder
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
	ConnectorBuilder
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
type ConnectorBuilder interface {
	Metadata(ctx context.Context) (*v2.ConnectorMetadata, error)
	Validate(ctx context.Context) (annotations.Annotations, error)
	ResourceSyncers(ctx context.Context) []ResourceSyncer
}

type builderImpl struct {
	resourceBuilders       map[string]ResourceSyncer
	resourceProvisioners   map[string]ResourceProvisioner
	resourceProvisionersV2 map[string]ResourceProvisionerV2
	resourceManagers       map[string]ResourceManager
	resourceDeleters       map[string]ResourceDeleter
	accountManager         AccountManager
	actionManager          CustomActionManager
	credentialManagers     map[string]CredentialManager
	eventFeed              EventProvider
	cb                     ConnectorBuilder
	ticketManager          TicketManager
	ticketingEnabled       bool
	m                      *metrics.M
	nowFunc                func() time.Time
}

func (b *builderImpl) BulkCreateTickets(ctx context.Context, request *v2.TicketsServiceBulkCreateTicketsRequest) (*v2.TicketsServiceBulkCreateTicketsResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.BulkCreateTickets")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.BulkCreateTicketsType
	if b.ticketManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: ticket manager not implemented")
	}

	reqBody := request.GetTicketRequests()
	if len(reqBody) == 0 {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: request body had no items")
	}

	ticketsResponse, err := b.ticketManager.BulkCreateTickets(ctx, request)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: creating tickets failed: %w", err)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.TicketsServiceBulkCreateTicketsResponse{
		Tickets: ticketsResponse.GetTickets(),
	}, nil
}

func (b *builderImpl) BulkGetTickets(ctx context.Context, request *v2.TicketsServiceBulkGetTicketsRequest) (*v2.TicketsServiceBulkGetTicketsResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.BulkGetTickets")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.BulkGetTicketsType
	if b.ticketManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: ticket manager not implemented")
	}

	reqBody := request.GetTicketRequests()
	if len(reqBody) == 0 {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: request body had no items")
	}

	ticketsResponse, err := b.ticketManager.BulkGetTickets(ctx, request)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: fetching tickets failed: %w", err)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.TicketsServiceBulkGetTicketsResponse{
		Tickets: ticketsResponse.GetTickets(),
	}, nil
}

func (b *builderImpl) ListTicketSchemas(ctx context.Context, request *v2.TicketsServiceListTicketSchemasRequest) (*v2.TicketsServiceListTicketSchemasResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.ListTicketSchemas")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListTicketSchemasType
	if b.ticketManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: ticket manager not implemented")
	}

	out, nextPageToken, annos, err := b.ticketManager.ListTicketSchemas(ctx, &pagination.Token{
		Size:  int(request.PageSize),
		Token: request.PageToken,
	})
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing ticket schemas failed: %w", err)
	}
	if request.PageToken != "" && request.PageToken == nextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing ticket schemas failed: next page token is the same as the current page token. this is most likely a connector bug")
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.TicketsServiceListTicketSchemasResponse{
		List:          out,
		NextPageToken: nextPageToken,
		Annotations:   annos,
	}, nil
}

func (b *builderImpl) CreateTicket(ctx context.Context, request *v2.TicketsServiceCreateTicketRequest) (*v2.TicketsServiceCreateTicketResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.CreateTicket")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.CreateTicketType
	if b.ticketManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: ticket manager not implemented")
	}

	reqBody := request.GetRequest()
	if reqBody == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: request body is nil")
	}
	cTicket := &v2.Ticket{
		DisplayName:  reqBody.GetDisplayName(),
		Description:  reqBody.GetDescription(),
		Status:       reqBody.GetStatus(),
		Labels:       reqBody.GetLabels(),
		CustomFields: reqBody.GetCustomFields(),
		RequestedFor: reqBody.GetRequestedFor(),
	}

	ticket, annos, err := b.ticketManager.CreateTicket(ctx, cTicket, request.GetSchema())
	var resp *v2.TicketsServiceCreateTicketResponse
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		if ticket != nil {
			resp = &v2.TicketsServiceCreateTicketResponse{
				Ticket:      ticket,
				Annotations: annos,
			}
		}
		return resp, fmt.Errorf("error: creating ticket failed: %w", err)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.TicketsServiceCreateTicketResponse{
		Ticket:      ticket,
		Annotations: annos,
	}, nil
}

func (b *builderImpl) GetTicket(ctx context.Context, request *v2.TicketsServiceGetTicketRequest) (*v2.TicketsServiceGetTicketResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.GetTicket")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GetTicketType
	if b.ticketManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: ticket manager not implemented")
	}

	var resp *v2.TicketsServiceGetTicketResponse
	ticket, annos, err := b.ticketManager.GetTicket(ctx, request.GetId())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		if ticket != nil {
			resp = &v2.TicketsServiceGetTicketResponse{
				Ticket:      ticket,
				Annotations: annos,
			}
		}
		return resp, fmt.Errorf("error: getting ticket failed: %w", err)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.TicketsServiceGetTicketResponse{
		Ticket:      ticket,
		Annotations: annos,
	}, nil
}

func (b *builderImpl) GetTicketSchema(ctx context.Context, request *v2.TicketsServiceGetTicketSchemaRequest) (*v2.TicketsServiceGetTicketSchemaResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.GetTicketSchema")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GetTicketSchemaType
	if b.ticketManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: ticket manager not implemented")
	}

	ticketSchema, annos, err := b.ticketManager.GetTicketSchema(ctx, request.GetId())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: getting ticket metadata failed: %w", err)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.TicketsServiceGetTicketSchemaResponse{
		Schema:      ticketSchema,
		Annotations: annos,
	}, nil
}

// NewConnector creates a new ConnectorServer for a new resource.
func NewConnector(ctx context.Context, in interface{}, opts ...Opt) (types.ConnectorServer, error) {
	switch c := in.(type) {
	case ConnectorBuilder:
		ret := &builderImpl{
			resourceBuilders:       make(map[string]ResourceSyncer),
			resourceProvisioners:   make(map[string]ResourceProvisioner),
			resourceProvisionersV2: make(map[string]ResourceProvisionerV2),
			resourceManagers:       make(map[string]ResourceManager),
			resourceDeleters:       make(map[string]ResourceDeleter),
			accountManager:         nil,
			actionManager:          nil,
			credentialManagers:     make(map[string]CredentialManager),
			cb:                     c,
			ticketManager:          nil,
			nowFunc:                time.Now,
		}

		err := ret.options(opts...)
		if err != nil {
			return nil, err
		}

		if ret.m == nil {
			ret.m = metrics.New(metrics.NewNoOpHandler(ctx))
		}

		if b, ok := c.(EventProvider); ok {
			ret.eventFeed = b
		}

		if ticketManager, ok := c.(TicketManager); ok {
			if ret.ticketManager != nil {
				return nil, fmt.Errorf("error: cannot set multiple ticket managers")
			}
			ret.ticketManager = ticketManager
		}

		if actionManager, ok := c.(CustomActionManager); ok {
			if ret.actionManager != nil {
				return nil, fmt.Errorf("error: cannot set multiple action managers")
			}
			ret.actionManager = actionManager
		}

		if registerActionManager, ok := c.(RegisterActionManager); ok {
			if ret.actionManager != nil {
				return nil, fmt.Errorf("error: cannot register multiple action managers")
			}
			actionManager, err := registerActionManager.RegisterActionManager(ctx)
			if err != nil {
				return nil, fmt.Errorf("error: registering action manager failed: %w", err)
			}
			if actionManager == nil {
				return nil, fmt.Errorf("error: action manager is nil")
			}
			ret.actionManager = actionManager
		}

		for _, rb := range c.ResourceSyncers(ctx) {
			rType := rb.ResourceType(ctx)
			if _, ok := ret.resourceBuilders[rType.Id]; ok {
				return nil, fmt.Errorf("error: duplicate resource type found for resource builder %s", rType.Id)
			}
			ret.resourceBuilders[rType.Id] = rb

			if err := validateProvisionerVersion(ctx, rb); err != nil {
				return nil, err
			}

			if provisioner, ok := rb.(ResourceProvisioner); ok {
				if _, ok := ret.resourceProvisioners[rType.Id]; ok {
					return nil, fmt.Errorf("error: duplicate resource type found for resource provisioner %s", rType.Id)
				}
				ret.resourceProvisioners[rType.Id] = provisioner
			}
			if provisioner, ok := rb.(ResourceProvisionerV2); ok {
				if _, ok := ret.resourceProvisionersV2[rType.Id]; ok {
					return nil, fmt.Errorf("error: duplicate resource type found for resource provisioner v2 %s", rType.Id)
				}
				ret.resourceProvisionersV2[rType.Id] = provisioner
			}

			if resourceManager, ok := rb.(ResourceManager); ok {
				if _, ok := ret.resourceManagers[rType.Id]; ok {
					return nil, fmt.Errorf("error: duplicate resource type found for resource manager %s", rType.Id)
				}
				ret.resourceManagers[rType.Id] = resourceManager
				// Support DeleteResourceV2 if connector implements both Create and Delete
				if _, ok := ret.resourceDeleters[rType.Id]; ok {
					// This should never happen
					return nil, fmt.Errorf("error: duplicate resource type found for resource deleter %s", rType.Id)
				}
				ret.resourceDeleters[rType.Id] = resourceManager
			} else {
				if resourceDeleter, ok := rb.(ResourceDeleter); ok {
					if _, ok := ret.resourceDeleters[rType.Id]; ok {
						return nil, fmt.Errorf("error: duplicate resource type found for resource deleter %s", rType.Id)
					}
					ret.resourceDeleters[rType.Id] = resourceDeleter
				}
			}

			if accountManager, ok := rb.(AccountManager); ok {
				if ret.accountManager != nil {
					return nil, fmt.Errorf("error: duplicate resource type found for account manager %s", rType.Id)
				}
				ret.accountManager = accountManager
			}

			if credentialManagers, ok := rb.(CredentialManager); ok {
				if _, ok := ret.credentialManagers[rType.Id]; ok {
					return nil, fmt.Errorf("error: duplicate resource type found for credential manager %s", rType.Id)
				}
				ret.credentialManagers[rType.Id] = credentialManagers
			}
		}
		return ret, nil

	case types.ConnectorServer:
		return c, nil

	default:
		return nil, fmt.Errorf("input was not a ConnectorBuilder or a ConnectorServer")
	}
}

type Opt func(b *builderImpl) error

func WithTicketingEnabled() Opt {
	return func(b *builderImpl) error {
		if _, ok := b.cb.(TicketManager); ok {
			b.ticketingEnabled = true
			return nil
		}
		return errors.New("external ticketing not supported")
	}
}

func WithMetricsHandler(h metrics.Handler) Opt {
	return func(b *builderImpl) error {
		b.m = metrics.New(h)
		return nil
	}
}

func (b *builderImpl) options(opts ...Opt) error {
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return err
		}
	}

	return nil
}

func validateProvisionerVersion(ctx context.Context, p ResourceSyncer) error {
	_, ok := p.(ResourceProvisioner)
	_, okV2 := p.(ResourceProvisionerV2)

	if ok && okV2 {
		return fmt.Errorf("error: resource type %s implements both ResourceProvisioner and ResourceProvisionerV2", p.ResourceType(ctx).Id)
	}
	return nil
}

// ListResourceTypes lists all available resource types.
func (b *builderImpl) ListResourceTypes(
	ctx context.Context,
	request *v2.ResourceTypesServiceListResourceTypesRequest,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.ListResourceTypes")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListResourceTypesType
	var out []*v2.ResourceType

	for _, rb := range b.resourceBuilders {
		out = append(out, rb.ResourceType(ctx))
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.ResourceTypesServiceListResourceTypesResponse{List: out}, nil
}

// ListResources returns all available resources for a given resource type ID.
func (b *builderImpl) ListResources(ctx context.Context, request *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.ListResources")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListResourcesType
	rb, ok := b.resourceBuilders[request.ResourceTypeId]
	if !ok {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: list resources with unknown resource type %s", request.ResourceTypeId)
	}

	out, nextPageToken, annos, err := rb.List(ctx, request.ParentResourceId, &pagination.Token{
		Size:  int(request.PageSize),
		Token: request.PageToken,
	})
	resp := &v2.ResourcesServiceListResourcesResponse{
		List:          out,
		NextPageToken: nextPageToken,
		Annotations:   annos,
	}
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing resources failed: %w", err)
	}
	if request.PageToken != "" && request.PageToken == nextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing resources failed: next page token is the same as the current page token. this is most likely a connector bug")
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}

// ListEntitlements returns all the entitlements for a given resource.
func (b *builderImpl) ListEntitlements(ctx context.Context, request *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.ListEntitlements")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListEntitlementsType
	rb, ok := b.resourceBuilders[request.Resource.Id.ResourceType]
	if !ok {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: list entitlements with unknown resource type %s", request.Resource.Id.ResourceType)
	}

	out, nextPageToken, annos, err := rb.Entitlements(ctx, request.Resource, &pagination.Token{
		Size:  int(request.PageSize),
		Token: request.PageToken,
	})
	resp := &v2.EntitlementsServiceListEntitlementsResponse{
		List:          out,
		NextPageToken: nextPageToken,
		Annotations:   annos,
	}
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing entitlements failed: %w", err)
	}
	if request.PageToken != "" && request.PageToken == nextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing entitlements failed: next page token is the same as the current page token. this is most likely a connector bug")
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}

// ListGrants lists all the grants for a given resource.
func (b *builderImpl) ListGrants(ctx context.Context, request *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.ListGrants")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListGrantsType
	rid := request.Resource.Id
	rb, ok := b.resourceBuilders[rid.ResourceType]
	if !ok {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: list entitlements with unknown resource type %s", rid.ResourceType)
	}

	out, nextPageToken, annos, err := rb.Grants(ctx, request.Resource, &pagination.Token{
		Size:  int(request.PageSize),
		Token: request.PageToken,
	})
	resp := &v2.GrantsServiceListGrantsResponse{
		List:          out,
		NextPageToken: nextPageToken,
		Annotations:   annos,
	}
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing grants for resource %s/%s failed: %w", rid.ResourceType, rid.Resource, err)
	}
	if request.PageToken != "" && request.PageToken == nextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return resp, fmt.Errorf("error: listing grants for resource %s/%s failed: next page token is the same as the current page token. this is most likely a connector bug",
			rid.ResourceType,
			rid.Resource)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}

// GetMetadata gets all metadata for a connector.
func (b *builderImpl) GetMetadata(ctx context.Context, request *v2.ConnectorServiceGetMetadataRequest) (*v2.ConnectorServiceGetMetadataResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.GetMetadata")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GetMetadataType
	md, err := b.cb.Metadata(ctx)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, err
	}

	md.Capabilities, err = getCapabilities(ctx, b)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, err
	}

	annos := annotations.Annotations(md.Annotations)
	if b.ticketManager != nil {
		annos.Append(&v2.ExternalTicketSettings{Enabled: b.ticketingEnabled})
	}
	md.Annotations = annos

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.ConnectorServiceGetMetadataResponse{Metadata: md}, nil
}

func validateCapabilityDetails(ctx context.Context, credDetails *v2.CredentialDetails) error {
	if credDetails.CapabilityAccountProvisioning != nil {
		// Ensure that the preferred option is included and is part of the supported options
		if credDetails.CapabilityAccountProvisioning.PreferredCredentialOption == v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED {
			return status.Error(codes.InvalidArgument, "error: preferred credential creation option is not set")
		}
		if !slices.Contains(credDetails.CapabilityAccountProvisioning.SupportedCredentialOptions, credDetails.CapabilityAccountProvisioning.PreferredCredentialOption) {
			return status.Error(codes.InvalidArgument, "error: preferred credential creation option is not part of the supported options")
		}
	}

	if credDetails.CapabilityCredentialRotation != nil {
		// Ensure that the preferred option is included and is part of the supported options
		if credDetails.CapabilityCredentialRotation.PreferredCredentialOption == v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED {
			return status.Error(codes.InvalidArgument, "error: preferred credential rotation option is not set")
		}
		if !slices.Contains(credDetails.CapabilityCredentialRotation.SupportedCredentialOptions, credDetails.CapabilityCredentialRotation.PreferredCredentialOption) {
			return status.Error(codes.InvalidArgument, "error: preferred credential rotation option is not part of the supported options")
		}
	}

	return nil
}

func getCredentialDetails(ctx context.Context, b *builderImpl) (*v2.CredentialDetails, error) {
	l := ctxzap.Extract(ctx)
	rv := &v2.CredentialDetails{}

	for _, rb := range b.resourceBuilders {
		if am, ok := rb.(AccountManager); ok {
			accountProvisioningCapabilityDetails, _, err := am.CreateAccountCapabilityDetails(ctx)
			if err != nil {
				l.Error("error: getting account provisioning details", zap.Error(err))
				return nil, fmt.Errorf("error: getting account provisioning details: %w", err)
			}
			rv.CapabilityAccountProvisioning = accountProvisioningCapabilityDetails
		}

		if cm, ok := rb.(CredentialManager); ok {
			credentialRotationCapabilityDetails, _, err := cm.RotateCapabilityDetails(ctx)
			if err != nil {
				l.Error("error: getting credential management details", zap.Error(err))
				return nil, fmt.Errorf("error: getting credential management details: %w", err)
			}
			rv.CapabilityCredentialRotation = credentialRotationCapabilityDetails
		}
	}

	err := validateCapabilityDetails(ctx, rv)
	if err != nil {
		return nil, fmt.Errorf("error: validating capability details: %w", err)
	}
	return rv, nil
}

// getCapabilities gets all capabilities for a connector.
func getCapabilities(ctx context.Context, b *builderImpl) (*v2.ConnectorCapabilities, error) {
	connectorCaps := make(map[v2.Capability]struct{})
	resourceTypeCapabilities := []*v2.ResourceTypeCapability{}
	for _, rb := range b.resourceBuilders {
		resourceTypeCapability := &v2.ResourceTypeCapability{
			ResourceType: rb.ResourceType(ctx),
			// Currently by default all resource types support sync.
			Capabilities: []v2.Capability{v2.Capability_CAPABILITY_SYNC},
		}
		connectorCaps[v2.Capability_CAPABILITY_SYNC] = struct{}{}
		if _, ok := rb.(ResourceProvisioner); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_PROVISION)
			connectorCaps[v2.Capability_CAPABILITY_PROVISION] = struct{}{}
		} else if _, ok = rb.(ResourceProvisionerV2); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_PROVISION)
			connectorCaps[v2.Capability_CAPABILITY_PROVISION] = struct{}{}
		}
		if _, ok := rb.(AccountManager); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_ACCOUNT_PROVISIONING)
			connectorCaps[v2.Capability_CAPABILITY_ACCOUNT_PROVISIONING] = struct{}{}
		}

		if _, ok := rb.(CredentialManager); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_CREDENTIAL_ROTATION)
			connectorCaps[v2.Capability_CAPABILITY_CREDENTIAL_ROTATION] = struct{}{}
		}

		if _, ok := rb.(ResourceManager); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_RESOURCE_CREATE, v2.Capability_CAPABILITY_RESOURCE_DELETE)
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_CREATE] = struct{}{}
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_DELETE] = struct{}{}
		} else if _, ok := rb.(ResourceDeleter); ok {
			resourceTypeCapability.Capabilities = append(resourceTypeCapability.Capabilities, v2.Capability_CAPABILITY_RESOURCE_DELETE)
			connectorCaps[v2.Capability_CAPABILITY_RESOURCE_DELETE] = struct{}{}
		}

		resourceTypeCapabilities = append(resourceTypeCapabilities, resourceTypeCapability)
	}
	sort.Slice(resourceTypeCapabilities, func(i, j int) bool {
		return resourceTypeCapabilities[i].ResourceType.GetId() < resourceTypeCapabilities[j].ResourceType.GetId()
	})

	if b.eventFeed != nil {
		connectorCaps[v2.Capability_CAPABILITY_EVENT_FEED] = struct{}{}
	}

	if b.ticketManager != nil {
		connectorCaps[v2.Capability_CAPABILITY_TICKETING] = struct{}{}
	}

	if b.actionManager != nil {
		connectorCaps[v2.Capability_CAPABILITY_ACTIONS] = struct{}{}
	}

	var caps []v2.Capability
	for c := range connectorCaps {
		caps = append(caps, c)
	}
	slices.Sort(caps)

	credDetails, err := getCredentialDetails(ctx, b)
	if err != nil {
		return nil, err
	}

	return &v2.ConnectorCapabilities{
		ResourceTypeCapabilities: resourceTypeCapabilities,
		ConnectorCapabilities:    caps,
		CredentialDetails:        credDetails,
	}, nil
}

// Validate validates the connector.
func (b *builderImpl) Validate(ctx context.Context, request *v2.ConnectorServiceValidateRequest) (*v2.ConnectorServiceValidateResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.Validate")
	defer span.End()

	annos, err := b.cb.Validate(ctx)
	if err != nil {
		return nil, err
	}

	return &v2.ConnectorServiceValidateResponse{Annotations: annos}, nil
}

func (b *builderImpl) Grant(ctx context.Context, request *v2.GrantManagerServiceGrantRequest) (*v2.GrantManagerServiceGrantResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.Grant")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.GrantType
	l := ctxzap.Extract(ctx)

	rt := request.Entitlement.Resource.Id.ResourceType
	provisioner, ok := b.resourceProvisioners[rt]
	if ok {
		annos, err := provisioner.Grant(ctx, request.Principal, request.Entitlement)
		if err != nil {
			l.Error("error: grant failed", zap.Error(err))
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: grant failed: %w", err)
		}

		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return &v2.GrantManagerServiceGrantResponse{Annotations: annos}, nil
	}

	provisionerV2, ok := b.resourceProvisionersV2[rt]
	if ok {
		grants, annos, err := provisionerV2.Grant(ctx, request.Principal, request.Entitlement)
		if err != nil {
			l.Error("error: grant failed", zap.Error(err))
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: grant failed: %w", err)
		}

		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return &v2.GrantManagerServiceGrantResponse{Annotations: annos, Grants: grants}, nil
	}

	l.Error("error: resource type does not have provisioner configured", zap.String("resource_type", rt))
	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, fmt.Errorf("error: resource type does not have provisioner configured")
}

func (b *builderImpl) Revoke(ctx context.Context, request *v2.GrantManagerServiceRevokeRequest) (*v2.GrantManagerServiceRevokeResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.Revoke")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.RevokeType

	l := ctxzap.Extract(ctx)

	rt := request.Grant.Entitlement.Resource.Id.ResourceType
	provisioner, ok := b.resourceProvisioners[rt]
	if ok {
		annos, err := provisioner.Revoke(ctx, request.Grant)
		if err != nil {
			l.Error("error: revoke failed", zap.Error(err))
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: revoke failed: %w", err)
		}
		return &v2.GrantManagerServiceRevokeResponse{Annotations: annos}, nil
	}

	provisionerV2, ok := b.resourceProvisionersV2[rt]
	if ok {
		annos, err := provisionerV2.Revoke(ctx, request.Grant)
		if err != nil {
			l.Error("error: revoke failed", zap.Error(err))
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: revoke failed: %w", err)
		}

		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return &v2.GrantManagerServiceRevokeResponse{Annotations: annos}, nil
	}

	l.Error("error: resource type does not have provisioner configured", zap.String("resource_type", rt))
	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, status.Error(codes.Unimplemented, "resource type does not have provisioner configured")
}

// GetAsset streams the asset to the client.
// FIXME(jirwin): Asset streaming is disabled.
func (b *builderImpl) GetAsset(request *v2.AssetServiceGetAssetRequest, server v2.AssetService_GetAssetServer) error {
	_, span := tracer.Start(server.Context(), "builderImpl.GetAsset")
	defer span.End()

	return nil
}

func (b *builderImpl) ListEvents(ctx context.Context, request *v2.ListEventsRequest) (*v2.ListEventsResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.ListEvents")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ListEventsType
	if b.eventFeed == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: event feed not implemented")
	}
	events, streamState, annotations, err := b.eventFeed.ListEvents(ctx, request.StartAt, &pagination.StreamToken{
		Size:   int(request.PageSize),
		Cursor: request.Cursor,
	})
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing events failed: %w", err)
	}
	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.ListEventsResponse{
		Events:      events,
		Cursor:      streamState.Cursor,
		HasMore:     streamState.HasMore,
		Annotations: annotations,
	}, nil
}

func (b *builderImpl) CreateResource(ctx context.Context, request *v2.CreateResourceRequest) (*v2.CreateResourceResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.CreateResource")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.CreateResourceType
	l := ctxzap.Extract(ctx)
	rt := request.GetResource().GetId().GetResourceType()
	manager, ok := b.resourceManagers[rt]
	if ok {
		resource, annos, err := manager.Create(ctx, request.Resource)
		if err != nil {
			l.Error("error: create resource failed", zap.Error(err))
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: create resource failed: %w", err)
		}
		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return &v2.CreateResourceResponse{Created: resource, Annotations: annos}, nil
	}
	l.Error("error: resource type does not have resource Create() configured", zap.String("resource_type", rt))
	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, status.Error(codes.Unimplemented, fmt.Sprintf("resource type %s does not have resource Create() configured", rt))
}

func (b *builderImpl) DeleteResource(ctx context.Context, request *v2.DeleteResourceRequest) (*v2.DeleteResourceResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.DeleteResource")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.DeleteResourceType

	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	var manager ResourceDeleter
	var ok bool
	manager, ok = b.resourceManagers[rt]
	if !ok {
		manager, ok = b.resourceDeleters[rt]
	}
	if ok {
		annos, err := manager.Delete(ctx, request.GetResourceId())
		if err != nil {
			l.Error("error: delete resource failed", zap.Error(err))
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: delete resource failed: %w", err)
		}
		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return &v2.DeleteResourceResponse{Annotations: annos}, nil
	}
	l.Error("error: resource type does not have resource Delete() configured", zap.String("resource_type", rt))
	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, status.Error(codes.Unimplemented, fmt.Sprintf("resource type %s does not have resource Delete() configured", rt))
}

func (b *builderImpl) DeleteResourceV2(ctx context.Context, request *v2.DeleteResourceV2Request) (*v2.DeleteResourceV2Response, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.DeleteResourceV2")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.DeleteResourceType

	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	var manager ResourceDeleter
	var ok bool
	manager, ok = b.resourceManagers[rt]
	if !ok {
		manager, ok = b.resourceDeleters[rt]
	}
	if ok {
		annos, err := manager.Delete(ctx, request.GetResourceId())
		if err != nil {
			l.Error("error: delete resource failed", zap.Error(err))
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, fmt.Errorf("error: delete resource failed: %w", err)
		}
		b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
		return &v2.DeleteResourceV2Response{Annotations: annos}, nil
	}
	l.Error("error: resource type does not have resource Delete() configured", zap.String("resource_type", rt))
	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, status.Error(codes.Unimplemented, fmt.Sprintf("resource type %s does not have resource Delete() configured", rt))
}

func (b *builderImpl) RotateCredential(ctx context.Context, request *v2.RotateCredentialRequest) (*v2.RotateCredentialResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.RotateCredential")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.RotateCredentialsType
	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	manager, ok := b.credentialManagers[rt]
	if !ok {
		l.Error("error: resource type does not have credential manager configured", zap.String("resource_type", rt))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Error(codes.Unimplemented, "resource type does not have credential manager configured")
	}

	plaintexts, annos, err := manager.Rotate(ctx, request.GetResourceId(), request.GetCredentialOptions())
	if err != nil {
		l.Error("error: rotate credentials on resource failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: rotate credentials on resource failed: %w", err)
	}

	pkem, err := crypto.NewEncryptionManager(request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: creating encryption manager failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: creating encryption manager failed: %w", err)
	}

	var encryptedDatas []*v2.EncryptedData
	for _, plaintextCredential := range plaintexts {
		encryptedData, err := pkem.Encrypt(ctx, plaintextCredential)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, err
		}
		encryptedDatas = append(encryptedDatas, encryptedData...)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.RotateCredentialResponse{
		Annotations:   annos,
		ResourceId:    request.GetResourceId(),
		EncryptedData: encryptedDatas,
	}, nil
}

func (b *builderImpl) Cleanup(ctx context.Context, request *v2.ConnectorServiceCleanupRequest) (*v2.ConnectorServiceCleanupResponse, error) {
	l := ctxzap.Extract(ctx)
	// Clear all http caches at the end of a sync. This must be run in the child process, which is why it's in this function and not in syncer.go
	err := uhttp.ClearCaches(ctx)
	if err != nil {
		l.Warn("error clearing http caches", zap.Error(err))
	}
	resp := &v2.ConnectorServiceCleanupResponse{}
	return resp, err
}

func (b *builderImpl) CreateAccount(ctx context.Context, request *v2.CreateAccountRequest) (*v2.CreateAccountResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.CreateAccount")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.CreateAccountType
	l := ctxzap.Extract(ctx)
	if b.accountManager == nil {
		l.Error("error: connector does not have account manager configured")
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Error(codes.Unimplemented, "connector does not have credential manager configured")
	}
	result, plaintexts, annos, err := b.accountManager.CreateAccount(ctx, request.GetAccountInfo(), request.GetCredentialOptions())
	if err != nil {
		l.Error("error: create account failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: create account failed: %w", err)
	}

	pkem, err := crypto.NewEncryptionManager(request.GetCredentialOptions(), request.GetEncryptionConfigs())
	if err != nil {
		l.Error("error: creating encryption manager failed", zap.Error(err))
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: creating encryption manager failed: %w", err)
	}

	var encryptedDatas []*v2.EncryptedData
	for _, plaintextCredential := range plaintexts {
		encryptedData, err := pkem.Encrypt(ctx, plaintextCredential)
		if err != nil {
			b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
			return nil, err
		}
		encryptedDatas = append(encryptedDatas, encryptedData...)
	}

	rv := &v2.CreateAccountResponse{
		EncryptedData: encryptedDatas,
		Annotations:   annos,
	}

	switch r := result.(type) {
	case *v2.CreateAccountResponse_SuccessResult:
		rv.Result = &v2.CreateAccountResponse_Success{Success: r}
	case *v2.CreateAccountResponse_ActionRequiredResult:
		rv.Result = &v2.CreateAccountResponse_ActionRequired{ActionRequired: r}
	default:
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, status.Error(codes.Unimplemented, fmt.Sprintf("unknown result type: %T", result))
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builderImpl) ListActionSchemas(ctx context.Context, request *v2.ListActionSchemasRequest) (*v2.ListActionSchemasResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.ListActionSchemas")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionListSchemasType
	if b.actionManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: action manager not implemented")
	}

	actionSchemas, annos, err := b.actionManager.ListActionSchemas(ctx)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing action schemas failed: %w", err)
	}

	rv := &v2.ListActionSchemasResponse{
		Schemas:     actionSchemas,
		Annotations: annos,
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builderImpl) GetActionSchema(ctx context.Context, request *v2.GetActionSchemaRequest) (*v2.GetActionSchemaResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.GetActionSchema")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionGetSchemaType
	if b.actionManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: action manager not implemented")
	}

	actionSchema, annos, err := b.actionManager.GetActionSchema(ctx, request.GetName())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: getting action schema failed: %w", err)
	}

	rv := &v2.GetActionSchemaResponse{
		Schema:      actionSchema,
		Annotations: annos,
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builderImpl) InvokeAction(ctx context.Context, request *v2.InvokeActionRequest) (*v2.InvokeActionResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.InvokeAction")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionInvokeType
	if b.actionManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: action manager not implemented")
	}

	id, status, resp, annos, err := b.actionManager.InvokeAction(ctx, request.GetName(), request.GetArgs())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: invoking action failed: %w", err)
	}

	rv := &v2.InvokeActionResponse{
		Id:          id,
		Name:        request.GetName(),
		Status:      status,
		Annotations: annos,
		Response:    resp,
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return rv, nil
}

func (b *builderImpl) GetActionStatus(ctx context.Context, request *v2.GetActionStatusRequest) (*v2.GetActionStatusResponse, error) {
	ctx, span := tracer.Start(ctx, "builderImpl.GetActionStatus")
	defer span.End()

	start := b.nowFunc()
	tt := tasks.ActionStatusType
	if b.actionManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: action manager not implemented")
	}

	status, name, rv, annos, err := b.actionManager.GetActionStatus(ctx, request.GetId())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: getting action status failed: %w", err)
	}

	resp := &v2.GetActionStatusResponse{
		Id:          request.GetId(),
		Name:        name,
		Status:      status,
		Annotations: annos,
		Response:    rv,
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return resp, nil
}
