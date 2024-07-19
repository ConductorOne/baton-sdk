package connectorbuilder

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/metrics"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-sdk/pkg/types/tasks"
)

type ResourceSyncer interface {
	ResourceType(ctx context.Context) *v2.ResourceType
	List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error)
	Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error)
	Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error)
}

type ResourceProvisioner interface {
	ResourceType(ctx context.Context) *v2.ResourceType
	Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error)
	Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error)
}

type ResourceProvisionerV2 interface {
	ResourceType(ctx context.Context) *v2.ResourceType
	Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error)
	Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error)
}

type ResourceManager interface {
	Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error)
	Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error)
}

type CreateAccountResponse interface {
	proto.Message
	GetIsCreateAccountResult() bool
}

type AccountManager interface {
	CreateAccount(ctx context.Context, accountInfo *v2.AccountInfo, credentialOptions *v2.CredentialOptions) (CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error)
}

type CredentialManager interface {
	Rotate(ctx context.Context, resourceId *v2.ResourceId, credentialOptions *v2.CredentialOptions) ([]*v2.PlaintextData, annotations.Annotations, error)
}

type EventProvider interface {
	ListEvents(ctx context.Context, earliestEvent *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error)
}

type TicketManager interface {
	GetTicket(ctx context.Context, ticketId string) (*v2.Ticket, annotations.Annotations, error)
	CreateTicket(ctx context.Context, ticket *v2.Ticket, schema *v2.TicketSchema) (*v2.Ticket, annotations.Annotations, error)
	GetTicketSchema(ctx context.Context, schemaID string) (*v2.TicketSchema, annotations.Annotations, error)
	ListTicketSchemas(ctx context.Context, pToken *pagination.Token) ([]*v2.TicketSchema, string, annotations.Annotations, error)
}

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
	accountManager         AccountManager
	credentialManagers     map[string]CredentialManager
	eventFeed              EventProvider
	cb                     ConnectorBuilder
	ticketManager          TicketManager
	ticketingEnabled       bool
	m                      *metrics.M
	nowFunc                func() time.Time
}

func (b *builderImpl) ListTicketSchemas(ctx context.Context, request *v2.TicketsServiceListTicketSchemasRequest) (*v2.TicketsServiceListTicketSchemasResponse, error) {
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
		Type:         reqBody.GetType(),
		Labels:       reqBody.GetLabels(),
		CustomFields: reqBody.GetCustomFields(),
		RequestedFor: reqBody.GetRequestedFor(),
	}

	ticket, annos, err := b.ticketManager.CreateTicket(ctx, cTicket, request.GetSchema())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: creating ticket failed: %w", err)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.TicketsServiceCreateTicketResponse{
		Ticket:      ticket,
		Annotations: annos,
	}, nil
}

func (b *builderImpl) GetTicket(ctx context.Context, request *v2.TicketsServiceGetTicketRequest) (*v2.TicketsServiceGetTicketResponse, error) {
	start := b.nowFunc()
	tt := tasks.GetTicketType
	if b.ticketManager == nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: ticket manager not implemented")
	}

	ticket, annos, err := b.ticketManager.GetTicket(ctx, request.GetId())
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: getting ticket failed: %w", err)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.TicketsServiceGetTicketResponse{
		Ticket:      ticket,
		Annotations: annos,
	}, nil
}

func (b *builderImpl) GetTicketSchema(ctx context.Context, request *v2.TicketsServiceGetTicketSchemaRequest) (*v2.TicketsServiceGetTicketSchemaResponse, error) {
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
			accountManager:         nil,
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

			if resourceManagers, ok := rb.(ResourceManager); ok {
				if _, ok := ret.resourceManagers[rType.Id]; ok {
					return nil, fmt.Errorf("error: duplicate resource type found for resource manager %s", rType.Id)
				}
				ret.resourceManagers[rType.Id] = resourceManagers
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
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing resources failed: %w", err)
	}
	if request.PageToken != "" && request.PageToken == nextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing resources failed: next page token is the same as the current page token. this is most likely a connector bug")
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.ResourcesServiceListResourcesResponse{
		List:          out,
		NextPageToken: nextPageToken,
		Annotations:   annos,
	}, nil
}

// ListEntitlements returns all the entitlements for a given resource.
func (b *builderImpl) ListEntitlements(ctx context.Context, request *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
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
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing entitlements failed: %w", err)
	}
	if request.PageToken != "" && request.PageToken == nextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing entitlements failed: next page token is the same as the current page token. this is most likely a connector bug")
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.EntitlementsServiceListEntitlementsResponse{
		List:          out,
		NextPageToken: nextPageToken,
		Annotations:   annos,
	}, nil
}

// ListGrants lists all the grants for a given resource.
func (b *builderImpl) ListGrants(ctx context.Context, request *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
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
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing grants for resource %s/%s failed: %w", rid.ResourceType, rid.Resource, err)
	}
	if request.PageToken != "" && request.PageToken == nextPageToken {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, fmt.Errorf("error: listing grants for resource %s/%s failed: next page token is the same as the current page token. this is most likely a connector bug",
			rid.ResourceType,
			rid.Resource)
	}

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.GrantsServiceListGrantsResponse{
		List:          out,
		NextPageToken: nextPageToken,
		Annotations:   annos,
	}, nil
}

// GetMetadata gets all metadata for a connector.
func (b *builderImpl) GetMetadata(ctx context.Context, request *v2.ConnectorServiceGetMetadataRequest) (*v2.ConnectorServiceGetMetadataResponse, error) {
	start := b.nowFunc()
	tt := tasks.GetMetadataType
	md, err := b.cb.Metadata(ctx)
	if err != nil {
		b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
		return nil, err
	}

	md.Capabilities = getCapabilities(ctx, b)

	annos := annotations.Annotations(md.Annotations)
	if b.ticketManager != nil {
		annos.Append(&v2.ExternalTicketSettings{Enabled: b.ticketingEnabled})
	}
	md.Annotations = annos

	b.m.RecordTaskSuccess(ctx, tt, b.nowFunc().Sub(start))
	return &v2.ConnectorServiceGetMetadataResponse{Metadata: md}, nil
}

// getCapabilities gets all capabilities for a connector.
func getCapabilities(ctx context.Context, b *builderImpl) *v2.ConnectorCapabilities {
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

	var caps []v2.Capability
	for c := range connectorCaps {
		caps = append(caps, c)
	}

	return &v2.ConnectorCapabilities{
		ResourceTypeCapabilities: resourceTypeCapabilities,
		ConnectorCapabilities:    caps,
	}
}

// Validate validates the connector.
func (b *builderImpl) Validate(ctx context.Context, request *v2.ConnectorServiceValidateRequest) (*v2.ConnectorServiceValidateResponse, error) {
	annos, err := b.cb.Validate(ctx)
	if err != nil {
		return nil, err
	}

	return &v2.ConnectorServiceValidateResponse{Annotations: annos}, nil
}

func (b *builderImpl) Grant(ctx context.Context, request *v2.GrantManagerServiceGrantRequest) (*v2.GrantManagerServiceGrantResponse, error) {
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
	return nil
}

func (b *builderImpl) ListEvents(ctx context.Context, request *v2.ListEventsRequest) (*v2.ListEventsResponse, error) {
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
	l.Error("error: resource type does not have resource manager configured", zap.String("resource_type", rt))
	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, status.Error(codes.Unimplemented, "resource type does not have resource manager configured")
}

func (b *builderImpl) DeleteResource(ctx context.Context, request *v2.DeleteResourceRequest) (*v2.DeleteResourceResponse, error) {
	start := b.nowFunc()
	tt := tasks.DeleteResourceType

	l := ctxzap.Extract(ctx)
	rt := request.GetResourceId().GetResourceType()
	manager, ok := b.resourceManagers[rt]
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
	l.Error("error: resource type does not have resource manager configured", zap.String("resource_type", rt))
	b.m.RecordTaskFailure(ctx, tt, b.nowFunc().Sub(start))
	return nil, status.Error(codes.Unimplemented, "resource type does not have resource manager configured")
}

func (b *builderImpl) RotateCredential(ctx context.Context, request *v2.RotateCredentialRequest) (*v2.RotateCredentialResponse, error) {
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

func (b *builderImpl) CreateAccount(ctx context.Context, request *v2.CreateAccountRequest) (*v2.CreateAccountResponse, error) {
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
