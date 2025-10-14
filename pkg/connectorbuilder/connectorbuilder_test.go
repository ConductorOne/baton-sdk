package connectorbuilder

import (
	"context"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testConnector struct {
	resourceSyncers []ResourceSyncer
}

func newTestConnector(resourceSyncers []ResourceSyncer) ConnectorBuilder {
	return &testConnector{resourceSyncers: resourceSyncers}
}

func (t *testConnector) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{
		DisplayName: "test-connector",
		Description: "A test connector",
	}, nil
}

func (t *testConnector) Validate(ctx context.Context) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

func (t *testConnector) ResourceSyncers(ctx context.Context) []ResourceSyncer {
	return t.resourceSyncers
}

type testResourceSyncer struct {
	resourceType *v2.ResourceType
}

func newTestResourceSyncer(resourceType string) ResourceSyncer {
	return &testResourceSyncer{
		resourceType: &v2.ResourceType{
			Id:          resourceType,
			DisplayName: "Test " + resourceType,
		},
	}
}

func (t *testResourceSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return t.resourceType
}

func (t *testResourceSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	return []*v2.Resource{
		{
			Id: &v2.ResourceId{
				ResourceType: t.resourceType.Id,
				Resource:     "test-resource-1",
			},
			DisplayName: "Test Resource 1",
		},
	}, "", annotations.Annotations{}, nil
}

func (t *testResourceSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return []*v2.Entitlement{
		{
			Resource:    resource,
			Id:          "test-entitlement",
			DisplayName: "Test Entitlement",
		},
	}, "", annotations.Annotations{}, nil
}

func (t *testResourceSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return []*v2.Grant{
		{
			Entitlement: &v2.Entitlement{
				Resource:    resource,
				Id:          "test-entitlement",
				DisplayName: "Test Entitlement",
			},
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     "test-user",
				},
				DisplayName: "Test User",
			},
			Id: "test-grant-1",
		},
	}, "", annotations.Annotations{}, nil
}

type testResourceSyncerV2WithTargetedSync struct {
	resourceType *v2.ResourceType
}

func newTestResourceSyncerV2WithTargetedSync(resourceType string) ResourceSyncerV2 {
	return &testResourceSyncerV2WithTargetedSync{
		resourceType: &v2.ResourceType{
			Id:          resourceType,
			DisplayName: "Test " + resourceType,
		},
	}
}

func (t *testResourceSyncerV2WithTargetedSync) ResourceType(ctx context.Context) *v2.ResourceType {
	return t.resourceType
}

func (t *testResourceSyncerV2WithTargetedSync) List(
	ctx context.Context,
	parentResourceID *v2.ResourceId,
	pToken *pagination.Token,
	opts resource.Options,
) ([]*v2.Resource, string, annotations.Annotations, error) {
	return []*v2.Resource{
		{
			Id: &v2.ResourceId{
				ResourceType: t.resourceType.Id,
				Resource:     "test-resource-1",
			},
			DisplayName: "Test Resource 1",
		},
	}, "", annotations.Annotations{}, nil
}

func (t *testResourceSyncerV2WithTargetedSync) Entitlements(
	ctx context.Context,
	resource *v2.Resource,
	pToken *pagination.Token,
	opts resource.Options,
) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return []*v2.Entitlement{
		{
			Resource:    resource,
			Id:          "test-entitlement",
			DisplayName: "Test Entitlement",
		},
	}, "", annotations.Annotations{}, nil
}

func (t *testResourceSyncerV2WithTargetedSync) Grants(
	ctx context.Context,
	resource *v2.Resource,
	pToken *pagination.Token,
	opts resource.Options,
) ([]*v2.Grant, string, annotations.Annotations, error) {
	return []*v2.Grant{
		{
			Entitlement: &v2.Entitlement{
				Resource:    resource,
				Id:          "test-entitlement",
				DisplayName: "Test Entitlement",
			},
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     "test-user",
				},
				DisplayName: "Test User",
			},
			Id: "test-grant-1",
		},
	}, "", annotations.Annotations{}, nil
}

func (t *testResourceSyncerV2WithTargetedSync) Get(
	ctx context.Context,
	resourceId *v2.ResourceId,
	parentResourceId *v2.ResourceId,
) (*v2.Resource, annotations.Annotations, error) {
	return &v2.Resource{
		Id:          resourceId,
		DisplayName: "V2 Targeted Resource " + resourceId.Resource,
	}, annotations.Annotations{}, nil
}

type testResourceManager struct {
	ResourceSyncer
}

func newTestResourceManager(resourceType string) ResourceManager {
	return &testResourceManager{newTestResourceSyncer(resourceType)}
}

func (t *testResourceManager) Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error) {
	return &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: t.ResourceType(ctx).Id,
			Resource:     "created-" + resource.DisplayName,
		},
		DisplayName: resource.DisplayName,
	}, annotations.Annotations{}, nil
}

func (t *testResourceManager) Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

type testResourceManagerV2 struct {
	ResourceSyncer
}

func newTestResourceManagerV2(resourceType string) ResourceManagerV2 {
	return &testResourceManagerV2{newTestResourceSyncer(resourceType)}
}

func (t *testResourceManagerV2) Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error) {
	return &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: t.ResourceType(ctx).Id,
			Resource:     "created-v2-" + resource.DisplayName,
		},
		DisplayName: resource.DisplayName,
	}, annotations.Annotations{}, nil
}

func (t *testResourceManagerV2) Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

type testResourceDeleter struct {
	ResourceSyncer
}

func newTestResourceDeleter(resourceType string) ResourceDeleter {
	return &testResourceDeleter{newTestResourceSyncer(resourceType)}
}

func (t *testResourceDeleter) Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

type testResourceDeleterV2 struct {
	ResourceSyncer
}

func newTestResourceDeleterV2(resourceType string) ResourceDeleterV2 {
	return &testResourceDeleterV2{newTestResourceSyncer(resourceType)}
}

func (t *testResourceDeleterV2) Delete(ctx context.Context, resourceId *v2.ResourceId, parentResourceID *v2.ResourceId) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

type testResourceProvisioner struct {
	ResourceSyncer
}

func newTestResourceProvisioner(resourceType string) ResourceProvisioner {
	return &testResourceProvisioner{newTestResourceSyncer(resourceType)}
}

func (t *testResourceProvisioner) Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

func (t *testResourceProvisioner) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

type testResourceProvisionerV2 struct {
	ResourceSyncer
}

func newTestResourceProvisionerV2(resourceType string) ResourceProvisionerV2 {
	return &testResourceProvisionerV2{newTestResourceSyncer(resourceType)}
}

type testResourceTargetedSyncer struct {
	ResourceSyncer
}

func newTestResourceTargetedSyncer(resourceType string) ResourceTargetedSyncer {
	return &testResourceTargetedSyncer{newTestResourceSyncer(resourceType)}
}

func (t *testResourceTargetedSyncer) Get(ctx context.Context, resourceId *v2.ResourceId, parentResourceId *v2.ResourceId) (*v2.Resource, annotations.Annotations, error) {
	return &v2.Resource{
		Id:          resourceId,
		DisplayName: "Targeted Resource " + resourceId.Resource,
	}, annotations.Annotations{}, nil
}

func (t *testResourceProvisionerV2) Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	return []*v2.Grant{
		{
			Entitlement: entitlement,
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     "test-user",
				},
				DisplayName: "Test User",
			},
			Id: "granted-grant-1",
		},
	}, annotations.Annotations{}, nil
}

func (t *testResourceProvisionerV2) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

type testAccountManager struct {
	ResourceSyncer
}

func newTestAccountManager(resourceType string) AccountManager {
	return &testAccountManager{newTestResourceSyncer(resourceType)}
}

func (t *testAccountManager) CreateAccount(
	ctx context.Context,
	accountInfo *v2.AccountInfo,
	credentialOptions *v2.LocalCredentialOptions,
) (CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error) {
	r := &v2.CreateAccountResponse_SuccessResult{
		IsCreateAccountResult: true,
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: t.ResourceType(ctx).Id,
				Resource:     "created-account",
			},
			DisplayName: "Test User",
		},
	}
	return r, []*v2.PlaintextData{}, annotations.Annotations{}, nil
}

func (t *testAccountManager) CreateAccountCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return nil, annotations.Annotations{}, nil
}

type testCredentialManager struct {
	ResourceSyncer
}

func newTestCredentialManager(resourceType string) CredentialManager {
	return &testCredentialManager{
		ResourceSyncer: newTestResourceSyncer(resourceType),
	}
}

func (t *testCredentialManager) Rotate(ctx context.Context, resourceId *v2.ResourceId, credentialOptions *v2.LocalCredentialOptions) ([]*v2.PlaintextData, annotations.Annotations, error) {
	return []*v2.PlaintextData{
		{
			Name:        "password",
			Description: "User password",
			Bytes:       []byte("new-password"),
		},
	}, annotations.Annotations{}, nil
}

func (t *testCredentialManager) RotateCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsCredentialRotation, annotations.Annotations, error) {
	return nil, annotations.Annotations{}, nil
}

type testEventProvider struct {
	ConnectorBuilder
}

func newTestEventProvider() EventProvider {
	return &testEventProvider{newTestConnector([]ResourceSyncer{})}
}

func (t *testEventProvider) ListEvents(
	ctx context.Context,
	earliestEvent *timestamppb.Timestamp,
	pToken *pagination.StreamToken,
) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	return []*v2.Event{
		{
			Id:         "test-event-1",
			OccurredAt: timestamppb.New(time.Now()),
		},
	}, &pagination.StreamState{}, annotations.Annotations{}, nil
}

type testEventProviderV2 struct {
	ConnectorBuilder
}

func newTestEventProviderV2() EventProviderV2 {
	return &testEventProviderV2{newTestConnector([]ResourceSyncer{})}
}

func (t *testEventProviderV2) EventFeeds(ctx context.Context) []EventFeed {
	return []EventFeed{
		&testEventFeed{},
	}
}

type testEventFeed struct{}

func (t *testEventFeed) EventFeedMetadata(ctx context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id: "test-feed",
	}
}

func (t *testEventFeed) ListEvents(ctx context.Context, earliestEvent *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	return []*v2.Event{
		{
			Id:         "test-event-v2-1",
			OccurredAt: timestamppb.New(time.Now()),
		},
	}, &pagination.StreamState{}, annotations.Annotations{}, nil
}

type testTicketManager struct {
	ConnectorBuilder
}

func newTestTicketManager() TicketManager {
	return &testTicketManager{newTestConnector([]ResourceSyncer{})}
}

func (t *testTicketManager) GetTicket(ctx context.Context, ticketId string) (*v2.Ticket, annotations.Annotations, error) {
	return &v2.Ticket{
		Id:          ticketId,
		DisplayName: "Test Ticket",
	}, annotations.Annotations{}, nil
}

func (t *testTicketManager) CreateTicket(ctx context.Context, ticket *v2.Ticket, schema *v2.TicketSchema) (*v2.Ticket, annotations.Annotations, error) {
	return &v2.Ticket{
		Id:          "created-" + ticket.DisplayName,
		DisplayName: ticket.DisplayName,
	}, annotations.Annotations{}, nil
}

func (t *testTicketManager) GetTicketSchema(ctx context.Context, schemaID string) (*v2.TicketSchema, annotations.Annotations, error) {
	return &v2.TicketSchema{
		Id:          schemaID,
		DisplayName: "Test Schema",
	}, annotations.Annotations{}, nil
}

func (t *testTicketManager) ListTicketSchemas(ctx context.Context, pToken *pagination.Token) ([]*v2.TicketSchema, string, annotations.Annotations, error) {
	return []*v2.TicketSchema{
		{
			Id:          "schema-1",
			DisplayName: "Test Schema 1",
		},
	}, "", annotations.Annotations{}, nil
}

func (t *testTicketManager) BulkCreateTickets(ctx context.Context, request *v2.TicketsServiceBulkCreateTicketsRequest) (*v2.TicketsServiceBulkCreateTicketsResponse, error) {
	return &v2.TicketsServiceBulkCreateTicketsResponse{
		Tickets: []*v2.TicketsServiceCreateTicketResponse{
			{
				Ticket: &v2.Ticket{
					Id:          "bulk-ticket-1",
					DisplayName: "Bulk Ticket 1",
				},
			},
		},
	}, nil
}

func (t *testTicketManager) BulkGetTickets(ctx context.Context, request *v2.TicketsServiceBulkGetTicketsRequest) (*v2.TicketsServiceBulkGetTicketsResponse, error) {
	return &v2.TicketsServiceBulkGetTicketsResponse{
		Tickets: []*v2.TicketsServiceGetTicketResponse{
			{
				Ticket: &v2.Ticket{
					Id:          "bulk-ticket-1",
					DisplayName: "Bulk Ticket 1",
				},
			},
		},
	}, nil
}

type testCustomActionManager struct{}

func newTestCustomActionManager() CustomActionManager {
	return &testCustomActionManager{}
}

func (t *testCustomActionManager) ListActionSchemas(ctx context.Context) ([]*v2.BatonActionSchema, annotations.Annotations, error) {
	return []*v2.BatonActionSchema{
		{
			Name:        "test-action",
			DisplayName: "Test Action",
		},
	}, annotations.Annotations{}, nil
}

func (t *testCustomActionManager) GetActionSchema(ctx context.Context, name string) (*v2.BatonActionSchema, annotations.Annotations, error) {
	return &v2.BatonActionSchema{
		Name:        name,
		DisplayName: "Test Action Schema",
	}, annotations.Annotations{}, nil
}

func (t *testCustomActionManager) InvokeAction(ctx context.Context, name string, args *structpb.Struct) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	return "action-id-123", v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, &structpb.Struct{}, annotations.Annotations{}, nil
}

func (t *testCustomActionManager) GetActionStatus(ctx context.Context, id string) (v2.BatonActionStatus, string, *structpb.Struct, annotations.Annotations, error) {
	return v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, "Action completed successfully", &structpb.Struct{}, annotations.Annotations{}, nil
}

type testRegisterActionManager struct {
	ConnectorBuilder
}

func newTestRegisterActionManager() RegisterActionManager {
	return &testRegisterActionManager{newTestConnector([]ResourceSyncer{})}
}

func (t *testRegisterActionManager) RegisterActionManager(ctx context.Context) (CustomActionManager, error) {
	return newTestCustomActionManager(), nil
}

func TestConnectorBuilder(t *testing.T) {
	ctx := context.Background()

	connector := newTestConnector([]ResourceSyncer{})
	server, err := NewConnector(ctx, connector)
	require.NoError(t, err)

	// Test Metadata
	metadata, err := server.GetMetadata(ctx, &v2.ConnectorServiceGetMetadataRequest{})
	require.NoError(t, err)
	require.Equal(t, "test-connector", metadata.Metadata.DisplayName)

	// Test Validate
	validateResp, err := server.Validate(ctx, &v2.ConnectorServiceValidateRequest{})
	require.NoError(t, err)
	require.NotNil(t, validateResp)
}

func TestResourceSyncer(t *testing.T) {
	ctx := context.Background()

	rsSyncer := newTestResourceSyncer("test-resource")
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	// Test List
	listResp, err := connector.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: "test-resource",
	})
	require.NoError(t, err)
	require.Len(t, listResp.List, 1)
	require.Equal(t, "test-resource-1", listResp.List[0].Id.Resource)

	// Test Entitlements
	entitlementsResp, err := connector.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "test-resource",
				Resource:     "test-resource-1",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, entitlementsResp.List, 1)
	require.Equal(t, "test-entitlement", entitlementsResp.List[0].Id)

	// Test Grants
	grantsResp, err := connector.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "test-resource",
				Resource:     "test-resource-1",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, grantsResp.List, 1)
	require.Equal(t, "test-user", grantsResp.List[0].Principal.Id.Resource)
}

func TestResourceTargetedSyncer(t *testing.T) {
	ctx := context.Background()

	// Test error case - ResourceSyncer without ResourceTargetedSyncer
	rsSyncer := &testResourceSyncer{&v2.ResourceType{Id: "test-resource"}}
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.GetResource(ctx, &v2.ResourceGetterServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		},
	})
	require.ErrorContains(t, err, "get resource with unknown resource type")

	// Test success case - ResourceTargetedSyncer implemented
	targetedSyncer := newTestResourceTargetedSyncer("test-resource")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{targetedSyncer}))
	require.NoError(t, err)

	resp, err := connector.GetResource(ctx, &v2.ResourceGetterServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Resource)
	require.Equal(t, "test-resource-1", resp.Resource.Id.Resource)
	require.Equal(t, "Targeted Resource test-resource-1", resp.Resource.DisplayName)
}

func TestResourceSyncerV2WithTargetedSync(t *testing.T) {
	ctx := context.Background()

	// Test case where connector implements ResourceSyncerV2 with Get method
	// This demonstrates the issue: ResourceSyncerV2 with Get method cannot be used
	// with the current ResourceTargetedSyncer interface due to method signature conflicts
	v2SyncerWithGet := newTestResourceSyncerV2WithTargetedSync("test-resource")

	// Create a ConnectorBuilder2 that uses ResourceSyncerV2
	connector2 := &testConnector2{resourceSyncers: []ResourceSyncerV2{v2SyncerWithGet}}
	connector, err := NewConnector(ctx, connector2)
	require.NoError(t, err)

	_, err = connector.GetResource(ctx, &v2.ResourceGetterServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		},
	})
	require.NoError(t, err)
}

type testConnector2 struct {
	resourceSyncers []ResourceSyncerV2
}

func (t *testConnector2) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{
		DisplayName: "test-connector-v2",
		Description: "A test connector v2",
	}, nil
}

func (t *testConnector2) Validate(ctx context.Context) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

func (t *testConnector2) ResourceSyncers(ctx context.Context) []ResourceSyncerV2 {
	return t.resourceSyncers
}

func TestResourceManager(t *testing.T) {
	ctx := context.Background()

	// Test error case - ResourceSyncer without ResourceManager
	rsSyncer := &testResourceSyncer{&v2.ResourceType{Id: "test-resource"}}

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.CreateResource(ctx, &v2.CreateResourceRequest{
		Resource: &v2.Resource{
			DisplayName: "New Resource",
			Id: &v2.ResourceId{
				ResourceType: "test-resource",
				Resource:     "test-resource-1",
			},
		},
	})
	require.ErrorContains(t, err, "resource type test-resource does not have resource Create() configured")

	// Test success case - ResourceManager implemented
	rsManager := newTestResourceManager("test-resource")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{rsManager}))
	require.NoError(t, err)

	// Test Create
	createResp, err := connector.CreateResource(ctx, &v2.CreateResourceRequest{
		Resource: &v2.Resource{
			DisplayName: "New Resource",
			Id: &v2.ResourceId{
				ResourceType: "test-resource",
				Resource:     "test-resource-1",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createResp.Created)
	require.Equal(t, "created-New Resource", createResp.Created.Id.Resource)

	// Test Delete
	deleteResp, err := connector.DeleteResource(ctx, &v2.DeleteResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp)

	_, err = connector.CreateResource(ctx, &v2.CreateResourceRequest{
		Resource: &v2.Resource{
			DisplayName: "New Resource",
			Id: &v2.ResourceId{
				ResourceType: "test-resource-2",
				Resource:     "test-resource-1",
			},
		},
	})
	require.ErrorContains(t, err, "resource type test-resource-2 does not have resource Create() configured")
}

func TestResourceManagerV2(t *testing.T) {
	ctx := context.Background()

	rsManagerV2 := newTestResourceManagerV2("test-resource")
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsManagerV2}))
	require.NoError(t, err)

	// Test Create
	createResp, err := connector.CreateResource(ctx, &v2.CreateResourceRequest{
		Resource: &v2.Resource{
			DisplayName: "New Resource V2",
			Id: &v2.ResourceId{
				ResourceType: "test-resource",
				Resource:     "test-resource-1",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createResp.Created)
	require.Equal(t, "created-v2-New Resource V2", createResp.Created.Id.Resource)

	// Test Delete V2
	deleteResp, err := connector.DeleteResource(ctx, &v2.DeleteResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		},
		ParentResourceId: &v2.ResourceId{
			ResourceType: "parent-resource",
			Resource:     "parent-1",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp)
}

func TestResourceDeleter(t *testing.T) {
	ctx := context.Background()

	rsDeleter := newTestResourceDeleter("test-resource")
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsDeleter}))
	require.NoError(t, err)

	// Test Delete
	deleteResp, err := connector.DeleteResource(ctx, &v2.DeleteResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp)
}

func TestResourceDeleterV2(t *testing.T) {
	ctx := context.Background()

	rsDeleterV2 := newTestResourceDeleterV2("test-resource")
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsDeleterV2}))
	require.NoError(t, err)

	// Test Delete V2
	deleteResp, err := connector.DeleteResourceV2(ctx, &v2.DeleteResourceV2Request{
		ResourceId: &v2.ResourceId{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		},
		ParentResourceId: &v2.ResourceId{
			ResourceType: "parent-resource",
			Resource:     "parent-1",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp)
}

func TestResourceProvisioner(t *testing.T) {
	ctx := context.Background()

	// Test error case - ResourceSyncer without ResourceProvisioner
	rsSyncer := &testResourceSyncer{&v2.ResourceType{Id: "test-resource"}}

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.Grant(ctx, &v2.GrantManagerServiceGrantRequest{
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "test-user",
			},
		},
		Entitlement: &v2.Entitlement{
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "test-resource",
					Resource:     "test-resource-1",
				},
			},
			Id: "test-entitlement",
		},
	})
	require.ErrorContains(t, err, "resource type does not have provisioner configured")

	// Test success case - ResourceProvisioner implemented
	rsProvisioner := newTestResourceProvisioner("test-resource")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{rsProvisioner}))
	require.NoError(t, err)

	// Test Grant
	grantResp, err := connector.Grant(ctx, &v2.GrantManagerServiceGrantRequest{
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "test-user",
			},
		},
		Entitlement: &v2.Entitlement{
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "test-resource",
					Resource:     "test-resource-1",
				},
			},
			Id: "test-entitlement",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, grantResp)

	// Test Revoke
	revokeResp, err := connector.Revoke(ctx, &v2.GrantManagerServiceRevokeRequest{
		Grant: &v2.Grant{
			Entitlement: &v2.Entitlement{
				Resource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: "test-resource",
						Resource:     "test-resource-1",
					},
				},
				Id: "test-entitlement",
			},
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     "test-user",
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, revokeResp)
}

func TestResourceProvisionerV2(t *testing.T) {
	ctx := context.Background()

	rsProvisionerV2 := newTestResourceProvisionerV2("test-resource")
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsProvisionerV2}))
	require.NoError(t, err)

	// Test Grant V2
	grantResp, err := connector.Grant(ctx, &v2.GrantManagerServiceGrantRequest{
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "user",
				Resource:     "test-user",
			},
		},
		Entitlement: &v2.Entitlement{
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "test-resource",
					Resource:     "test-resource-1",
				},
			},
			Id: "test-entitlement",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, grantResp)
	require.Len(t, grantResp.Grants, 1)
	require.Equal(t, "test-user", grantResp.Grants[0].Principal.Id.Resource)

	// Test Revoke V2
	revokeResp, err := connector.Revoke(ctx, &v2.GrantManagerServiceRevokeRequest{
		Grant: &v2.Grant{
			Entitlement: &v2.Entitlement{
				Resource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: "test-resource",
						Resource:     "test-resource-1",
					},
				},
				Id: "test-entitlement",
			},
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "user",
					Resource:     "test-user",
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, revokeResp)
}

func TestAccountManager(t *testing.T) {
	ctx := context.Background()

	// Test error case - ResourceSyncer without AccountManager
	rsSyncer := &testResourceSyncer{&v2.ResourceType{Id: "user"}}

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.CreateAccount(ctx, &v2.CreateAccountRequest{
		AccountInfo: &v2.AccountInfo{
			Profile: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"display_name": {
						Kind: &structpb.Value_StringValue{
							StringValue: "Test User",
						},
					},
				},
			},
		},
		CredentialOptions: &v2.CredentialOptions{
			Options: &v2.CredentialOptions_NoPassword_{
				NoPassword: &v2.CredentialOptions_NoPassword{},
			},
		},
	})
	require.ErrorContains(t, err, "connector does not have account manager configured")

	// Test success case - AccountManager implemented
	accountManager := newTestAccountManager("user")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{accountManager}))
	require.NoError(t, err)

	// Test CreateAccount
	createAccountResp, err := connector.CreateAccount(ctx, &v2.CreateAccountRequest{
		AccountInfo: &v2.AccountInfo{
			Profile: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"display_name": {
						Kind: &structpb.Value_StringValue{
							StringValue: "Test User",
						},
					},
				},
			},
		},
		CredentialOptions: &v2.CredentialOptions{
			Options: &v2.CredentialOptions_NoPassword_{
				NoPassword: &v2.CredentialOptions_NoPassword{},
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, createAccountResp)
	require.Equal(t, "created-account", createAccountResp.GetSuccess().GetResource().GetId().GetResource())
	require.Equal(t, "Test User", createAccountResp.GetSuccess().GetResource().GetDisplayName())
}

func TestCredentialManager(t *testing.T) {
	ctx := context.Background()

	// Test error case - ResourceSyncer without CredentialManager
	rsSyncer := &testResourceSyncer{&v2.ResourceType{Id: "user"}}

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.RotateCredential(ctx, &v2.RotateCredentialRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user",
		},
		CredentialOptions: &v2.CredentialOptions{
			Options: &v2.CredentialOptions_NoPassword_{
				NoPassword: &v2.CredentialOptions_NoPassword{},
			},
		},
	})
	require.ErrorContains(t, err, "resource type does not have credential manager configured")

	// Test success case - CredentialManager implemented
	credentialManager := newTestCredentialManager("user")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{credentialManager}))
	require.NoError(t, err)

	// Test RotateCredential
	rotateResp, err := connector.RotateCredential(ctx, &v2.RotateCredentialRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "test-user",
		},
		CredentialOptions: &v2.CredentialOptions{
			Options: &v2.CredentialOptions_NoPassword_{
				NoPassword: &v2.CredentialOptions_NoPassword{},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, rotateResp)
}

func TestEventProvider(t *testing.T) {
	ctx := context.Background()

	eventProvider := newTestEventProvider()
	connector, err := NewConnector(ctx, eventProvider)
	require.NoError(t, err)

	// Test ListEvents
	listEventsResp, err := connector.ListEvents(ctx, &v2.ListEventsRequest{
		EventFeedId: LegacyBatonFeedId,
	})
	require.NoError(t, err)
	require.Len(t, listEventsResp.Events, 1)
	require.Equal(t, "test-event-1", listEventsResp.Events[0].Id)
}

func TestEventProviderV2(t *testing.T) {
	ctx := context.Background()

	eventProviderV2 := newTestEventProviderV2()
	connector, err := NewConnector(ctx, eventProviderV2)
	require.NoError(t, err)

	listEventFeedsResp, err := connector.ListEventFeeds(ctx, &v2.ListEventFeedsRequest{})
	require.NoError(t, err)
	require.NotNil(t, listEventFeedsResp)
	require.Len(t, listEventFeedsResp.List, 1)
	require.Equal(t, "test-feed", listEventFeedsResp.List[0].Id)

	listEventsResp, err := connector.ListEvents(ctx, &v2.ListEventsRequest{
		EventFeedId: "test-feed",
	})
	require.NoError(t, err)
	require.Len(t, listEventsResp.Events, 1)
	require.Equal(t, "test-event-v2-1", listEventsResp.Events[0].Id)
}

func TestTicketManager(t *testing.T) {
	ctx := context.Background()

	ticketManager := newTestTicketManager()
	connector, err := NewConnector(ctx, ticketManager)
	require.NoError(t, err)

	// Test GetTicket
	getTicketResp, err := connector.GetTicket(ctx, &v2.TicketsServiceGetTicketRequest{
		Id: "test-ticket-1",
	})
	require.NoError(t, err)
	require.Equal(t, "test-ticket-1", getTicketResp.Ticket.Id)

	// Test CreateTicket
	createTicketResp, err := connector.CreateTicket(ctx, &v2.TicketsServiceCreateTicketRequest{
		Request: &v2.TicketRequest{
			DisplayName:  "New Ticket",
			Description:  "New Ticket",
			Status:       &v2.TicketStatus{},
			Type:         &v2.TicketType{},
			Labels:       []string{},
			CustomFields: map[string]*v2.TicketCustomField{},
			RequestedFor: &v2.Resource{},
		},
		Schema: &v2.TicketSchema{
			Id: "test-schema",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "created-New Ticket", createTicketResp.Ticket.Id)

	// Test GetTicketSchema
	getSchemaResp, err := connector.GetTicketSchema(ctx, &v2.TicketsServiceGetTicketSchemaRequest{
		Id: "test-schema",
	})
	require.NoError(t, err)
	require.Equal(t, "test-schema", getSchemaResp.Schema.Id)

	// Test ListTicketSchemas
	listSchemasResp, err := connector.ListTicketSchemas(ctx, &v2.TicketsServiceListTicketSchemasRequest{})
	require.NoError(t, err)
	require.Len(t, listSchemasResp.List, 1)
	require.Equal(t, "schema-1", listSchemasResp.List[0].Id)

	// Test BulkCreateTickets
	bulkCreateResp, err := connector.BulkCreateTickets(ctx, &v2.TicketsServiceBulkCreateTicketsRequest{
		TicketRequests: []*v2.TicketsServiceCreateTicketRequest{
			{
				Request: &v2.TicketRequest{
					DisplayName: "Bulk Ticket 1",
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, bulkCreateResp.Tickets, 1)
	require.Equal(t, "bulk-ticket-1", bulkCreateResp.Tickets[0].Ticket.Id)

	// Test BulkGetTickets
	bulkGetResp, err := connector.BulkGetTickets(ctx, &v2.TicketsServiceBulkGetTicketsRequest{
		TicketRequests: []*v2.TicketsServiceGetTicketRequest{
			{
				Id: "bulk-ticket-1",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, bulkGetResp.Tickets, 1)
	require.Equal(t, "bulk-ticket-1", bulkGetResp.Tickets[0].Ticket.Id)
}

func TestCustomActionManager(t *testing.T) {
	ctx := context.Background()

	actionManager := newTestRegisterActionManager()
	connector, err := NewConnector(ctx, actionManager)
	require.NoError(t, err)

	// Test ListActionSchemas
	listSchemasResp, err := connector.ListActionSchemas(ctx, &v2.ListActionSchemasRequest{})
	require.NoError(t, err)
	require.Len(t, listSchemasResp.Schemas, 1)
	require.Equal(t, "test-action", listSchemasResp.Schemas[0].Name)

	// Test GetActionSchema
	getSchemaResp, err := connector.GetActionSchema(ctx, &v2.GetActionSchemaRequest{
		Name: "test-action",
	})
	require.NoError(t, err)
	require.Equal(t, "test-action", getSchemaResp.Schema.Name)

	// Test InvokeAction
	invokeResp, err := connector.InvokeAction(ctx, &v2.InvokeActionRequest{
		Name: "test-action",
		Args: &structpb.Struct{},
	})
	require.NoError(t, err)
	require.Equal(t, "action-id-123", invokeResp.Id)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, invokeResp.Status)

	// Test GetActionStatus
	statusResp, err := connector.GetActionStatus(ctx, &v2.GetActionStatusRequest{
		Id: "action-id-123",
	})
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, statusResp.Status)
	require.Equal(t, "Action completed successfully", statusResp.Name)
}

func TestDeleteResourceV2(t *testing.T) {
	ctx := context.Background()

	rsSyncer := &testResourceSyncer{&v2.ResourceType{Id: "test-resource"}}
	rsID := &v2.ResourceId{
		ResourceType: rsSyncer.ResourceType(ctx).Id,
	}

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.DeleteResource(ctx, &v2.DeleteResourceRequest{
		ResourceId: rsID,
	})
	require.ErrorContains(t, err, "resource type test-resource does not have resource Delete() configured")

	rsManager := newTestResourceManager("test-resource")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{rsManager}))
	require.NoError(t, err)

	_, err = connector.DeleteResource(ctx, &v2.DeleteResourceRequest{
		ResourceId: rsID,
	})
	require.NoError(t, err)
	_, err = connector.DeleteResourceV2(ctx, &v2.DeleteResourceV2Request{
		ResourceId: rsID,
	})
	require.NoError(t, err)
}

func TestGetCapabilities(t *testing.T) {
	ctx := context.Background()

	t.Run("BasicResourceSyncer", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestResourceSyncer("test-resource"),
		}))
		require.NoError(t, err)

		// Get the builder to call getCapabilities
		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.getCapabilities(ctx)
		require.NoError(t, err)
		require.NotNil(t, caps)

		// Should have SYNC capability
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_SYNC)
		require.Len(t, caps.ResourceTypeCapabilities, 1)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_SYNC)
	})

	t.Run("ResourceTargetedSyncer", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestResourceTargetedSyncer("test-resource"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.getCapabilities(ctx)
		require.NoError(t, err)

		// Should have SYNC and TARGETED_SYNC capabilities
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_TARGETED_SYNC)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_TARGETED_SYNC)
	})

	t.Run("ResourceProvisioner", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestResourceProvisioner("test-resource"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.getCapabilities(ctx)
		require.NoError(t, err)

		// Should have SYNC and PROVISION capabilities
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_PROVISION)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_PROVISION)
	})

	t.Run("AccountManager", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestAccountManager("test-resource"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.getCapabilities(ctx)
		require.NoError(t, err)

		// Should have SYNC and ACCOUNT_PROVISIONING capabilities
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_ACCOUNT_PROVISIONING)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_ACCOUNT_PROVISIONING)
	})

	t.Run("CredentialManager", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestCredentialManager("test-resource"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.getCapabilities(ctx)
		require.NoError(t, err)

		// Should have SYNC and CREDENTIAL_ROTATION capabilities
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_CREDENTIAL_ROTATION)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_CREDENTIAL_ROTATION)
	})

	t.Run("ResourceManager", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestResourceManager("test-resource"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.getCapabilities(ctx)
		require.NoError(t, err)

		// Should have SYNC, RESOURCE_CREATE, and RESOURCE_DELETE capabilities
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_RESOURCE_CREATE)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_RESOURCE_DELETE)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_RESOURCE_CREATE)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_RESOURCE_DELETE)
	})

	t.Run("ResourceDeleter", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestResourceDeleter("test-resource"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.getCapabilities(ctx)
		require.NoError(t, err)

		// Should have SYNC and RESOURCE_DELETE capabilities (but not RESOURCE_CREATE)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_RESOURCE_DELETE)
		require.NotContains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_RESOURCE_CREATE)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_RESOURCE_DELETE)
		require.NotContains(t, caps.ResourceTypeCapabilities[0].Capabilities, v2.Capability_CAPABILITY_RESOURCE_CREATE)
	})

	t.Run("EventFeeds", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestEventProvider())
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.getCapabilities(ctx)
		require.NoError(t, err)

		// Should have EVENT_FEED_V2 capability (but not SYNC since no resource syncers)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_EVENT_FEED_V2)
		require.NotContains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_SYNC)
	})

	t.Run("TicketManager", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestTicketManager())
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.getCapabilities(ctx)
		require.NoError(t, err)

		// Should have TICKETING capability (but not SYNC since no resource syncers)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_TICKETING)
		require.NotContains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_SYNC)
	})

	t.Run("ActionManager", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestRegisterActionManager())
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.getCapabilities(ctx)
		require.NoError(t, err)

		// Should have ACTIONS capability (but not SYNC since no resource syncers)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_ACTIONS)
		require.NotContains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_SYNC)
	})

	t.Run("MultipleResourceTypes", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestResourceSyncer("resource-1"),
			newTestResourceTargetedSyncer("resource-2"),
			newTestResourceProvisioner("resource-3"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.getCapabilities(ctx)
		require.NoError(t, err)

		// Should have capabilities from all resource types
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_TARGETED_SYNC)
		require.Contains(t, caps.ConnectorCapabilities, v2.Capability_CAPABILITY_PROVISION)
		require.Len(t, caps.ResourceTypeCapabilities, 3)

		// Verify resource types are sorted by ID
		require.Equal(t, "resource-1", caps.ResourceTypeCapabilities[0].ResourceType.Id)
		require.Equal(t, "resource-2", caps.ResourceTypeCapabilities[1].ResourceType.Id)
		require.Equal(t, "resource-3", caps.ResourceTypeCapabilities[2].ResourceType.Id)
	})

	t.Run("EmptyResourceBuilders", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.getCapabilities(ctx)
		require.NoError(t, err)

		// Should have no capabilities
		require.Empty(t, caps.ConnectorCapabilities)
		require.Empty(t, caps.ResourceTypeCapabilities)
	})
}
