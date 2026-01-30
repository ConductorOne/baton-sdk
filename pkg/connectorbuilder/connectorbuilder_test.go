package connectorbuilder

import (
	"context"
	"fmt"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testConnector struct {
	resourceSyncers []ResourceSyncer
}

type testConnectorV2 struct {
	resourceSyncers []ResourceSyncerV2
}

func newTestConnector(resourceSyncers []ResourceSyncer) ConnectorBuilder {
	return &testConnector{resourceSyncers: resourceSyncers}
}

func (t *testConnector) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return v2.ConnectorMetadata_builder{
		DisplayName: "test-connector",
		Description: "A test connector",
	}.Build(), nil
}

func (t *testConnector) Validate(ctx context.Context) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

func (t *testConnector) ResourceSyncers(ctx context.Context) []ResourceSyncer {
	return t.resourceSyncers
}

func newTestConnectorV2(resourceSyncers []ResourceSyncerV2) ConnectorBuilderV2 {
	return &testConnectorV2{resourceSyncers: resourceSyncers}
}
func (t *testConnectorV2) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return v2.ConnectorMetadata_builder{
		DisplayName: "test-connector",
		Description: "A test connector",
	}.Build(), nil
}

func (t *testConnectorV2) Validate(ctx context.Context) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

func (t *testConnectorV2) ResourceSyncers(ctx context.Context) []ResourceSyncerV2 {
	return t.resourceSyncers
}

type testResourceSyncer struct {
	resourceType *v2.ResourceType
}

func newTestResourceSyncer(resourceType string) ResourceSyncer {
	return &testResourceSyncer{
		resourceType: v2.ResourceType_builder{
			Id:          resourceType,
			DisplayName: "Test " + resourceType,
		}.Build(),
	}
}

func (t *testResourceSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return t.resourceType
}

func (t *testResourceSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	return []*v2.Resource{
		v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: t.resourceType.GetId(),
				Resource:     "test-resource-1",
			}.Build(),
			DisplayName: "Test Resource 1",
		}.Build(),
	}, "", annotations.Annotations{}, nil
}

func (t *testResourceSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return []*v2.Entitlement{
		v2.Entitlement_builder{
			Resource:    resource,
			Id:          "test-entitlement",
			DisplayName: "Test Entitlement",
		}.Build(),
	}, "", annotations.Annotations{}, nil
}

func (t *testResourceSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return []*v2.Grant{
		v2.Grant_builder{
			Entitlement: v2.Entitlement_builder{
				Resource:    resource,
				Id:          "test-entitlement",
				DisplayName: "Test Entitlement",
			}.Build(),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     "test-user",
				}.Build(),
				DisplayName: "Test User",
			}.Build(),
			Id: "test-grant-1",
		}.Build(),
	}, "", annotations.Annotations{}, nil
}

type testResourceSyncerV2 struct {
	resourceType *v2.ResourceType
}

func newTestResourceSyncerV2(resourceType string) ResourceSyncerV2 {
	return &testResourceSyncerV2{
		resourceType: v2.ResourceType_builder{
			Id:          resourceType,
			DisplayName: "Test " + resourceType,
		}.Build(),
	}
}

func (t *testResourceSyncerV2) ResourceType(ctx context.Context) *v2.ResourceType {
	return t.resourceType
}

func (t *testResourceSyncerV2) List(ctx context.Context, parentResourceID *v2.ResourceId, opts rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error) {
	syncResults := &rs.SyncOpResults{
		NextPageToken: "",
		Annotations:   annotations.Annotations{},
	}
	return []*v2.Resource{
		v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: t.resourceType.GetId(),
				Resource:     "test-resource-1",
			}.Build(),
			DisplayName: "Test Resource 1",
		}.Build(),
	}, syncResults, nil
}

func (t *testResourceSyncerV2) Entitlements(ctx context.Context, resource *v2.Resource, opts rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	syncResults := &rs.SyncOpResults{
		NextPageToken: "",
		Annotations:   annotations.Annotations{},
	}
	return []*v2.Entitlement{
		v2.Entitlement_builder{
			Resource:    resource,
			Id:          "test-entitlement",
			DisplayName: "Test Entitlement",
		}.Build(),
	}, syncResults, nil
}

func (t *testResourceSyncerV2) Grants(ctx context.Context, resource *v2.Resource, opts rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error) {
	syncResults := &rs.SyncOpResults{
		NextPageToken: "",
		Annotations:   annotations.Annotations{},
	}
	return []*v2.Grant{
		v2.Grant_builder{
			Entitlement: v2.Entitlement_builder{
				Resource:    resource,
				Id:          "test-entitlement",
				DisplayName: "Test Entitlement",
			}.Build(),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     "test-user",
				}.Build(),
				DisplayName: "Test User",
			}.Build(),
			Id: "test-grant-1",
		}.Build(),
	}, syncResults, nil
}

type testResourceSyncerV2WithTargetedSync struct {
	resourceType *v2.ResourceType
}

func newTestResourceSyncerV2WithTargetedSync(resourceType string) ResourceSyncerV2 {
	return &testResourceSyncerV2WithTargetedSync{
		resourceType: v2.ResourceType_builder{
			Id:          resourceType,
			DisplayName: "Test " + resourceType,
		}.Build(),
	}
}

func (t *testResourceSyncerV2WithTargetedSync) ResourceType(ctx context.Context) *v2.ResourceType {
	return t.resourceType
}

func (t *testResourceSyncerV2WithTargetedSync) List(
	ctx context.Context,
	parentResourceID *v2.ResourceId,
	opts rs.SyncOpAttrs,
) ([]*v2.Resource, *rs.SyncOpResults, error) {
	return []*v2.Resource{
		v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: t.resourceType.GetId(),
				Resource:     "test-resource-1",
			}.Build(),
			DisplayName: "Test Resource 1",
		}.Build(),
	}, &rs.SyncOpResults{NextPageToken: "", Annotations: annotations.Annotations{}}, nil
}

func (t *testResourceSyncerV2WithTargetedSync) StaticEntitlements(ctx context.Context, opts rs.SyncOpAttrs) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return []*v2.Entitlement{}, &rs.SyncOpResults{NextPageToken: "", Annotations: annotations.Annotations{}}, nil
}

func (t *testResourceSyncerV2WithTargetedSync) Entitlements(
	ctx context.Context,
	r *v2.Resource,
	opts rs.SyncOpAttrs,
) ([]*v2.Entitlement, *rs.SyncOpResults, error) {
	return []*v2.Entitlement{
		v2.Entitlement_builder{
			Resource:    r,
			Id:          "test-entitlement",
			DisplayName: "Test Entitlement",
		}.Build(),
	}, &rs.SyncOpResults{NextPageToken: "", Annotations: annotations.Annotations{}}, nil
}

func (t *testResourceSyncerV2WithTargetedSync) Grants(
	ctx context.Context,
	r *v2.Resource,
	opts rs.SyncOpAttrs,
) ([]*v2.Grant, *rs.SyncOpResults, error) {
	return []*v2.Grant{
		v2.Grant_builder{
			Entitlement: v2.Entitlement_builder{
				Resource:    r,
				Id:          "test-entitlement",
				DisplayName: "Test Entitlement",
			}.Build(),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     "test-user",
				}.Build(),
				DisplayName: "Test User",
			}.Build(),
			Id: "test-grant-1",
		}.Build(),
	}, &rs.SyncOpResults{NextPageToken: "", Annotations: annotations.Annotations{}}, nil
}

func (t *testResourceSyncerV2WithTargetedSync) Get(
	ctx context.Context,
	resourceId *v2.ResourceId,
	parentResourceId *v2.ResourceId,
) (*v2.Resource, annotations.Annotations, error) {
	return v2.Resource_builder{
		Id:          resourceId,
		DisplayName: "V2 Targeted Resource " + resourceId.GetResource(),
	}.Build(), annotations.Annotations{}, nil
}

type testResourceManager struct {
	ResourceSyncer
}

func newTestResourceManager(resourceType string) ResourceManager {
	return &testResourceManager{newTestResourceSyncer(resourceType)}
}

func (t *testResourceManager) Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error) {
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: t.ResourceType(ctx).GetId(),
			Resource:     "created-" + resource.GetDisplayName(),
		}.Build(),
		DisplayName: resource.GetDisplayName(),
	}.Build(), annotations.Annotations{}, nil
}

func (t *testResourceManager) Delete(ctx context.Context, resourceId *v2.ResourceId) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

type testResourceManagerV2 struct {
	ResourceSyncerV2
}

func newTestResourceManagerV2(resourceType string) ResourceManagerV2 {
	return &testResourceManagerV2{newTestResourceSyncerV2(resourceType)}
}

func (t *testResourceManagerV2) Create(ctx context.Context, resource *v2.Resource) (*v2.Resource, annotations.Annotations, error) {
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: t.ResourceType(ctx).GetId(),
			Resource:     "created-v2-" + resource.GetDisplayName(),
		}.Build(),
		DisplayName: resource.GetDisplayName(),
	}.Build(), annotations.Annotations{}, nil
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
	ResourceSyncerV2
}

func newTestResourceDeleterV2(resourceType string) ResourceDeleterV2 {
	return &testResourceDeleterV2{newTestResourceSyncerV2(resourceType)}
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
	ResourceSyncerV2
}

func newTestResourceProvisionerV2(resourceType string) ResourceProvisionerV2 {
	return &testResourceProvisionerV2{newTestResourceSyncerV2(resourceType)}
}

type testResourceTargetedSyncer struct {
	ResourceSyncer
}

func newTestResourceTargetedSyncer(resourceType string) ResourceTargetedSyncer {
	return &testResourceTargetedSyncer{newTestResourceSyncer(resourceType)}
}

func (t *testResourceTargetedSyncer) Get(ctx context.Context, resourceId *v2.ResourceId, parentResourceId *v2.ResourceId) (*v2.Resource, annotations.Annotations, error) {
	return v2.Resource_builder{
		Id:          resourceId,
		DisplayName: "Targeted Resource " + resourceId.GetResource(),
	}.Build(), annotations.Annotations{}, nil
}

func (t *testResourceProvisionerV2) Grant(ctx context.Context, resource *v2.Resource, entitlement *v2.Entitlement) ([]*v2.Grant, annotations.Annotations, error) {
	return []*v2.Grant{
		v2.Grant_builder{
			Entitlement: entitlement,
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     "test-user",
				}.Build(),
				DisplayName: "Test User",
			}.Build(),
			Id: "granted-grant-1",
		}.Build(),
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
	r := v2.CreateAccountResponse_SuccessResult_builder{
		IsCreateAccountResult: true,
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: t.ResourceType(ctx).GetId(),
				Resource:     "created-account",
			}.Build(),
			DisplayName: "Test User",
		}.Build(),
	}.Build()
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
		v2.PlaintextData_builder{
			Name:        "password",
			Description: "User password",
			Bytes:       []byte("new-password"),
		}.Build(),
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
		v2.Event_builder{
			Id:         "test-event-1",
			OccurredAt: timestamppb.New(time.Now()),
		}.Build(),
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
	return v2.EventFeedMetadata_builder{
		Id: "test-feed",
	}.Build()
}

func (t *testEventFeed) ListEvents(ctx context.Context, earliestEvent *timestamppb.Timestamp, pToken *pagination.StreamToken) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	return []*v2.Event{
		v2.Event_builder{
			Id:         "test-event-v2-1",
			OccurredAt: timestamppb.New(time.Now()),
		}.Build(),
	}, &pagination.StreamState{}, annotations.Annotations{}, nil
}

type testTicketManager struct {
	ConnectorBuilder
}

func newTestTicketManager() TicketManager {
	return &testTicketManager{newTestConnector([]ResourceSyncer{})}
}

func (t *testTicketManager) GetTicket(ctx context.Context, ticketId string) (*v2.Ticket, annotations.Annotations, error) {
	return v2.Ticket_builder{
		Id:          ticketId,
		DisplayName: "Test Ticket",
	}.Build(), annotations.Annotations{}, nil
}

func (t *testTicketManager) CreateTicket(ctx context.Context, ticket *v2.Ticket, schema *v2.TicketSchema) (*v2.Ticket, annotations.Annotations, error) {
	return v2.Ticket_builder{
		Id:          "created-" + ticket.GetDisplayName(),
		DisplayName: ticket.GetDisplayName(),
	}.Build(), annotations.Annotations{}, nil
}

func (t *testTicketManager) GetTicketSchema(ctx context.Context, schemaID string) (*v2.TicketSchema, annotations.Annotations, error) {
	return v2.TicketSchema_builder{
		Id:          schemaID,
		DisplayName: "Test Schema",
	}.Build(), annotations.Annotations{}, nil
}

func (t *testTicketManager) ListTicketSchemas(ctx context.Context, pToken *pagination.Token) ([]*v2.TicketSchema, string, annotations.Annotations, error) {
	return []*v2.TicketSchema{
		v2.TicketSchema_builder{
			Id:          "schema-1",
			DisplayName: "Test Schema 1",
		}.Build(),
	}, "", annotations.Annotations{}, nil
}

func (t *testTicketManager) BulkCreateTickets(ctx context.Context, request *v2.TicketsServiceBulkCreateTicketsRequest) (*v2.TicketsServiceBulkCreateTicketsResponse, error) {
	return v2.TicketsServiceBulkCreateTicketsResponse_builder{
		Tickets: []*v2.TicketsServiceCreateTicketResponse{
			v2.TicketsServiceCreateTicketResponse_builder{
				Ticket: v2.Ticket_builder{
					Id:          "bulk-ticket-1",
					DisplayName: "Bulk Ticket 1",
				}.Build(),
			}.Build(),
		},
	}.Build(), nil
}

func (t *testTicketManager) BulkGetTickets(ctx context.Context, request *v2.TicketsServiceBulkGetTicketsRequest) (*v2.TicketsServiceBulkGetTicketsResponse, error) {
	return v2.TicketsServiceBulkGetTicketsResponse_builder{
		Tickets: []*v2.TicketsServiceGetTicketResponse{
			v2.TicketsServiceGetTicketResponse_builder{
				Ticket: v2.Ticket_builder{
					Id:          "bulk-ticket-1",
					DisplayName: "Bulk Ticket 1",
				}.Build(),
			}.Build(),
		},
	}.Build(), nil
}

type testCustomActionManager struct{}

func newTestCustomActionManager() CustomActionManager {
	return &testCustomActionManager{}
}

func (t *testCustomActionManager) ListActionSchemas(ctx context.Context, resourceTypeID string) ([]*v2.BatonActionSchema, annotations.Annotations, error) {
	return []*v2.BatonActionSchema{
		v2.BatonActionSchema_builder{
			Name:        "test-action",
			DisplayName: "Test Action",
		}.Build(),
	}, annotations.Annotations{}, nil
}

func (t *testCustomActionManager) GetActionSchema(ctx context.Context, name string) (*v2.BatonActionSchema, annotations.Annotations, error) {
	return v2.BatonActionSchema_builder{
		Name:        name,
		DisplayName: "Test Action Schema",
	}.Build(), annotations.Annotations{}, nil
}

func (t *testCustomActionManager) InvokeAction(
	ctx context.Context,
	name string,
	resourceTypeID string,
	args *structpb.Struct,
) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	return "action-id-123", v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, &structpb.Struct{}, annotations.Annotations{}, nil
}

func (t *testCustomActionManager) GetActionStatus(ctx context.Context, id string) (v2.BatonActionStatus, string, *structpb.Struct, annotations.Annotations, error) {
	return v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, "Action completed successfully", &structpb.Struct{}, annotations.Annotations{}, nil
}

func (t *testCustomActionManager) GetTypeRegistry(ctx context.Context, resourceTypeID string) (actions.ActionRegistry, error) {
	return nil, nil
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
	require.Equal(t, "test-connector", metadata.GetMetadata().GetDisplayName())

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
	listResp, err := connector.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: "test-resource",
	}.Build())
	require.NoError(t, err)
	require.Len(t, listResp.GetList(), 1)
	require.Equal(t, "test-resource-1", listResp.GetList()[0].GetId().GetResource())

	// Test Entitlements
	entitlementsResp, err := connector.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "test-resource",
				Resource:     "test-resource-1",
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Len(t, entitlementsResp.GetList(), 1)
	require.Equal(t, "test-entitlement", entitlementsResp.GetList()[0].GetId())

	// Test Grants
	grantsResp, err := connector.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "test-resource",
				Resource:     "test-resource-1",
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Len(t, grantsResp.GetList(), 1)
	require.Equal(t, "test-user", grantsResp.GetList()[0].GetPrincipal().GetId().GetResource())
}

func TestResourceTargetedSyncer(t *testing.T) {
	ctx := context.Background()

	// Test error case - ResourceSyncer without ResourceTargetedSyncer
	rsSyncer := &testResourceSyncer{v2.ResourceType_builder{Id: "test-resource"}.Build()}
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.GetResource(ctx, v2.ResourceGetterServiceGetResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		}.Build(),
	}.Build())
	require.ErrorContains(t, err, "get resource with unknown resource type")

	// Test success case - ResourceTargetedSyncer implemented
	targetedSyncer := newTestResourceTargetedSyncer("test-resource")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{targetedSyncer}))
	require.NoError(t, err)

	resp, err := connector.GetResource(ctx, v2.ResourceGetterServiceGetResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, resp.GetResource())
	require.Equal(t, "test-resource-1", resp.GetResource().GetId().GetResource())
	require.Equal(t, "Targeted Resource test-resource-1", resp.GetResource().GetDisplayName())
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

	_, err = connector.GetResource(ctx, v2.ResourceGetterServiceGetResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		}.Build(),
	}.Build())
	require.NoError(t, err)
}

type testConnector2 struct {
	resourceSyncers []ResourceSyncerV2
}

func (t *testConnector2) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return v2.ConnectorMetadata_builder{
		DisplayName: "test-connector-v2",
		Description: "A test connector v2",
	}.Build(), nil
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
	rsSyncer := &testResourceSyncer{v2.ResourceType_builder{Id: "test-resource"}.Build()}

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.CreateResource(ctx, v2.CreateResourceRequest_builder{
		Resource: v2.Resource_builder{
			DisplayName: "New Resource",
			Id: v2.ResourceId_builder{
				ResourceType: "test-resource",
				Resource:     "test-resource-1",
			}.Build(),
		}.Build(),
	}.Build())
	require.ErrorContains(t, err, "resource type test-resource does not have resource Create() configured")

	// Test success case - ResourceManager implemented
	rsManager := newTestResourceManager("test-resource")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{rsManager}))
	require.NoError(t, err)

	// Test Create
	createResp, err := connector.CreateResource(ctx, v2.CreateResourceRequest_builder{
		Resource: v2.Resource_builder{
			DisplayName: "New Resource",
			Id: v2.ResourceId_builder{
				ResourceType: "test-resource",
				Resource:     "test-resource-1",
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, createResp.GetCreated())
	require.Equal(t, "created-New Resource", createResp.GetCreated().GetId().GetResource())

	// Test Delete
	deleteResp, err := connector.DeleteResource(ctx, v2.DeleteResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, deleteResp)

	_, err = connector.CreateResource(ctx, v2.CreateResourceRequest_builder{
		Resource: v2.Resource_builder{
			DisplayName: "New Resource",
			Id: v2.ResourceId_builder{
				ResourceType: "test-resource-2",
				Resource:     "test-resource-1",
			}.Build(),
		}.Build(),
	}.Build())
	require.ErrorContains(t, err, "resource type test-resource-2 does not have resource Create() configured")
}

func TestResourceManagerV2(t *testing.T) {
	ctx := context.Background()

	rsManagerV2 := newTestResourceManagerV2("test-resource")
	connector, err := NewConnector(ctx, newTestConnectorV2([]ResourceSyncerV2{rsManagerV2}))
	require.NoError(t, err)

	// Test Create
	createResp, err := connector.CreateResource(ctx, v2.CreateResourceRequest_builder{
		Resource: v2.Resource_builder{
			DisplayName: "New Resource V2",
			Id: v2.ResourceId_builder{
				ResourceType: "test-resource",
				Resource:     "test-resource-1",
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, createResp.GetCreated())
	require.Equal(t, "created-v2-New Resource V2", createResp.GetCreated().GetId().GetResource())

	// Test Delete V2
	deleteResp, err := connector.DeleteResource(ctx, v2.DeleteResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		}.Build(),
		ParentResourceId: v2.ResourceId_builder{
			ResourceType: "parent-resource",
			Resource:     "parent-1",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, deleteResp)
}

func TestResourceDeleter(t *testing.T) {
	ctx := context.Background()

	rsDeleter := newTestResourceDeleter("test-resource")
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsDeleter}))
	require.NoError(t, err)

	// Test Delete
	deleteResp, err := connector.DeleteResource(ctx, v2.DeleteResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, deleteResp)
}

func TestResourceDeleterV2(t *testing.T) {
	ctx := context.Background()

	rsDeleterV2 := newTestResourceDeleterV2("test-resource")
	connector, err := NewConnector(ctx, newTestConnectorV2([]ResourceSyncerV2{rsDeleterV2}))
	require.NoError(t, err)

	// Test Delete V2
	deleteResp, err := connector.DeleteResourceV2(ctx, v2.DeleteResourceV2Request_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "test-resource",
			Resource:     "test-resource-1",
		}.Build(),
		ParentResourceId: v2.ResourceId_builder{
			ResourceType: "parent-resource",
			Resource:     "parent-1",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, deleteResp)
}

func TestResourceProvisioner(t *testing.T) {
	ctx := context.Background()

	// Test error case - ResourceSyncer without ResourceProvisioner
	rsSyncer := &testResourceSyncer{v2.ResourceType_builder{Id: "test-resource"}.Build()}

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.Grant(ctx, v2.GrantManagerServiceGrantRequest_builder{
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "test-user",
			}.Build(),
		}.Build(),
		Entitlement: v2.Entitlement_builder{
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "test-resource",
					Resource:     "test-resource-1",
				}.Build(),
			}.Build(),
			Id: "test-entitlement",
		}.Build(),
	}.Build())
	require.ErrorContains(t, err, "does not have provisioner configured")

	// Test success case - ResourceProvisioner implemented
	rsProvisioner := newTestResourceProvisioner("test-resource")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{rsProvisioner}))
	require.NoError(t, err)

	// Test Grant
	grantResp, err := connector.Grant(ctx, v2.GrantManagerServiceGrantRequest_builder{
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "test-user",
			}.Build(),
		}.Build(),
		Entitlement: v2.Entitlement_builder{
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "test-resource",
					Resource:     "test-resource-1",
				}.Build(),
			}.Build(),
			Id: "test-entitlement",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, grantResp)

	// Test Revoke
	revokeResp, err := connector.Revoke(ctx, v2.GrantManagerServiceRevokeRequest_builder{
		Grant: v2.Grant_builder{
			Entitlement: v2.Entitlement_builder{
				Resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: "test-resource",
						Resource:     "test-resource-1",
					}.Build(),
				}.Build(),
				Id: "test-entitlement",
			}.Build(),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     "test-user",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, revokeResp)
}

func TestResourceProvisionerV2(t *testing.T) {
	ctx := context.Background()

	rsProvisionerV2 := newTestResourceProvisionerV2("test-resource")
	connector, err := NewConnector(ctx, newTestConnectorV2([]ResourceSyncerV2{rsProvisionerV2}))
	require.NoError(t, err)

	// Test Grant V2
	grantResp, err := connector.Grant(ctx, v2.GrantManagerServiceGrantRequest_builder{
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "test-user",
			}.Build(),
		}.Build(),
		Entitlement: v2.Entitlement_builder{
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "test-resource",
					Resource:     "test-resource-1",
				}.Build(),
			}.Build(),
			Id: "test-entitlement",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, grantResp)
	require.Len(t, grantResp.GetGrants(), 1)
	require.Equal(t, "test-user", grantResp.GetGrants()[0].GetPrincipal().GetId().GetResource())

	// Test Revoke V2
	revokeResp, err := connector.Revoke(ctx, v2.GrantManagerServiceRevokeRequest_builder{
		Grant: v2.Grant_builder{
			Entitlement: v2.Entitlement_builder{
				Resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: "test-resource",
						Resource:     "test-resource-1",
					}.Build(),
				}.Build(),
				Id: "test-entitlement",
			}.Build(),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     "test-user",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, revokeResp)
}

func TestAccountManager(t *testing.T) {
	ctx := context.Background()

	// Test error case - ResourceSyncer without AccountManager
	rsSyncer := &testResourceSyncer{v2.ResourceType_builder{Id: "user"}.Build()}

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.CreateAccount(ctx, v2.CreateAccountRequest_builder{
		AccountInfo: v2.AccountInfo_builder{
			Profile: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"display_name": {
						Kind: &structpb.Value_StringValue{
							StringValue: "Test User",
						},
					},
				},
			},
		}.Build(),
		CredentialOptions: v2.CredentialOptions_builder{
			NoPassword: &v2.CredentialOptions_NoPassword{},
		}.Build(),
	}.Build())
	require.ErrorContains(t, err, "connector does not have account manager configured")

	// Test success case - AccountManager implemented
	accountManager := newTestAccountManager("user")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{accountManager}))
	require.NoError(t, err)

	// Test CreateAccount
	createAccountResp, err := connector.CreateAccount(ctx, v2.CreateAccountRequest_builder{
		AccountInfo: v2.AccountInfo_builder{
			Profile: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"display_name": {
						Kind: &structpb.Value_StringValue{
							StringValue: "Test User",
						},
					},
				},
			},
		}.Build(),
		CredentialOptions: v2.CredentialOptions_builder{
			NoPassword: &v2.CredentialOptions_NoPassword{},
		}.Build(),
	}.Build())

	require.NoError(t, err)
	require.NotNil(t, createAccountResp)
	require.Equal(t, "created-account", createAccountResp.GetSuccess().GetResource().GetId().GetResource())
	require.Equal(t, "Test User", createAccountResp.GetSuccess().GetResource().GetDisplayName())
}

func TestCredentialManager(t *testing.T) {
	ctx := context.Background()

	// Test error case - ResourceSyncer without CredentialManager
	rsSyncer := &testResourceSyncer{v2.ResourceType_builder{Id: "user"}.Build()}

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.RotateCredential(ctx, v2.RotateCredentialRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "user",
			Resource:     "test-user",
		}.Build(),
		CredentialOptions: v2.CredentialOptions_builder{
			NoPassword: &v2.CredentialOptions_NoPassword{},
		}.Build(),
	}.Build())
	require.ErrorContains(t, err, "resource type does not have credential manager configured")

	// Test success case - CredentialManager implemented
	credentialManager := newTestCredentialManager("user")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{credentialManager}))
	require.NoError(t, err)

	// Test RotateCredential
	rotateResp, err := connector.RotateCredential(ctx, v2.RotateCredentialRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "user",
			Resource:     "test-user",
		}.Build(),
		CredentialOptions: v2.CredentialOptions_builder{
			NoPassword: &v2.CredentialOptions_NoPassword{},
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, rotateResp)
}

func TestEventProvider(t *testing.T) {
	ctx := context.Background()

	eventProvider := newTestEventProvider()
	connector, err := NewConnector(ctx, eventProvider)
	require.NoError(t, err)

	// Test ListEvents
	listEventsResp, err := connector.ListEvents(ctx, v2.ListEventsRequest_builder{
		EventFeedId: LegacyBatonFeedId,
	}.Build())
	require.NoError(t, err)
	require.Len(t, listEventsResp.GetEvents(), 1)
	require.Equal(t, "test-event-1", listEventsResp.GetEvents()[0].GetId())
}

func TestEventProviderV2(t *testing.T) {
	ctx := context.Background()

	eventProviderV2 := newTestEventProviderV2()
	connector, err := NewConnector(ctx, eventProviderV2)
	require.NoError(t, err)

	listEventFeedsResp, err := connector.ListEventFeeds(ctx, &v2.ListEventFeedsRequest{})
	require.NoError(t, err)
	require.NotNil(t, listEventFeedsResp)
	require.Len(t, listEventFeedsResp.GetList(), 1)
	require.Equal(t, "test-feed", listEventFeedsResp.GetList()[0].GetId())

	listEventsResp, err := connector.ListEvents(ctx, v2.ListEventsRequest_builder{
		EventFeedId: "test-feed",
	}.Build())
	require.NoError(t, err)
	require.Len(t, listEventsResp.GetEvents(), 1)
	require.Equal(t, "test-event-v2-1", listEventsResp.GetEvents()[0].GetId())
}

func TestTicketManager(t *testing.T) {
	ctx := context.Background()

	ticketManager := newTestTicketManager()
	connector, err := NewConnector(ctx, ticketManager)
	require.NoError(t, err)

	// Test GetTicket
	getTicketResp, err := connector.GetTicket(ctx, v2.TicketsServiceGetTicketRequest_builder{
		Id: "test-ticket-1",
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "test-ticket-1", getTicketResp.GetTicket().GetId())

	// Test CreateTicket
	createTicketResp, err := connector.CreateTicket(ctx, v2.TicketsServiceCreateTicketRequest_builder{
		Request: v2.TicketRequest_builder{
			DisplayName:  "New Ticket",
			Description:  "New Ticket",
			Status:       &v2.TicketStatus{},
			Type:         &v2.TicketType{},
			Labels:       []string{},
			CustomFields: map[string]*v2.TicketCustomField{},
			RequestedFor: &v2.Resource{},
		}.Build(),
		Schema: v2.TicketSchema_builder{
			Id: "test-schema",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "created-New Ticket", createTicketResp.GetTicket().GetId())

	// Test GetTicketSchema
	getSchemaResp, err := connector.GetTicketSchema(ctx, v2.TicketsServiceGetTicketSchemaRequest_builder{
		Id: "test-schema",
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "test-schema", getSchemaResp.GetSchema().GetId())

	// Test ListTicketSchemas
	listSchemasResp, err := connector.ListTicketSchemas(ctx, &v2.TicketsServiceListTicketSchemasRequest{})
	require.NoError(t, err)
	require.Len(t, listSchemasResp.GetList(), 1)
	require.Equal(t, "schema-1", listSchemasResp.GetList()[0].GetId())

	// Test BulkCreateTickets
	bulkCreateResp, err := connector.BulkCreateTickets(ctx, v2.TicketsServiceBulkCreateTicketsRequest_builder{
		TicketRequests: []*v2.TicketsServiceCreateTicketRequest{
			v2.TicketsServiceCreateTicketRequest_builder{
				Request: v2.TicketRequest_builder{
					DisplayName: "Bulk Ticket 1",
				}.Build(),
			}.Build(),
		},
	}.Build())
	require.NoError(t, err)
	require.Len(t, bulkCreateResp.GetTickets(), 1)
	require.Equal(t, "bulk-ticket-1", bulkCreateResp.GetTickets()[0].GetTicket().GetId())

	// Test BulkGetTickets
	bulkGetResp, err := connector.BulkGetTickets(ctx, v2.TicketsServiceBulkGetTicketsRequest_builder{
		TicketRequests: []*v2.TicketsServiceGetTicketRequest{
			v2.TicketsServiceGetTicketRequest_builder{
				Id: "bulk-ticket-1",
			}.Build(),
		},
	}.Build())
	require.NoError(t, err)
	require.Len(t, bulkGetResp.GetTickets(), 1)
	require.Equal(t, "bulk-ticket-1", bulkGetResp.GetTickets()[0].GetTicket().GetId())
}

func TestCustomActionManager(t *testing.T) {
	ctx := context.Background()

	actionManager := newTestRegisterActionManager()
	connector, err := NewConnector(ctx, actionManager)
	require.NoError(t, err)

	// Test ListActionSchemas
	listSchemasResp, err := connector.ListActionSchemas(ctx, &v2.ListActionSchemasRequest{})
	require.NoError(t, err)
	require.Len(t, listSchemasResp.GetSchemas(), 1)
	require.Equal(t, "test-action", listSchemasResp.GetSchemas()[0].GetName())

	// Test GetActionSchema
	getSchemaResp, err := connector.GetActionSchema(ctx, v2.GetActionSchemaRequest_builder{
		Name: "test-action",
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "test-action", getSchemaResp.GetSchema().GetName())

	// Test InvokeAction
	invokeResp, err := connector.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name: "test-action",
		Args: &structpb.Struct{},
	}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, invokeResp.GetId())
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, invokeResp.GetStatus())

	// Test GetActionStatus using the ID returned from InvokeAction
	statusResp, err := connector.GetActionStatus(ctx, v2.GetActionStatusRequest_builder{
		Id: invokeResp.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, statusResp.GetStatus())
	require.Equal(t, "test-action", statusResp.GetName())
}

// testGlobalActionProvider implements GlobalActionProvider for testing.
type testGlobalActionProvider struct {
	ConnectorBuilder
}

func newTestGlobalActionProvider() *testGlobalActionProvider {
	return &testGlobalActionProvider{newTestConnector([]ResourceSyncer{})}
}

func (t *testGlobalActionProvider) GlobalActions(ctx context.Context, registry actions.ActionRegistry) error {
	schema := v2.BatonActionSchema_builder{
		Name:        "global-test-action",
		DisplayName: "Global Test Action",
	}.Build()
	handler := func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		return &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"result": structpb.NewStringValue("global-action-executed"),
			},
		}, nil, nil
	}
	return registry.Register(ctx, schema, handler)
}

func TestGlobalActionProvider(t *testing.T) {
	ctx := context.Background()

	provider := newTestGlobalActionProvider()
	connector, err := NewConnector(ctx, provider)
	require.NoError(t, err)

	// Test ListActionSchemas
	listSchemasResp, err := connector.ListActionSchemas(ctx, &v2.ListActionSchemasRequest{})
	require.NoError(t, err)
	require.Len(t, listSchemasResp.GetSchemas(), 1)
	require.Equal(t, "global-test-action", listSchemasResp.GetSchemas()[0].GetName())

	// Test GetActionSchema
	getSchemaResp, err := connector.GetActionSchema(ctx, v2.GetActionSchemaRequest_builder{
		Name: "global-test-action",
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "global-test-action", getSchemaResp.GetSchema().GetName())

	// Test InvokeAction
	invokeResp, err := connector.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name: "global-test-action",
		Args: &structpb.Struct{},
	}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, invokeResp.GetId())
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, invokeResp.GetStatus())
	require.Equal(t, "global-action-executed", invokeResp.GetResponse().GetFields()["result"].GetStringValue())

	// Test GetActionStatus
	statusResp, err := connector.GetActionStatus(ctx, v2.GetActionStatusRequest_builder{
		Id: invokeResp.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, statusResp.GetStatus())
}

// testResourceActionProviderSyncer implements ResourceActionProvider for testing.
type testResourceActionProviderSyncer struct {
	resourceType *v2.ResourceType
}

func newTestResourceActionProviderSyncer(resourceType string) *testResourceActionProviderSyncer {
	return &testResourceActionProviderSyncer{
		resourceType: v2.ResourceType_builder{
			Id:          resourceType,
			DisplayName: "Test " + resourceType,
		}.Build(),
	}
}

func (t *testResourceActionProviderSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return t.resourceType
}

func (t *testResourceActionProviderSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (t *testResourceActionProviderSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (t *testResourceActionProviderSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (t *testResourceActionProviderSyncer) ResourceActions(ctx context.Context, registry actions.ActionRegistry) error {
	schema := v2.BatonActionSchema_builder{
		Name:        "resource-test-action",
		DisplayName: "Resource Test Action",
	}.Build()
	handler := func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		return &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"result": structpb.NewStringValue("resource-action-executed"),
			},
		}, nil, nil
	}
	return registry.Register(ctx, schema, handler)
}

func TestResourceActionProvider(t *testing.T) {
	ctx := context.Background()

	rsSyncer := newTestResourceActionProviderSyncer("test-resource")
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	// Test ListActionSchemas with resource type filter
	listSchemasResp, err := connector.ListActionSchemas(ctx, v2.ListActionSchemasRequest_builder{
		ResourceTypeId: "test-resource",
	}.Build())
	require.NoError(t, err)
	require.Len(t, listSchemasResp.GetSchemas(), 1)
	require.Equal(t, "resource-test-action", listSchemasResp.GetSchemas()[0].GetName())
	require.Equal(t, "test-resource", listSchemasResp.GetSchemas()[0].GetResourceTypeId())

	// Test ListActionSchemas without filter - should include all actions
	listAllResp, err := connector.ListActionSchemas(ctx, &v2.ListActionSchemasRequest{})
	require.NoError(t, err)
	require.Len(t, listAllResp.GetSchemas(), 1)

	// Test InvokeAction with resource type
	invokeResp, err := connector.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name:           "resource-test-action",
		ResourceTypeId: "test-resource",
		Args:           &structpb.Struct{},
	}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, invokeResp.GetId())
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, invokeResp.GetStatus())
	require.Equal(t, "resource-action-executed", invokeResp.GetResponse().GetFields()["result"].GetStringValue())
}

func TestHasActionsMethod(t *testing.T) {
	ctx := context.Background()

	// Test without any actions
	m := actions.NewActionManager(ctx)
	require.False(t, m.HasActions())

	// Test with global action
	schema := v2.BatonActionSchema_builder{Name: "test"}.Build()
	handler := func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		return nil, nil, nil
	}
	err := m.Register(ctx, schema, handler)
	require.NoError(t, err)
	require.True(t, m.HasActions())

	// Test with only resource-scoped action
	m2 := actions.NewActionManager(ctx)
	require.False(t, m2.HasActions())
	err = m2.RegisterResourceAction(ctx, "resource-type", schema, handler)
	require.NoError(t, err)
	require.True(t, m2.HasActions())
}

func TestBackwardCompatibilityWrapper(t *testing.T) {
	ctx := context.Background()

	// Use the legacy RegisterActionManager interface
	actionManager := newTestRegisterActionManager()
	connector, err := NewConnector(ctx, actionManager)
	require.NoError(t, err)

	// Verify actions are registered through the wrapper
	listSchemasResp, err := connector.ListActionSchemas(ctx, &v2.ListActionSchemasRequest{})
	require.NoError(t, err)
	require.Len(t, listSchemasResp.GetSchemas(), 1)
	require.Equal(t, "test-action", listSchemasResp.GetSchemas()[0].GetName())

	// Invoke action - should work through the wrapper
	invokeResp, err := connector.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name: "test-action",
		Args: &structpb.Struct{},
	}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, invokeResp.GetId())
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, invokeResp.GetStatus())

	// Get status - should work with SDK-generated ID
	statusResp, err := connector.GetActionStatus(ctx, v2.GetActionStatusRequest_builder{
		Id: invokeResp.GetId(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, statusResp.GetStatus())
}

func TestDeleteResourceV2(t *testing.T) {
	ctx := context.Background()

	rsSyncer := &testResourceSyncer{v2.ResourceType_builder{Id: "test-resource"}.Build()}
	rsID := v2.ResourceId_builder{
		ResourceType: rsSyncer.ResourceType(ctx).GetId(),
	}.Build()

	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{rsSyncer}))
	require.NoError(t, err)

	_, err = connector.DeleteResource(ctx, v2.DeleteResourceRequest_builder{
		ResourceId: rsID,
	}.Build())
	require.ErrorContains(t, err, "resource type test-resource does not have resource Delete() configured")

	rsManager := newTestResourceManager("test-resource")
	connector, err = NewConnector(ctx, newTestConnector([]ResourceSyncer{rsManager}))
	require.NoError(t, err)

	_, err = connector.DeleteResource(ctx, v2.DeleteResourceRequest_builder{
		ResourceId: rsID,
	}.Build())
	require.NoError(t, err)
	_, err = connector.DeleteResourceV2(ctx, v2.DeleteResourceV2Request_builder{
		ResourceId: rsID,
	}.Build())
	require.NoError(t, err)
}

func TestGetCapabilities(t *testing.T) {
	ctx := context.Background()

	t.Run("BasicResourceSyncer", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestResourceSyncer("test-resource"),
		}))
		require.NoError(t, err)

		// Get the builder to call GetCapabilities
		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.GetCapabilities(ctx)
		require.NoError(t, err)
		require.NotNil(t, caps)

		// Should have SYNC capability
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Len(t, caps.GetResourceTypeCapabilities(), 1)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_SYNC)
	})

	t.Run("ResourceTargetedSyncer", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestResourceTargetedSyncer("test-resource"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.GetCapabilities(ctx)
		require.NoError(t, err)

		// Should have SYNC and TARGETED_SYNC capabilities
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_TARGETED_SYNC)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_TARGETED_SYNC)
	})

	t.Run("ResourceProvisioner", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestResourceProvisioner("test-resource"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.GetCapabilities(ctx)
		require.NoError(t, err)

		// Should have SYNC and PROVISION capabilities
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_PROVISION)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_PROVISION)
	})

	t.Run("AccountManager", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestAccountManager("test-resource"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.GetCapabilities(ctx)
		require.NoError(t, err)

		// Should have SYNC and ACCOUNT_PROVISIONING capabilities
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_ACCOUNT_PROVISIONING)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_ACCOUNT_PROVISIONING)
	})

	t.Run("CredentialManager", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestCredentialManager("test-resource"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.GetCapabilities(ctx)
		require.NoError(t, err)

		// Should have SYNC and CREDENTIAL_ROTATION capabilities
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ROTATION)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_CREDENTIAL_ROTATION)
	})

	t.Run("ResourceManager", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestResourceManager("test-resource"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.GetCapabilities(ctx)
		require.NoError(t, err)

		// Should have SYNC, RESOURCE_CREATE, and RESOURCE_DELETE capabilities
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_RESOURCE_CREATE)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_RESOURCE_DELETE)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_RESOURCE_CREATE)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_RESOURCE_DELETE)
	})

	t.Run("ResourceDeleter", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{
			newTestResourceDeleter("test-resource"),
		}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.GetCapabilities(ctx)
		require.NoError(t, err)

		// Should have SYNC and RESOURCE_DELETE capabilities (but not RESOURCE_CREATE)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_RESOURCE_DELETE)
		require.NotContains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_RESOURCE_CREATE)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_RESOURCE_DELETE)
		require.NotContains(t, caps.GetResourceTypeCapabilities()[0].GetCapabilities(), v2.Capability_CAPABILITY_RESOURCE_CREATE)
	})

	t.Run("EventFeeds", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestEventProvider())
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.GetCapabilities(ctx)
		require.NoError(t, err)

		// Should have EVENT_FEED_V2 capability (but not SYNC since no resource syncers)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_EVENT_FEED_V2)
		require.NotContains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_SYNC)
	})

	t.Run("TicketManager", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestTicketManager())
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.GetCapabilities(ctx)
		require.NoError(t, err)

		// Should have TICKETING capability (but not SYNC since no resource syncers)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_TICKETING)
		require.NotContains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_SYNC)
	})

	t.Run("ActionManager", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestRegisterActionManager())
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.GetCapabilities(ctx)
		require.NoError(t, err)

		// Should have ACTIONS capability (but not SYNC since no resource syncers)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_ACTIONS)
		require.NotContains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_SYNC)
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

		caps, err := builder.GetCapabilities(ctx)
		require.NoError(t, err)

		// Should have capabilities from all resource types
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_SYNC)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_TARGETED_SYNC)
		require.Contains(t, caps.GetConnectorCapabilities(), v2.Capability_CAPABILITY_PROVISION)
		require.Len(t, caps.GetResourceTypeCapabilities(), 3)

		// Verify resource types are sorted by ID
		require.Equal(t, "resource-1", caps.GetResourceTypeCapabilities()[0].GetResourceType().GetId())
		require.Equal(t, "resource-2", caps.GetResourceTypeCapabilities()[1].GetResourceType().GetId())
		require.Equal(t, "resource-3", caps.GetResourceTypeCapabilities()[2].GetResourceType().GetId())
	})

	t.Run("EmptyResourceBuilders", func(t *testing.T) {
		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{}))
		require.NoError(t, err)

		builder, ok := connector.(*builder)
		require.True(t, ok)

		caps, err := builder.GetCapabilities(ctx)
		require.NoError(t, err)

		// Should have no capabilities
		require.Empty(t, caps.GetConnectorCapabilities())
		require.Empty(t, caps.GetResourceTypeCapabilities())
	})
}

func TestResourceSyncerPagination(t *testing.T) {
	ctx := context.Background()

	t.Run("ListResources pagination", func(t *testing.T) {
		// Create a mock resource syncer that supports pagination
		paginatedSyncer := &testPaginatedResourceSyncer{
			resourceType: v2.ResourceType_builder{
				Id:          "test-resource",
				DisplayName: "Test Resource",
			}.Build(),
			totalResources: 5, // Simulate 5 total resources
		}

		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{paginatedSyncer}))
		require.NoError(t, err)

		// Test first page (no page token)
		firstPageResp, err := connector.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId: "test-resource",
		}.Build())
		require.NoError(t, err)
		require.Len(t, firstPageResp.GetList(), 2)                   // First page has 2 items
		require.Equal(t, "page-2", firstPageResp.GetNextPageToken()) // Should return next page token

		// Test second page (with page token)
		secondPageResp, err := connector.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId: "test-resource",
			PageToken:      "page-2",
		}.Build())
		require.NoError(t, err)
		require.Len(t, secondPageResp.GetList(), 2)                   // Second page has 2 items
		require.Equal(t, "page-4", secondPageResp.GetNextPageToken()) // Should return next page token

		// Test third page (with page token)
		thirdPageResp, err := connector.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId: "test-resource",
			PageToken:      "page-4",
		}.Build())
		require.NoError(t, err)
		require.Len(t, thirdPageResp.GetList(), 1)         // Last page has 1 item
		require.Empty(t, thirdPageResp.GetNextPageToken()) // Should return empty token (last page)

		// Test invalid page token
		invalidPageResp, err := connector.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId: "test-resource",
			PageToken:      "invalid-token",
		}.Build())
		require.NoError(t, err)
		require.Empty(t, invalidPageResp.GetList())          // Should return empty list
		require.Empty(t, invalidPageResp.GetNextPageToken()) // Should return empty token
	})

	t.Run("ListEntitlements pagination", func(t *testing.T) {
		paginatedSyncer := &testPaginatedResourceSyncer{
			resourceType: v2.ResourceType_builder{
				Id:          "test-resource",
				DisplayName: "Test Resource",
			}.Build(),
			totalEntitlements: 3, // Simulate 3 total entitlements
		}

		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{paginatedSyncer}))
		require.NoError(t, err)

		testResource := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "test-resource",
				Resource:     "test-resource-1",
			}.Build(),
		}.Build()

		// Test first page (no page token)
		firstPageResp, err := connector.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			Resource: testResource,
		}.Build())
		require.NoError(t, err)
		require.Len(t, firstPageResp.GetList(), 2) // First page has 2 items
		require.Equal(t, "entitlement-page-2", firstPageResp.GetNextPageToken())

		// Test second page (with page token)
		secondPageResp, err := connector.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			Resource:  testResource,
			PageToken: "entitlement-page-2",
		}.Build())
		require.NoError(t, err)
		require.Len(t, secondPageResp.GetList(), 1)         // Last page has 1 item
		require.Empty(t, secondPageResp.GetNextPageToken()) // Should return empty token (last page)
	})

	t.Run("ListGrants pagination", func(t *testing.T) {
		paginatedSyncer := &testPaginatedResourceSyncer{
			resourceType: v2.ResourceType_builder{
				Id:          "test-resource",
				DisplayName: "Test Resource",
			}.Build(),
			totalGrants: 4, // Simulate 4 total grants
		}

		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{paginatedSyncer}))
		require.NoError(t, err)

		testResource := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "test-resource",
				Resource:     "test-resource-1",
			}.Build(),
		}.Build()

		// Test first page (no page token)
		firstPageResp, err := connector.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			Resource: testResource,
		}.Build())
		require.NoError(t, err)
		require.Len(t, firstPageResp.GetList(), 2) // First page has 2 items
		require.Equal(t, "grant-page-2", firstPageResp.GetNextPageToken())

		// Test second page (with page token)
		secondPageResp, err := connector.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			Resource:  testResource,
			PageToken: "grant-page-2",
		}.Build())
		require.NoError(t, err)
		require.Len(t, secondPageResp.GetList(), 2)         // Second page has 2 items
		require.Empty(t, secondPageResp.GetNextPageToken()) // Should return empty token (last page)
	})

	t.Run("Single page results", func(t *testing.T) {
		// Test syncer that returns all results in one page
		singlePageSyncer := &testSinglePageResourceSyncer{
			resourceType: v2.ResourceType_builder{
				Id:          "single-resource",
				DisplayName: "Single Resource",
			}.Build(),
		}

		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{singlePageSyncer}))
		require.NoError(t, err)

		// Test that single page returns empty next page token
		resp, err := connector.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId: "single-resource",
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 1)         // Single item
		require.Empty(t, resp.GetNextPageToken()) // Should return empty token
	})

	t.Run("Empty results", func(t *testing.T) {
		// Test syncer that returns no results
		emptySyncer := &testEmptyResourceSyncer{
			resourceType: v2.ResourceType_builder{
				Id:          "empty-resource",
				DisplayName: "Empty Resource",
			}.Build(),
		}

		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{emptySyncer}))
		require.NoError(t, err)

		// Test that empty results return empty next page token
		resp, err := connector.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId: "empty-resource",
		}.Build())
		require.NoError(t, err)
		require.Empty(t, resp.GetList())          // No items
		require.Empty(t, resp.GetNextPageToken()) // Should return empty token
	})

	t.Run("Same page token returned", func(t *testing.T) {
		// Test syncer that returns the same page token (edge case)
		sameTokenSyncer := &testSameTokenResourceSyncer{
			resourceType: v2.ResourceType_builder{
				Id:          "same-token-resource",
				DisplayName: "Same Token Resource",
			}.Build(),
		}

		connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{sameTokenSyncer}))
		require.NoError(t, err)

		// Test that same token scenario is handled correctly - should return an error
		resp, err := connector.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId: "same-token-resource",
			PageToken:      "same-token",
		}.Build())
		require.Error(t, err) // Should return an error for same page token
		require.ErrorContains(t, err, "next page token unchanged")
		// Response should contain the data returned before the error was detected
		require.NotNil(t, resp)
		require.Empty(t, resp.GetList())                        // Should return empty list
		require.Equal(t, "same-token", resp.GetNextPageToken()) // Should return the same token
	})
}

type testPaginatedResourceSyncer struct {
	resourceType      *v2.ResourceType
	totalResources    int
	totalEntitlements int
	totalGrants       int
}

func (t *testPaginatedResourceSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return t.resourceType
}

func (t *testPaginatedResourceSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	var resources []*v2.Resource
	var nextPageToken string

	if pToken == nil || pToken.Token == "" {
		// First page - return first 2 resources
		for i := 1; i <= 2 && i <= t.totalResources; i++ {
			resources = append(resources, v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: t.resourceType.GetId(),
					Resource:     fmt.Sprintf("resource-%d", i),
				}.Build(),
				DisplayName: fmt.Sprintf("Resource %d", i),
			}.Build())
		}
		if t.totalResources > 2 {
			nextPageToken = "page-2"
		}
		return resources, nextPageToken, annotations.Annotations{}, nil
	}

	if pToken.Token == "page-2" {
		// Second page - return next 2 resources
		for i := 3; i <= 4 && i <= t.totalResources; i++ {
			resources = append(resources, v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: t.resourceType.GetId(),
					Resource:     fmt.Sprintf("resource-%d", i),
				}.Build(),
				DisplayName: fmt.Sprintf("Resource %d", i),
			}.Build())
		}
		if t.totalResources > 4 {
			nextPageToken = "page-4"
		}
		return resources, nextPageToken, annotations.Annotations{}, nil
	}

	if pToken.Token == "page-4" {
		// Third page - return remaining resources
		for i := 5; i <= t.totalResources; i++ {
			resources = append(resources, v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: t.resourceType.GetId(),
					Resource:     fmt.Sprintf("resource-%d", i),
				}.Build(),
				DisplayName: fmt.Sprintf("Resource %d", i),
			}.Build())
		}
		// No more pages
	}

	return resources, nextPageToken, annotations.Annotations{}, nil
}

func (t *testPaginatedResourceSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var entitlements []*v2.Entitlement
	var nextPageToken string

	if pToken == nil || pToken.Token == "" {
		// First page - return first 2 entitlements
		for i := 1; i <= 2 && i <= t.totalEntitlements; i++ {
			entitlements = append(entitlements, v2.Entitlement_builder{
				Resource: resource,
				Id:       fmt.Sprintf("entitlement-%d", i),
			}.Build())
		}
		if t.totalEntitlements > 2 {
			nextPageToken = "entitlement-page-2"
		}
	} else if pToken.Token == "entitlement-page-2" {
		// Second page - return remaining entitlements
		for i := 3; i <= t.totalEntitlements; i++ {
			entitlements = append(entitlements, v2.Entitlement_builder{
				Resource: resource,
				Id:       fmt.Sprintf("entitlement-%d", i),
			}.Build())
		}
		// No more pages
	}

	return entitlements, nextPageToken, annotations.Annotations{}, nil
}

func (t *testPaginatedResourceSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	var grants []*v2.Grant
	var nextPageToken string

	if pToken == nil || pToken.Token == "" {
		// First page - return first 2 grants
		for i := 1; i <= 2 && i <= t.totalGrants; i++ {
			grants = append(grants, v2.Grant_builder{
				Principal: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: "user",
						Resource:     fmt.Sprintf("user-%d", i),
					}.Build(),
				}.Build(),
				Entitlement: v2.Entitlement_builder{
					Resource: resource,
					Id:       fmt.Sprintf("entitlement-%d", i),
				}.Build(),
			}.Build())
		}
		if t.totalGrants > 2 {
			nextPageToken = "grant-page-2"
		}
	} else if pToken.Token == "grant-page-2" {
		// Second page - return remaining grants
		for i := 3; i <= t.totalGrants; i++ {
			grants = append(grants, v2.Grant_builder{
				Principal: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: "user",
						Resource:     fmt.Sprintf("user-%d", i),
					}.Build(),
				}.Build(),
				Entitlement: v2.Entitlement_builder{
					Resource: resource,
					Id:       fmt.Sprintf("entitlement-%d", i),
				}.Build(),
			}.Build())
		}
		// No more pages
	}

	return grants, nextPageToken, annotations.Annotations{}, nil
}

type testSinglePageResourceSyncer struct {
	resourceType *v2.ResourceType
}

func (t *testSinglePageResourceSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return t.resourceType
}

func (t *testSinglePageResourceSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	return []*v2.Resource{
		v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: t.resourceType.GetId(),
				Resource:     "single-resource-1",
			}.Build(),
			DisplayName: "Single Resource 1",
		}.Build(),
	}, "", annotations.Annotations{}, nil
}

func (t *testSinglePageResourceSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return []*v2.Entitlement{
		v2.Entitlement_builder{
			Resource: resource,
			Id:       "single-entitlement",
		}.Build(),
	}, "", annotations.Annotations{}, nil
}

func (t *testSinglePageResourceSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return []*v2.Grant{
		v2.Grant_builder{
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     "single-user",
				}.Build(),
			}.Build(),
			Entitlement: v2.Entitlement_builder{
				Resource: resource,
				Id:       "single-entitlement",
			}.Build(),
		}.Build(),
	}, "", annotations.Annotations{}, nil
}

type testEmptyResourceSyncer struct {
	resourceType *v2.ResourceType
}

func (t *testEmptyResourceSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return t.resourceType
}

func (t *testEmptyResourceSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	return []*v2.Resource{}, "", annotations.Annotations{}, nil
}

func (t *testEmptyResourceSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return []*v2.Entitlement{}, "", annotations.Annotations{}, nil
}

func (t *testEmptyResourceSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return []*v2.Grant{}, "", annotations.Annotations{}, nil
}

type testSameTokenResourceSyncer struct {
	resourceType *v2.ResourceType
}

func (t *testSameTokenResourceSyncer) ResourceType(ctx context.Context) *v2.ResourceType {
	return t.resourceType
}

func (t *testSameTokenResourceSyncer) List(ctx context.Context, parentResourceID *v2.ResourceId, pToken *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	if pToken != nil && pToken.Token == "same-token" {
		// Return same token - this should trigger the edge case handling
		return []*v2.Resource{}, "same-token", annotations.Annotations{}, nil
	}
	return []*v2.Resource{}, "", annotations.Annotations{}, nil
}

func (t *testSameTokenResourceSyncer) Entitlements(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return []*v2.Entitlement{}, "", annotations.Annotations{}, nil
}

func (t *testSameTokenResourceSyncer) Grants(ctx context.Context, resource *v2.Resource, pToken *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return []*v2.Grant{}, "", annotations.Annotations{}, nil
}
