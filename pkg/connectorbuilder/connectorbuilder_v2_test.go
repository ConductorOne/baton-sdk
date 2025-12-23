package connectorbuilder

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestConnectorBuilderV2_FullCapabilities(t *testing.T) {
	ctx := context.Background()

	// Create a connector that implements ConnectorBuilder2 with multiple V2 interfaces
	fullConnector := &testConnectorBuilderV2Full{
		resourceSyncers: []ResourceSyncerV2{
			newTestResourceSyncerV2WithProvisioner("resource-1"),
			newTestResourceSyncerV2WithManager("resource-2"),
			newTestResourceSyncerV2WithAccountProvisioner("user-1"),
		},
		hasActionManager: true,
		hasEventProvider: true,
	}

	connector, err := NewConnector(ctx, fullConnector)
	require.NoError(t, err)

	// Verify the connector was created successfully
	require.NotNil(t, connector)

	// Test ResourceSyncerV2 functionality
	resp, err := connector.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: "resource-1",
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.GetList(), 1)
	require.Equal(t, "resource-1-1", resp.GetList()[0].GetId().GetResource())

	// Test ResourceProvisionerV2 functionality
	grantResp, err := connector.Grant(ctx, v2.GrantManagerServiceGrantRequest_builder{
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "user-1",
			}.Build(),
		}.Build(),
		Entitlement: v2.Entitlement_builder{
			Id: "entitlement-1",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "resource-1",
					Resource:     "resource-1-1",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, grantResp)
	require.Len(t, grantResp.GetGrants(), 1)
	require.Equal(t, "grant-v2-1", grantResp.GetGrants()[0].GetId())

	// Test ResourceManagerV2 functionality
	createResp, err := connector.CreateResource(ctx, v2.CreateResourceRequest_builder{
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "resource-2",
				Resource:     "new-resource",
			}.Build(),
			DisplayName: "New Resource",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, createResp)
	require.NotNil(t, createResp.GetCreated())
	require.Equal(t, "new-resource", createResp.GetCreated().GetId().GetResource())

	// Test ResourceDeleterV2 functionality
	deleteResp, err := connector.DeleteResource(ctx, v2.DeleteResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{
			ResourceType: "resource-2",
			Resource:     "resource-to-delete",
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, deleteResp)

	// Test RegisterActionManager functionality
	actionResp, err := connector.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name: "test-action",
		Args: &structpb.Struct{},
	}.Build())
	require.NoError(t, err)
	require.NotNil(t, actionResp)
	require.NotNil(t, actionResp.GetResponse())

	require.NotEmpty(t, actionResp.GetId())

	// Test legacy event feed functionality
	eventResp, err := connector.ListEventFeeds(ctx, &v2.ListEventFeedsRequest{})
	require.NoError(t, err)
	require.NotNil(t, eventResp)
	require.Len(t, eventResp.GetList(), 1)
	require.Equal(t, "baton_feed_event", eventResp.GetList()[0].GetId())

	// Test metadata (should include capabilities)
	metadataResp, err := connector.GetMetadata(ctx, &v2.ConnectorServiceGetMetadataRequest{})
	require.NoError(t, err)
	require.NotNil(t, metadataResp)
	require.NotNil(t, metadataResp.GetMetadata())
	require.NotNil(t, metadataResp.GetMetadata().GetCapabilities())
}

type testConnectorBuilderV2Full struct {
	resourceSyncers  []ResourceSyncerV2
	hasActionManager bool
	hasEventProvider bool
}

func (t *testConnectorBuilderV2Full) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return v2.ConnectorMetadata_builder{
		DisplayName: "test-connector-v2-full",
		Description: "A test connector v2 with ResourceSyncerV2s",
	}.Build(), nil
}

func (t *testConnectorBuilderV2Full) Validate(ctx context.Context) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

func (t *testConnectorBuilderV2Full) ResourceSyncers(ctx context.Context) []ResourceSyncerV2 {
	return t.resourceSyncers
}

func (t *testConnectorBuilderV2Full) RegisterActionManager(ctx context.Context) (CustomActionManager, error) {
	if !t.hasActionManager {
		return nil, &ActionManagerNotImplementedError{}
	}
	return &testCustomActionManager{}, nil
}

func (t *testConnectorBuilderV2Full) ListEvents(
	ctx context.Context,
	earliestEvent *timestamppb.Timestamp,
	pToken *pagination.StreamToken,
) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	return []*v2.Event{
		v2.Event_builder{
			Id: "test-event-1",
		}.Build(),
	}, &pagination.StreamState{}, annotations.Annotations{}, nil
}

func newTestResourceSyncerV2WithAccountProvisioner(resourceType string) ResourceSyncerV2 {
	return &testResourceSyncerV2WithAccountProvisioner{
		testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: resourceType},
	}
}

func newTestResourceSyncerV2WithProvisioner(resourceType string) ResourceSyncerV2 {
	return &testResourceSyncerV2WithProvisioner{
		testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: resourceType},
	}
}

func newTestResourceSyncerV2WithManager(resourceType string) ResourceSyncerV2 {
	return &testResourceSyncerV2WithManager{
		testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: resourceType},
	}
}

type testResourceSyncerV2Simple struct {
	resourceType string
}

func (t *testResourceSyncerV2Simple) ResourceType(ctx context.Context) *v2.ResourceType {
	return v2.ResourceType_builder{
		Id:          t.resourceType,
		DisplayName: "Test " + t.resourceType,
	}.Build()
}

func (t *testResourceSyncerV2Simple) List(
	ctx context.Context,
	parentResourceID *v2.ResourceId,
	opts resource.SyncOpAttrs,
) ([]*v2.Resource, *resource.SyncOpResults, error) {
	return []*v2.Resource{
		v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: t.resourceType,
				Resource:     t.resourceType + "-1",
			}.Build(),
			DisplayName: "Test Resource",
		}.Build(),
	}, nil, nil
}

func (t *testResourceSyncerV2Simple) StaticEntitlements(ctx context.Context, opts resource.SyncOpAttrs) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	return []*v2.Entitlement{}, nil, nil
}

func (t *testResourceSyncerV2Simple) Entitlements(
	ctx context.Context,
	resource *v2.Resource,
	opts resource.SyncOpAttrs,
) ([]*v2.Entitlement, *resource.SyncOpResults, error) {
	return []*v2.Entitlement{
		v2.Entitlement_builder{
			Id:          "entitlement-1",
			DisplayName: "Test Entitlement",
			Resource:    resource,
		}.Build(),
	}, nil, nil
}

func (t *testResourceSyncerV2Simple) Grants(
	ctx context.Context,
	resource *v2.Resource,
	opts resource.SyncOpAttrs,
) ([]*v2.Grant, *resource.SyncOpResults, error) {
	return []*v2.Grant{
		v2.Grant_builder{
			Id: "grant-1",
		}.Build(),
	}, nil, nil
}

var _ AccountManagerV2 = &testResourceSyncerV2WithAccountProvisioner{}

type testResourceSyncerV2WithAccountProvisioner struct {
	testResourceSyncerV2Simple
}

func (t *testResourceSyncerV2WithAccountProvisioner) CreateAccount(
	ctx context.Context,
	accountInfo *v2.AccountInfo,
	credentialOptions *v2.LocalCredentialOptions,
) (CreateAccountResponse, []*v2.PlaintextData, annotations.Annotations, error) {
	return nil, nil, annotations.Annotations{}, nil
}

func (t *testResourceSyncerV2WithAccountProvisioner) CreateAccountCapabilityDetails(ctx context.Context) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return nil, annotations.Annotations{}, nil
}

type testResourceSyncerV2WithProvisioner struct {
	testResourceSyncerV2Simple
}

func (t *testResourceSyncerV2WithProvisioner) Grant(
	ctx context.Context,
	principal *v2.Resource,
	entitlement *v2.Entitlement,
) ([]*v2.Grant, annotations.Annotations, error) {
	return []*v2.Grant{
		v2.Grant_builder{
			Id: "grant-v2-1",
		}.Build(),
	}, annotations.Annotations{}, nil
}

func (t *testResourceSyncerV2WithProvisioner) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

type testResourceSyncerV2WithManager struct {
	testResourceSyncerV2Simple
}

func (t *testResourceSyncerV2WithManager) Create(
	ctx context.Context,
	resource *v2.Resource,
) (*v2.Resource, annotations.Annotations, error) {
	return resource, annotations.Annotations{}, nil
}

func (t *testResourceSyncerV2WithManager) Delete(
	ctx context.Context,
	resourceId *v2.ResourceId,
	parentResourceID *v2.ResourceId,
) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

type ActionManagerNotImplementedError struct{}

func (e *ActionManagerNotImplementedError) Error() string { return "action manager not implemented" }

type EventProviderNotImplementedError struct{}

func (e *EventProviderNotImplementedError) Error() string { return "event provider not implemented" }
