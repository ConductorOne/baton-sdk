package connectorbuilder

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// --- Swap test connector (simple, no action manager) ---

type testSwapConnector struct {
	resourceSyncers []ResourceSyncerV2
}

func (t *testSwapConnector) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return v2.ConnectorMetadata_builder{
		DisplayName: "test-swap-connector",
	}.Build(), nil
}

func (t *testSwapConnector) Validate(ctx context.Context) (annotations.Annotations, error) {
	return annotations.Annotations{}, nil
}

func (t *testSwapConnector) ResourceSyncers(ctx context.Context) []ResourceSyncerV2 {
	return t.resourceSyncers
}

// --- Swap test fixtures ---

type testResourceSyncerV2WithSwap struct {
	testResourceSyncerV2WithProvisioner
	swapCalled bool
}

func newTestResourceSyncerV2WithSwap(resourceType string) *testResourceSyncerV2WithSwap {
	return &testResourceSyncerV2WithSwap{
		testResourceSyncerV2WithProvisioner: testResourceSyncerV2WithProvisioner{
			testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: resourceType},
		},
	}
}

func (t *testResourceSyncerV2WithSwap) Swap(
	ctx context.Context,
	currentGrant *v2.Grant,
	newEntitlement *v2.Entitlement,
) ([]*v2.Grant, annotations.Annotations, error) {
	t.swapCalled = true
	return []*v2.Grant{
		v2.Grant_builder{Id: "swap-grant-1"}.Build(),
	}, annotations.Annotations{}, nil
}

type testResourceSyncerV2WithFailingGrant struct {
	testResourceSyncerV2Simple
	revokeCalled bool
	grantCalled  bool
}

func newTestResourceSyncerV2WithFailingGrant(resourceType string) *testResourceSyncerV2WithFailingGrant {
	return &testResourceSyncerV2WithFailingGrant{
		testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: resourceType},
	}
}

func (t *testResourceSyncerV2WithFailingGrant) Grant(
	ctx context.Context,
	principal *v2.Resource,
	entitlement *v2.Entitlement,
) ([]*v2.Grant, annotations.Annotations, error) {
	t.grantCalled = true
	return nil, nil, status.Error(codes.PermissionDenied, "grant denied")
}

func (t *testResourceSyncerV2WithFailingGrant) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	t.revokeCalled = true
	return annotations.Annotations{}, nil
}

// --- Swap tests ---

func makeSwapRequest(resourceType string) *v2.SwapServiceSwapRequest {
	return v2.SwapServiceSwapRequest_builder{
		RevokeGrant: v2.Grant_builder{
			Id: "old-grant-1",
			Entitlement: v2.Entitlement_builder{
				Id: "old-entitlement-1",
				Resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: resourceType,
						Resource:     resourceType + "-1",
					}.Build(),
				}.Build(),
			}.Build(),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     "user-1",
				}.Build(),
			}.Build(),
		}.Build(),
		GrantEntitlement: v2.Entitlement_builder{
			Id: "new-entitlement-1",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: resourceType,
					Resource:     resourceType + "-1",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
}

func TestSwapNativeHappyPath(t *testing.T) {
	ctx := context.Background()
	syncer := newTestResourceSyncerV2WithSwap("resource-1")

	connector, err := NewConnector(ctx, &testSwapConnector{
		resourceSyncers: []ResourceSyncerV2{syncer},
	})
	require.NoError(t, err)

	resp, err := connector.Swap(ctx, makeSwapRequest("resource-1"))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.GetGrants(), 1)
	require.Equal(t, "swap-grant-1", resp.GetGrants()[0].GetId())
	require.True(t, syncer.swapCalled)
}

func TestSwapFallbackHappyPath(t *testing.T) {
	ctx := context.Background()

	connector, err := NewConnector(ctx, &testSwapConnector{
		resourceSyncers: []ResourceSyncerV2{
			newTestResourceSyncerV2WithProvisioner("resource-1"),
		},
	})
	require.NoError(t, err)

	resp, err := connector.Swap(ctx, makeSwapRequest("resource-1"))
	require.NoError(t, err)
	require.NotNil(t, resp)
	// Fallback Grant returns "grant-v2-1" from testResourceSyncerV2WithProvisioner
	require.Len(t, resp.GetGrants(), 1)
	require.Equal(t, "grant-v2-1", resp.GetGrants()[0].GetId())
}

func TestSwapFallbackPartialFailure(t *testing.T) {
	ctx := context.Background()
	syncer := newTestResourceSyncerV2WithFailingGrant("resource-1")

	connector, err := NewConnector(ctx, &testSwapConnector{
		resourceSyncers: []ResourceSyncerV2{syncer},
	})
	require.NoError(t, err)

	_, err = connector.Swap(ctx, makeSwapRequest("resource-1"))
	require.Error(t, err)
	require.True(t, syncer.revokeCalled, "revoke should have been called")
	require.True(t, syncer.grantCalled, "grant should have been called")

	// Verify it's a gRPC Internal error with SwapPartialFailure detail
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Internal, st.Code())

	details := st.Details()
	require.Len(t, details, 1)
	partialFailure, ok := details[0].(*v2.SwapPartialFailure)
	require.True(t, ok)
	require.Equal(t, "old-grant-1", partialFailure.GetRevokedGrant().GetId())
	require.Equal(t, "new-entitlement-1", partialFailure.GetFailedGrantEntitlement().GetId())
}

func TestSwapResourceTypeMismatch(t *testing.T) {
	ctx := context.Background()

	connector, err := NewConnector(ctx, &testSwapConnector{
		resourceSyncers: []ResourceSyncerV2{
			newTestResourceSyncerV2WithProvisioner("resource-1"),
		},
	})
	require.NoError(t, err)

	req := v2.SwapServiceSwapRequest_builder{
		RevokeGrant: v2.Grant_builder{
			Id: "old-grant",
			Entitlement: v2.Entitlement_builder{
				Id: "old-ent",
				Resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{
						ResourceType: "resource-1",
						Resource:     "r1",
					}.Build(),
				}.Build(),
			}.Build(),
			Principal: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "user",
					Resource:     "user-1",
				}.Build(),
			}.Build(),
		}.Build(),
		GrantEntitlement: v2.Entitlement_builder{
			Id: "new-ent",
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: "resource-2", // Different type!
					Resource:     "r2",
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()

	_, err = connector.Swap(ctx, req)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
}

func TestSwapNoProvisioner(t *testing.T) {
	ctx := context.Background()

	// Simple syncer with no provisioner
	connector, err := NewConnector(ctx, &testSwapConnector{
		resourceSyncers: []ResourceSyncerV2{
			&testResourceSyncerV2Simple{resourceType: "resource-1"},
		},
	})
	require.NoError(t, err)

	_, err = connector.Swap(ctx, makeSwapRequest("resource-1"))
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unimplemented, st.Code())
}

func TestSwapCapabilityDetection(t *testing.T) {
	ctx := context.Background()

	// Connector with SwapProvisioner
	swapConnector, err := NewConnector(ctx, &testSwapConnector{
		resourceSyncers: []ResourceSyncerV2{
			newTestResourceSyncerV2WithSwap("resource-with-swap"),
			newTestResourceSyncerV2WithProvisioner("resource-without-swap"),
		},
	})
	require.NoError(t, err)

	metadataResp, err := swapConnector.GetMetadata(ctx, &v2.ConnectorServiceGetMetadataRequest{})
	require.NoError(t, err)

	caps := metadataResp.GetMetadata().GetCapabilities()
	require.NotNil(t, caps)

	for _, rtCap := range caps.GetResourceTypeCapabilities() {
		if rtCap.GetResourceType().GetId() == "resource-with-swap" {
			require.Contains(t, rtCap.GetCapabilities(), v2.Capability_CAPABILITY_ATOMIC_SWAP,
				"resource-with-swap should have CAPABILITY_ATOMIC_SWAP")
		}
		if rtCap.GetResourceType().GetId() == "resource-without-swap" {
			require.NotContains(t, rtCap.GetCapabilities(), v2.Capability_CAPABILITY_ATOMIC_SWAP,
				"resource-without-swap should NOT have CAPABILITY_ATOMIC_SWAP")
		}
	}
}

// --- Exclusion group validation tests ---

func TestValidateExclusionGroup_NoneOK(t *testing.T) {
	ents := []*v2.Entitlement{
		v2.Entitlement_builder{Id: "ent-1"}.Build(),
		v2.Entitlement_builder{Id: "ent-2"}.Build(),
	}
	err := validateExclusionGroupAnnotations(ents)
	require.NoError(t, err)
}

func TestValidateExclusionGroup_SingleOK(t *testing.T) {
	annos := annotations.Annotations{}
	annos.Append(&v2.EntitlementExclusionGroup{ExclusionGroupId: "tier"})

	ents := []*v2.Entitlement{
		v2.Entitlement_builder{Id: "ent-1", Annotations: annos}.Build(),
	}
	err := validateExclusionGroupAnnotations(ents)
	require.NoError(t, err)
}

func TestValidateExclusionGroup_DuplicateRejected(t *testing.T) {
	annos := annotations.Annotations{}
	annos.Append(&v2.EntitlementExclusionGroup{ExclusionGroupId: "tier-a"})
	annos.Append(&v2.EntitlementExclusionGroup{ExclusionGroupId: "tier-b"})

	ents := []*v2.Entitlement{
		v2.Entitlement_builder{Id: "bad-ent", Annotations: annos}.Build(),
	}
	err := validateExclusionGroupAnnotations(ents)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.InvalidArgument, st.Code())
	require.Contains(t, st.Message(), "bad-ent")
}

func TestValidateExclusionGroup_MixedEntitlements(t *testing.T) {
	goodAnnos := annotations.Annotations{}
	goodAnnos.Append(&v2.EntitlementExclusionGroup{ExclusionGroupId: "tier"})

	badAnnos := annotations.Annotations{}
	badAnnos.Append(&v2.EntitlementExclusionGroup{ExclusionGroupId: "tier-a"})
	badAnnos.Append(&v2.EntitlementExclusionGroup{ExclusionGroupId: "tier-b"})

	ents := []*v2.Entitlement{
		v2.Entitlement_builder{Id: "good-ent", Annotations: goodAnnos}.Build(),
		v2.Entitlement_builder{Id: "bad-ent", Annotations: badAnnos}.Build(),
	}
	err := validateExclusionGroupAnnotations(ents)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad-ent")
}
