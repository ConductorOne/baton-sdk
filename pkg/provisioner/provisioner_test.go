package provisioner

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/types"
)

// fakeGrantConnectorClient captures the requests handed to Grant/Revoke so
// tests can inspect exactly what the connector received. All other
// ConnectorClient methods are left as the nil embedded interface and must
// not be called by these tests.
type fakeGrantConnectorClient struct {
	types.ConnectorClient

	grantReq  *v2.GrantManagerServiceGrantRequest
	revokeReq *v2.GrantManagerServiceRevokeRequest
}

func (f *fakeGrantConnectorClient) Grant(ctx context.Context, in *v2.GrantManagerServiceGrantRequest, opts ...grpc.CallOption) (*v2.GrantManagerServiceGrantResponse, error) {
	f.grantReq = in
	return v2.GrantManagerServiceGrantResponse_builder{}.Build(), nil
}

func (f *fakeGrantConnectorClient) Revoke(ctx context.Context, in *v2.GrantManagerServiceRevokeRequest, opts ...grpc.CallOption) (*v2.GrantManagerServiceRevokeResponse, error) {
	f.revokeReq = in
	return v2.GrantManagerServiceRevokeResponse_builder{}.Build(), nil
}

// buildHydrationTestStore writes a group resource (the entitlement's
// resource), a user resource (the grant principal) and one entitlement +
// grant connecting them, each carrying Profile data that only a fully
// hydrated v2.Resource would preserve. It returns a freshly reopened
// read/write store so reads go through the same code path a real
// `baton grant`/`baton revoke` invocation would use.
func buildHydrationTestStore(t *testing.T, ctx context.Context, engine c1zstore.Engine) connectorstore.Reader {
	t.Helper()

	path := filepath.Join(t.TempDir(), "test.c1z")

	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
	require.NoError(t, err)

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	require.NoError(t, store.PutResourceTypes(ctx, userRT, groupRT))

	groupProfile, err := structpb.NewStruct(map[string]interface{}{"href": "https://example.com/folders/g1"})
	require.NoError(t, err)
	group := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "Group One",
		Profile:     groupProfile,
	}.Build()

	userProfile, err := structpb.NewStruct(map[string]interface{}{"email": "alice@example.com"})
	require.NoError(t, err)
	user := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		DisplayName: "Alice",
		Profile:     userProfile,
	}.Build()
	require.NoError(t, store.PutResources(ctx, group, user))

	member := v2.Entitlement_builder{
		Id:          "member",
		Resource:    group,
		DisplayName: "Member",
		Slug:        "member",
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
	}.Build()
	require.NoError(t, store.PutEntitlements(ctx, member))

	grant := v2.Grant_builder{
		Id:          "grant1",
		Principal:   user,
		Entitlement: member,
	}.Build()
	require.NoError(t, store.PutGrants(ctx, grant))

	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	reopened, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close(ctx) })

	return reopened
}

func TestProvisionerGrantHydratesResources(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			ctx := context.Background()
			store := buildHydrationTestStore(t, ctx, engine)

			cc := &fakeGrantConnectorClient{}
			p := &Provisioner{
				store:              store,
				connector:          cc,
				grantEntitlementID: "member",
				grantPrincipalID:   "alice",
				grantPrincipalType: "user",
			}

			require.NoError(t, p.grant(ctx))
			require.NotNil(t, cc.grantReq)

			principal := cc.grantReq.GetPrincipal()
			require.Equal(t, "Alice", principal.GetDisplayName())
			require.Equal(t, "alice@example.com", principal.GetProfile().GetFields()["email"].GetStringValue())

			hydratedEntitlement := cc.grantReq.GetEntitlement()
			entResource := hydratedEntitlement.GetResource()
			require.Equal(t, "Group One", entResource.GetDisplayName(),
				"entitlement.Resource must be the fully hydrated group, not an identity-only stub")
			require.Equal(t, "https://example.com/folders/g1", entResource.GetProfile().GetFields()["href"].GetStringValue(),
				"entitlement.Resource.Profile must survive so a connector's Grant() can read it")

			require.Equal(t, "member", hydratedEntitlement.GetSlug(),
				"hydrateEntitlementResource must preserve the entitlement's own fields, not just splice in Resource")
			require.Equal(t, "Member", hydratedEntitlement.GetDisplayName())
			require.Equal(t, v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT, hydratedEntitlement.GetPurpose())
		})
	}
}

func TestProvisionerRevokeHydratesResources(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
		t.Run(string(engine), func(t *testing.T) {
			ctx := context.Background()
			store := buildHydrationTestStore(t, ctx, engine)

			cc := &fakeGrantConnectorClient{}
			p := &Provisioner{
				store:         store,
				connector:     cc,
				revokeGrantID: "grant1",
			}

			require.NoError(t, p.revoke(ctx))
			require.NotNil(t, cc.revokeReq)

			principal := cc.revokeReq.GetGrant().GetPrincipal()
			require.Equal(t, "Alice", principal.GetDisplayName())
			require.Equal(t, "alice@example.com", principal.GetProfile().GetFields()["email"].GetStringValue())

			hydratedEntitlement := cc.revokeReq.GetGrant().GetEntitlement()
			entResource := hydratedEntitlement.GetResource()
			require.Equal(t, "Group One", entResource.GetDisplayName(),
				"entitlement.Resource must be the fully hydrated group, not an identity-only stub")
			require.Equal(t, "https://example.com/folders/g1", entResource.GetProfile().GetFields()["href"].GetStringValue(),
				"entitlement.Resource.Profile must survive so a connector's Revoke() can read it")

			require.Equal(t, "member", hydratedEntitlement.GetSlug(),
				"hydrateEntitlementResource must preserve the entitlement's own fields, not just splice in Resource")
			require.Equal(t, "Member", hydratedEntitlement.GetDisplayName())
			require.Equal(t, v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT, hydratedEntitlement.GetPurpose())
		})
	}
}
