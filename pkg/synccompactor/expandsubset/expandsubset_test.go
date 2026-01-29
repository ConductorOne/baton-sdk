package expandsubset

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

func newTempC1Z(t *testing.T) (*dotc1z.C1File, func()) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "expandsubset.c1z")
	f, err := dotc1z.NewC1ZFile(context.Background(), path, dotc1z.WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	cleanup := func() {
		_ = f.Close(context.Background())
		_ = os.Remove(path)
	}
	return f, cleanup
}

func TestExpandGrantsForEntitlementSubset(t *testing.T) {
	ctx := context.Background()

	f, cleanup := newTempC1Z(t)
	defer cleanup()

	syncID, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, f.PutResourceTypes(ctx, v2.ResourceType_builder{Id: "group"}.Build(), v2.ResourceType_builder{Id: "user"}.Build()))

	group1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	user1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	require.NoError(t, f.PutResources(ctx, group1, user1))

	entA := v2.Entitlement_builder{Id: "ent_a", Resource: group1, Slug: "member"}.Build()
	entB := v2.Entitlement_builder{Id: "ent_b", Resource: group1, Slug: "admin"}.Build()
	require.NoError(t, f.PutEntitlements(ctx, entA, entB))

	gDirect := v2.Grant_builder{Id: "g_direct", Entitlement: entA, Principal: user1}.Build()
	require.NoError(t, f.PutGrants(ctx, gDirect))

	edgeAnno := v2.GrantExpansionEdges_builder{EntitlementIds: []string{"ent_a"}}.Build()
	gRel := v2.Grant_builder{
		Id:          "g_rel",
		Entitlement: entB,
		Principal:   group1,
		Annotations: annotations.New(edgeAnno),
	}.Build()
	require.NoError(t, f.PutGrants(ctx, gRel))

	require.NoError(t, f.BuildEntitlementEdgesForSync(ctx, syncID))
	require.NoError(t, ExpandGrantsForEntitlementSubset(ctx, f, syncID, []string{"ent_a"}))

	resp, err := f.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: entB,
	}.Build())
	require.NoError(t, err)
	found := false
	for _, g := range resp.GetList() {
		if g.GetPrincipal().GetId().GetResourceType() == "user" && g.GetPrincipal().GetId().GetResource() == "u1" {
			found = true
			break
		}
	}
	require.True(t, found, "expected an expanded grant for user u1 on ent_b")
}
