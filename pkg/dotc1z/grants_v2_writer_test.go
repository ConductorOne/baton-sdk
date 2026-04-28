package dotc1z

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// newTestC1z constructs a fresh c1z in t.TempDir(), starts a full sync,
// and seeds two resource types (group, user), two resources (g1, u1),
// and two entitlements (ent1, ent2 on g1). Returns the file, sync id,
// and a cleanup that closes the c1z. Extra C1ZOptions (e.g.
// WithV2GrantsWriter) are forwarded to NewC1ZFile.
//
// No binary .c1z files live on disk beyond the test process — fixtures
// are generated at test-setup time (2026-04-22 orchestration decision).
func newTestC1z(ctx context.Context, t *testing.T, opts ...C1ZOption) (*C1File, string, func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.c1z")

	c1f, err := NewC1ZFile(ctx, path, opts...)
	require.NoError(t, err)

	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	groupRT := v2.ResourceType_builder{Id: "group"}.Build()
	userRT := v2.ResourceType_builder{Id: "user"}.Build()
	require.NoError(t, c1f.PutResourceTypes(ctx, groupRT, userRT))

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "Group 1"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "User 1"}.Build()
	require.NoError(t, c1f.PutResources(ctx, g1, u1))

	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1, DisplayName: "Ent 1"}.Build()
	ent2 := v2.Entitlement_builder{Id: "ent2", Resource: g1, DisplayName: "Ent 2"}.Build()
	require.NoError(t, c1f.PutEntitlements(ctx, ent1, ent2))

	cleanup := func() {
		require.NoError(t, c1f.Close(ctx))
	}
	return c1f, syncID, cleanup
}

// readRawGrantBlob returns the raw data blob stored for a grant row,
// bypassing the reader's hydration path.
func readRawGrantBlob(ctx context.Context, t *testing.T, c1f *C1File, externalID string) []byte {
	t.Helper()
	var data []byte
	err := c1f.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT data FROM %s WHERE external_id = ?", grants.Name()),
		externalID,
	).Scan(&data)
	require.NoError(t, err)
	return data
}

func TestV2GrantsWriter_RoundTrip(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t, WithV2GrantsWriter(true))
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "Group 1"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "User 1"}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1, DisplayName: "Ent 1"}.Build()

	grant := v2.Grant_builder{
		Id:          "grant-1",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()
	require.NoError(t, c1f.PutGrants(ctx, grant))

	// The on-disk blob must not contain Entitlement or Principal.
	raw := readRawGrantBlob(ctx, t, c1f, "grant-1")
	bare := &v2.Grant{}
	require.NoError(t, proto.Unmarshal(raw, bare))
	require.Nil(t, bare.GetEntitlement(), "slim blob should have nil Entitlement on disk")
	require.Nil(t, bare.GetPrincipal(), "slim blob should have nil Principal on disk")
	require.Equal(t, "grant-1", bare.GetId(), "Grant.Id is preserved in the blob")

	// The reader must reconstitute identity via stub-from-columns. Stubs
	// carry only Id + nested Resource.Id — DisplayName and other rich
	// fields are zero-value (documented contract; callers needing rich
	// data fetch via GetEntitlement / GetResource).
	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	got := resp.GetList()[0]
	require.Equal(t, "grant-1", got.GetId())
	require.Equal(t, "ent1", got.GetEntitlement().GetId())
	require.Equal(t, "group", got.GetEntitlement().GetResource().GetId().GetResourceType())
	require.Equal(t, "g1", got.GetEntitlement().GetResource().GetId().GetResource())
	require.Equal(t, "user", got.GetPrincipal().GetId().GetResourceType())
	require.Equal(t, "u1", got.GetPrincipal().GetId().GetResource())
	require.Empty(t, got.GetEntitlement().GetDisplayName(), "stub Entitlement carries no DisplayName")
	require.Empty(t, got.GetPrincipal().GetDisplayName(), "stub Principal carries no DisplayName")
}

func TestV2GrantsWriter_DefaultWritesFullBlob(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	require.NoError(t, c1f.PutGrants(ctx, v2.Grant_builder{
		Id:          "grant-full",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()))

	raw := readRawGrantBlob(ctx, t, c1f, "grant-full")
	bare := &v2.Grant{}
	require.NoError(t, proto.Unmarshal(raw, bare))
	require.NotNil(t, bare.GetEntitlement(), "default writer must embed Entitlement in blob")
	require.NotNil(t, bare.GetPrincipal(), "default writer must embed Principal in blob")
	require.Equal(t, "ent1", bare.GetEntitlement().GetId())
}

// TestV2GrantsReader_FullBlobPassThrough confirms that when the reader
// encounters a row that already has Entitlement / Principal populated,
// it does not overwrite them or issue unnecessary hydration queries.
func TestV2GrantsReader_FullBlobPassThrough(t *testing.T) {
	ctx := context.Background()
	// Writer off — rows are full blobs.
	c1f, _, cleanup := newTestC1z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "Original U1"}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1, DisplayName: "Original Ent 1"}.Build()

	require.NoError(t, c1f.PutGrants(ctx, v2.Grant_builder{
		Id:          "grant-full",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()))

	// Mutate the entitlement/resource rows so a hydration pass would be
	// observably different. If the reader hydrates, we'd see "Changed".
	mutatedEnt := v2.Entitlement_builder{Id: "ent1", Resource: g1, DisplayName: "Changed Ent"}.Build()
	require.NoError(t, c1f.PutEntitlements(ctx, mutatedEnt))
	mutatedU1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "Changed U1"}.Build()
	require.NoError(t, c1f.PutResources(ctx, mutatedU1))

	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	got := resp.GetList()[0]
	require.Equal(t, "Original Ent 1", got.GetEntitlement().GetDisplayName(), "full blob must pass through unchanged")
	require.Equal(t, "Original U1", got.GetPrincipal().GetDisplayName(), "full blob must pass through unchanged")
}

// TestV2GrantsReader_MixedBlobs writes some rows with the slim writer
// and some with the full writer into the same v1_grants table, then
// verifies the reader returns correct Grants for each. Rows are
// distinguished per-blob (proto3 nil field detection), no schema marker.
func TestV2GrantsReader_MixedBlobs(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t)
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(), DisplayName: "Group 1"}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "User 1"}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1, DisplayName: "Ent 1"}.Build()
	ent2 := v2.Entitlement_builder{Id: "ent2", Resource: g1, DisplayName: "Ent 2"}.Build()

	// First grant written with slim disabled (full blob).
	c1f.v2GrantsWriter = false
	require.NoError(t, c1f.PutGrants(ctx, v2.Grant_builder{
		Id:          "grant-full",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()))

	// Second grant written with slim enabled (stripped blob).
	c1f.v2GrantsWriter = true
	require.NoError(t, c1f.PutGrants(ctx, v2.Grant_builder{
		Id:          "grant-slim",
		Entitlement: ent2,
		Principal:   u1,
	}.Build()))

	// On-disk: one full, one slim.
	fullBare := &v2.Grant{}
	require.NoError(t, proto.Unmarshal(readRawGrantBlob(ctx, t, c1f, "grant-full"), fullBare))
	require.NotNil(t, fullBare.GetEntitlement(), "grant-full row should be full blob")

	slimBare := &v2.Grant{}
	require.NoError(t, proto.Unmarshal(readRawGrantBlob(ctx, t, c1f, "grant-slim"), slimBare))
	require.Nil(t, slimBare.GetEntitlement(), "grant-slim row should be slim blob")
	require.Nil(t, slimBare.GetPrincipal(), "grant-slim row should be slim blob")

	// Reader returns both correctly.
	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 2)
	byID := map[string]*v2.Grant{}
	for _, g := range resp.GetList() {
		byID[g.GetId()] = g
	}
	require.Equal(t, "ent1", byID["grant-full"].GetEntitlement().GetId())
	require.Equal(t, "u1", byID["grant-full"].GetPrincipal().GetId().GetResource())
	require.Equal(t, "ent2", byID["grant-slim"].GetEntitlement().GetId())
	require.Equal(t, "u1", byID["grant-slim"].GetPrincipal().GetId().GetResource())
}

// TestV2GrantsReader_StubsIgnoreEntitlementTable proves that slim-row
// hydration does not depend on v1_entitlements or v1_resources existing.
// We write slim grants, manually wipe both tables, then read the grants
// back and verify identity is intact on the hydrated stubs. If the
// reader were still joining (the pre-stub design), this test would fail
// with empty IDs.
func TestV2GrantsReader_StubsIgnoreEntitlementTable(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t, WithV2GrantsWriter(true))
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	require.NoError(t, c1f.PutGrants(ctx, v2.Grant_builder{
		Id:          "grant-stub",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()))

	// Wipe both join targets — stubs should be unaffected.
	_, err := c1f.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", entitlements.Name()))
	require.NoError(t, err)
	_, err = c1f.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s", resources.Name()))
	require.NoError(t, err)

	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	got := resp.GetList()[0]
	require.Equal(t, "grant-stub", got.GetId())
	// Identity must round-trip from the grants row's columns.
	require.Equal(t, "ent1", got.GetEntitlement().GetId())
	require.Equal(t, "group", got.GetEntitlement().GetResource().GetId().GetResourceType())
	require.Equal(t, "g1", got.GetEntitlement().GetResource().GetId().GetResource())
	require.Equal(t, "user", got.GetPrincipal().GetId().GetResourceType())
	require.Equal(t, "u1", got.GetPrincipal().GetId().GetResource())
}

// TestV2GrantsReader_PaginatedSlim verifies that paged reads of slim
// rows reconstitute identity correctly across page boundaries. With
// stub-from-columns hydration there's no batched join; this test
// catches regressions in the per-row stub construction or the
// pageToken handoff between pages.
func TestV2GrantsReader_PaginatedSlim(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t, WithV2GrantsWriter(true))
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1, DisplayName: "Ent 1"}.Build()
	ent2 := v2.Entitlement_builder{Id: "ent2", Resource: g1, DisplayName: "Ent 2"}.Build()

	// Seed a pool of users so hydration has to fan out.
	const nUsers = 50
	users := make([]*v2.Resource, nUsers)
	for i := range users {
		u := v2.Resource_builder{
			Id:          v2.ResourceId_builder{ResourceType: "user", Resource: fmt.Sprintf("u%d", i)}.Build(),
			DisplayName: fmt.Sprintf("User %d", i),
		}.Build()
		users[i] = u
	}
	require.NoError(t, c1f.PutResources(ctx, users...))

	// Write 500 slim grants: each user touched 10 times, alternating entitlements.
	var grantsOut []*v2.Grant
	for i := range 500 {
		user := users[i%nUsers]
		ent := ent1
		if i%2 == 1 {
			ent = ent2
		}
		g := v2.Grant_builder{
			Id:          fmt.Sprintf("grant-%d", i),
			Entitlement: ent,
			Principal:   user,
		}.Build()
		grantsOut = append(grantsOut, g)
	}
	require.NoError(t, c1f.PutGrants(ctx, grantsOut...))

	// Page through everything. Each page must hydrate every slim row.
	seen := 0
	pageToken := ""
	for {
		resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageSize:  100,
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			require.NotNil(t, g.GetEntitlement(), "grant %s entitlement not hydrated", g.GetId())
			require.NotNil(t, g.GetPrincipal(), "grant %s principal not hydrated", g.GetId())
			require.Contains(t, []string{"ent1", "ent2"}, g.GetEntitlement().GetId())
			require.Equal(t, "user", g.GetPrincipal().GetId().GetResourceType())
			seen++
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, 500, seen)
}

// TestV2GetGrant_Hydrates confirms the single-row GetGrant path also
// hydrates slim blobs.
func TestV2GetGrant_Hydrates(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t, WithV2GrantsWriter(true))
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "User 1"}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1, DisplayName: "Ent 1"}.Build()

	require.NoError(t, c1f.PutGrants(ctx, v2.Grant_builder{
		Id:          "grant-get",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()))

	// Blob is slim.
	bare := &v2.Grant{}
	require.NoError(t, proto.Unmarshal(readRawGrantBlob(ctx, t, c1f, "grant-get"), bare))
	require.Nil(t, bare.GetEntitlement())
	require.Nil(t, bare.GetPrincipal())

	// GetGrant resolves sync via the currentSyncID cascade (sync is still open).
	req := reader_v2.GrantsReaderServiceGetGrantRequest_builder{GrantId: "grant-get"}.Build()
	resp, err := c1f.GetGrant(ctx, req)
	require.NoError(t, err)
	got := resp.GetGrant()
	require.Equal(t, "grant-get", got.GetId())
	require.Equal(t, "ent1", got.GetEntitlement().GetId())
	require.Equal(t, "u1", got.GetPrincipal().GetId().GetResource())
	require.Empty(t, got.GetEntitlement().GetDisplayName(), "stub carries identity only")
}

// TestExpandableGrants_Unchanged confirms that the expansion reader path
// still works identically with full vs. slim input rows. Expansion is
// stored in the `expansion` column (not in the data blob), so the slim
// writer must not affect it.
func TestExpandableGrants_Unchanged(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t, WithV2GrantsWriter(true))
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	ent2 := v2.Entitlement_builder{Id: "ent2", Resource: g1}.Build()

	expandableGrant := v2.Grant_builder{
		Id:          "grant-expandable-slim",
		Entitlement: ent2,
		Principal:   u1,
		Annotations: annotations.New(v2.GrantExpandable_builder{
			EntitlementIds:  []string{"ent1"},
			Shallow:         true,
			ResourceTypeIds: []string{"user"},
		}.Build()),
	}.Build()

	require.NoError(t, c1f.PutGrants(ctx, expandableGrant))

	// Expansion column is populated regardless of slim writer.
	var expansionBytes []byte
	require.NoError(t, c1f.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT expansion FROM %s WHERE external_id=?", grants.Name()),
		"grant-expandable-slim",
	).Scan(&expansionBytes))
	require.NotEmpty(t, expansionBytes, "expansion column must be populated for slim grants with expandable annotation")

	// The expansion-aware list reader hydrates the slim grant AND returns
	// the expansion def.
	resp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode: connectorstore.GrantListModePayloadWithExpansion,
	})
	require.NoError(t, err)
	require.Len(t, resp.Rows, 1)
	row := resp.Rows[0]
	require.NotNil(t, row.Grant)
	require.Equal(t, "ent2", row.Grant.GetEntitlement().GetId(), "expansion-aware reader must hydrate slim grants")
	require.NotNil(t, row.Expansion)
	require.Equal(t, []string{"ent1"}, row.Expansion.SourceEntitlementIDs)
	require.True(t, row.Expansion.Shallow)
}

// Ensure the newTestC1z helper path does not leave files behind.
func TestNewTestC1z_Cleanup(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t)
	path, err := c1f.OutputFilepath()
	require.NoError(t, err)
	cleanup()
	// Close must have written the file out; the TempDir cleanup handled
	// by *testing.T removes it after the test.
	_, statErr := os.Stat(path)
	require.NoError(t, statErr, "expected c1z written on close; got %v", statErr)
}

// TestV2GrantsReader_ListGrantsForEntitlement_Slim exercises the
// entitlement-filter branch of listGrantsGeneric against slim rows.
// The filter is a different request type than plain ListGrants, and
// before the multi-reviewer pass the filter paths had no slim coverage.
func TestV2GrantsReader_ListGrantsForEntitlement_Slim(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t, WithV2GrantsWriter(true))
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "User 1"}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1, DisplayName: "Ent 1"}.Build()
	ent2 := v2.Entitlement_builder{Id: "ent2", Resource: g1}.Build()

	require.NoError(t, c1f.PutGrants(ctx,
		v2.Grant_builder{Id: "g-on-ent1", Entitlement: ent1, Principal: u1}.Build(),
		v2.Grant_builder{Id: "g-on-ent2", Entitlement: ent2, Principal: u1}.Build(),
	))

	resp, err := c1f.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: ent1,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	got := resp.GetList()[0]
	require.Equal(t, "g-on-ent1", got.GetId())
	require.Equal(t, "ent1", got.GetEntitlement().GetId(), "stub hydration must fire on the filter path")
	require.Equal(t, "u1", got.GetPrincipal().GetId().GetResource())
}

// TestV2GrantsReader_ListGrantsForPrincipal_Slim covers the
// principal-filter branch of listGrantsGeneric.
func TestV2GrantsReader_ListGrantsForPrincipal_Slim(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t, WithV2GrantsWriter(true))
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "User 1"}.Build()
	u2 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build(), DisplayName: "User 2"}.Build()
	require.NoError(t, c1f.PutResources(ctx, u2))
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1}.Build()

	require.NoError(t, c1f.PutGrants(ctx,
		v2.Grant_builder{Id: "g-u1", Entitlement: ent1, Principal: u1}.Build(),
		v2.Grant_builder{Id: "g-u2", Entitlement: ent1, Principal: u2}.Build(),
	))

	resp, err := c1f.ListGrantsForPrincipal(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		PrincipalId: v2.ResourceId_builder{ResourceType: "user", Resource: "u2"}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	got := resp.GetList()[0]
	require.Equal(t, "g-u2", got.GetId())
	require.Equal(t, "u2", got.GetPrincipal().GetId().GetResource(), "stub hydration must resolve the filtered principal")
	require.Equal(t, "user", got.GetPrincipal().GetId().GetResourceType())
	require.Equal(t, "ent1", got.GetEntitlement().GetId())
}

// TestV2GrantsReader_ListGrantsForResourceType_Slim covers the
// resource-type-filter branch of listGrantsGeneric.
func TestV2GrantsReader_ListGrantsForResourceType_Slim(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t, WithV2GrantsWriter(true))
	defer cleanup()

	roleRT := v2.ResourceType_builder{Id: "role"}.Build()
	require.NoError(t, c1f.PutResourceTypes(ctx, roleRT))

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	r1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "role", Resource: "r1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	require.NoError(t, c1f.PutResources(ctx, r1))

	entG := v2.Entitlement_builder{Id: "ent-group", Resource: g1}.Build()
	entR := v2.Entitlement_builder{Id: "ent-role", Resource: r1}.Build()
	require.NoError(t, c1f.PutEntitlements(ctx, entR))

	require.NoError(t, c1f.PutGrants(ctx,
		v2.Grant_builder{Id: "g-group", Entitlement: entG, Principal: u1}.Build(),
		v2.Grant_builder{Id: "g-role", Entitlement: entR, Principal: u1}.Build(),
	))

	resp, err := c1f.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
		ResourceTypeId: "role",
	}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	got := resp.GetList()[0]
	require.Equal(t, "g-role", got.GetId())
	require.Equal(t, "ent-role", got.GetEntitlement().GetId())
	require.Equal(t, "role", got.GetEntitlement().GetResource().GetId().GetResourceType())
}

// TestV2GrantsReader_ListGrantsInternalPayload_Slim exercises the
// GrantListModePayload branch of listGrantsPayloadInternal, which was
// uncovered (only the WithExpansion branch had slim-path coverage).
func TestV2GrantsReader_ListGrantsInternalPayload_Slim(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t, WithV2GrantsWriter(true))
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "User 1"}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1, DisplayName: "Ent 1"}.Build()

	require.NoError(t, c1f.PutGrants(ctx, v2.Grant_builder{
		Id:          "g-payload",
		Entitlement: ent1,
		Principal:   u1,
	}.Build()))

	// EndSync so resolveSyncIDForPayloadQuery takes the viewSyncID path.
	require.NoError(t, c1f.EndSync(ctx))

	resp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode: connectorstore.GrantListModePayload,
	})
	require.NoError(t, err)
	require.Len(t, resp.Rows, 1)
	got := resp.Rows[0].Grant
	require.Equal(t, "g-payload", got.GetId())
	require.Equal(t, "ent1", got.GetEntitlement().GetId(), "payload-mode internal reader must stub-hydrate slim rows")
	require.Equal(t, "u1", got.GetPrincipal().GetId().GetResource())
	require.Equal(t, "user", got.GetPrincipal().GetId().GetResourceType())
}

// TestV2GrantsWriter_PreserveExpansionMode_Slim guards against
// regression: the slim writer must still strip Entitlement / Principal
// from the data blob when grants are upserted in PreserveExpansion
// mode. That mode is how the sync expander (pkg/sync/expand) writes
// every expansion-generated grant — the exact population the feature
// was built for. A prior iteration early-returned for PreserveExpansion
// before the slim step, silently storing full blobs for the majority
// of grants on expansion-heavy tenants.
func TestV2GrantsWriter_PreserveExpansionMode_Slim(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t, WithV2GrantsWriter(true))
	defer cleanup()

	g1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	u1 := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(), DisplayName: "User 1"}.Build()
	ent1 := v2.Entitlement_builder{Id: "ent1", Resource: g1, DisplayName: "Ent 1"}.Build()

	err := c1f.UpsertGrants(ctx, connectorstore.GrantUpsertOptions{
		Mode: connectorstore.GrantUpsertModePreserveExpansion,
	}, v2.Grant_builder{
		Id:          "grant-preserve",
		Entitlement: ent1,
		Principal:   u1,
	}.Build())
	require.NoError(t, err)

	// Data blob must be slim.
	bare := &v2.Grant{}
	require.NoError(t, proto.Unmarshal(readRawGrantBlob(ctx, t, c1f, "grant-preserve"), bare))
	require.Nil(t, bare.GetEntitlement(), "PreserveExpansion + slim must strip Entitlement from the blob")
	require.Nil(t, bare.GetPrincipal(), "PreserveExpansion + slim must strip Principal from the blob")

	// Reader round-trips correctly via stub-from-columns.
	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	got := resp.GetList()[0]
	require.Equal(t, "ent1", got.GetEntitlement().GetId())
	require.Equal(t, "u1", got.GetPrincipal().GetId().GetResource())
}

// TestV2GrantsReader_EmptyTable_NoHydrationWork confirms the
// sawSlim / empty-list short-circuit paths don't error and don't do
// hydration work when there are no grants.
func TestV2GrantsReader_EmptyTable_NoHydrationWork(t *testing.T) {
	ctx := context.Background()
	c1f, _, cleanup := newTestC1z(ctx, t, WithV2GrantsWriter(true))
	defer cleanup()

	// ListGrants with no grants written.
	resp, err := c1f.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Empty(t, resp.GetList())
	require.Equal(t, "", resp.GetNextPageToken())

	// Internal paths.
	payloadResp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode: connectorstore.GrantListModePayload,
	})
	require.NoError(t, err)
	require.Empty(t, payloadResp.Rows)

	exResp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode: connectorstore.GrantListModePayloadWithExpansion,
	})
	require.NoError(t, err)
	require.Empty(t, exResp.Rows)
}
