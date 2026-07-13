package pebble

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetEntitlementResourceResourceType(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	rt := v2.ResourceType_builder{Id: "user", DisplayName: "User",
		Traits: []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER}}.Build()
	require.NoError(t, a.PutResourceTypes(ctx, rt))
	r := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
		DisplayName: "Alice",
	}.Build()
	require.NoError(t, a.PutResources(ctx, r))
	ent := v2.Entitlement_builder{
		Id: "github-read",
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
		}.Build(),
		DisplayName: "Read",
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
	}.Build()
	require.NoError(t, a.PutEntitlements(ctx, ent))

	// GetResourceType
	rtResp, err := a.GetResourceType(ctx, reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest_builder{
		ResourceTypeId: "user",
	}.Build())
	require.NoError(t, err, "GetResourceType")
	require.Equal(t, "User", rtResp.GetResourceType().GetDisplayName(), "GetResourceType display")
	require.Len(t, rtResp.GetResourceType().GetTraits(), 1, "GetResourceType traits")

	// GetResource
	rResp, err := a.GetResource(ctx, reader_v2.ResourcesReaderServiceGetResourceRequest_builder{
		ResourceId: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
	}.Build())
	require.NoError(t, err, "GetResource")
	require.Equal(t, "Alice", rResp.GetResource().GetDisplayName(), "GetResource display")

	// GetEntitlement
	// Stored external ids are verbatim; the fixture seeded the raw id.
	readEntID := "github-read"
	eResp, err := a.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: readEntID,
	}.Build())
	require.NoError(t, err, "GetEntitlement")
	require.Equal(t, readEntID, eResp.GetEntitlement().GetId(), "GetEntitlement id")
	require.Equal(t, v2.Entitlement_PURPOSE_VALUE_PERMISSION, eResp.GetEntitlement().GetPurpose(), "GetEntitlement purpose")

	// Missing entity returns gRPC NotFound — the Adapter normalizes
	// pebble.ErrNotFound at the boundary so engine-agnostic
	// consumers (pkg/sync, pkg/sync/expand) can use one sentinel
	// across both engines. See adapter_errors.go.
	_, err = a.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
		EntitlementId: "does-not-exist",
	}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "missing entitlement")
}

func TestListGrantsForEntitlementAndResourceType(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// 5 grants on ent-A with user principals, 3 on ent-A with group
	// principals, 2 on ent-B with user principals.
	grants := []*v2.Grant{}
	for i := 0; i < 5; i++ {
		grants = append(grants, mkV2Grant("u-a-"+strconv.Itoa(i), "ent-A", "user", "u"+strconv.Itoa(i)))
	}
	for i := 0; i < 3; i++ {
		grants = append(grants, mkV2Grant("g-a-"+strconv.Itoa(i), "ent-A", "group", "g"+strconv.Itoa(i)))
	}
	for i := 0; i < 2; i++ {
		grants = append(grants, mkV2Grant("u-b-"+strconv.Itoa(i), "ent-B", "user", "u"+strconv.Itoa(i)))
	}
	require.NoError(t, a.PutGrants(ctx, grants...))

	// ListGrantsForEntitlement(ent-A) → 8 grants total
	resp, err := a.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v2.Entitlement_builder{Id: canonicalTestEntID("ent-A")}.Build(),
		PageSize:    100,
	}.Build())
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 8, "ListGrantsForEntitlement(ent-A)")

	// ListGrantsForEntitlement(ent-A) filtered by principal RT=user → 5
	respFiltered, err := a.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement:              v2.Entitlement_builder{Id: canonicalTestEntID("ent-A")}.Build(),
		PrincipalResourceTypeIds: []string{"user"},
		PageSize:                 100,
	}.Build())
	require.NoError(t, err)
	require.Len(t, respFiltered.GetList(), 5, "ListGrantsForEntitlement(ent-A, user)")

	// ListGrantsForEntitlement(ent-A) filtered by principal ID=user/u1 → 1
	respPrincipal, err := a.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: v2.Entitlement_builder{Id: canonicalTestEntID("ent-A")}.Build(),
		PrincipalId: v2.ResourceId_builder{
			ResourceType: "user",
			Resource:     "u1",
		}.Build(),
		PageSize: 100,
	}.Build())
	require.NoError(t, err)
	require.Len(t, respPrincipal.GetList(), 1, "ListGrantsForEntitlement(ent-A, user/u1)")
	require.Equal(t, canonicalTestGrantID("ent-A", "user", "u1"), respPrincipal.GetList()[0].GetId(), "ListGrantsForEntitlement(ent-A, user/u1) id")

	var principalPageIDs []string
	principalPageToken := ""
	for {
		respPrincipalPage, err := a.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: v2.Entitlement_builder{Id: canonicalTestEntID("ent-A")}.Build(),
			PrincipalId: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "u1",
			}.Build(),
			PageSize:  1,
			PageToken: principalPageToken,
		}.Build())
		require.NoError(t, err)
		for _, g := range respPrincipalPage.GetList() {
			principalPageIDs = append(principalPageIDs, g.GetId())
		}
		principalPageToken = respPrincipalPage.GetNextPageToken()
		if principalPageToken == "" {
			break
		}
	}
	require.Len(t, principalPageIDs, 1, "paginated ListGrantsForEntitlement(ent-A, user/u1)")
	require.Equal(t, canonicalTestGrantID("ent-A", "user", "u1"), principalPageIDs[0], "paginated ListGrantsForEntitlement(ent-A, user/u1)")

	// ListGrantsForResourceType(user) → 7 grants (5 ent-A + 2 ent-B)
	rtResp, err := a.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
		ResourceTypeId: "user",
		PageSize:       100,
	}.Build())
	require.NoError(t, err)
	require.Len(t, rtResp.GetList(), 7, "ListGrantsForResourceType(user)")
}

// TestListGrantsForResourceTypePagination drives the new
// idxGrantByPrincipalResourceType index across a page boundary
// and verifies that updates flip-flop the index correctly.
func TestListGrantsForResourceTypePagination(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	const userCount = 50
	const groupCount = 20
	grants := make([]*v2.Grant, 0, userCount+groupCount)
	for i := 0; i < userCount; i++ {
		grants = append(grants, mkV2Grant("u-"+strconv.Itoa(i), "ent-A", "user", "u"+strconv.Itoa(i)))
	}
	for i := 0; i < groupCount; i++ {
		grants = append(grants, mkV2Grant("g-"+strconv.Itoa(i), "ent-A", "group", "g"+strconv.Itoa(i)))
	}
	require.NoError(t, a.PutGrants(ctx, grants...))

	// Walk all user grants across multiple pages.
	collected := map[string]bool{}
	cursor := ""
	for i := 0; i < 20; i++ {
		resp, err := a.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
			ResourceTypeId: "user",
			PageSize:       7,
			PageToken:      cursor,
		}.Build())
		require.NoError(t, err, "page %d", i)
		for _, g := range resp.GetList() {
			require.Equal(t, "user", g.GetPrincipal().GetId().GetResourceType(), "non-user grant returned")
			require.False(t, collected[g.GetId()], "duplicate grant id %q", g.GetId())
			collected[g.GetId()] = true
		}
		cursor = resp.GetNextPageToken()
		if cursor == "" {
			break
		}
	}
	require.Equal(t, userCount, len(collected), "paginated user grants")

	// Re-put a subset with a different principal RT. With structured
	// identity, principal is part of the grant key, so these are new
	// grants rather than external-id overwrites: user stays 50 and group
	// rises to 30.
	updated := make([]*v2.Grant, 0, 10)
	for i := 0; i < 10; i++ {
		updated = append(updated, mkV2Grant("u-"+strconv.Itoa(i), "ent-A", "group", "moved-"+strconv.Itoa(i)))
	}
	require.NoError(t, a.PutGrants(ctx, updated...))

	respUser, err := a.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
		ResourceTypeId: "user", PageSize: 1000,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, userCount, len(respUser.GetList()), "after structured-identity add, user grants")

	respGroup, err := a.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
		ResourceTypeId: "group", PageSize: 1000,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, groupCount+10, len(respGroup.GetList()), "after structured-identity add, group grants")
}

// TestBulkByIdsRoundtripPebble exercises the Adapter's
// ListResourcesByIds / ListEntitlementsByIds / GetResourceTypes
// (RFC §A3 — collapse N point Gets into one round-trip).
func TestBulkByIdsRoundtripPebble(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	rtUser := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	rtGroup := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	rtApp := v2.ResourceType_builder{Id: "app", DisplayName: "App"}.Build()
	require.NoError(t, a.PutResourceTypes(ctx, rtUser, rtGroup, rtApp))
	mkRes := func(rt, id string) *v2.Resource {
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(),
		}.Build()
	}
	require.NoError(t, a.PutResources(ctx,
		mkRes("user", "u1"), mkRes("user", "u2"),
		mkRes("group", "g1"),
	))
	require.NoError(t, a.PutEntitlements(ctx,
		v2.Entitlement_builder{Id: "ent-A", Resource: mkRes("app", "github")}.Build(),
		v2.Entitlement_builder{Id: "ent-B", Resource: mkRes("app", "github")}.Build(),
	))

	t.Run("ListEntitlementsByIds", func(t *testing.T) {
		resp, err := a.ListEntitlementsByIds(ctx, reader_v2.EntitlementsReaderServiceListEntitlementsByIdsRequest_builder{
			EntitlementIds: []string{"ent-A", "ent-zzz", "ent-B"},
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 2)
	})
	t.Run("ListResourcesByIds", func(t *testing.T) {
		ids := []*v2.ResourceId{
			v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build(),
			v2.ResourceId_builder{ResourceType: "user", Resource: "u-missing"}.Build(),
			v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		}
		resp, err := a.ListResourcesByIds(ctx, reader_v2.ResourcesReaderServiceListResourcesByIdsRequest_builder{
			ResourceIds: ids,
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 2)
	})
}

// TestListGrantsForEntitlementsPebble exercises the new batched
// reader against the Pebble adapter (RFC §A4).
func TestListGrantsForEntitlementsPebble(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	mkRes := func(rt, id string) *v2.Resource {
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(),
		}.Build()
	}
	appRes := mkRes("app", "gh")
	require.NoError(t, a.PutResources(ctx, appRes))
	entA := v2.Entitlement_builder{Id: "ent-A", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "A"}.Build()
	entB := v2.Entitlement_builder{Id: "ent-B", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "B"}.Build()
	entC := v2.Entitlement_builder{Id: "ent-C", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "C"}.Build()
	require.NoError(t, a.PutEntitlements(ctx, entA, entB, entC))
	mkGrant := func(id, entID, princRT, princID string) *v2.Grant {
		return v2.Grant_builder{
			Id:          id,
			Entitlement: v2.Entitlement_builder{Id: entID, Resource: appRes}.Build(),
			Principal:   mkRes(princRT, princID),
		}.Build()
	}
	grants := []*v2.Grant{}
	for i := 0; i < 5; i++ {
		grants = append(grants, mkGrant("a-u-"+strconv.Itoa(i), "ent-A", "user", "u"+strconv.Itoa(i)))
	}
	for i := 0; i < 3; i++ {
		grants = append(grants, mkGrant("b-g-"+strconv.Itoa(i), "ent-B", "group", "g"+strconv.Itoa(i)))
	}
	for i := 0; i < 7; i++ {
		grants = append(grants, mkGrant("c-u-"+strconv.Itoa(i), "ent-C", "user", "uc"+strconv.Itoa(i)))
	}
	require.NoError(t, a.PutGrants(ctx, grants...))

	t.Run("aggregate all", func(t *testing.T) {
		resp, err := a.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
			Entitlements: []*v2.Entitlement{entA, entB, entC},
			PageSize:     100,
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 15)
	})
	t.Run("pagination crosses entitlement boundary", func(t *testing.T) {
		seen := map[string]bool{}
		token := ""
		for i := 0; i < 10; i++ {
			resp, err := a.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
				Entitlements: []*v2.Entitlement{entA, entB, entC},
				PageSize:     4,
				PageToken:    token,
			}.Build())
			require.NoError(t, err)
			for _, g := range resp.GetList() {
				require.False(t, seen[g.GetId()], "dup grant %s", g.GetId())
				seen[g.GetId()] = true
			}
			token = resp.GetNextPageToken()
			if token == "" {
				break
			}
		}
		require.Len(t, seen, 15)
	})
	t.Run("checksum mismatch resets", func(t *testing.T) {
		resp1, err := a.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
			Entitlements: []*v2.Entitlement{entA, entB, entC},
			PageSize:     2,
		}.Build())
		require.NoError(t, err)
		require.NotEmpty(t, resp1.GetNextPageToken(), "first page should overflow")
		resp2, err := a.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
			Entitlements: []*v2.Entitlement{entA, entB},
			PageSize:     2,
			PageToken:    resp1.GetNextPageToken(),
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp2.GetList(), 2, "after checksum mismatch")
		require.Equal(t, "ent-A", resp2.GetList()[0].GetEntitlement().GetId(), "after reset first ent")
	})
	t.Run("reorder mid-pagination resets", func(t *testing.T) {
		// The cursor resumes by POSITIONAL index into the entitlement
		// list, so a reordered list (same set!) must reset the scan, not
		// resume — an order-insensitive checksum would bless the token
		// while the positional index resumed over a different
		// entitlement, silently skipping and re-returning grants.
		resp1, err := a.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
			Entitlements: []*v2.Entitlement{entA, entB, entC},
			PageSize:     6, // past entA (5 grants), into entB at index 1
		}.Build())
		require.NoError(t, err)
		require.NotEmpty(t, resp1.GetNextPageToken(), "first page should overflow")

		reordered := []*v2.Entitlement{entC, entB, entA}
		resp2, err := a.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
			Entitlements: reordered,
			PageSize:     6,
			PageToken:    resp1.GetNextPageToken(),
		}.Build())
		require.NoError(t, err)
		require.NotEmpty(t, resp2.GetList(), "reset page")
		require.Equal(t, "ent-C", resp2.GetList()[0].GetEntitlement().GetId(),
			"reordered list must restart from its own first entitlement")

		// Paging the reordered list to completion from the reset point
		// yields every grant exactly once.
		seen := map[string]bool{}
		token := ""
		for i := 0; i < 10; i++ {
			resp, err := a.ListGrantsForEntitlements(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest_builder{
				Entitlements: reordered,
				PageSize:     4,
				PageToken:    token,
			}.Build())
			require.NoError(t, err)
			for _, g := range resp.GetList() {
				require.False(t, seen[g.GetId()], "dup grant %s after reorder", g.GetId())
				seen[g.GetId()] = true
			}
			token = resp.GetNextPageToken()
			if token == "" {
				break
			}
		}
		require.Len(t, seen, 15, "reordered pagination must cover every grant")
	})
}

// TestCrossKeyspaceCursorRejected pins the page-token clamp: a cursor is a
// raw key minted for one keyspace, and feeding it to a different record
// type's list call must fail with ErrInvalidPageToken — not silently start
// the scan inside the foreign keyspace and serve its values as this type's
// records.
func TestCrossKeyspaceCursorRejected(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	appRes := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "app", Resource: "gh"}.Build(),
	}.Build()
	require.NoError(t, a.PutResources(ctx, appRes))
	ents := make([]*v2.Entitlement, 0, 4)
	for i := 0; i < 4; i++ {
		ents = append(ents, v2.Entitlement_builder{
			Id:       "ent-" + strconv.Itoa(i),
			Resource: appRes,
			Purpose:  v2.Entitlement_PURPOSE_VALUE_PERMISSION,
			Slug:     "s" + strconv.Itoa(i),
		}.Build())
	}
	require.NoError(t, a.PutEntitlements(ctx, ents...))
	users := make([]*v2.Resource, 0, 4)
	for i := 0; i < 4; i++ {
		users = append(users, v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u" + strconv.Itoa(i)}.Build(),
		}.Build())
	}
	require.NoError(t, a.PutResources(ctx, users...))

	// Mint a genuine entitlement-keyspace cursor.
	entResp, err := a.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		PageSize: 2,
	}.Build())
	require.NoError(t, err)
	entToken := entResp.GetNextPageToken()
	require.NotEmpty(t, entToken, "entitlement page should overflow")

	// Feed it to the resources reader: foreign keyspace, must be rejected.
	_, err = a.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: "user",
		PageToken:      entToken,
	}.Build())
	require.ErrorIs(t, err, ErrInvalidPageToken, "entitlement cursor fed to resources reader must fail as an invalid page token")

	// And the reverse: a resources cursor fed to the entitlements reader.
	resResp, err := a.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: "user",
		PageSize:       2,
	}.Build())
	require.NoError(t, err)
	resToken := resResp.GetNextPageToken()
	require.NotEmpty(t, resToken, "resource page should overflow")
	_, err = a.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
		PageToken: resToken,
	}.Build())
	require.ErrorIs(t, err, ErrInvalidPageToken, "resource cursor fed to entitlements reader must fail as an invalid page token")
}

// TestStreamingReaderPebble exercises iter.Seq2 streaming on the
// Pebble adapter (RFC §B3).
func TestStreamingReaderPebble(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	mkRes := func(rt, id string) *v2.Resource {
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(),
		}.Build()
	}
	appRes := mkRes("app", "gh")
	require.NoError(t, a.PutResources(ctx, appRes))
	entA := v2.Entitlement_builder{Id: "ent-A", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "A"}.Build()
	require.NoError(t, a.PutEntitlements(ctx, entA))
	const userCount = 25
	users := make([]*v2.Resource, userCount)
	for i := 0; i < userCount; i++ {
		users[i] = mkRes("user", "u"+strconv.Itoa(i))
	}
	require.NoError(t, a.PutResources(ctx, users...))
	grants := make([]*v2.Grant, userCount)
	for i := 0; i < userCount; i++ {
		grants[i] = v2.Grant_builder{
			Id:          "g-" + strconv.Itoa(i),
			Entitlement: entA,
			Principal:   mkRes("user", "u"+strconv.Itoa(i)),
		}.Build()
	}
	require.NoError(t, a.PutGrants(ctx, grants...))

	t.Run("StreamGrants yields all", func(t *testing.T) {
		seen := 0
		for g, err := range a.StreamGrants(ctx, syncID, connectorstore.StreamGrantsOptions{}) {
			require.NoError(t, err)
			require.NotNil(t, g)
			seen++
		}
		require.Equal(t, userCount, seen)
	})
	t.Run("StreamResources RT filter", func(t *testing.T) {
		seen := 0
		for r, err := range a.StreamResources(ctx, syncID, connectorstore.StreamResourcesOptions{ResourceTypeID: "user"}) {
			require.NoError(t, err)
			require.Equal(t, "user", r.GetId().GetResourceType())
			seen++
		}
		require.Equal(t, userCount, seen)
	})
	t.Run("StreamEntitlements", func(t *testing.T) {
		seen := 0
		for e, err := range a.StreamEntitlements(ctx, syncID) {
			require.NoError(t, err)
			require.Equal(t, "ent-A", e.GetId())
			seen++
		}
		require.Equal(t, 1, seen)
	})
	t.Run("early stop honored", func(t *testing.T) {
		count := 0
		for _, err := range a.StreamGrants(ctx, syncID, connectorstore.StreamGrantsOptions{}) {
			require.NoError(t, err)
			count++
			if count == 3 {
				break
			}
		}
		require.Equal(t, 3, count)
	})
	// Compile-time check.
	var _ connectorstore.StreamingReader = (*Adapter)(nil)
}

// TestListGrantsForPrincipalPebble covers the parity gap audit
// found on 2026-05-26: C1File has ListGrantsForPrincipal but
// Adapter didn't. Verifies the principal-scoped grant index walk
// and the optional entitlement narrowing.
func TestListGrantsForPrincipalPebble(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	mkRes := func(rt, id string) *v2.Resource {
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(),
		}.Build()
	}
	appRes := mkRes("app", "gh")
	require.NoError(t, a.PutResources(ctx, appRes))
	entA := v2.Entitlement_builder{Id: "ent-A", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "A"}.Build()
	entB := v2.Entitlement_builder{Id: "ent-B", Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: "B"}.Build()
	require.NoError(t, a.PutEntitlements(ctx, entA, entB))
	mkGrant := func(id, entID, princRT, princID string) *v2.Grant {
		return v2.Grant_builder{
			Id:          id,
			Entitlement: v2.Entitlement_builder{Id: entID, Resource: appRes}.Build(),
			Principal:   mkRes(princRT, princID),
		}.Build()
	}
	require.NoError(t, a.PutGrants(ctx,
		mkGrant("g1", "ent-A", "user", "alice"),
		mkGrant("g2", "ent-B", "user", "alice"),
		mkGrant("g3", "ent-A", "user", "bob"),
	))

	t.Run("returns all grants for principal", func(t *testing.T) {
		resp, err := a.ListGrantsForPrincipal(ctx, reader_v2.GrantsReaderServiceListGrantsForPrincipalRequest_builder{
			PrincipalId: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
			PageSize:    100,
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 2)
	})

	t.Run("entitlement filter narrows", func(t *testing.T) {
		resp, err := a.ListGrantsForPrincipal(ctx, reader_v2.GrantsReaderServiceListGrantsForPrincipalRequest_builder{
			Entitlement: v2.Entitlement_builder{Id: "ent-A"}.Build(),
			PrincipalId: v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build(),
			PageSize:    100,
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 1, "filter")
		require.Equal(t, "g1", resp.GetList()[0].GetId())
	})

	t.Run("missing principal errors", func(t *testing.T) {
		_, err := a.ListGrantsForPrincipal(ctx, reader_v2.GrantsReaderServiceListGrantsForPrincipalRequest_builder{
			PageSize: 100,
		}.Build())
		require.ErrorContains(t, err, "missing principal_id")
	})
}

// TestListGrantsForPrincipalEntitlementPointLookup locks in the
// entitlement+principal point-lookup path: entitlement + principal is the
// full primary grant key, so the narrowed request must resolve in a single
// page regardless of how many grants the principal holds on other
// entitlements. The old by_principal scan with post-filter returned empty
// pages with a next token here.
func TestListGrantsForPrincipalEntitlementPointLookup(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	mkRes := func(rt, id string) *v2.Resource {
		return v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build(),
		}.Build()
	}
	appRes := mkRes("app", "gh")
	require.NoError(t, a.PutResources(ctx, appRes))
	const entCount = 20
	ents := make([]*v2.Entitlement, entCount)
	grants := make([]*v2.Grant, entCount)
	for i := range ents {
		id := "ent-" + strconv.Itoa(i)
		ents[i] = v2.Entitlement_builder{Id: id, Resource: appRes, Purpose: v2.Entitlement_PURPOSE_VALUE_PERMISSION, Slug: id}.Build()
		grants[i] = v2.Grant_builder{
			Id:          "g-" + strconv.Itoa(i),
			Entitlement: v2.Entitlement_builder{Id: id, Resource: appRes}.Build(),
			Principal:   mkRes("user", "alice"),
		}.Build()
	}
	require.NoError(t, a.PutEntitlements(ctx, ents...))
	require.NoError(t, a.PutGrants(ctx, grants...))
	alice := v2.ResourceId_builder{ResourceType: "user", Resource: "alice"}.Build()

	t.Run("single page even when page size is small", func(t *testing.T) {
		// Page size 2 with 20 grants on other entitlements: the point
		// lookup must return the one match on the first page with no
		// next token.
		resp, err := a.ListGrantsForPrincipal(ctx, reader_v2.GrantsReaderServiceListGrantsForPrincipalRequest_builder{
			Entitlement: v2.Entitlement_builder{Id: "ent-19", Resource: appRes}.Build(),
			PrincipalId: alice,
			PageSize:    2,
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetList(), 1)
		require.Equal(t, "g-19", resp.GetList()[0].GetId())
		require.Empty(t, resp.GetNextPageToken())
	})

	t.Run("unknown entitlement yields empty response", func(t *testing.T) {
		resp, err := a.ListGrantsForPrincipal(ctx, reader_v2.GrantsReaderServiceListGrantsForPrincipalRequest_builder{
			Entitlement: v2.Entitlement_builder{Id: "ent-nope"}.Build(),
			PrincipalId: alice,
			PageSize:    100,
		}.Build())
		require.NoError(t, err)
		require.Empty(t, resp.GetList())
		require.Empty(t, resp.GetNextPageToken())
	})

	t.Run("no grant for entitlement+principal yields empty response", func(t *testing.T) {
		resp, err := a.ListGrantsForPrincipal(ctx, reader_v2.GrantsReaderServiceListGrantsForPrincipalRequest_builder{
			Entitlement: v2.Entitlement_builder{Id: "ent-0", Resource: appRes}.Build(),
			PrincipalId: v2.ResourceId_builder{ResourceType: "user", Resource: "bob"}.Build(),
			PageSize:    100,
		}.Build())
		require.NoError(t, err)
		require.Empty(t, resp.GetList())
		require.Empty(t, resp.GetNextPageToken())
	})
}

// TestListStaticEntitlementsPebble is a parity smoke test for the
// always-empty static-entitlements RPC. Both backends return an
// empty list; this test guards against a future divergence.
func TestListStaticEntitlementsPebble(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	resp, err := a.ListStaticEntitlements(ctx, v2.EntitlementsServiceListStaticEntitlementsRequest_builder{}.Build())
	require.NoError(t, err)
	require.Empty(t, resp.GetList())
	require.Empty(t, resp.GetNextPageToken())
}

func TestSyncsReaderMethods(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)

	// Single-sync contract: a file holds exactly one sync, so these
	// reader methods operate on at most one record.
	finishedID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, a.EndSync(ctx))

	// GetSync(finished) → has ended_at.
	resp, err := a.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{
		SyncId: finishedID,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, finishedID, resp.GetSync().GetId(), "GetSync id")
	require.NotNil(t, resp.GetSync().GetEndedAt(), "GetSync ended_at")
	require.NotNil(t, resp.GetSync().GetStats(), "GetSync stats")

	// ListSyncs → exactly the one sync.
	listResp, err := a.ListSyncs(ctx, reader_v2.SyncsReaderServiceListSyncsRequest_builder{
		PageSize: 100,
	}.Build())
	require.NoError(t, err)
	require.Len(t, listResp.GetSyncs(), 1, "ListSyncs")

	// GetLatestFinishedSync → the finished sync, by Any and by its type;
	// a non-matching type filter resolves nil.
	latest, err := a.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, finishedID, latest.GetSync().GetId(), "GetLatestFinishedSync")
	latestFull, err := a.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{
		SyncType: string(connectorstore.SyncTypeFull),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, finishedID, latestFull.GetSync().GetId(), "GetLatestFinishedSync(full)")
	latestNone, err := a.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{
		SyncType: string(connectorstore.SyncTypePartial),
	}.Build())
	require.NoError(t, err)
	require.Nil(t, latestNone.GetSync(), "GetLatestFinishedSync(partial)")

	// Starting a new sync REPLACES the finished one: the replacement is
	// in-progress and the prior sync is gone.
	inProgressID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	resp3, err := a.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{
		SyncId: inProgressID,
	}.Build())
	require.NoError(t, err)
	require.Nil(t, resp3.GetSync().GetEndedAt(), "GetSync(in-progress) ended_at")
	_, err = a.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{
		SyncId: finishedID,
	}.Build())
	require.Error(t, err, "GetSync(replaced %q)", finishedID)
	latestAfter, err := a.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{}.Build())
	require.NoError(t, err)
	require.Nil(t, latestAfter.GetSync(), "GetLatestFinishedSync after replacement")
}

func TestStatsAndGrantStats(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	require.NoError(t, a.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user"}.Build(),
		v2.ResourceType_builder{Id: "group"}.Build()))
	require.NoError(t, a.PutResources(ctx,
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "a"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "b"}.Build()}.Build(),
		v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "c"}.Build()}.Build()))
	require.NoError(t, a.PutGrants(ctx,
		mkV2Grant("g1", "e1", "user", "a"),
		mkV2Grant("g2", "e1", "user", "b"),
		mkV2Grant("g3", "e2", "group", "c")))

	err = a.EndSync(ctx)
	require.NoError(t, err)

	stats, err := a.Stats(ctx, connectorstore.SyncTypeAny, syncID)
	require.NoError(t, err)
	require.Equal(t, int64(2), stats["resource_types"], "stats[resource_types]")
	require.Equal(t, int64(3), stats["resources"], "stats[resources]")
	require.Equal(t, int64(3), stats["grants"], "stats[grants]")

	// GrantStats partitions by entitlement resource type. Our
	// mkV2Grant always sets the entitlement's resource to (app, github),
	// so all 3 grants count under "app".
	gs, err := a.GrantStats(ctx, connectorstore.SyncTypeAny, syncID)
	require.NoError(t, err)
	require.Equal(t, int64(3), gs["app"], "GrantStats[app]")

	sync, err := a.GetSync(ctx, &reader_v2.SyncsReaderServiceGetSyncRequest{SyncId: syncID})
	require.NoError(t, err)

	syncStats := sync.GetSync().GetStats()
	require.NotNil(t, syncStats, "GetSync stats")

	// Validate all fields for syncStats.
	require.Equal(t, int64(2), syncStats.GetResourceTypes(), "syncStats.ResourceTypes")
	require.Equal(t, int64(3), syncStats.GetResources(), "syncStats.Resources")
	// No entitlements were written, only grants that reference them.
	require.Equal(t, int64(0), syncStats.GetEntitlements(), "syncStats.Entitlements")
	require.Equal(t, int64(3), syncStats.GetGrants(), "syncStats.Grants")
	require.Equal(t, map[string]int64{"user": 2, "group": 1}, syncStats.GetResourcesByResourceType(), "syncStats.ResourcesByResourceType")
	require.Empty(t, syncStats.GetEntitlementsByResourceType(), "syncStats.EntitlementsByResourceType")
	// GrantsByResourceType partitions by the entitlement's resource type,
	// which mkV2Grant always sets to "app".
	require.Equal(t, map[string]int64{"app": 3}, syncStats.GetGrantsByResourceType(), "syncStats.GrantsByResourceType")
}
