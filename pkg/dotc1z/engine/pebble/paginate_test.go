package pebble

import (
	"context"
	"strconv"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// pageThroughGrants walks the adapter's ListGrants with a fixed
// page_size until next_page_token is empty. Returns the count of
// distinct grant IDs seen + a flag for whether we observed any
// duplicates. A bug in pagination (e.g. cursor returning the same
// key twice) shows up as duplicates or as wrong counts.
func pageThroughGrants(t *testing.T, a *Adapter, pageSize uint32, expectedTotal int) {
	t.Helper()
	ctx := context.Background()
	seen := make(map[string]struct{}, expectedTotal)
	pageToken := ""
	pages := 0
	for {
		pages++
		require.LessOrEqual(t, pages, expectedTotal+1, "pagination did not terminate after %d pages (expected ~%d)", pages, (expectedTotal/int(pageSize))+1)
		resp, err := a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageSize:  pageSize,
			PageToken: pageToken,
		}.Build())
		require.NoErrorf(t, err, "ListGrants page %d", pages)
		got := resp.GetList()
		if pageSize > 0 && uint32(len(got)) > pageSize { //nolint:gosec // page count is bounded
			require.LessOrEqualf(
				t, uint32(len(got)), pageSize, //nolint:gosec // page count is bounded
				"page %d returned %d records, want <= %d", pages, len(got), pageSize,
			)
		}
		for _, g := range got {
			id := g.GetId()
			_, dup := seen[id]
			require.Falsef(t, dup, "pagination duplicate: grant %q appeared twice", id)
			seen[id] = struct{}{}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, expectedTotal, len(seen), "paginated total (pages=%d, last token=%q)", pages, pageToken)
}

func TestListGrantsPagination(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	// 250 grants — enough to exercise multiple pages at any reasonable
	// page_size we test below.
	const total = 250
	grants := make([]*v2.Grant, total)
	for i := 0; i < total; i++ {
		grants[i] = mkV2Grant(
			"grant-"+strconv.Itoa(i),
			"ent-"+strconv.Itoa(i%5),
			"user",
			"user-"+strconv.Itoa(i),
		)
	}
	require.NoErrorf(t, a.PutGrants(ctx, grants...), "PutGrants")

	t.Run("page=10", func(t *testing.T) { pageThroughGrants(t, a, 10, total) })
	t.Run("page=50", func(t *testing.T) { pageThroughGrants(t, a, 50, total) })
	t.Run("page=100", func(t *testing.T) { pageThroughGrants(t, a, 100, total) })
	t.Run("page=250_exact", func(t *testing.T) { pageThroughGrants(t, a, 250, total) })
	t.Run("page=251_overshoot", func(t *testing.T) { pageThroughGrants(t, a, 251, total) })
	t.Run("page=default_zero", func(t *testing.T) {
		// page_size=0 clamps to DefaultPageSize (10000); single page expected.
		resp, err := a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
		require.NoError(t, err)
		require.Equal(t, total, len(resp.GetList()), "default page: got %d records, want %d", len(resp.GetList()), total)
		require.Empty(t, resp.GetNextPageToken(), "default page should not have next token, got %q", resp.GetNextPageToken())
	})
}

// TestListGrantsPaginationByEntitlementResource is the
// ListGrants(req.Resource = entitlement-side resource) pagination
// test. Matches the SQLite contract: "all grants whose entitlement
// is on this resource". The pre-fix Pebble path interpreted
// req.Resource as a principal filter (PaginateGrantsByPrincipal),
// breaking this semantic; see Bug 4 in the audit. The fix routes
// req.Resource through the new by_entitlement_resource index.
//
// All `total` grants live on the SAME entitlement, whose resource
// is mkV2Grant's hardcoded (app/github) entitlement resource, so
// the index covers every record and pagination should yield the
// full set.
func TestListGrantsPaginationByEntitlementResource(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	const total = 100
	for i := 0; i < total; i++ {
		// All grants share the same entitlement resource (app/github
		// — see mkV2Grant) so the by_entitlement_resource index
		// covers every record. Principals vary so the test wouldn't
		// have worked accidentally via a principal-side filter.
		require.NoError(t, a.PutGrants(ctx, mkV2Grant(
			"grant-"+strconv.Itoa(i),
			"ent-"+strconv.Itoa(i%5),
			"user",
			"alice-"+strconv.Itoa(i),
		)))
	}

	// Walk with page_size=15, filtered by entitlement-resource.
	seen := map[string]struct{}{}
	pages := 0
	pageToken := ""
	entitlementResource := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
	}.Build()
	for {
		pages++
		require.LessOrEqual(t, pages, 50, "pagination did not terminate: %d pages", pages)
		resp, err := a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			Resource:  entitlementResource,
			PageSize:  15,
			PageToken: pageToken,
		}.Build())
		require.NoErrorf(t, err, "ListGrants by entitlement-resource page %d", pages)
		for _, g := range resp.GetList() {
			seen[g.GetId()] = struct{}{}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, total, len(seen), "by-entitlement-resource paginated total")
}

func TestListResourcesPagination(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	const total = 75
	resources := make([]*v2.Resource, total)
	for i := 0; i < total; i++ {
		resources[i] = v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "user-" + strconv.Itoa(i),
			}.Build(),
			DisplayName: "User " + strconv.Itoa(i),
		}.Build()
	}
	require.NoError(t, a.PutResources(ctx, resources...))

	seen := map[string]struct{}{}
	pageToken := ""
	for {
		resp, err := a.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			PageSize:  20,
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, r := range resp.GetList() {
			seen[r.GetId().GetResource()] = struct{}{}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, total, len(seen), "paginated resources")
}

func TestListEntitlementsPagination(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	const total = 50
	entitlements := make([]*v2.Entitlement, total)
	for i := 0; i < total; i++ {
		entitlements[i] = v2.Entitlement_builder{
			Id: "ent-" + strconv.Itoa(i),
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
			}.Build(),
		}.Build()
	}
	require.NoError(t, a.PutEntitlements(ctx, entitlements...))

	seen := map[string]struct{}{}
	pageToken := ""
	for {
		resp, err := a.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageSize:  7,
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, e := range resp.GetList() {
			seen[e.GetId()] = struct{}{}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, total, len(seen), "paginated entitlements")
}

func TestListResourceTypesPagination(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	rts := make([]*v2.ResourceType, 30)
	for i := 0; i < 30; i++ {
		rts[i] = v2.ResourceType_builder{
			Id:          "rt-" + strconv.Itoa(i),
			DisplayName: "RT " + strconv.Itoa(i),
		}.Build()
	}
	require.NoError(t, a.PutResourceTypes(ctx, rts...))
	seen := map[string]struct{}{}
	pageToken := ""
	for {
		resp, err := a.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
			PageSize:  8,
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, rt := range resp.GetList() {
			seen[rt.GetId()] = struct{}{}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	require.Equal(t, 30, len(seen), "paginated resource_types")
}

func TestPageTokenMalformed(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	// not-base64 — must surface ErrInvalidPageToken so a buggy caller
	// who corrupts the token gets a clear error rather than silently
	// returning the first page again.
	_, err = a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
		PageToken: "not!base64!!!",
	}.Build())
	require.ErrorIs(t, err, ErrInvalidPageToken, "malformed token: got err=%v, want ErrInvalidPageToken", err)
}

func TestPaginationClampedPageSize(t *testing.T) {
	// page_size > MaxPageSize must clamp; verify against the engine
	// directly so we know the clamp is in the engine, not just the adapter.
	ctx := context.Background()
	e, _ := newTestEngine(t)
	syncID := ksuid.New().String()
	require.NoError(t, e.bindCurrentSync(syncID))
	const total = 50
	for i := 0; i < total; i++ {
		r := &v3.GrantRecord{}
		require.NoError(t, e.PutGrantRecord(ctx, makeGrant(syncID, "g"+strconv.Itoa(i), "ent", "user-"+strconv.Itoa(i))))
		_ = r
	}
	// Passing 0 should clamp to DefaultPageSize and return all 50.
	recs, next, err := e.PaginateGrants(ctx, "", 0)
	require.NoError(t, err)
	require.Equal(t, total, len(recs), "clamp(0): got %d, want %d", len(recs), total)
	require.Empty(t, next, "clamp(0): expected empty next cursor, got %q", next)
	// Passing MaxPageSize+1 should clamp identically.
	recs2, _, err := e.PaginateGrants(ctx, "", MaxPageSize+1)
	require.NoError(t, err)
	require.Equal(t, total, len(recs2), "clamp(max+1): got %d, want %d", len(recs2), total)
}
