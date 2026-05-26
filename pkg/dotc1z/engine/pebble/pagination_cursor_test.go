package pebble

import (
	"context"
	"strconv"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestPostFilterPaginationDoesNotSkip is the regression guard for
// the "post-filter pagination cursor" bug the PR review flagged.
//
// Scenario: ListResources with rtFilter set + a page size of 3.
// The engine returns up to 12 records per fetch (4× over-fetch).
// If the inner loop breaks at len(out) == 3 while there are still
// matching records later in the engine page, the buggy
// implementation returned the engine's end-of-page cursor — which
// caused the next page request to skip the remaining matches.
//
// We seed 8 "user" resources interleaved with 8 "group" resources
// so that any honest page-3-at-a-time iteration must return all 8
// users across multiple pages.
func TestPostFilterPaginationDoesNotSkip(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}

	const total = 8
	all := make([]*v2.Resource, 0, 2*total)
	for i := 0; i < total; i++ {
		all = append(all,
			v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u-" + strconv.Itoa(i)}.Build(),
			}.Build(),
			v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g-" + strconv.Itoa(i)}.Build(),
			}.Build(),
		)
	}
	if err := a.PutResources(ctx, all...); err != nil {
		t.Fatalf("PutResources: %v", err)
	}

	seen := make(map[string]bool, total)
	pageToken := ""
	pages := 0
	for {
		pages++
		if pages > 10 {
			t.Fatalf("ListResources did not terminate after %d pages", pages)
		}
		resp, err := a.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId: "user",
			PageSize:       3,
			PageToken:      pageToken,
		}.Build())
		if err != nil {
			t.Fatalf("ListResources: %v", err)
		}
		for _, r := range resp.GetList() {
			if r.GetId().GetResourceType() != "user" {
				t.Errorf("got non-user resource in page: %v", r)
			}
			seen[r.GetId().GetResource()] = true
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}

	if len(seen) != total {
		t.Errorf("post-filter ListResources missed records: got %d users (%v), want %d", len(seen), seen, total)
	}
}

// TestListGrantsForEntitlementPostFilterDoesNotSkip is the same
// shape regression for ListGrantsForEntitlement with a principal
// resource_type_id filter. Seeds grants on entitlement ent-A whose
// principals are interleaved user/group; the page-3 iteration must
// still see every user-principal grant.
func TestListGrantsForEntitlementPostFilterDoesNotSkip(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	const total = 8
	grants := make([]*v2.Grant, 0, 2*total)
	for i := 0; i < total; i++ {
		grants = append(grants,
			mkV2Grant("u-grant-"+strconv.Itoa(i), "ent-A", "user", "u"+strconv.Itoa(i)),
			mkV2Grant("g-grant-"+strconv.Itoa(i), "ent-A", "group", "g"+strconv.Itoa(i)),
		)
	}
	if err := a.PutGrants(ctx, grants...); err != nil {
		t.Fatalf("PutGrants: %v", err)
	}

	seen := make(map[string]bool, total)
	pageToken := ""
	pages := 0
	for {
		pages++
		if pages > 10 {
			t.Fatalf("ListGrantsForEntitlement did not terminate after %d pages", pages)
		}
		resp, err := a.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: v2.Entitlement_builder{
				Id: "ent-A",
				Resource: v2.Resource_builder{
					Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
				}.Build(),
			}.Build(),
			PrincipalResourceTypeIds: []string{"user"},
			PageSize:                 3,
			PageToken:                pageToken,
		}.Build())
		if err != nil {
			t.Fatalf("ListGrantsForEntitlement: %v", err)
		}
		for _, g := range resp.GetList() {
			if rt := g.GetPrincipal().GetId().GetResourceType(); rt != "user" {
				t.Errorf("got non-user principal: %s", rt)
			}
			seen[g.GetId()] = true
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	if len(seen) != total {
		t.Errorf("post-filter ListGrantsForEntitlement missed records: got %d (%v), want %d", len(seen), seen, total)
	}
}

// TestListGrantsForResourceTypePostFilterDoesNotSkip walks the
// same regression for the rtFilter variant on ListGrantsForResourceType.
func TestListGrantsForResourceTypePostFilterDoesNotSkip(t *testing.T) {
	ctx := context.Background()
	a := newAdapter(t)
	if _, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	const total = 8
	grants := make([]*v2.Grant, 0, 2*total)
	for i := 0; i < total; i++ {
		grants = append(grants,
			mkV2Grant("u-grant-"+strconv.Itoa(i), "ent-A", "user", "u"+strconv.Itoa(i)),
			mkV2Grant("g-grant-"+strconv.Itoa(i), "ent-A", "group", "g"+strconv.Itoa(i)),
		)
	}
	if err := a.PutGrants(ctx, grants...); err != nil {
		t.Fatalf("PutGrants: %v", err)
	}

	seen := make(map[string]bool, total)
	pageToken := ""
	pages := 0
	for {
		pages++
		if pages > 10 {
			t.Fatalf("ListGrantsForResourceType did not terminate after %d pages", pages)
		}
		resp, err := a.ListGrantsForResourceType(ctx, reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest_builder{
			ResourceTypeId: "user",
			PageSize:       3,
			PageToken:      pageToken,
		}.Build())
		if err != nil {
			t.Fatalf("ListGrantsForResourceType: %v", err)
		}
		for _, g := range resp.GetList() {
			if rt := g.GetPrincipal().GetId().GetResourceType(); rt != "user" {
				t.Errorf("got non-user principal: %s", rt)
			}
			seen[g.GetId()] = true
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	if len(seen) != total {
		t.Errorf("post-filter ListGrantsForResourceType missed records: got %d (%v), want %d", len(seen), seen, total)
	}
}
