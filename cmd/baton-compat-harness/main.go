//go:build compatharness

// Command baton-compat-harness is one half of the two-artifact checkpoint
// compatibility instrument (docs/BUG_CATCHING.md §2: multi-artifact
// properties are review-blind — the property under test is a function of
// TWO SDK versions, so a test that only executes HEAD can check at most
// half of it).
//
// The same source compiles against two checkouts of baton-sdk — HEAD
// ("new") and a pinned past release ("old", see driver_test.go) — and the
// two binaries exchange a real c1z containing a mid-flight checkpoint:
//
//	-mode gen    start a sync and interrupt it via run-duration expiry,
//	             leaving an unfinished sync run + checkpoint in the c1z.
//	             Under the new binary the checkpoint holds type-scoped and
//	             spawned cursor actions; under the old binary it holds a
//	             mid-pagination per-resource grants root.
//	-mode resume resume the same c1z to completion (no delays, no fan-out)
//	             and report the final resource/entitlement/grant counts.
//
// The oracle is counted rows, not error text: whatever a resumer does with
// a foreign-version checkpoint (resume it, refuse it, or restart the sync),
// the sealed store must end content-complete. The bug this instrument
// exists for — an old SDK zero-filling a newer token and silently sealing
// an incomplete sync — is invisible to everything except the count.
//
// The connector is version-skew realistic: its resource type always carries
// TypeScopedEntitlements/TypeScopedGrants annotations (like a connector
// built against the new SDK), and it serves both the per-resource calls an
// old syncer makes and the type-scoped calls a new syncer makes.
//
// The final stdout line is COMPAT_RESULT <json> for the driver to parse.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

const (
	numGroups        = 12
	resourcePageSize = 4
)

type compatConnector struct {
	// gen-mode behavior knobs.
	slowPerResourceGrants time.Duration // old-binary gen: stall per-resource grant calls
	fanoutAndBlock        bool          // new-binary gen: spawn sibling cursors, block all but the first

	groupType *v2.ResourceType
	userType  *v2.ResourceType
	groups    []*v2.Resource
	user      *v2.Resource
	entsByRes map[string]*v2.Entitlement
	grantsBy  map[string]*v2.Grant

	v2.AssetServiceClient
	v2.GrantManagerServiceClient
	v2.ResourceManagerServiceClient
	v2.AccountManagerServiceClient
	v2.ResourceDeleterServiceClient
	v2.CredentialManagerServiceClient
	v2.EventServiceClient
	v2.TicketsServiceClient
	v2.ActionServiceClient
	v2.ResourceGetterServiceClient
	v2.EntitlementsServiceClient
}

func newCompatConnector() (*compatConnector, error) {
	c := &compatConnector{
		entsByRes: make(map[string]*v2.Entitlement),
		grantsBy:  make(map[string]*v2.Grant),
	}
	c.groupType = v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
		Annotations: annotations.New(&v2.TypeScopedEntitlements{}, &v2.TypeScopedGrants{}),
	}.Build()
	c.userType = v2.ResourceType_builder{
		Id:          "user",
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
	}.Build()
	user, err := rs.NewUserResource("user-1", c.userType, "User 1", nil)
	if err != nil {
		return nil, err
	}
	c.user = user
	for i := 0; i < numGroups; i++ {
		id := fmt.Sprintf("g%02d", i)
		group, err := rs.NewGroupResource(id, c.groupType, "Group "+id, nil)
		if err != nil {
			return nil, err
		}
		c.groups = append(c.groups, group)
		ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(c.userType))
		// Key by the resource's actual stored ID (NewGroupResource derives
		// it from the objectID argument, not the name).
		key := group.GetId().GetResource()
		c.entsByRes[key] = ent
		c.grantsBy[key] = gt.NewGrant(group, "member", user.GetId())
	}
	return c, nil
}

func (c *compatConnector) ListResourceTypes(
	_ context.Context, _ *v2.ResourceTypesServiceListResourceTypesRequest, _ ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{
		List: []*v2.ResourceType{c.groupType, c.userType},
	}.Build(), nil
}

func (c *compatConnector) ListResources(
	_ context.Context, in *v2.ResourcesServiceListResourcesRequest, _ ...grpc.CallOption,
) (*v2.ResourcesServiceListResourcesResponse, error) {
	if in.GetResourceTypeId() == "user" {
		return v2.ResourcesServiceListResourcesResponse_builder{
			List: []*v2.Resource{c.user},
		}.Build(), nil
	}
	// Paginate groups so a grants root planner is mid-pagination when a
	// gen-mode run expires.
	start := 0
	if in.GetPageToken() != "" {
		if _, err := fmt.Sscanf(in.GetPageToken(), "page-%d", &start); err != nil {
			return nil, fmt.Errorf("compat: bad resource page token %q", in.GetPageToken())
		}
	}
	end := start + resourcePageSize
	next := ""
	if end < len(c.groups) {
		next = fmt.Sprintf("page-%d", end)
	} else {
		end = len(c.groups)
	}
	return v2.ResourcesServiceListResourcesResponse_builder{
		List:          c.groups[start:end],
		NextPageToken: next,
	}.Build(), nil
}

func (c *compatConnector) ListEntitlements(
	_ context.Context, in *v2.EntitlementsServiceListEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	if reqAnnos.Contains(&v2.TypeScopedEntitlements{}) {
		// New syncer, whole type in one page.
		ents := make([]*v2.Entitlement, 0, len(c.groups))
		for _, g := range c.groups {
			ents = append(ents, c.entsByRes[g.GetId().GetResource()])
		}
		return v2.EntitlementsServiceListEntitlementsResponse_builder{List: ents}.Build(), nil
	}
	// Old syncer, per resource.
	ent, ok := c.entsByRes[in.GetResource().GetId().GetResource()]
	if !ok {
		return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
	}
	return v2.EntitlementsServiceListEntitlementsResponse_builder{
		List: []*v2.Entitlement{ent},
	}.Build(), nil
}

func (c *compatConnector) ListStaticEntitlements(
	_ context.Context, _ *v2.EntitlementsServiceListStaticEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{}.Build(), nil
}

func (c *compatConnector) ListGrants(
	ctx context.Context, in *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	if reqAnnos.Contains(&v2.TypeScopedGrants{}) {
		return c.listGrantsTypeScoped(ctx, in)
	}
	// Per-resource path (old syncer).
	resourceID := in.GetResource().GetId().GetResource()
	if c.slowPerResourceGrants > 0 {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-time.After(c.slowPerResourceGrants):
		}
	}
	grant, ok := c.grantsBy[resourceID]
	if !ok {
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	return v2.GrantsServiceListGrantsResponse_builder{List: []*v2.Grant{grant}}.Build(), nil
}

func (c *compatConnector) listGrantsTypeScoped(
	ctx context.Context, in *v2.GrantsServiceListGrantsRequest,
) (*v2.GrantsServiceListGrantsResponse, error) {
	if !c.fanoutAndBlock {
		// Resume mode: whole type in one page, whatever the cursor. A
		// resumed spawned cursor re-fetches the full type; duplicate
		// upserts are idempotent by identity.
		grants := make([]*v2.Grant, 0, len(c.groups))
		for _, g := range c.groups {
			grants = append(grants, c.grantsBy[g.GetId().GetResource()])
		}
		return v2.GrantsServiceListGrantsResponse_builder{List: grants}.Build(), nil
	}
	// Gen mode: planner spawns sibling cursors; only the first returns,
	// the rest block until the run duration cancels them, stranding
	// spawned+type-scoped actions in the checkpoint.
	switch tok := in.GetPageToken(); {
	case tok == "":
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: annotations.New(v2.EnqueuePageTokens_builder{
				PageTokens: []string{"ts-0", "ts-1", "ts-2", "ts-3"},
			}.Build()),
		}.Build(), nil
	case tok == "ts-0":
		return v2.GrantsServiceListGrantsResponse_builder{
			List: []*v2.Grant{
				c.grantsBy[c.groups[0].GetId().GetResource()],
				c.grantsBy[c.groups[1].GetId().GetResource()],
				c.grantsBy[c.groups[2].GetId().GetResource()],
			},
		}.Build(), nil
	case strings.HasPrefix(tok, "ts-"):
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-time.After(30 * time.Second):
			return nil, fmt.Errorf("compat: blocked cursor %s was never cancelled", tok)
		}
	default:
		return nil, fmt.Errorf("compat: unexpected type-scoped grants token %q", in.GetPageToken())
	}
}

func (c *compatConnector) GetMetadata(
	_ context.Context, _ *v2.ConnectorServiceGetMetadataRequest, _ ...grpc.CallOption,
) (*v2.ConnectorServiceGetMetadataResponse, error) {
	return v2.ConnectorServiceGetMetadataResponse_builder{
		Metadata: v2.ConnectorMetadata_builder{DisplayName: "compat-harness"}.Build(),
	}.Build(), nil
}

func (c *compatConnector) Validate(
	_ context.Context, _ *v2.ConnectorServiceValidateRequest, _ ...grpc.CallOption,
) (*v2.ConnectorServiceValidateResponse, error) {
	return v2.ConnectorServiceValidateResponse_builder{}.Build(), nil
}

func (c *compatConnector) Cleanup(
	_ context.Context, _ *v2.ConnectorServiceCleanupRequest, _ ...grpc.CallOption,
) (*v2.ConnectorServiceCleanupResponse, error) {
	return v2.ConnectorServiceCleanupResponse_builder{}.Build(), nil
}

func countList(ctx context.Context, store c1zstore.Store) (int, int, int, error) {
	resources := 0
	pageToken := ""
	for {
		resp, err := store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return 0, 0, 0, fmt.Errorf("list resources: %w", err)
		}
		resources += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	ents := 0
	pageToken = ""
	for {
		resp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return 0, 0, 0, fmt.Errorf("list entitlements: %w", err)
		}
		ents += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	grants := 0
	pageToken = ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return 0, 0, 0, fmt.Errorf("list grants: %w", err)
		}
		grants += len(resp.GetList())
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return resources, ents, grants, nil
}

// syncRunLister is the store surface the run inspection needs; asserted
// dynamically because the harness compiles against two SDK versions.
type syncRunLister interface {
	ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
}

// inspectRuns records checkpoint evidence for the driver's meta-assertions:
// a gen run must leave an UNFINISHED sync whose token actually carries the
// adversarial state (spawned/type-scoped actions under the new binary) —
// otherwise the exchange tests a conveniently quiet checkpoint and the
// harness is vacuous for the bug class it exists for.
func inspectRuns(ctx context.Context, store c1zstore.Store, result *compatResult) {
	lister, ok := store.(syncRunLister)
	if !ok {
		return
	}
	runs, _, err := lister.ListSyncRuns(ctx, "", 100)
	if err != nil {
		return
	}
	for _, run := range runs {
		if run.EndedAt != nil {
			continue
		}
		result.UnfinishedRuns++
		result.TokenLen += len(run.SyncToken)
		if strings.Contains(run.SyncToken, "spawned") {
			result.TokenSpawned = true
		}
		if strings.Contains(run.SyncToken, "type_scoped") {
			result.TokenTypeScoped = true
		}
	}
}

func countRows(ctx context.Context, c1zPath, tmpDir string, result *compatResult) (int, int, int, error) {
	store, err := dotc1z.NewStore(ctx, c1zPath, dotc1z.WithTmpDir(tmpDir))
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = store.Close(ctx) }()
	inspectRuns(ctx, store, result)
	return countList(ctx, store)
}

// compatResult is the machine-readable summary the driver parses from the
// final COMPAT_RESULT stdout line.
type compatResult struct {
	Mode        string `json:"mode"`
	SyncErr     string `json:"sync_err,omitempty"`
	NotComplete bool   `json:"not_complete"`
	Resources   int    `json:"resources"`
	Ents        int    `json:"entitlements"`
	Grants      int    `json:"grants"`
	CountErr    string `json:"count_err,omitempty"`
	// Checkpoint evidence (see inspectRuns).
	UnfinishedRuns  int  `json:"unfinished_runs"`
	TokenLen        int  `json:"token_len"`
	TokenSpawned    bool `json:"token_spawned"`
	TokenTypeScoped bool `json:"token_type_scoped"`
}

func run() error {
	mode := flag.String("mode", "", "gen or resume")
	c1zPath := flag.String("c1z", "", "path to c1z file")
	runDuration := flag.Duration("run-duration", 2*time.Second, "gen-mode run duration")
	flag.Parse()
	if *mode == "" || *c1zPath == "" {
		return fmt.Errorf("usage: -mode gen|resume -c1z path")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	ctx = ctxzap.ToContext(ctx, logger)

	connector, err := newCompatConnector()
	if err != nil {
		return err
	}
	store, err := dotc1z.NewStore(ctx, *c1zPath, dotc1z.WithTmpDir(os.TempDir()))
	if err != nil {
		return fmt.Errorf("new store: %w", err)
	}
	opts := []sdksync.SyncOpt{
		sdksync.WithConnectorStore(store),
		sdksync.WithTmpDir(os.TempDir()),
		sdksync.WithWorkerCount(2),
	}
	if *mode == "gen" {
		connector.fanoutAndBlock = true
		connector.slowPerResourceGrants = 400 * time.Millisecond
		opts = append(opts, sdksync.WithRunDuration(*runDuration))
	}

	s, err := sdksync.NewSyncer(ctx, connector, opts...)
	if err != nil {
		return fmt.Errorf("new syncer: %w", err)
	}
	syncErr := s.Sync(ctx)
	if closeErr := s.Close(ctx); closeErr != nil {
		return fmt.Errorf("close: %w", closeErr)
	}

	result := compatResult{Mode: *mode}
	if syncErr != nil {
		result.SyncErr = syncErr.Error()
		result.NotComplete = strings.Contains(syncErr.Error(), sdksync.ErrSyncNotComplete.Error())
	}
	var countErr error
	result.Resources, result.Ents, result.Grants, countErr = countRows(ctx, *c1zPath, os.TempDir(), &result)
	if countErr != nil {
		result.CountErr = countErr.Error()
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	fmt.Printf("COMPAT_RESULT %s\n", encoded)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}
}
