package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Replay side-effect regression tests: response-row-driven side effects
// (child scheduling, external-resource-match processing, related-resource
// insertion) must survive a warm replay round exactly as a cold sync
// would produce them. Each test runs sync 1 cold, sync 2 as a 304-style
// replay, and asserts the side effect's output is present in sync 2.

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	pebbleengine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	"github.com/conductorone/baton-sdk/pkg/types"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// runSourceCacheSyncClient is runSourceCacheSync for wrapper connectors
// that embed sourceCacheMockConnector but override list methods: the
// OUTER value must be handed to the syncer or the overrides never run.
func runSourceCacheSyncClient(ctx context.Context, t *testing.T, cc types.ConnectorClient, path, prevPath, tmpDir string, extraOpts ...SyncOpt) {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	opts := []SyncOpt{WithConnectorStore(store), WithTmpDir(tmpDir), WithFailFastInvariants()}
	if prevPath != "" {
		opts = append(opts, WithPreviousSyncC1ZPath(prevPath))
	}
	opts = append(opts, extraOpts...)
	syncer, err := NewSyncer(ctx, cc, opts...)
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))
}

// listResourcesInFile reopens a finished Pebble c1z and returns its
// resources of one type, keyed by resource ID.
func listResourcesInFile(ctx context.Context, t *testing.T, path, resourceTypeID string) map[string]*v2.Resource {
	t.Helper()
	reopen, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { _ = reopen.Close(ctx) }()

	sync, err := reopen.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, sync)
	require.NoError(t, reopen.SetCurrentSync(ctx, sync.ID))

	out := map[string]*v2.Resource{}
	pageToken := ""
	for {
		resp, err := reopen.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
			ResourceTypeId: resourceTypeID,
			PageToken:      pageToken,
		}.Build())
		require.NoError(t, err)
		for _, r := range resp.GetList() {
			out[r.GetId().GetResource()] = r
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return out
}

// ---------------------------------------------------------------------
// 1. Replayed parent resources must still schedule child resource syncs.
// ---------------------------------------------------------------------

// childReplayConnector serves a parent resource type as a source-cache
// scope (fresh on miss, 304 replay on hit) whose resources carry
// ChildResourceType annotations. Child resources are only returned when
// the request names a parent — the standard parent/child connector shape.
type childReplayConnector struct {
	*sourceCacheMockConnector

	parentType *v2.ResourceType
	childType  *v2.ResourceType
	parents    []*v2.Resource
	children   map[string][]*v2.Resource // parent resource id -> children

	resourceEtag       string
	resourceLookupHits int
}

func (c *childReplayConnector) ListResources(ctx context.Context, in *v2.ResourcesServiceListResourcesRequest, opts ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	switch in.GetResourceTypeId() {
	case c.parentType.GetId():
		scope := sourcecache.HashScope("mock://resources/" + c.parentType.GetId())
		c.mu.Lock()
		lookup := c.lookup
		etag := c.resourceEtag
		c.mu.Unlock()
		if lookup == nil {
			lookup = sourcecache.NoopLookup{}
		}
		entry, found, err := lookup.Lookup(ctx, sourcecache.RowKindResources, scope)
		if err != nil {
			return nil, err
		}
		if found && entry.CacheValidator == etag {
			c.mu.Lock()
			c.resourceLookupHits++
			c.mu.Unlock()
			return v2.ResourcesServiceListResourcesResponse_builder{
				Annotations: annotations.New(v2.SourceCacheReplay_builder{
					ScopeKey:       scope,
					CacheValidator: etag,
				}.Build()),
			}.Build(), nil
		}
		return v2.ResourcesServiceListResourcesResponse_builder{
			List: c.parents,
			Annotations: annotations.New(v2.SourceCacheRecord_builder{
				ScopeKey:       scope,
				CacheValidator: etag,
			}.Build()),
		}.Build(), nil
	case c.childType.GetId():
		if in.GetParentResourceId() == nil {
			// Children are only reachable through a parent.
			return v2.ResourcesServiceListResourcesResponse_builder{}.Build(), nil
		}
		return v2.ResourcesServiceListResourcesResponse_builder{
			List: c.children[in.GetParentResourceId().GetResource()],
		}.Build(), nil
	}
	return c.sourceCacheMockConnector.ListResources(ctx, in, opts...)
}

// TestSourceCache_ReplaySchedulesChildResources pins the child-scheduling
// contract under replay: a 304 replay of a parent resources page copies
// the parents engine-side and returns no rows, so the response-row loop
// that normally pushes child SyncResources actions never sees them. The
// replay path must schedule those children itself, or a warm sync
// silently drops whole subtrees.
func TestSourceCache_ReplaySchedulesChildResources(t *testing.T) {
	for _, workers := range workerCounts {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			runReplaySchedulesChildResourcesScenario(t, workerSyncOpts(workers))
		})
	}
}

func runReplaySchedulesChildResourcesScenario(t *testing.T, workerOpts []SyncOpt) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	parentRT := v2.ResourceType_builder{Id: "sc_parent", DisplayName: "Parent"}.Build()
	childRT := v2.ResourceType_builder{Id: "sc_child", DisplayName: "Child"}.Build()

	parent1, err := rs.NewResource("Parent 1", parentRT, "parent_1",
		rs.WithAnnotation(v2.ChildResourceType_builder{ResourceTypeId: childRT.GetId()}.Build()),
		rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}),
	)
	require.NoError(t, err)
	child1, err := rs.NewResource("Child 1", childRT, "child_1",
		rs.WithParentResourceID(parent1.GetId()),
		rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}),
	)
	require.NoError(t, err)
	child2, err := rs.NewResource("Child 2", childRT, "child_2",
		rs.WithParentResourceID(parent1.GetId()),
		rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}),
	)
	require.NoError(t, err)

	inner := newSourceCacheMockConnector()
	inner.rtDB = []*v2.ResourceType{parentRT, childRT}
	mc := &childReplayConnector{
		sourceCacheMockConnector: inner,
		parentType:               parentRT,
		childType:                childRT,
		parents:                  []*v2.Resource{parent1},
		children: map[string][]*v2.Resource{
			parent1.GetId().GetResource(): {child1, child2},
		},
		resourceEtag: `W/"resources-v1"`,
	}

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	sync2 := filepath.Join(tmpDir, "sync2.c1z")

	runSourceCacheSyncClient(ctx, t, mc, sync1, "", tmpDir, workerOpts...)
	require.Zero(t, mc.resourceLookupHits, "sync 1 has no previous sync to hit")
	require.Len(t, listResourcesInFile(ctx, t, sync1, childRT.GetId()), 2,
		"sanity: cold sync must fetch children through the parent")

	// Upstream unchanged: the parent page replays. The children must
	// still be scheduled and fetched.
	runSourceCacheSyncClient(ctx, t, mc, sync2, sync1, tmpDir, workerOpts...)
	require.NotZero(t, mc.resourceLookupHits, "sanity: sync 2 must have replayed the parent page")

	parents2 := listResourcesInFile(ctx, t, sync2, parentRT.GetId())
	require.Contains(t, parents2, parent1.GetId().GetResource(), "replayed parent must be present")

	children2 := listResourcesInFile(ctx, t, sync2, childRT.GetId())
	require.Contains(t, children2, child1.GetId().GetResource(),
		"child resources of a replayed parent must be scheduled and synced")
	require.Contains(t, children2, child2.GetId().GetResource(),
		"child resources of a replayed parent must be scheduled and synced")
}

// ---------------------------------------------------------------------
// 2. External-resource-match grants must survive a warm replay.
// ---------------------------------------------------------------------

// TestSourceCache_ReplayPreservesExternalMatchGrants pins the
// external-match contract under replay. Cold sync 1 emits a
// scope-stamped grant carrying ExternalResourceMatchID; the
// external-resources step transforms it into a grant on the matched
// external principal and deletes the source grant. Sync 2's 304 replay
// must carry the transformed grant forward — and the replay path must
// re-arm HasExternalResourcesGrants so match processing still runs on
// replayed rows.
func TestSourceCache_ReplayPreservesExternalMatchGrants(t *testing.T) {
	for _, workers := range workerCounts {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			runReplayPreservesExternalMatchGrantsScenario(t, workerSyncOpts(workers))
		})
	}
}

func runReplayPreservesExternalMatchGrantsScenario(t *testing.T, workerOpts []SyncOpt) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	// External c1z: one user the match annotation will resolve to.
	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)
	externalUser, err := externalMc.AddUserProfile(ctx, "ext_user_1", map[string]any{})
	require.NoError(t, err)

	externalC1z := filepath.Join(tmpDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1z), WithTmpDir(tmpDir))
	require.NoError(t, err)
	require.NoError(t, externalSyncer.Sync(ctx))
	require.NoError(t, externalSyncer.Close(ctx))

	// Internal connector: one group whose grants page is a source-cache
	// scope. The single grant references a placeholder principal and
	// carries ExternalResourceMatchID pointing at the external user.
	mc := newSourceCacheMockConnector()
	internalGroup, ent, err := mc.AddGroup(ctx, "internal_group")
	require.NoError(t, err)

	placeholder := v2.ResourceId_builder{ResourceType: "user", Resource: "placeholder_1"}.Build()
	sourceGrant := gt.NewGrant(internalGroup, "member", placeholder,
		gt.WithAnnotation(v2.ExternalResourceMatchID_builder{
			Id: externalUser.GetId().GetResource(),
		}.Build()),
	)
	mc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{sourceGrant}
	mc.etagByResource[internalGroup.GetId().GetResource()] = `W/"grants-v1"`

	transformedGrantID := gt.NewGrantID(externalUser, sourceGrant.GetEntitlement())
	_ = ent

	extOpts := append([]SyncOpt{WithExternalResourceC1ZPath(externalC1z)}, workerOpts...)

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	sync3 := filepath.Join(tmpDir, "sync3.c1z")

	runSourceCacheSync(ctx, t, mc, sync1, "", tmpDir, extOpts...)
	grants1 := listGrantsInFile(ctx, t, sync1)
	require.Contains(t, grants1, transformedGrantID, "sanity: cold sync must produce the transformed grant")
	require.NotContains(t, grants1, sourceGrant.GetId(), "sanity: the placeholder source grant must be deleted")

	// Upstream unchanged: the grants scope replays. The transformed
	// grant must survive.
	runSourceCacheSync(ctx, t, mc, sync2, sync1, tmpDir, extOpts...)
	require.NotZero(t, mc.lookupHits, "sanity: sync 2 must have replayed")

	grants2 := listGrantsInFile(ctx, t, sync2)
	require.Contains(t, grants2, transformedGrantID,
		"transformed external-match grant must survive a 304 replay of its source scope")
	require.NotContains(t, grants2, sourceGrant.GetId())

	// And the chain must not decay: sync 2's file must still work as the
	// next replay source.
	runSourceCacheSync(ctx, t, mc, sync3, sync2, tmpDir, extOpts...)
	grants3 := listGrantsInFile(ctx, t, sync3)
	require.Contains(t, grants3, transformedGrantID,
		"transformed grant must survive a second consecutive replay round")
}

// ---------------------------------------------------------------------
// 3. InsertResourceGrants resources must survive a warm replay.
// ---------------------------------------------------------------------

// insertResourceGrantsConnector serves one grants page (for target) as a
// source-cache scope whose response carries InsertResourceGrants: the
// grants reference a resource no ListResources call ever returns, so the
// only way the resource reaches the store is grant-driven insertion.
type insertResourceGrantsConnector struct {
	*sourceCacheMockConnector

	target       string
	insertGrants []*v2.Grant
}

func (c *insertResourceGrantsConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	resourceID := in.GetResource().GetId().GetResource()
	if resourceID != c.target {
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	scope := sourcecache.HashScope("mock://grants/insert/" + resourceID)

	c.mu.Lock()
	lookup := c.lookup
	currentEtag := c.etagByResource[resourceID]
	c.mu.Unlock()
	if lookup == nil {
		lookup = sourcecache.NoopLookup{}
	}
	entry, found, err := lookup.Lookup(ctx, sourcecache.RowKindGrants, scope)
	if err != nil {
		return nil, err
	}
	if found && entry.CacheValidator == currentEtag {
		c.mu.Lock()
		c.lookupHits++
		c.mu.Unlock()
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: annotations.New(v2.SourceCacheReplay_builder{
				ScopeKey:       scope,
				CacheValidator: currentEtag,
			}.Build()),
		}.Build(), nil
	}
	c.mu.Lock()
	c.lookupMisses++
	c.mu.Unlock()
	return v2.GrantsServiceListGrantsResponse_builder{
		List: c.insertGrants,
		Annotations: annotations.New(
			v2.SourceCacheRecord_builder{ScopeKey: scope, CacheValidator: currentEtag}.Build(),
			&v2.InsertResourceGrants{},
		),
	}.Build(), nil
}

// TestSourceCache_ReplayPreservesInsertResourceGrants pins related-
// resource insertion under replay: a grants page annotated with
// InsertResourceGrants writes the grants' entitlement resources into the
// resources table. On a 304 replay the grant rows are copied engine-side
// but the response has no rows, so the insertion loop never runs — the
// replay must recreate those resource rows or sync 2 holds grants whose
// resources do not exist.
func TestSourceCache_ReplayPreservesInsertResourceGrants(t *testing.T) {
	for _, workers := range workerCounts {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			runReplayPreservesInsertResourceGrantsScenario(t, workerSyncOpts(workers))
		})
	}
}

func runReplayPreservesInsertResourceGrantsScenario(t *testing.T, workerOpts []SyncOpt) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	projectRT := v2.ResourceType_builder{Id: "project", DisplayName: "Project"}.Build()

	inner := newSourceCacheMockConnector()
	inner.rtDB = append(inner.rtDB, projectRT)
	internalGroup, _, err := inner.AddGroup(ctx, "g1")
	require.NoError(t, err)
	alice, err := inner.AddUser(ctx, "alice")
	require.NoError(t, err)

	// The project resource is NEVER returned by ListResources — it only
	// exists as the embedded resource of the grant below.
	projectRes, err := rs.NewResource("Project 1", projectRT, "proj_1",
		rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}),
	)
	require.NoError(t, err)
	projectGrant := gt.NewGrant(projectRes, "admin", alice)

	inner.etagByResource[internalGroup.GetId().GetResource()] = `W/"insert-v1"`
	mc := &insertResourceGrantsConnector{
		sourceCacheMockConnector: inner,
		target:                   internalGroup.GetId().GetResource(),
		insertGrants:             []*v2.Grant{projectGrant},
	}

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	sync2 := filepath.Join(tmpDir, "sync2.c1z")

	runSourceCacheSyncClient(ctx, t, mc, sync1, "", tmpDir, workerOpts...)
	projects1 := listResourcesInFile(ctx, t, sync1, projectRT.GetId())
	require.Contains(t, projects1, "proj_1", "sanity: cold sync must insert the grant's resource")
	require.Equal(t, "Project 1", projects1["proj_1"].GetDisplayName())
	require.Contains(t, listGrantsInFile(ctx, t, sync1), projectGrant.GetId())

	// Upstream unchanged: the grants scope replays. The inserted
	// resource must survive alongside its grants.
	runSourceCacheSyncClient(ctx, t, mc, sync2, sync1, tmpDir, workerOpts...)
	require.NotZero(t, mc.lookupHits, "sanity: sync 2 must have replayed")

	grants2 := listGrantsInFile(ctx, t, sync2)
	require.Contains(t, grants2, projectGrant.GetId(), "replayed grant must be present")

	projects2 := listResourcesInFile(ctx, t, sync2, projectRT.GetId())
	require.Contains(t, projects2, "proj_1",
		"InsertResourceGrants resource must be recreated when its grants page replays")
	require.Equal(t, "Project 1", projects2["proj_1"].GetDisplayName(),
		"recreated resource must be a faithful copy of the previous sync's row")
}

// ---------------------------------------------------------------------
// 4. Replayed entitlements must participate in exclusion-group checks.
// ---------------------------------------------------------------------

// exclusionGroupReplayConnector serves entitlements pages as source-cache
// scopes for resources with a registered validator (fresh on miss, 304
// replay on hit); resources without one list entitlements plain.
type exclusionGroupReplayConnector struct {
	*sourceCacheMockConnector

	entEtags      map[string]string // resource id -> upstream validator
	entLookupHits int
	entLookupMiss int
}

func (c *exclusionGroupReplayConnector) ListEntitlements(
	ctx context.Context,
	in *v2.EntitlementsServiceListEntitlementsRequest,
	opts ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	resourceID := in.GetResource().GetId().GetResource()
	c.mu.Lock()
	lookup := c.lookup
	etag := c.entEtags[resourceID]
	c.mu.Unlock()
	if etag == "" {
		return c.sourceCacheMockConnector.ListEntitlements(ctx, in, opts...)
	}
	scope := sourcecache.HashScope("mock://entitlements/" + resourceID)
	if lookup == nil {
		lookup = sourcecache.NoopLookup{}
	}
	entry, found, err := lookup.Lookup(ctx, sourcecache.RowKindEntitlements, scope)
	if err != nil {
		return nil, err
	}
	if found && entry.CacheValidator == etag {
		c.mu.Lock()
		c.entLookupHits++
		c.mu.Unlock()
		return v2.EntitlementsServiceListEntitlementsResponse_builder{
			Annotations: annotations.New(v2.SourceCacheReplay_builder{
				ScopeKey:       scope,
				CacheValidator: etag,
			}.Build()),
		}.Build(), nil
	}
	c.mu.Lock()
	c.entLookupMiss++
	c.mu.Unlock()
	return v2.EntitlementsServiceListEntitlementsResponse_builder{
		List: c.entDB[resourceID],
		Annotations: annotations.New(v2.SourceCacheRecord_builder{
			ScopeKey:       scope,
			CacheValidator: etag,
		}.Build()),
	}.Build(), nil
}

// TestSourceCache_ReplayedEntitlementsKeepExclusionGroupValidation pins
// the exclusion-group invariants under replay: validation normally runs
// on response rows only, so a replayed entitlement's exclusion-group
// membership would silently stop participating — a duplicate default
// split across a replayed scope and a fresh scope would pass a warm sync
// that a full resync hard-fails.
func TestSourceCache_ReplayedEntitlementsKeepExclusionGroupValidation(t *testing.T) {
	for _, workers := range workerCounts {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			runReplayedEntitlementExclusionGroupScenario(t, workerSyncOpts(workers))
		})
	}
}

func runReplayedEntitlementExclusionGroupScenario(t *testing.T, workerOpts []SyncOpt) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	inner := newSourceCacheMockConnector()
	mc := &exclusionGroupReplayConnector{
		sourceCacheMockConnector: inner,
		entEtags:                 map[string]string{},
	}

	defaultExclusionGroup := v2.EntitlementExclusionGroup_builder{
		ExclusionGroupId: "eg1",
		IsDefault:        true,
	}.Build()

	// Group A: its entitlements page is a scope, carrying the exclusion
	// group's default entitlement.
	groupA, entA, err := inner.AddGroup(ctx, "group_a")
	require.NoError(t, err)
	entAAnnos := annotations.Annotations(entA.GetAnnotations())
	entAAnnos.Update(defaultExclusionGroup)
	entA.SetAnnotations(entAAnnos)
	mc.entEtags[groupA.GetId().GetResource()] = `W/"ents-a-v1"`

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	runSourceCacheSyncClient(ctx, t, mc, sync1, "", tmpDir, workerOpts...)
	require.NotZero(t, mc.entLookupMiss, "sanity: sync 1 must fetch group A's entitlements fresh")

	// Group B appears with a FRESH entitlement claiming the same
	// exclusion group's default. A full resync would fail; the warm sync
	// (group A's page replays) must fail identically.
	groupB, entB, err := inner.AddGroup(ctx, "group_b")
	require.NoError(t, err)
	entBAnnos := annotations.Annotations(entB.GetAnnotations())
	entBAnnos.Update(defaultExclusionGroup)
	entB.SetAnnotations(entBAnnos)
	_ = groupB

	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	store, err := dotc1z.NewStore(ctx, sync2,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	opts := append([]SyncOpt{WithConnectorStore(store), WithTmpDir(tmpDir), WithPreviousSyncC1ZPath(sync1)}, workerOpts...)
	syncer, err := NewSyncer(ctx, mc, opts...)
	require.NoError(t, err)
	err = syncer.Sync(ctx)
	require.Error(t, err,
		"a duplicate exclusion-group default split across a replayed and a fresh scope must fail the warm sync like a cold sync would")
	require.Contains(t, err.Error(), "multiple default entitlements")
	require.NoError(t, syncer.Close(ctx))
	require.NotZero(t, mc.entLookupHits, "sanity: sync 2 must have replayed group A's entitlements page")
}

// ---------------------------------------------------------------------
// Partial syncs must not replay.
// ---------------------------------------------------------------------

// TestSourceCache_PartialSyncDegradesToCold pins the full-sync-only gate:
// a partial (targeted) sync with a warm previous file must install the
// no-op lookup — every scope misses, the connector fetches fresh — rather
// than replaying whole scopes into a targeted subset (which could
// resurrect out-of-target rows, and whose related-resource fetch only
// runs over response rows).
func TestSourceCache_PartialSyncDegradesToCold(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	group, ent, alice, _ := sourceCacheTestFixtures(t)
	g1 := gt.NewGrant(group, "member", alice)

	mc := newSourceCacheMockConnector()
	mc.AddResource(ctx, group)
	mc.AddResource(ctx, alice)
	mc.entDB[group.GetId().GetResource()] = []*v2.Entitlement{ent}
	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{g1}
	mc.etagByResource[group.GetId().GetResource()] = `W/"etag-v1"`

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	runSourceCacheSync(ctx, t, mc, sync1, "", tmpDir)
	require.Zero(t, mc.lookupHits)

	// Partial sync targeting the group, with the warm file supplied: the
	// connector's lookups must all MISS despite the warm previous sync.
	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	store, err := dotc1z.NewStore(ctx, sync2,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	missesBefore := mc.lookupMisses
	syncer, err := NewSyncer(ctx, mc,
		WithConnectorStore(store),
		WithTmpDir(tmpDir),
		WithPreviousSyncC1ZPath(sync1),
		WithTargetedSyncResources([]*v2.Resource{group}),
	)
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))

	require.Zero(t, mc.lookupHits, "a partial sync must never get a source-cache lookup hit")
	require.Greater(t, mc.lookupMisses, missesBefore,
		"sanity: the connector must have looked up and missed (no-op lookup installed)")
}

// ---------------------------------------------------------------------
// Non-full previous syncs must not replay.
// ---------------------------------------------------------------------

// TestSourceCache_PartialPreviousSyncDegradesToCold pins the type half of
// the replay-source predicate (UsableAsReplaySource): a PARTIAL-typed
// previous sync must never serve replay, even when its file carries a
// manifest entry over stamped rows. The fixture plants exactly that —
// a partial sync with a valid-looking manifest — so a pass proves the
// run-record TYPE gate causes the degradation, not an incidentally empty
// manifest. Partial syncs are subsets: their rows do not cover the
// scopes any validator vouches for.
func TestSourceCache_PartialPreviousSyncDegradesToCold(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	group, ent, alice, _ := sourceCacheTestFixtures(t)
	g1 := gt.NewGrant(group, "member", alice)
	grantsScope := grantScopeForResource(group.GetId().GetResource())

	// Hand-build a PARTIAL sync whose grants are stamped with the scope
	// the connector will look up, manifest entry included.
	sync1 := filepath.Join(tmpDir, "partial-prev.c1z")
	prevStore, err := dotc1z.NewStore(ctx, sync1,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	_, isNew, err := prevStore.StartOrResumeSync(ctx, connectorstore.SyncTypePartial, "")
	require.NoError(t, err)
	require.True(t, isNew)
	require.NoError(t, prevStore.PutResourceTypes(ctx, groupResourceType, userResourceType))
	require.NoError(t, prevStore.PutResources(ctx, group, alice))
	require.NoError(t, prevStore.PutEntitlements(ctx, ent))
	require.NoError(t, prevStore.PutGrants(sourcecache.WithScope(ctx, grantsScope), g1))
	scPrev, ok := any(prevStore).(dotc1z.SourceCacheStore)
	require.True(t, ok)
	require.NoError(t, scPrev.PutSourceCacheEntry(ctx, sourcecache.RowKindGrants, grantsScope, `W/"etag-v1"`))
	require.NoError(t, prevStore.EndSync(ctx))
	require.NoError(t, prevStore.Close(ctx))

	mc := newSourceCacheMockConnector()
	mc.AddResource(ctx, group)
	mc.AddResource(ctx, alice)
	mc.entDB[group.GetId().GetResource()] = []*v2.Entitlement{ent}
	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{g1}
	mc.etagByResource[group.GetId().GetResource()] = `W/"etag-v1"`

	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	missesBefore := mc.lookupMisses
	runSourceCacheSync(ctx, t, mc, sync2, sync1, tmpDir)
	require.Zero(t, mc.lookupHits,
		"a partial-typed previous sync must never produce a source-cache lookup hit, even with a planted manifest entry")
	require.Greater(t, mc.lookupMisses, missesBefore,
		"sanity: the connector must have looked up and missed (no-op lookup installed)")
}

// ---------------------------------------------------------------------
// Compacted previous syncs must not replay.
// ---------------------------------------------------------------------

// TestSourceCache_CompactedPreviousSyncDegradesToCold pins the METADATA
// gate: a previous sync whose run record carries the compacted flag must
// never serve replay — independent of its manifest contents. The test
// leaves the manifest fully intact and only flips the flag, so a pass
// proves the gate itself (not the compactor's manifest clearing) causes
// the degradation. Compaction invalidates validators categorically: a
// keep-newer merge is not a row set any input's etag describes.
func TestSourceCache_CompactedPreviousSyncDegradesToCold(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	group, ent, alice, _ := sourceCacheTestFixtures(t)
	g1 := gt.NewGrant(group, "member", alice)

	mc := newSourceCacheMockConnector()
	mc.AddResource(ctx, group)
	mc.AddResource(ctx, alice)
	mc.entDB[group.GetId().GetResource()] = []*v2.Entitlement{ent}
	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{g1}
	mc.etagByResource[group.GetId().GetResource()] = `W/"etag-v1"`

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	runSourceCacheSync(ctx, t, mc, sync1, "", tmpDir)

	// Flip ONLY the compacted flag on sync 1's run record; its
	// source-cache manifest stays intact and would otherwise replay.
	marker, err := dotc1z.NewStore(ctx, sync1,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	run, err := marker.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, run)
	engineHolder, ok := any(marker).(interface{ PebbleEngine() *pebbleengine.Engine })
	require.True(t, ok)
	rec, err := engineHolder.PebbleEngine().GetSyncRunRecord(ctx, run.ID)
	require.NoError(t, err)
	rec.SetCompacted(true)
	require.NoError(t, engineHolder.PebbleEngine().PutSyncRunRecord(ctx, rec))
	require.True(t, pebbleengine.MarkStoreDirty(marker), "the flag write must persist through Close")
	require.NoError(t, marker.Close(ctx))

	// Warm sync against the compacted-marked file: every lookup must
	// MISS despite the intact manifest.
	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	missesBefore := mc.lookupMisses
	runSourceCacheSync(ctx, t, mc, sync2, sync1, tmpDir)
	require.Zero(t, mc.lookupHits,
		"a compacted previous sync must never produce a source-cache lookup hit")
	require.Greater(t, mc.lookupMisses, missesBefore,
		"sanity: the connector must have looked up and missed (no-op lookup installed)")
}

// ---------------------------------------------------------------------
// 5. Continuation answers accumulated across bounces must stay
//    acceptable to the connector-side answer parser.
// ---------------------------------------------------------------------

// TestSourceCacheContinuation_AnswerAccumulationBeyondOneAsk pins the
// cap coherence between the two sides of the ask/answer continuation:
// answers accumulate across bounces (every re-invoke carries the union),
// so a maximal first ask (4096 queries, the ask cap) followed by ONE
// late scope — explicitly legal per the annotation contract — produces
// 4097 answers on the next re-invoke. The connector-side parser must
// accept that union; capping it at one ask's size hard-fails a legal
// exchange.
func TestSourceCacheContinuation_AnswerAccumulationBeyondOneAsk(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)

	store := &stubSourceCacheStore{}
	s := &syncer{
		state: newState(),
		sourceCache: syncerSourceCache{
			enabled:        true,
			current:        store,
			prev:           store,
			lookup:         staticLookup{etag: "e"},
			replayedScopes: &replayedScopeSet{},
		},
	}

	bigAsk := make([]sourcecache.Query, 0, 4096)
	for i := 0; i < 4096; i++ {
		bigAsk = append(bigAsk, sourcecache.Query{
			RowKind:  sourcecache.RowKindGrants,
			ScopeKey: fmt.Sprintf("scope-%04d", i),
		})
	}
	lateAsk := []sourcecache.Query{{RowKind: sourcecache.RowKindGrants, ScopeKey: "late-scope"}}

	err = s.withSourceCacheContinuation(ctx, "test-op", func(extra annotations.Annotations, attempt int) (listAttempt, error) {
		answersMsg := &v2.SourceCacheLookupAnswers{}
		hasAnswers, err := extra.Pick(answersMsg)
		if err != nil {
			return listAttempt{}, err
		}
		if hasAnswers {
			// The connector-side boundary every re-invoke passes through
			// (connectorbuilder's continuationOpAttrs): the syncer's
			// accumulated union must never exceed what it accepts.
			if _, err := sourcecache.AnswersFromProto(answersMsg); err != nil {
				return listAttempt{}, err
			}
		}
		switch attempt {
		case 0:
			require.False(t, hasAnswers)
			return listAttempt{annos: annotations.New(sourcecache.AskProto(bigAsk))}, nil
		case 1:
			require.Len(t, answersMsg.GetAnswers(), 4096)
			// Phase 2 legally queries one scope it did not ask before.
			return listAttempt{annos: annotations.New(sourcecache.AskProto(lateAsk))}, nil
		default:
			require.Len(t, answersMsg.GetAnswers(), 4097)
			return listAttempt{}, nil
		}
	})
	require.NoError(t, err,
		"a maximal first ask plus one late scope is a legal exchange; the accumulated answer union must be accepted end to end")
}

// TestSourceCacheContinuation_OversizedAskFailsLoudly pins the intended
// hard boundary on ONE ask: more than 4096 queries in a single ask is a
// connector bug (shard via EnqueuePageTokens instead) and must fail the sync
// with a descriptive error, not silently truncate.
func TestSourceCacheContinuation_OversizedAskFailsLoudly(t *testing.T) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)

	store := &stubSourceCacheStore{}
	s := &syncer{
		state: newState(),
		sourceCache: syncerSourceCache{
			enabled:        true,
			current:        store,
			prev:           store,
			lookup:         staticLookup{etag: "e"},
			replayedScopes: &replayedScopeSet{},
		},
	}

	oversized := make([]sourcecache.Query, 0, 4097)
	for i := 0; i < 4097; i++ {
		oversized = append(oversized, sourcecache.Query{
			RowKind:  sourcecache.RowKindGrants,
			ScopeKey: fmt.Sprintf("scope-%04d", i),
		})
	}
	err = s.withSourceCacheContinuation(ctx, "test-op", func(extra annotations.Annotations, attempt int) (listAttempt, error) {
		return listAttempt{annos: annotations.New(sourcecache.AskProto(oversized))}, nil
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "max 4096")
}
