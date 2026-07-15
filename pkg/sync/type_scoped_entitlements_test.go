package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// End-to-end harness for TYPE-SCOPED entitlements (v2.TypeScopedEntitlements +
// EnqueuePageTokens), mirroring type_scoped_grants_test.go: entitlement rows for a
// whole resource type are served through connector-defined chunk cursors
// instead of one ListEntitlements call per group. Each chunk is its own
// source-cache scope, so warm syncs replay unchanged chunks and
// overlay/tombstone the changed ones.

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// chunkedEntitlementsMockConnector serves group entitlements exclusively
// through type-scoped cursors.
type chunkedEntitlementsMockConnector struct {
	*mockConnector

	mu     sync.Mutex
	lookup sourcecache.Lookup

	groupIDs []string            // sorted; chunk i = groupIDs[i*50:(i+1)*50]
	ents     map[string][]string // groupID -> entitlement slugs (e.g. "member")
	resByID  map[string]*v2.Resource

	tokenByChunk map[int]string // last validator issued per chunk
	generation   int

	addsByChunk       map[int][]*v2.Entitlement
	deletedIDsByChunk map[int][]string

	unstampedExtra bool

	planningCalls      int
	coldChunkRounds    int
	warmChunkRounds    int
	perResourceGroupLE int
	unstampedPages     int
}

var typeScopedEntGroupRT = v2.ResourceType_builder{
	Id:          "group",
	DisplayName: "Group",
	Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
	Annotations: annotations.New(&v2.TypeScopedEntitlements{}),
}.Build()

func newChunkedEntitlementsMockConnector(t *testing.T, groupCount int, entsPerGroup int) *chunkedEntitlementsMockConnector {
	t.Helper()
	mc := &chunkedEntitlementsMockConnector{
		mockConnector:     newMockConnector(),
		ents:              map[string][]string{},
		resByID:           map[string]*v2.Resource{},
		tokenByChunk:      map[int]string{},
		addsByChunk:       map[int][]*v2.Entitlement{},
		deletedIDsByChunk: map[int][]string{},
	}
	mc.rtDB = append(mc.rtDB, typeScopedEntGroupRT, userResourceType)

	userIDs := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		uid := fmt.Sprintf("user-%02d", i)
		u, err := rs.NewUserResource(uid, userResourceType, uid, nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
		require.NoError(t, err)
		mc.AddResource(context.Background(), u)
		mc.resByID[uid] = u
		userIDs = append(userIDs, uid)
	}
	_ = userIDs

	slugs := []string{"member", "admin", "viewer"}
	for i := 0; i < groupCount; i++ {
		gid := fmt.Sprintf("group-%03d", i)
		g, err := rs.NewGroupResource(gid, typeScopedEntGroupRT, gid, nil)
		require.NoError(t, err)
		mc.AddResource(context.Background(), g)
		mc.resByID[gid] = g
		mc.groupIDs = append(mc.groupIDs, gid)

		for e := 0; e < entsPerGroup; e++ {
			mc.ents[gid] = append(mc.ents[gid], slugs[e%len(slugs)])
		}
	}
	sort.Strings(mc.groupIDs)
	return mc
}

func (mc *chunkedEntitlementsMockConnector) SetSourceCache(_ context.Context, lookup sourcecache.Lookup) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if lookup == nil {
		lookup = sourcecache.NoopLookup{}
	}
	mc.lookup = lookup
}

func (mc *chunkedEntitlementsMockConnector) Validate(context.Context, *v2.ConnectorServiceValidateRequest, ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return v2.ConnectorServiceValidateResponse_builder{
		Annotations: annotations.New(v2.SourceCacheCapability_builder{
			Mode: v2.SourceCacheCapability_MODE_READ_WRITE,
		}.Build()),
	}.Build(), nil
}

func (mc *chunkedEntitlementsMockConnector) chunkCount() int {
	return (len(mc.groupIDs) + chunkSize - 1) / chunkSize
}

func (mc *chunkedEntitlementsMockConnector) chunkGroups(chunk int) []string {
	lo := chunk * chunkSize
	hi := lo + chunkSize
	if hi > len(mc.groupIDs) {
		hi = len(mc.groupIDs)
	}
	return mc.groupIDs[lo:hi]
}

func entChunkScope(chunk int) string {
	return fmt.Sprintf("groups/ents/chunk/%03d?sig=testsig", chunk)
}

func (mc *chunkedEntitlementsMockConnector) groupEnt(gid, slug string) *v2.Entitlement {
	return et.NewAssignmentEntitlement(mc.resByID[gid], slug, et.WithGrantableTo(userResourceType))
}

func (mc *chunkedEntitlementsMockConnector) chunkEntitlements(chunk int) []*v2.Entitlement {
	var out []*v2.Entitlement
	for _, gid := range mc.chunkGroups(chunk) {
		for _, slug := range mc.ents[gid] {
			out = append(out, mc.groupEnt(gid, slug))
		}
	}
	return out
}

func (mc *chunkedEntitlementsMockConnector) mutateAddEntitlement(gid, slug string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.ents[gid] = append(mc.ents[gid], slug)
	chunk := sort.SearchStrings(mc.groupIDs, gid) / chunkSize
	mc.addsByChunk[chunk] = append(mc.addsByChunk[chunk], mc.groupEnt(gid, slug))
}

func (mc *chunkedEntitlementsMockConnector) mutateRemoveEntitlement(gid, slug string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	kept := mc.ents[gid][:0]
	for _, s := range mc.ents[gid] {
		if s != slug {
			kept = append(kept, s)
		}
	}
	mc.ents[gid] = kept
	chunk := sort.SearchStrings(mc.groupIDs, gid) / chunkSize
	mc.deletedIDsByChunk[chunk] = append(mc.deletedIDsByChunk[chunk], mc.groupEnt(gid, slug).GetId())
}

func (mc *chunkedEntitlementsMockConnector) clearDeltas() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.addsByChunk = map[int][]*v2.Entitlement{}
	mc.deletedIDsByChunk = map[int][]string{}
}

func (mc *chunkedEntitlementsMockConnector) ListEntitlements(
	ctx context.Context, in *v2.EntitlementsServiceListEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	rid := in.GetResource().GetId()
	if rid.GetResourceType() != typeScopedEntGroupRT.GetId() {
		return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
	}
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	if !reqAnnos.Contains(&v2.TypeScopedEntitlements{}) {
		mc.mu.Lock()
		mc.perResourceGroupLE++
		mc.mu.Unlock()
		return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
	}

	tok := in.GetPageToken()
	if tok == "" {
		mc.mu.Lock()
		mc.planningCalls++
		n := mc.chunkCount()
		mc.mu.Unlock()
		tokens := make([]string, 0, n)
		for i := 0; i < n; i++ {
			tokens = append(tokens, fmt.Sprintf("chunk=%d", i))
		}
		return v2.EntitlementsServiceListEntitlementsResponse_builder{
			Annotations: annotations.New(v2.EnqueuePageTokens_builder{
				PageTokens:     tokens,
				EstimatedTotal: int64(len(mc.groupIDs)),
			}.Build()),
		}.Build(), nil
	}

	if tok == "chunk=0&extra" {
		mc.mu.Lock()
		mc.unstampedPages++
		mc.mu.Unlock()
		return v2.EntitlementsServiceListEntitlementsResponse_builder{
			List: mc.unstampedExtraEntitlements(),
		}.Build(), nil
	}

	var chunk, page int
	if n, err := fmt.Sscanf(tok, "chunk=%d&page=%d", &chunk, &page); err != nil || n != 2 {
		if _, err := fmt.Sscanf(tok, "chunk=%d", &chunk); err != nil {
			return nil, fmt.Errorf("bad page token %q", tok)
		}
		page = 0
	}
	scope := entChunkScope(chunk)

	mc.mu.Lock()
	lookup := mc.lookup
	lastTok := mc.tokenByChunk[chunk]
	adds := mc.addsByChunk[chunk]
	deleted := mc.deletedIDsByChunk[chunk]
	gen := mc.generation
	mc.mu.Unlock()
	if lookup == nil {
		lookup = sourcecache.NoopLookup{}
	}

	newTok := fmt.Sprintf("tok-c%d-g%d", chunk, gen)
	rotate := func() {
		mc.mu.Lock()
		mc.tokenByChunk[chunk] = newTok
		mc.mu.Unlock()
	}

	if page == 0 {
		entry, found, err := lookup.Lookup(ctx, sourcecache.RowKindEntitlements, scope)
		if err != nil {
			return nil, err
		}
		if found && lastTok != "" && entry.CacheValidator == lastTok {
			mc.mu.Lock()
			mc.warmChunkRounds++
			mc.mu.Unlock()
			rotate()
			next := ""
			if mc.unstampedExtra && chunk == 0 {
				next = "chunk=0&extra"
			}
			return v2.EntitlementsServiceListEntitlementsResponse_builder{
				List: adds,
				Annotations: annotations.New(v2.SourceCacheReplay_builder{
					ScopeKey:       scope,
					CacheValidator: newTok,
					Overlay:        true,
					DeletedIds:     deleted,
				}.Build()),
				NextPageToken: next,
			}.Build(), nil
		}
	}

	ents := mc.chunkEntitlements(chunk)
	half := len(ents) / 2
	if page == 0 {
		mc.mu.Lock()
		mc.coldChunkRounds++
		mc.mu.Unlock()
		return v2.EntitlementsServiceListEntitlementsResponse_builder{
			List: ents[:half],
			Annotations: annotations.New(v2.SourceCacheRecord_builder{
				ScopeKey: scope,
			}.Build()),
			NextPageToken: fmt.Sprintf("chunk=%d&page=1", chunk),
		}.Build(), nil
	}
	rotate()
	next := ""
	if mc.unstampedExtra && chunk == 0 {
		next = "chunk=0&extra"
	}
	return v2.EntitlementsServiceListEntitlementsResponse_builder{
		List: ents[half:],
		Annotations: annotations.New(v2.SourceCacheRecord_builder{
			ScopeKey:       scope,
			CacheValidator: newTok,
		}.Build()),
		NextPageToken: next,
	}.Build(), nil
}

func (mc *chunkedEntitlementsMockConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	// Type-scoped entitlements types still get a per-resource grants fan-out
	// unless also annotated TypeScopedGrants; return empty so grants don't
	// interfere with entitlement assertions.
	return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
}

func (mc *chunkedEntitlementsMockConnector) unstampedExtraEntitlements() []*v2.Entitlement {
	g := mc.resByID[mc.groupIDs[0]]
	return []*v2.Entitlement{
		et.NewAssignmentEntitlement(g, "extra-a", et.WithGrantableTo(userResourceType)),
		et.NewAssignmentEntitlement(g, "extra-b", et.WithGrantableTo(userResourceType)),
	}
}

func requireSameEntitlementIDs(t *testing.T, want, got map[string]*v2.Entitlement, msg string) {
	t.Helper()
	var missing, extra []string
	for id := range want {
		if _, ok := got[id]; !ok {
			missing = append(missing, id)
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok {
			extra = append(extra, id)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	require.Emptyf(t, missing, "%s: entitlements missing: %s", msg, strings.Join(missing, ", "))
	require.Emptyf(t, extra, "%s: unexpected entitlements: %s", msg, strings.Join(extra, ", "))
}

func listEntitlementsInFile(ctx context.Context, t *testing.T, path string) map[string]*v2.Entitlement {
	t.Helper()
	reopen, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { _ = reopen.Close(ctx) }()

	syncMeta, err := reopen.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, syncMeta)
	require.NoError(t, reopen.SetCurrentSync(ctx, syncMeta.ID))

	out := map[string]*v2.Entitlement{}
	pageToken := ""
	for {
		resp, err := reopen.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, e := range resp.GetList() {
			out[e.GetId()] = e
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return out
}

func runChunkedEntitlementsSync(ctx context.Context, t *testing.T, mc *chunkedEntitlementsMockConnector, path, prevPath, tmpDir string, extraOpts ...SyncOpt) {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	opts := []SyncOpt{WithConnectorStore(store), WithTmpDir(tmpDir)}
	if prevPath != "" {
		opts = append(opts, WithPreviousSyncC1ZPath(prevPath))
	}
	opts = append(opts, extraOpts...)
	syncer, err := NewSyncer(ctx, mc, opts...)
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))
}

func TestTypeScopedEntitlementsMixedStampedUnstampedCursor(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	mc := newChunkedEntitlementsMockConnector(t, 10, 1)
	mc.unstampedExtra = true
	require.Equal(t, 1, mc.chunkCount())

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	runChunkedEntitlementsSync(ctx, t, mc, sync1, "", tmpDir)
	require.Equal(t, 1, mc.coldChunkRounds)
	require.Equal(t, 1, mc.unstampedPages)

	ents1 := listEntitlementsInFile(ctx, t, sync1)
	require.Len(t, ents1, 12) // 10 member + 2 unstamped

	mc.generation = 1
	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	runChunkedEntitlementsSync(ctx, t, mc, sync2, sync1, tmpDir)
	require.Equal(t, 1, mc.warmChunkRounds)
	require.Equal(t, 1, mc.coldChunkRounds)
	require.Equal(t, 2, mc.unstampedPages)

	requireSameEntitlementIDs(t, ents1, listEntitlementsInFile(ctx, t, sync2), "warm sync with mixed pages vs cold")

	mc.unstampedExtra = false
	mc.generation = 2
	sync3 := filepath.Join(tmpDir, "sync3.c1z")
	runChunkedEntitlementsSync(ctx, t, mc, sync3, sync2, tmpDir)
	require.Equal(t, 2, mc.warmChunkRounds)
	require.Equal(t, 2, mc.unstampedPages)

	ents3 := listEntitlementsInFile(ctx, t, sync3)
	require.Len(t, ents3, 10, "unstamped rows must not ride replay")
	for _, e := range mc.unstampedExtraEntitlements() {
		require.NotContains(t, ents3, e.GetId())
	}
}

func TestTypeScopedEntitlementsChunkedDelta(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			runChunkedEntitlementsDeltaScenario(t, workers)
		})
	}
}

func runChunkedEntitlementsDeltaScenario(t *testing.T, workers int) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	var workerOpts []SyncOpt
	if workers > 0 {
		workerOpts = append(workerOpts, WithWorkerCount(workers))
	}

	mc := newChunkedEntitlementsMockConnector(t, 120, 1)
	require.Equal(t, 3, mc.chunkCount())

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	runChunkedEntitlementsSync(ctx, t, mc, sync1, "", tmpDir, workerOpts...)

	require.Equal(t, 1, mc.planningCalls)
	require.Equal(t, 3, mc.coldChunkRounds)
	require.Zero(t, mc.warmChunkRounds)
	require.Zero(t, mc.perResourceGroupLE)

	ents1 := listEntitlementsInFile(ctx, t, sync1)
	require.Len(t, ents1, 120)

	mc.generation = 1
	mc.mutateAddEntitlement("group-071", "admin")
	mc.mutateRemoveEntitlement("group-115", "member")

	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	runChunkedEntitlementsSync(ctx, t, mc, sync2, sync1, tmpDir, workerOpts...)

	require.Equal(t, 2, mc.planningCalls)
	require.Equal(t, 3, mc.coldChunkRounds)
	require.Equal(t, 3, mc.warmChunkRounds)
	require.Zero(t, mc.perResourceGroupLE)

	ents2 := listEntitlementsInFile(ctx, t, sync2)
	require.Len(t, ents2, 120, "one add and one remove cancel out")
	require.Contains(t, ents2, mc.groupEnt("group-071", "admin").GetId(), "overlay add must land")

	mc.clearDeltas()
	control := filepath.Join(tmpDir, "control.c1z")
	runChunkedEntitlementsSync(ctx, t, mc, control, "", tmpDir, workerOpts...)
	requireSameEntitlementIDs(t, listEntitlementsInFile(ctx, t, control), ents2, "warm vs cold control")
}

func TestTypeScopedEntitlementsTargetedSyncSkipsPerResource(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	mc := newChunkedEntitlementsMockConnector(t, 5, 1)
	target := mc.resByID["group-000"]

	// Full sync first so the resource type (with annotation) is stored.
	full := filepath.Join(tmpDir, "full.c1z")
	runChunkedEntitlementsSync(ctx, t, mc, full, "", tmpDir)
	require.Zero(t, mc.perResourceGroupLE)

	mc.planningCalls = 0
	mc.perResourceGroupLE = 0
	mc.coldChunkRounds = 0
	mc.warmChunkRounds = 0

	targeted := filepath.Join(tmpDir, "targeted.c1z")
	store, err := dotc1z.NewStore(ctx, targeted,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	syncer, err := NewSyncer(ctx, mc,
		WithConnectorStore(store),
		WithTmpDir(tmpDir),
		WithPreviousSyncC1ZPath(full),
		WithTargetedSyncResources([]*v2.Resource{target}),
	)
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))

	require.Zero(t, mc.perResourceGroupLE, "targeted sync must not fan out per-resource ListEntitlements for a type-scoped type")
	require.Zero(t, mc.planningCalls, "targeted sync must not run the type-scoped entitlements planner either")
}
