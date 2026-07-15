package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// End-to-end harness for TYPE-SCOPED grants (v2.TypeScopedGrants +
// EnqueuePageTokens), modeled on Microsoft Graph's chunked groups/delta:
// membership grants for a whole resource type are served through
// connector-defined 50-id chunk cursors instead of one ListGrants call per
// group. Each chunk is its own source-cache scope, so warm syncs replay
// unchanged chunks and overlay/tombstone the changed ones.
//
// The syncer-side behaviors under test:
//   - the grants planner excludes the annotated type from the per-resource
//     fan-out and enqueues exactly one type-scoped action,
//   - the planning call's EnqueuePageTokens annotation enqueues one action per
//     chunk, each checkpointed/paginated independently,
//   - chunk scopes replay warm with overlay adds and canonical-id
//     tombstones, and equivalence against a cold control sync holds.

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
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

const chunkSize = 50

// chunkedGrantsMockConnector serves group-membership grants exclusively
// through type-scoped cursors, mimicking chunked Graph delta:
//
//   - planning call (empty page token): no rows; EnqueuePageTokens with one
//     token per 50-group chunk.
//   - chunk cursor, lookup miss (cold): enumerate the chunk's grants,
//     paginated, final page carrying the chunk's validator.
//   - chunk cursor, lookup hit (warm): replay + overlay adds + canonical
//     grant-id tombstones + rotated validator.
type chunkedGrantsMockConnector struct {
	*mockConnector

	mu     sync.Mutex
	lookup sourcecache.Lookup

	groupIDs []string            // sorted; chunk i = groupIDs[i*50:(i+1)*50]
	members  map[string][]string // groupID -> userIDs
	entByID  map[string]*v2.Entitlement
	resByID  map[string]*v2.Resource

	tokenByChunk map[int]string // last validator issued per chunk
	generation   int

	// per-sync delta state, applied on warm rounds
	addsByChunk       map[int][]*v2.Grant
	deletedIDsByChunk map[int][]string

	// unstampedExtra, when set, chains one UNSTAMPED page after chunk 0's
	// scoped pages — cold and warm alike. Models the Okta shape: a
	// type-scoped cursor whose members pages are scope-stamped, followed
	// by fresh pages with no change signal (group role assignments) that
	// carry no source-cache annotations and must be re-emitted every sync.
	unstampedExtra bool

	// observability for assertions
	planningCalls      int
	coldChunkRounds    int
	warmChunkRounds    int
	perResourceGroupLG int
	unstampedPages     int
}

var typeScopedGroupRT = v2.ResourceType_builder{
	Id:          "group",
	DisplayName: "Group",
	Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
	Annotations: annotations.New(&v2.TypeScopedGrants{}),
}.Build()

func newChunkedGrantsMockConnector(t *testing.T, groupCount int, membersPerGroup int) *chunkedGrantsMockConnector {
	t.Helper()
	mc := &chunkedGrantsMockConnector{
		mockConnector:     newMockConnector(),
		members:           map[string][]string{},
		entByID:           map[string]*v2.Entitlement{},
		resByID:           map[string]*v2.Resource{},
		tokenByChunk:      map[int]string{},
		addsByChunk:       map[int][]*v2.Grant{},
		deletedIDsByChunk: map[int][]string{},
	}
	mc.rtDB = append(mc.rtDB, typeScopedGroupRT, userResourceType)

	// A pool of users, each carrying SkipEntitlementsAndGrants so only the
	// group type participates in the grants phase.
	userIDs := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		uid := fmt.Sprintf("user-%02d", i)
		u, err := rs.NewUserResource(uid, userResourceType, uid, nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
		require.NoError(t, err)
		mc.AddResource(context.Background(), u)
		mc.resByID[uid] = u
		userIDs = append(userIDs, uid)
	}

	for i := 0; i < groupCount; i++ {
		gid := fmt.Sprintf("group-%03d", i)
		g, err := rs.NewGroupResource(gid, typeScopedGroupRT, gid, nil)
		require.NoError(t, err)
		mc.AddResource(context.Background(), g)
		mc.resByID[gid] = g
		mc.groupIDs = append(mc.groupIDs, gid)

		ent := et.NewAssignmentEntitlement(g, "member", et.WithGrantableTo(userResourceType))
		ent.SetSlug("member")
		mc.entByID[gid] = ent
		mc.entDB[gid] = []*v2.Entitlement{ent}

		for m := 0; m < membersPerGroup; m++ {
			mc.members[gid] = append(mc.members[gid], userIDs[(i+m)%len(userIDs)])
		}
	}
	sort.Strings(mc.groupIDs)
	return mc
}

func (mc *chunkedGrantsMockConnector) SetSourceCache(_ context.Context, lookup sourcecache.Lookup) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if lookup == nil {
		lookup = sourcecache.NoopLookup{}
	}
	mc.lookup = lookup
}

func (mc *chunkedGrantsMockConnector) Validate(context.Context, *v2.ConnectorServiceValidateRequest, ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return v2.ConnectorServiceValidateResponse_builder{
		Annotations: annotations.New(v2.SourceCacheCapability_builder{
			Mode: v2.SourceCacheCapability_MODE_READ_WRITE,
		}.Build()),
	}.Build(), nil
}

func (mc *chunkedGrantsMockConnector) chunkCount() int {
	return (len(mc.groupIDs) + chunkSize - 1) / chunkSize
}

func (mc *chunkedGrantsMockConnector) chunkGroups(chunk int) []string {
	lo := chunk * chunkSize
	hi := lo + chunkSize
	if hi > len(mc.groupIDs) {
		hi = len(mc.groupIDs)
	}
	return mc.groupIDs[lo:hi]
}

func chunkScope(chunk int) string {
	return fmt.Sprintf("groups/delta/chunk/%03d?sig=testsig", chunk)
}

func (mc *chunkedGrantsMockConnector) memberGrant(gid, uid string) *v2.Grant {
	return gt.NewGrant(mc.resByID[gid], "member", mc.resByID[uid].GetId())
}

func (mc *chunkedGrantsMockConnector) chunkGrants(chunk int) []*v2.Grant {
	var out []*v2.Grant
	for _, gid := range mc.chunkGroups(chunk) {
		for _, uid := range mc.members[gid] {
			out = append(out, mc.memberGrant(gid, uid))
		}
	}
	return out
}

// mutateAddMember journals a warm-round overlay add and updates live state
// (so a cold control sync sees the same tenant).
func (mc *chunkedGrantsMockConnector) mutateAddMember(gid, uid string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.members[gid] = append(mc.members[gid], uid)
	chunk := sort.SearchStrings(mc.groupIDs, gid) / chunkSize
	mc.addsByChunk[chunk] = append(mc.addsByChunk[chunk], mc.memberGrant(gid, uid))
}

// mutateRemoveMember journals a warm-round tombstone and updates live state.
func (mc *chunkedGrantsMockConnector) mutateRemoveMember(gid, uid string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	kept := mc.members[gid][:0]
	for _, m := range mc.members[gid] {
		if m != uid {
			kept = append(kept, m)
		}
	}
	mc.members[gid] = kept
	chunk := sort.SearchStrings(mc.groupIDs, gid) / chunkSize
	mc.deletedIDsByChunk[chunk] = append(mc.deletedIDsByChunk[chunk], mc.memberGrant(gid, uid).GetId())
}

func (mc *chunkedGrantsMockConnector) clearDeltas() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.addsByChunk = map[int][]*v2.Grant{}
	mc.deletedIDsByChunk = map[int][]string{}
}

func (mc *chunkedGrantsMockConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	rid := in.GetResource().GetId()
	if rid.GetResourceType() != typeScopedGroupRT.GetId() {
		// Users are annotated SkipEntitlementsAndGrants; nothing else
		// should arrive here.
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	if !reqAnnos.Contains(&v2.TypeScopedGrants{}) {
		// The planner must never fan out per-resource for the annotated
		// type; recorded and asserted in the test.
		mc.mu.Lock()
		mc.perResourceGroupLG++
		mc.mu.Unlock()
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}

	// --- type-scoped call ---
	tok := in.GetPageToken()
	if tok == "" {
		// Planning call: spawn one cursor per chunk, no rows.
		mc.mu.Lock()
		mc.planningCalls++
		n := mc.chunkCount()
		mc.mu.Unlock()
		tokens := make([]string, 0, n)
		for i := 0; i < n; i++ {
			tokens = append(tokens, fmt.Sprintf("chunk=%d", i))
		}
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: annotations.New(v2.EnqueuePageTokens_builder{
				PageTokens:     tokens,
				EstimatedTotal: int64(len(mc.groupIDs)),
			}.Build()),
		}.Build(), nil
	}

	if tok == "chunk=0&extra" {
		// The unstamped trailing page: fresh rows, NO source-cache
		// annotations, emitted on every sync (there is no validator for
		// them, so there is no legal replay).
		mc.mu.Lock()
		mc.unstampedPages++
		mc.mu.Unlock()
		return v2.GrantsServiceListGrantsResponse_builder{
			List: mc.unstampedExtraGrants(),
		}.Build(), nil
	}

	var chunk, page int
	if n, err := fmt.Sscanf(tok, "chunk=%d&page=%d", &chunk, &page); err != nil || n != 2 {
		if _, err := fmt.Sscanf(tok, "chunk=%d", &chunk); err != nil {
			return nil, fmt.Errorf("bad page token %q", tok)
		}
		page = 0
	}
	scope := chunkScope(chunk)

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
		entry, found, err := lookup.Lookup(ctx, sourcecache.RowKindGrants, scope)
		if err != nil {
			return nil, err
		}
		if found && lastTok != "" && entry.CacheValidator == lastTok {
			// Warm round: one page — replay, overlay adds, tombstones,
			// rotated validator. With unstampedExtra set, chunk 0 chains
			// its unstamped trailing page after the replay page: scoped
			// and unscoped pages coexisting in one cursor's stream.
			mc.mu.Lock()
			mc.warmChunkRounds++
			mc.mu.Unlock()
			rotate()
			next := ""
			if mc.unstampedExtra && chunk == 0 {
				next = "chunk=0&extra"
			}
			return v2.GrantsServiceListGrantsResponse_builder{
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

	// Cold enumeration for this chunk, two pages to exercise cursor
	// checkpointing: page 0 = first half + interim scope (no etag),
	// page 1 = rest + validator.
	grants := mc.chunkGrants(chunk)
	half := len(grants) / 2
	if page == 0 {
		mc.mu.Lock()
		mc.coldChunkRounds++
		mc.mu.Unlock()
		return v2.GrantsServiceListGrantsResponse_builder{
			List: grants[:half],
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
	return v2.GrantsServiceListGrantsResponse_builder{
		List: grants[half:],
		Annotations: annotations.New(v2.SourceCacheRecord_builder{
			ScopeKey:       scope,
			CacheValidator: newTok,
		}.Build()),
		NextPageToken: next,
	}.Build(), nil
}

// unstampedExtraGrants are the rows served by the unstamped trailing page:
// "admin" grants on group-000 for two fixed users. They exist only while
// the connector emits them — nothing replays them.
func (mc *chunkedGrantsMockConnector) unstampedExtraGrants() []*v2.Grant {
	g := mc.resByID[mc.groupIDs[0]]
	return []*v2.Grant{
		gt.NewGrant(g, "admin", mc.resByID["user-08"].GetId()),
		gt.NewGrant(g, "admin", mc.resByID["user-09"].GetId()),
	}
}

func requireSameGrantIDs(t *testing.T, want, got map[string]*v2.Grant, msg string) {
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
	require.Emptyf(t, missing, "%s: grants missing: %s", msg, strings.Join(missing, ", "))
	require.Emptyf(t, extra, "%s: unexpected grants: %s", msg, strings.Join(extra, ", "))
}

func runChunkedSync(ctx context.Context, t *testing.T, mc *chunkedGrantsMockConnector, path, prevPath, tmpDir string, extraOpts ...SyncOpt) {
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

// TestTypeScopedGrantsMixedStampedUnstampedCursor pins a contract point
// proven in the field by the baton-okta POC: within ONE type-scoped
// cursor's page stream, scope-stamped pages (members, replayable) and
// unstamped fresh pages (rows with no change signal, e.g. group role
// assignments) coexist. Stamping is per-page context, not per-cursor
// state, so:
//
//   - warm syncs replay the stamped pages while the unstamped pages are
//     re-emitted fresh by the connector, and the union matches a cold
//     sync exactly;
//   - unstamped rows never ride replay: the sync only contains them while
//     the connector emits them.
func TestTypeScopedGrantsMixedStampedUnstampedCursor(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	// 10 groups => 1 chunk, 3 members each => 30 member grants, plus 2
	// unstamped "admin" grants from the trailing extra page.
	mc := newChunkedGrantsMockConnector(t, 10, 3)
	mc.unstampedExtra = true
	require.Equal(t, 1, mc.chunkCount())

	// --- Sync 1: cold — scoped member pages then the unstamped page ----
	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	runChunkedSync(ctx, t, mc, sync1, "", tmpDir)
	require.Equal(t, 1, mc.coldChunkRounds)
	require.Equal(t, 1, mc.unstampedPages, "cold sync serves the unstamped trailing page")

	grants1 := listGrantsInFile(ctx, t, sync1)
	require.Len(t, grants1, 32)

	// --- Sync 2: warm — replay page chains into the unstamped page -----
	mc.generation = 1
	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	runChunkedSync(ctx, t, mc, sync2, sync1, tmpDir)
	require.Equal(t, 1, mc.warmChunkRounds, "member pages replay warm")
	require.Equal(t, 1, mc.coldChunkRounds, "no cold re-enumeration")
	require.Equal(t, 2, mc.unstampedPages, "unstamped page is re-served on the warm sync — it never replays")

	grants2 := listGrantsInFile(ctx, t, sync2)
	requireSameGrantIDs(t, grants1, grants2, "warm sync with mixed pages vs cold")

	// --- Sync 3: warm, connector stops emitting the unstamped page -----
	// The admin grants must vanish: they were never scope-stamped, so
	// nothing replays them. If they survive, unstamped rows are leaking
	// into replay.
	mc.unstampedExtra = false
	mc.generation = 2
	sync3 := filepath.Join(tmpDir, "sync3.c1z")
	runChunkedSync(ctx, t, mc, sync3, sync2, tmpDir)
	require.Equal(t, 2, mc.warmChunkRounds)
	require.Equal(t, 2, mc.unstampedPages, "no unstamped page served on sync 3")

	grants3 := listGrantsInFile(ctx, t, sync3)
	require.Len(t, grants3, 30, "unstamped rows must not ride replay")
	for _, g := range mc.unstampedExtraGrants() {
		require.NotContains(t, grants3, g.GetId())
	}
}

func TestTypeScopedGrantsChunkedDelta(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			runChunkedDeltaScenario(t, workers)
		})
	}
}

func runChunkedDeltaScenario(t *testing.T, workers int) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	var workerOpts []SyncOpt
	if workers > 0 {
		workerOpts = append(workerOpts, WithWorkerCount(workers))
	}

	// 120 groups => 3 chunks (50/50/20), 3 members each => 360 grants.
	mc := newChunkedGrantsMockConnector(t, 120, 3)
	require.Equal(t, 3, mc.chunkCount())

	// --- Sync 1: cold ---------------------------------------------------
	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	runChunkedSync(ctx, t, mc, sync1, "", tmpDir, workerOpts...)

	require.Equal(t, 1, mc.planningCalls, "one planning call per sync")
	require.Equal(t, 3, mc.coldChunkRounds, "every chunk enumerates cold on the first sync")
	require.Zero(t, mc.warmChunkRounds)
	require.Zero(t, mc.perResourceGroupLG, "planner must not fan out per-resource for the annotated type")

	grants1 := listGrantsInFile(ctx, t, sync1)
	require.Len(t, grants1, 360)

	// --- Sync 2: warm with changes in two chunks ------------------------
	mc.generation = 1
	mc.mutateAddMember("group-071", "user-00")                     // chunk 1
	mc.mutateRemoveMember("group-115", mc.members["group-115"][0]) // chunk 2

	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	runChunkedSync(ctx, t, mc, sync2, sync1, tmpDir, workerOpts...)

	require.Equal(t, 2, mc.planningCalls)
	require.Equal(t, 3, mc.coldChunkRounds, "no chunk re-enumerates on the warm sync")
	require.Equal(t, 3, mc.warmChunkRounds, "every chunk replays warm")
	require.Zero(t, mc.perResourceGroupLG)

	grants2 := listGrantsInFile(ctx, t, sync2)
	require.Len(t, grants2, 360, "one add and one remove cancel out")
	require.Contains(t, grants2, mc.memberGrant("group-071", "user-00").GetId(), "overlay add must land")

	// Equivalence: a cold control sync against the same tenant state must
	// produce the same grant set.
	mc.clearDeltas()
	control := filepath.Join(tmpDir, "control.c1z")
	runChunkedSync(ctx, t, mc, control, "", tmpDir, workerOpts...)
	require.Equal(t, 6, mc.coldChunkRounds, "control (no previous c1z) re-enumerates every chunk")
	controlGrants := listGrantsInFile(ctx, t, control)
	requireSameGrantIDs(t, controlGrants, grants2, "warm sync vs cold control")

	// --- Sync 3: no-op warm chained off sync 2 --------------------------
	mc.generation = 2
	sync3 := filepath.Join(tmpDir, "sync3.c1z")
	runChunkedSync(ctx, t, mc, sync3, sync2, tmpDir, workerOpts...)
	require.Equal(t, 6, mc.warmChunkRounds, "no-op round replays all chunks (validators from sync 2 rotated)")
	require.Equal(t, 6, mc.coldChunkRounds, "no cold rounds on the no-op sync")
	grants3 := listGrantsInFile(ctx, t, sync3)
	requireSameGrantIDs(t, grants2, grants3, "no-op warm sync vs previous")
}
