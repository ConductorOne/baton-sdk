package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Per-resource SpawnCursors: parallel warm revalidation for page-numbered
// APIs (the GitHub shape). On page one of a resource's grant collection the
// connector already knows every other page's URL and stored validator, so
// it answers page one and spawns pages 2..N as sibling actions; each
// spawned page independently hits or misses its source-cache lookup.
//
// Under test:
//   - SpawnCursors on a per-resource grants response fans out sibling
//     actions carrying the resource identity (no warning, no drops);
//   - spawned pages are ordinary pages: replay on hit, cold fetch on miss
//     (page boundary shifted), and a cold spawned page may CHAIN to a
//     fresh page via NextPageToken (probe past a full tail);
//   - reader-surface equivalence against a cold control sync, at
//     workers=0 (sequential) and workers=4 (parallel).

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
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
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

const spawnPageSize = 4

// pagedSpawnMockConnector serves each group's member grants through
// numbered pages of size spawnPageSize. Page 1 carries its own rows plus
// SpawnCursors for every other page the previous sync recorded (warm) or
// that exist upstream (cold). Every page is its own source-cache scope
// (page-URL style). Membership mutations shift page contents, so mutated
// pages miss their lookup and refetch cold — including discovering fresh
// tail pages a warm spawn never knew about.
type pagedSpawnMockConnector struct {
	*mockConnector

	mu     sync.Mutex
	lookup sourcecache.Lookup

	groupIDs []string
	members  map[string][]string // groupID -> userIDs, page k = members[k*4:(k+1)*4]
	// prevMembers models the connector's knowledge of the STORED page
	// chain (what the previous sync recorded): the spawn horizon on warm
	// syncs, and the row counts behind the full-tail probe decision. The
	// test snapshots it after each sync, mirroring what a real connector
	// derives from its stored validators.
	prevMembers map[string][]string
	resByID     map[string]*v2.Resource

	replayPages int
	coldPages   int
	probePages  int

	// failPageOnce injects a one-shot error when serving "gid/page" —
	// simulating a crash while spawned siblings are pending in the
	// checkpointed action stack.
	failPageOnce map[string]bool
	// servedPages counts servePage attempts per "gid/page" (including the
	// failed attempt), so resume tests can see which pages re-executed.
	servedPages map[string]int
}

func newPagedSpawnMockConnector(t *testing.T, groupCount, membersPerGroup int) *pagedSpawnMockConnector {
	t.Helper()
	mc := &pagedSpawnMockConnector{
		mockConnector: newMockConnector(),
		members:       map[string][]string{},
		prevMembers:   map[string][]string{},
		resByID:       map[string]*v2.Resource{},
		failPageOnce:  map[string]bool{},
		servedPages:   map[string]int{},
	}
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	for i := 0; i < 30; i++ {
		uid := fmt.Sprintf("user-%02d", i)
		u, err := rs.NewUserResource(uid, userResourceType, uid, nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
		require.NoError(t, err)
		mc.AddResource(context.Background(), u)
		mc.resByID[uid] = u
	}
	for i := 0; i < groupCount; i++ {
		gid := fmt.Sprintf("group-%02d", i)
		g, err := rs.NewGroupResource(gid, groupResourceType, gid, nil)
		require.NoError(t, err)
		mc.AddResource(context.Background(), g)
		mc.resByID[gid] = g
		mc.groupIDs = append(mc.groupIDs, gid)
		mc.entDB[gid] = []*v2.Entitlement{mkMemberEnt(g)}
		for m := 0; m < membersPerGroup; m++ {
			mc.members[gid] = append(mc.members[gid], fmt.Sprintf("user-%02d", (i*3+m)%30))
		}
	}
	return mc
}

func (mc *pagedSpawnMockConnector) SetSourceCache(_ context.Context, lookup sourcecache.Lookup) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if lookup == nil {
		lookup = sourcecache.NoopLookup{}
	}
	mc.lookup = lookup
}

func (mc *pagedSpawnMockConnector) Validate(context.Context, *v2.ConnectorServiceValidateRequest, ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return v2.ConnectorServiceValidateResponse_builder{
		Annotations: annotations.New(v2.SourceCacheCapability_builder{
			Mode: v2.SourceCacheCapability_MODE_READ_WRITE,
		}.Build()),
	}.Build(), nil
}

func pageScopeFor(gid string, page int) string {
	return fmt.Sprintf("groups/%s/members?page=%d", gid, page)
}

func (mc *pagedSpawnMockConnector) pageRows(gid string, page int) []string {
	lo := page * spawnPageSize
	hi := lo + spawnPageSize
	all := mc.members[gid]
	if lo >= len(all) {
		return nil
	}
	if hi > len(all) {
		hi = len(all)
	}
	return all[lo:hi]
}

func (mc *pagedSpawnMockConnector) grantsFor(gid string, uids []string) []*v2.Grant {
	out := make([]*v2.Grant, 0, len(uids))
	for _, uid := range uids {
		out = append(out, gt.NewGrant(mc.resByID[gid], "member", mc.resByID[uid].GetId()))
	}
	return out
}

// pageEtag is a content-hash-style validator: stable across syncs when the
// page's rows are unchanged, different otherwise. Growth always changes
// the stored tail page's content (rows shift into it), so the last stored
// page misses its lookup and chains — which is how fresh tail pages get
// discovered on warm syncs.
func pageEtag(gid string, page int, rows []string) string {
	return fmt.Sprintf("etag:%s:%d:%v", gid, page, rows)
}

// prevPageCount is the connector's stored-chain knowledge: how many pages
// the previous sync recorded for gid. The test snapshots members →
// prevMembers after each sync, mirroring what a real connector derives
// from its stored validators.
func (mc *pagedSpawnMockConnector) prevPageCount(gid string) int {
	return (len(mc.prevMembers[gid]) + spawnPageSize - 1) / spawnPageSize
}

func (mc *pagedSpawnMockConnector) snapshotPrev() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.prevMembers = map[string][]string{}
	for gid, uids := range mc.members {
		mc.prevMembers[gid] = append([]string(nil), uids...)
	}
}

// servePage answers one numbered page: lookup hit with matching validator →
// replay (no rows); miss/changed → cold rows + scope. A cold page chains to
// page+1 only when page+1 is NOT covered by a spawned sibling (i.e. beyond
// the stored chain) — that's the probe-past-tail path discovering pages the
// previous sync never had.
func (mc *pagedSpawnMockConnector) servePage(ctx context.Context, gid string, page int) (*v2.GrantsServiceListGrantsResponse, error) {
	pageKey := fmt.Sprintf("%s/%d", gid, page)
	mc.mu.Lock()
	mc.servedPages[pageKey]++
	if mc.failPageOnce[pageKey] {
		delete(mc.failPageOnce, pageKey)
		mc.mu.Unlock()
		return nil, fmt.Errorf("injected crash serving %s", pageKey)
	}
	lookup := mc.lookup
	rows := mc.pageRows(gid, page)
	etag := pageEtag(gid, page, rows)
	prevPages := mc.prevPageCount(gid)
	hasNextUpstream := len(mc.pageRows(gid, page+1)) > 0
	mc.mu.Unlock()
	if lookup == nil {
		lookup = sourcecache.NoopLookup{}
	}
	scope := pageScopeFor(gid, page)

	entry, found, err := lookup.LookupPreviousSourceCache(ctx, sourcecache.RowKindGrants, scope)
	if err != nil {
		return nil, err
	}
	if found && entry.ETag == etag {
		mc.mu.Lock()
		mc.replayPages++
		mc.mu.Unlock()
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: annotations.New(v2.SourceCacheReplay_builder{
				ScopeHash: scope,
				Etag:      etag,
			}.Build()),
		}.Build(), nil
	}

	next := ""
	if hasNextUpstream && page+1 >= prevPages {
		// page+1 exists upstream but was never stored, so no sibling was
		// spawned for it — this cold page's chain is its only route in.
		next = fmt.Sprintf("page=%d", page+1)
	}
	mc.mu.Lock()
	mc.coldPages++
	if page >= prevPages && prevPages > 0 {
		mc.probePages++
	}
	mc.mu.Unlock()
	return v2.GrantsServiceListGrantsResponse_builder{
		List: mc.grantsFor(gid, rows),
		Annotations: annotations.New(v2.SourceCacheScope_builder{
			ScopeHash: scope,
			Etag:      etag,
		}.Build()),
		NextPageToken: next,
	}.Build(), nil
}

func (mc *pagedSpawnMockConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	gid := in.GetResource().GetId().GetResource()
	if in.GetResource().GetId().GetResourceType() != groupResourceType.GetId() {
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}

	tok := in.GetPageToken()
	if tok != "" {
		var page int
		if _, err := fmt.Sscanf(tok, "page=%d", &page); err != nil {
			return nil, fmt.Errorf("bad page token %q", tok)
		}
		return mc.servePage(ctx, gid, page)
	}

	// Page one: serve it, then spawn a sibling for every OTHER page of the
	// STORED chain (the pages whose URLs and validators the previous sync
	// recorded). Cold first syncs have no stored chain and just paginate
	// serially; pages beyond the stored tail are discovered by the last
	// stored page missing its lookup and chaining (see servePage).
	resp, err := mc.servePage(ctx, gid, 0)
	if err != nil {
		return nil, err
	}
	mc.mu.Lock()
	prevPages := mc.prevPageCount(gid)
	mc.mu.Unlock()

	var tokens []string
	for p := 1; p < prevPages; p++ {
		tokens = append(tokens, fmt.Sprintf("page=%d", p))
	}
	if len(tokens) > 0 {
		annos := annotations.Annotations(resp.GetAnnotations())
		annos.Update(v2.SpawnCursors_builder{PageTokens: tokens}.Build())
		resp.SetAnnotations(annos)
		// Page one's own chain ends here; siblings carry the rest.
		resp.SetNextPageToken("")
	}
	return resp, nil
}

func mkMemberEnt(group *v2.Resource) *v2.Entitlement {
	ent := v2.Entitlement_builder{
		Id:          fmt.Sprintf("%s:%s:member", group.GetId().GetResourceType(), group.GetId().GetResource()),
		Resource:    group,
		DisplayName: "member",
		Slug:        "member",
		GrantableTo: []*v2.ResourceType{userResourceType},
	}.Build()
	return ent
}

// runSpawnSync runs one sync and returns the grant-coverage counter for
// the group type — the number the "more grant resources than resources"
// anomaly warning compares against the type's resource total. With spawn
// fan-out it must equal the group count exactly (one per resource), not
// one per spawned page.
func runSpawnSync(ctx context.Context, t *testing.T, mc *pagedSpawnMockConnector, path, prevPath, tmpDir string, workers int) int {
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
	if workers > 0 {
		opts = append(opts, WithWorkerCount(workers))
	}
	sc, err := NewSyncer(ctx, mc, opts...)
	require.NoError(t, err)
	require.NoError(t, sc.Sync(ctx))
	impl, ok := sc.(*syncer)
	require.True(t, ok)
	covered := impl.counts.GrantsProgress(groupResourceType.GetId())
	require.NoError(t, sc.Close(ctx))
	return covered
}

func TestPerResourceSpawnCursorsWarmRevalidation(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			ctx, err := logging.Init(context.Background())
			require.NoError(t, err)
			tmpDir := t.TempDir()

			// 6 groups × 11 members = 3 pages each (4/4/3), 66 grants.
			mc := newPagedSpawnMockConnector(t, 6, 11)

			// --- Sync 1: cold — no stored chain, serial pagination. -------
			sync1 := filepath.Join(tmpDir, "sync1.c1z")
			covered := runSpawnSync(ctx, t, mc, sync1, "", tmpDir, workers)
			require.Equal(t, 6, covered, "cold: each group counts once toward grant coverage")
			grants1 := listGrantsInFile(ctx, t, sync1)
			require.Len(t, grants1, 66)
			require.Zero(t, mc.replayPages)
			require.Equal(t, 18, mc.coldPages, "6 groups × 3 pages fetch cold")
			mc.snapshotPrev()

			// --- Sync 2: warm, unchanged — page 1 spawns pages 2..3 per
			// group and EVERY page replays. --------------------------------
			sync2 := filepath.Join(tmpDir, "sync2.c1z")
			covered = runSpawnSync(ctx, t, mc, sync2, sync1, tmpDir, workers)
			require.Equal(t, 6, covered,
				"warm fan-out: a group with 3 spawned/replayed pages still counts ONCE — spawned siblings must not inflate coverage past the resource total")
			require.Equal(t, 18, mc.replayPages, "every page of every group replays")
			require.Equal(t, 18, mc.coldPages, "no cold fetches on the unchanged warm sync")
			requireSameGrantIDs(t, grants1, listGrantsInFile(ctx, t, sync2), "warm vs cold")
			mc.snapshotPrev()

			// --- Sync 3: warm with churn:
			//   group-00 gains 2 members → its stored tail page (page 2)
			//   changes content, misses its lookup, fetches cold, and CHAINS
			//   into a brand-new page 3 no sibling was spawned for;
			//   group-01 loses 3 members → its stored page 2 now returns
			//   zero rows (spawned probe of a vanished tail). --------------
			mc.mu.Lock()
			mc.members["group-00"] = append(mc.members["group-00"], "user-20", "user-21")
			mc.members["group-01"] = mc.members["group-01"][:8] // exactly 2 pages now
			mc.mu.Unlock()

			sync3 := filepath.Join(tmpDir, "sync3.c1z")
			runSpawnSync(ctx, t, mc, sync3, sync2, tmpDir, workers)
			grants3 := listGrantsInFile(ctx, t, sync3)
			require.Positive(t, mc.probePages, "the fresh tail page must be reached by a cold page chaining past the stored horizon")

			// Cold control against the same tenant state.
			control := filepath.Join(tmpDir, "control.c1z")
			runSpawnSync(ctx, t, mc, control, "", tmpDir, workers)
			requireSameGrantIDs(t, listGrantsInFile(ctx, t, control), grants3, "churned warm vs cold control")
		})
	}
}

// TestPerResourceSpawnCursorsResumeWithPendingSiblings pins suspend/resume
// across a spawn fan-out. A warm sync crashes while serving one of a
// group's spawned sibling pages and is then resumed on the same c1z.
// Checkpoints are rate-limited (minCheckpointInterval), so resume
// granularity is at-least-once for everything since the last persisted
// checkpoint — possibly the whole grants phase in a fast sync. The
// invariants are therefore:
//
//   - no DROPS: the crashed sibling and the sibling spawned alongside it
//     both execute by the time the resumed sync finishes;
//   - no DUPES on double-execution: pages that committed before the crash
//     re-run after resume and must be idempotent — replay re-copies, cold
//     re-upserts, and the final file is grant-for-grant identical to a
//     cold control sync.
//
// (The complementary serialization half — a checkpoint can never hold
// "page finished but siblings missing", and the Spawned marker survives
// the round trip — is pinned by TestSpawnedActionsSurviveCheckpoint.)
func TestPerResourceSpawnCursorsResumeWithPendingSiblings(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			ctx, err := logging.Init(context.Background())
			require.NoError(t, err)
			tmpDir := t.TempDir()

			// 6 groups × 11 members = 3 pages each.
			mc := newPagedSpawnMockConnector(t, 6, 11)

			sync1 := filepath.Join(tmpDir, "sync1.c1z")
			runSpawnSync(ctx, t, mc, sync1, "", tmpDir, workers)
			mc.snapshotPrev()

			// Churn group-01 so the warm sync mixes replayed and cold
			// spawned pages across the crash boundary.
			mc.mu.Lock()
			mc.servedPages = map[string]int{} // count only the warm attempts
			mc.members["group-01"] = mc.members["group-01"][:9]
			// Crash while serving group-00's spawned page 1: page 0 (the
			// spawner) has already finished, so pages 1 and 2 are pending
			// Spawned actions in the checkpoint when the sync dies.
			mc.failPageOnce["group-00/1"] = true
			mc.mu.Unlock()

			opts := []SyncOpt{WithTmpDir(tmpDir), WithPreviousSyncC1ZPath(sync1)}
			if workers > 0 {
				opts = append(opts, WithWorkerCount(workers))
			}

			sync2 := filepath.Join(tmpDir, "sync2.c1z")
			crashed, err := NewSyncer(ctx, mc, append(opts, WithC1ZPath(sync2))...)
			require.NoError(t, err)
			err = crashed.Sync(ctx)
			require.ErrorContains(t, err, "injected crash", "the warm sync must die mid-fan-out")
			// Close on a cancelled context so the in-flight sync stays
			// resumable instead of being finalized (the established
			// interrupt pattern from the child-resources resume test).
			cancelledCtx, cancel := context.WithCancel(ctx)
			cancel()
			require.NoError(t, crashed.Close(cancelledCtx))

			resumed, err := NewSyncer(ctx, mc, append(opts, WithC1ZPath(sync2))...)
			require.NoError(t, err)
			require.NoError(t, resumed.Sync(ctx))
			require.NoError(t, resumed.Close(ctx))

			mc.mu.Lock()
			require.GreaterOrEqual(t, mc.servedPages["group-00/1"], 2,
				"the crashed sibling must be re-executed on resume")
			require.GreaterOrEqual(t, mc.servedPages["group-00/2"], 1,
				"the sibling spawned alongside the crashed one must not be dropped")
			mc.mu.Unlock()

			// No drops, no duplicates: cold control equivalence. Note the
			// resumed file also re-executed pages that committed before the
			// crash — this passing IS the double-execution idempotency
			// assertion.
			control := filepath.Join(tmpDir, "control.c1z")
			runSpawnSync(ctx, t, mc, control, "", tmpDir, workers)
			requireSameGrantIDs(t, listGrantsInFile(ctx, t, control), listGrantsInFile(ctx, t, sync2),
				"resumed warm sync vs cold control")
		})
	}
}

// TestSpawnedActionsSurviveCheckpoint pins the state-level half of
// spawn-fan-out resume: the exact sequence syncGrantsForResource performs
// (push siblings, then finish the spawner — one state mutation window, so
// no checkpoint can see "finished but siblings missing") marshals into a
// sync token from which a fresh state pops every sibling with its Spawned
// marker and resource identity intact. If the marker were dropped in the
// round trip, a resumed sync would double-count grant coverage for every
// pending sibling.
func TestSpawnedActionsSurviveCheckpoint(t *testing.T) {
	ctx := context.Background()
	st := newState()
	require.NoError(t, st.Unmarshal(""))

	// Drain the implicit init action and stand up a grants action for one
	// resource, as the syncer would.
	st.FinishAction(ctx, st.Current())
	st.PushAction(ctx, Action{Op: SyncGrantsOp, ResourceTypeID: "group", ResourceID: "group-00"})
	spawner := st.Current()

	// nextPageOrFinishAction order: push spawned siblings, then finish the
	// spawning action.
	for _, tok := range []string{"page=1", "page=2"} {
		st.PushAction(ctx, Action{
			Op:             SyncGrantsOp,
			ResourceTypeID: "group",
			ResourceID:     "group-00",
			PageToken:      tok,
			Spawned:        true,
		})
	}
	st.FinishAction(ctx, spawner)

	token, err := st.Marshal()
	require.NoError(t, err)

	resumed := newState()
	require.NoError(t, resumed.Unmarshal(token))

	var siblings []Action
	for resumed.Current() != nil {
		a := *resumed.Current()
		siblings = append(siblings, a)
		resumed.FinishAction(ctx, &a)
	}
	require.Len(t, siblings, 2, "both pending siblings must survive the checkpoint round trip")
	tokens := map[string]bool{}
	for _, a := range siblings {
		require.True(t, a.Spawned, "the Spawned marker must survive serialization (progress accounting depends on it)")
		require.Equal(t, "group-00", a.ResourceID, "the resource identity rides the action across resume")
		require.Equal(t, SyncGrantsOp, a.Op)
		tokens[a.PageToken] = true
	}
	require.Equal(t, map[string]bool{"page=1": true, "page=2": true}, tokens)
}

// TestPerResourceSpawnCursorsChurnSoak is the randomized soak (the shape
// that caught real bugs in both the Entra and GitHub POCs): seeded rounds
// of membership churn against the paged-spawn connector, each round
// running a warm chained sync at workers=4 and asserting reader-surface
// equivalence against a cold control sync of the same tenant state. Churn
// shifts page boundaries arbitrarily, so every round mixes replayed pages,
// cold refetches of shifted pages, vanished tails (zero-row probes), and
// fresh tails discovered by chaining — concurrently.
func TestPerResourceSpawnCursorsChurnSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test")
	}
	for _, seed := range []int64{1, 7} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			ctx, err := logging.Init(context.Background())
			require.NoError(t, err)
			tmpDir := t.TempDir()
			rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test seed

			mc := newPagedSpawnMockConnector(t, 8, 9)

			prev := filepath.Join(tmpDir, "sync0.c1z")
			runSpawnSync(ctx, t, mc, prev, "", tmpDir, 4)
			mc.snapshotPrev()

			for round := 1; round <= 6; round++ {
				// Churn: random adds/removes across random groups. Removes
				// from the middle shift every following page of the group;
				// adds can grow a fresh tail page.
				mc.mu.Lock()
				for i := 0; i < 1+rng.Intn(4); i++ {
					gid := mc.groupIDs[rng.Intn(len(mc.groupIDs))]
					cur := mc.members[gid]
					if rng.Intn(2) == 0 && len(cur) > 1 {
						k := rng.Intn(len(cur))
						mc.members[gid] = append(append([]string(nil), cur[:k]...), cur[k+1:]...)
					} else {
						uid := fmt.Sprintf("user-%02d", rng.Intn(30))
						if !slices.Contains(cur, uid) {
							mc.members[gid] = append(cur, uid)
						}
					}
				}
				mc.mu.Unlock()

				warm := filepath.Join(tmpDir, fmt.Sprintf("warm-%d.c1z", round))
				runSpawnSync(ctx, t, mc, warm, prev, tmpDir, 4)
				warmGrants := listGrantsInFile(ctx, t, warm)

				control := filepath.Join(tmpDir, fmt.Sprintf("control-%d.c1z", round))
				runSpawnSync(ctx, t, mc, control, "", tmpDir, 4)
				requireSameGrantIDs(t, listGrantsInFile(ctx, t, control), warmGrants,
					fmt.Sprintf("round %d: warm chain vs cold control", round))

				mc.snapshotPrev()
				prev = warm
			}
			require.Positive(t, mc.replayPages, "soak must actually exercise replay")
			require.Positive(t, mc.coldPages, "soak must actually exercise cold refetch")
		})
	}
}
