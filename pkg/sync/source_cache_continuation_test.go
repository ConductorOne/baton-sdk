package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Syncer-side tests for the source-cache lookup continuation (ask/answer).
// The mocks here play the LAMBDA side of the protocol by hand: they do NOT
// implement sourcecache.SourceCacheSetter (no direct lookup install — exactly the
// single-shot-transport topology), and instead answer list RPCs with
// SourceCacheLookupAsk and consume SourceCacheLookupAnswers from the
// re-invoked request.

import (
	"fmt"
	"path/filepath"
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
	"github.com/conductorone/baton-sdk/pkg/types"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"

	"context"
)

func runContinuationSync(ctx context.Context, t *testing.T, mc types.ConnectorClient, path, prevPath, tmpDir string, workers int) error {
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
	syncer, err := NewSyncer(ctx, mc, opts...)
	require.NoError(t, err)
	syncErr := syncer.Sync(ctx)
	if syncErr != nil {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_ = syncer.Close(cctx)
		return syncErr
	}
	require.NoError(t, syncer.Close(ctx))
	return nil
}

// continuationMockConnector: one grants page per group, one scope per
// group. Lambda-side protocol by hand; misuse modes for the loud-failure
// paths.
type continuationMockConnector struct {
	*mockConnector

	mu            sync.Mutex
	etagByGroup   map[string]string
	grantsByGroup map[string][]*v2.Grant

	askAlways    bool // never satisfied: bounce-cap test
	askWithRows  bool // protocol violation: ask + rows
	askNoOffer   bool // protocol violation: ask on offerless request
	askWithSpawn bool // protocol violation: ask + EnqueuePageTokens

	asks        int
	replayPages int
	coldPages   int
	offerSeen   bool
}

func (mc *continuationMockConnector) Validate(context.Context, *v2.ConnectorServiceValidateRequest, ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return v2.ConnectorServiceValidateResponse_builder{
		Annotations: annotations.New(v2.SourceCacheCapability_builder{
			Mode: v2.SourceCacheCapability_MODE_READ_WRITE,
		}.Build()),
	}.Build(), nil
}

func groupScope(gid string) string { return "groups/" + gid + "/members" }

func (mc *continuationMockConnector) askResponse(scope string, rows []*v2.Grant) *v2.GrantsServiceListGrantsResponse {
	mc.mu.Lock()
	mc.asks++
	mc.mu.Unlock()
	return v2.GrantsServiceListGrantsResponse_builder{
		List: rows,
		Annotations: annotations.New(sourcecache.AskProto([]sourcecache.Query{
			{RowKind: sourcecache.RowKindGrants, ScopeKey: scope},
		})),
	}.Build()
}

func (mc *continuationMockConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	gid := in.GetResource().GetId().GetResource()
	if in.GetResource().GetId().GetResourceType() != groupResourceType.GetId() {
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	scope := groupScope(gid)
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	hasOffer := reqAnnos.Contains(&v2.SourceCacheLookupOffer{})
	answersMsg := &v2.SourceCacheLookupAnswers{}
	hasAnswers, err := reqAnnos.Pick(answersMsg)
	if err != nil {
		return nil, err
	}
	mc.mu.Lock()
	if hasOffer {
		mc.offerSeen = true
	}
	etag := mc.etagByGroup[gid]
	rows := mc.grantsByGroup[gid]
	mc.mu.Unlock()

	if mc.askAlways {
		return mc.askResponse(scope, nil), nil
	}
	if mc.askNoOffer && !hasAnswers {
		return mc.askResponse(scope, nil), nil
	}
	if mc.askWithRows {
		return mc.askResponse(scope, rows), nil
	}
	if mc.askWithSpawn && !hasAnswers && hasOffer {
		resp := mc.askResponse(scope, nil)
		annos := annotations.Annotations(resp.GetAnnotations())
		annos.Update(v2.EnqueuePageTokens_builder{PageTokens: []string{"bogus"}}.Build())
		resp.SetAnnotations(annos)
		return resp, nil
	}

	if hasAnswers {
		answers, answersErr := sourcecache.AnswersFromProto(answersMsg)
		if answersErr != nil {
			return nil, answersErr
		}
		for _, a := range answers {
			if a.ScopeKey != scope || a.RowKind != sourcecache.RowKindGrants {
				continue
			}
			if a.Found && a.CacheValidator == etag {
				mc.mu.Lock()
				mc.replayPages++
				mc.mu.Unlock()
				return v2.GrantsServiceListGrantsResponse_builder{
					Annotations: annotations.New(v2.SourceCacheReplay_builder{
						ScopeKey:       scope,
						CacheValidator: etag,
					}.Build()),
				}.Build(), nil
			}
			break // answered but stale/missing: cold below
		}
	} else if hasOffer {
		// Lookup-before-fetch, deferred: phase 1 does no upstream work.
		return mc.askResponse(scope, nil), nil
	}

	mc.mu.Lock()
	mc.coldPages++
	mc.mu.Unlock()
	return v2.GrantsServiceListGrantsResponse_builder{
		List: rows,
		Annotations: annotations.New(v2.SourceCacheRecord_builder{
			ScopeKey:       scope,
			CacheValidator: etag,
		}.Build()),
	}.Build(), nil
}

func newContinuationMockConnector(t *testing.T, groups int) *continuationMockConnector {
	t.Helper()
	ctx := context.Background()
	mc := &continuationMockConnector{
		mockConnector: newMockConnector(),
		etagByGroup:   map[string]string{},
		grantsByGroup: map[string][]*v2.Grant{},
	}
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
	for i := 0; i < groups*2; i++ {
		uid := fmt.Sprintf("user-%02d", i)
		u, err := rs.NewUserResource(uid, userResourceType, uid, nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
		require.NoError(t, err)
		mc.AddResource(ctx, u)
	}
	for i := 0; i < groups; i++ {
		gid := fmt.Sprintf("group-%02d", i)
		g, err := rs.NewGroupResource(gid, groupResourceType, gid, nil)
		require.NoError(t, err)
		mc.AddResource(ctx, g)
		mc.entDB[gid] = []*v2.Entitlement{mkMemberEnt(g)}
		mc.etagByGroup[gid] = "etag-v1"
		for m := 0; m < 2; m++ {
			uid := fmt.Sprintf("user-%02d", (i*2+m)%(groups*2))
			ur, err := rs.NewUserResource(uid, userResourceType, uid, nil)
			require.NoError(t, err)
			mc.grantsByGroup[gid] = append(mc.grantsByGroup[gid], gt.NewGrant(g, "member", ur.GetId()))
		}
	}
	return mc
}

// TestSourceCacheContinuation_WarmReplay is the whole protocol, end to
// end, against the real syncer loop: cold sync (no offer → no asks), warm
// sync (offer → one ask per group → answers → replay), and a third sync
// chained off the replay-only file (manifest carried forward through
// ask-path pages).
func TestSourceCacheContinuation_WarmReplay(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			ctx, err := logging.Init(context.Background())
			require.NoError(t, err)
			tmpDir := t.TempDir()
			mc := newContinuationMockConnector(t, 5)

			sync1 := filepath.Join(tmpDir, "sync1.c1z")
			require.NoError(t, runContinuationSync(ctx, t, mc, sync1, "", tmpDir, workers))
			require.False(t, mc.offerSeen, "cold sync (no previous c1z) must not offer lookups")
			require.Zero(t, mc.asks, "no offer → a compliant connector never asks")
			require.Equal(t, 5, mc.coldPages)
			grants1 := listGrantsInFile(ctx, t, sync1)
			require.Len(t, grants1, 10)

			sync2 := filepath.Join(tmpDir, "sync2.c1z")
			require.NoError(t, runContinuationSync(ctx, t, mc, sync2, sync1, tmpDir, workers))
			require.True(t, mc.offerSeen)
			require.Equal(t, 5, mc.asks, "one ask (bounce) per group page")
			require.Equal(t, 5, mc.replayPages, "every page replays after its answer")
			require.Equal(t, 5, mc.coldPages, "no cold fetches on the warm sync")
			requireSameGrantIDs(t, grants1, listGrantsInFile(ctx, t, sync2), "warm-via-continuation vs cold")

			// Replay-only file remains a valid replay source.
			sync3 := filepath.Join(tmpDir, "sync3.c1z")
			require.NoError(t, runContinuationSync(ctx, t, mc, sync3, sync2, tmpDir, workers))
			require.Equal(t, 10, mc.replayPages)
			requireSameGrantIDs(t, grants1, listGrantsInFile(ctx, t, sync3), "chained continuation sync")

			// Churn one group: its answer goes stale → cold refetch.
			mc.mu.Lock()
			g0 := mc.grantsByGroup["group-00"]
			mc.grantsByGroup["group-00"] = g0[:1]
			mc.etagByGroup["group-00"] = "etag-v2"
			mc.mu.Unlock()
			sync4 := filepath.Join(tmpDir, "sync4.c1z")
			require.NoError(t, runContinuationSync(ctx, t, mc, sync4, sync3, tmpDir, workers))
			require.Equal(t, 6, mc.coldPages, "stale answer → exactly one cold refetch")
			grants4 := listGrantsInFile(ctx, t, sync4)
			require.Len(t, grants4, 9)
			require.NotContains(t, grants4, g0[1].GetId())
		})
	}
}

// verdictSpawnMock is the planner shape both consumer POCs asked for: the
// ORIGIN call batch-asks for every stored page's validator in ONE bounce
// (LookupMany semantics), then its phase-2 response serves page 0 and
// spawns sibling cursors whose tokens CARRY the verdicts. Siblings serve
// replay/cold from their token alone — zero asks, zero bounces. This pins
// the two contract commitments from the review briefs: the ask/answer loop
// composes with a phase-2 EnqueuePageTokens response, and a validator resolved
// in an EARLIER action of the same sync legally rides page tokens.
type verdictSpawnMock struct {
	*mockConnector

	mu        sync.Mutex
	members   map[string][]string // 2 rows per page
	prevPages map[string]int
	resByID   map[string]*v2.Resource

	originAsks  int
	siblingAsks int
	replays     int
	colds       int
}

func (mc *verdictSpawnMock) Validate(context.Context, *v2.ConnectorServiceValidateRequest, ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return v2.ConnectorServiceValidateResponse_builder{
		Annotations: annotations.New(v2.SourceCacheCapability_builder{
			Mode: v2.SourceCacheCapability_MODE_READ_WRITE,
		}.Build()),
	}.Build(), nil
}

func vsPageScope(gid string, page int) string { return fmt.Sprintf("g/%s/members?page=%d", gid, page) }

func (mc *verdictSpawnMock) pageRows(gid string, page int) []string {
	all := mc.members[gid]
	lo, hi := page*2, page*2+2
	if lo >= len(all) {
		return nil
	}
	if hi > len(all) {
		hi = len(all)
	}
	return all[lo:hi]
}

func vsPageEtag(gid string, page int, rows []string) string {
	return fmt.Sprintf("v|%s|%d|%v", gid, page, rows)
}

func (mc *verdictSpawnMock) servePage(gid string, page int, chain bool) *v2.GrantsServiceListGrantsResponse {
	rows := mc.pageRows(gid, page)
	grants := make([]*v2.Grant, 0, len(rows))
	g := mc.resByID[gid]
	for _, uid := range rows {
		grants = append(grants, gt.NewGrant(g, "member", mc.resByID[uid].GetId()))
	}
	next := ""
	if chain && len(mc.pageRows(gid, page+1)) > 0 {
		next = fmt.Sprintf("page=%d", page+1)
	}
	mc.colds++
	return v2.GrantsServiceListGrantsResponse_builder{
		List: grants,
		Annotations: annotations.New(v2.SourceCacheRecord_builder{
			ScopeKey:       vsPageScope(gid, page),
			CacheValidator: vsPageEtag(gid, page, rows),
		}.Build()),
		NextPageToken: next,
	}.Build()
}

func (mc *verdictSpawnMock) ListGrants(_ context.Context, in *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	gid := in.GetResource().GetId().GetResource()
	if in.GetResource().GetId().GetResourceType() != groupResourceType.GetId() {
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if tok := in.GetPageToken(); tok != "" {
		// Sibling with a verdict-carrying token: "page=N|1|<etag>".
		var page int
		var found int
		var etag string
		if n, _ := fmt.Sscanf(tok, "page=%d|%d|", &page, &found); n == 2 {
			if i := strings.Index(tok, "|"); i >= 0 {
				if j := strings.Index(tok[i+1:], "|"); j >= 0 {
					etag = tok[i+1+j+1:]
				}
			}
			rows := mc.pageRows(gid, page)
			if found == 1 && etag == vsPageEtag(gid, page, rows) {
				mc.replays++
				return v2.GrantsServiceListGrantsResponse_builder{
					Annotations: annotations.New(v2.SourceCacheReplay_builder{
						ScopeKey:       vsPageScope(gid, page),
						CacheValidator: etag,
					}.Build()),
				}.Build(), nil
			}
			// Stale/missing verdict: cold, still no ask.
			return mc.servePage(gid, page, false), nil
		}
		// Plain cold chain token: "page=N".
		if _, err := fmt.Sscanf(tok, "page=%d", &page); err != nil {
			return nil, fmt.Errorf("bad token %q", tok)
		}
		return mc.servePage(gid, page, true), nil
	}

	// Origin call.
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	answersMsg := &v2.SourceCacheLookupAnswers{}
	hasAnswers, err := reqAnnos.Pick(answersMsg)
	if err != nil {
		return nil, err
	}
	prev := mc.prevPages[gid]

	if !hasAnswers {
		if reqAnnos.Contains(&v2.SourceCacheLookupOffer{}) && prev > 0 {
			// ONE batch ask for the whole stored chain.
			mc.originAsks++
			queries := make([]sourcecache.Query, 0, prev)
			for p := 0; p < prev; p++ {
				queries = append(queries, sourcecache.Query{RowKind: sourcecache.RowKindGrants, ScopeKey: vsPageScope(gid, p)})
			}
			return v2.GrantsServiceListGrantsResponse_builder{
				Annotations: annotations.New(sourcecache.AskProto(queries)),
			}.Build(), nil
		}
		// Cold first sync: serial chain.
		return mc.servePage(gid, 0, true), nil
	}

	// Phase 2: serve page 0 per its answer, spawn verdict-token siblings.
	byScope := map[string]sourcecache.Answer{}
	answers, answersErr := sourcecache.AnswersFromProto(answersMsg)
	if answersErr != nil {
		return nil, answersErr
	}
	for _, a := range answers {
		byScope[a.ScopeKey] = a
	}
	var resp *v2.GrantsServiceListGrantsResponse
	if a, ok := byScope[vsPageScope(gid, 0)]; ok && a.Found && a.CacheValidator == vsPageEtag(gid, 0, mc.pageRows(gid, 0)) {
		mc.replays++
		resp = v2.GrantsServiceListGrantsResponse_builder{
			Annotations: annotations.New(v2.SourceCacheReplay_builder{
				ScopeKey:       vsPageScope(gid, 0),
				CacheValidator: a.CacheValidator,
			}.Build()),
		}.Build()
	} else {
		resp = mc.servePage(gid, 0, false)
	}
	var tokens []string
	for p := 1; p < prev; p++ {
		a := byScope[vsPageScope(gid, p)]
		found := 0
		if a.Found {
			found = 1
		}
		tokens = append(tokens, fmt.Sprintf("page=%d|%d|%s", p, found, a.CacheValidator))
	}
	if len(tokens) > 0 {
		annos := annotations.Annotations(resp.GetAnnotations())
		annos.Update(v2.EnqueuePageTokens_builder{PageTokens: tokens}.Build())
		resp.SetAnnotations(annos)
		resp.SetNextPageToken("")
	}
	return resp, nil
}

func TestSourceCacheContinuation_Phase2EnqueuePageTokens(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			ctx, err := logging.Init(context.Background())
			require.NoError(t, err)
			tmpDir := t.TempDir()

			mc := &verdictSpawnMock{
				mockConnector: newMockConnector(),
				members:       map[string][]string{},
				prevPages:     map[string]int{},
				resByID:       map[string]*v2.Resource{},
			}
			mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
			for i := 0; i < 8; i++ {
				uid := fmt.Sprintf("user-%02d", i)
				u, err := rs.NewUserResource(uid, userResourceType, uid, nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
				require.NoError(t, err)
				mc.AddResource(ctx, u)
				mc.resByID[uid] = u
			}
			for i := 0; i < 2; i++ {
				gid := fmt.Sprintf("group-%02d", i)
				g, err := rs.NewGroupResource(gid, groupResourceType, gid, nil)
				require.NoError(t, err)
				mc.AddResource(ctx, g)
				mc.resByID[gid] = g
				mc.entDB[gid] = []*v2.Entitlement{mkMemberEnt(g)}
				for m := 0; m < 4; m++ {
					mc.members[gid] = append(mc.members[gid], fmt.Sprintf("user-%02d", (i*4+m)%8))
				}
			}

			sync1 := filepath.Join(tmpDir, "sync1.c1z")
			require.NoError(t, runContinuationSync(ctx, t, mc, sync1, "", tmpDir, workers))
			require.Zero(t, mc.originAsks)
			require.Equal(t, 4, mc.colds, "cold: 2 groups × 2 chained pages")
			grants1 := listGrantsInFile(ctx, t, sync1)
			require.Len(t, grants1, 8)
			mc.mu.Lock()
			for gid := range mc.members {
				mc.prevPages[gid] = 2
			}
			mc.mu.Unlock()

			sync2 := filepath.Join(tmpDir, "sync2.c1z")
			require.NoError(t, runContinuationSync(ctx, t, mc, sync2, sync1, tmpDir, workers))
			require.Equal(t, 2, mc.originAsks, "ONE batch ask per group's origin action")
			require.Zero(t, mc.siblingAsks, "verdict-carrying siblings never ask")
			require.Equal(t, 4, mc.replays, "page 0 (phase 2) + spawned page 1, per group")
			require.Equal(t, 4, mc.colds, "no cold fetches on the warm sync")
			requireSameGrantIDs(t, grants1, listGrantsInFile(ctx, t, sync2), "planner-batch warm vs cold")

			// Churn group-01's tail page: its verdict goes stale, the
			// sibling refetches cold from the token alone — still no ask.
			mc.mu.Lock()
			mc.members["group-01"][3] = "user-00"
			mc.mu.Unlock()
			sync3 := filepath.Join(tmpDir, "sync3.c1z")
			require.NoError(t, runContinuationSync(ctx, t, mc, sync3, sync2, tmpDir, workers))
			require.Zero(t, mc.siblingAsks)

			control := filepath.Join(tmpDir, "control.c1z")
			require.NoError(t, runContinuationSync(ctx, t, mc, control, "", tmpDir, workers))
			requireSameGrantIDs(t, listGrantsInFile(ctx, t, control), listGrantsInFile(ctx, t, sync3), "churned warm vs cold control")
		})
	}
}

func TestSourceCacheContinuation_BounceCapFails(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()
	mc := newContinuationMockConnector(t, 1)

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	require.NoError(t, runContinuationSync(ctx, t, mc, sync1, "", tmpDir, 0))

	mc.askAlways = true
	err = runContinuationSync(ctx, t, mc, filepath.Join(tmpDir, "sync2.c1z"), sync1, tmpDir, 0)
	require.Error(t, err)
	require.True(t,
		strings.Contains(err.Error(), "bounce cap") || strings.Contains(err.Error(), "re-asked only already-answered"),
		"a connector that never stops asking on ONE request must fail loudly, got: %v", err)
}

// multiPagePlannerMock is the Entra planner shape: a TYPE-SCOPED grants
// action whose planning walk spans SIX pages, each of which asks once
// (LookupMany over the chunk scopes it is about to spawn) before serving.
// Six asks exceed the per-request bounce cap of four — the sync must still
// complete, because the cap is per REQUEST (same page token) and every
// NextPageToken advance is a fresh request. The final planning page spawns
// the real cursor; it must spawn exactly once (the ask response never
// reaches EnqueuePageTokens processing; the re-invoke serves the same page).
type multiPagePlannerMock struct {
	*mockConnector

	mu             sync.Mutex
	groupIDs       []string
	resByID        map[string]*v2.Resource
	asks           int
	cursorServes   int
	spawns         int
	markerMissing  bool // TypeScopedGrants marker absent on any request
	answersScope   string
	answersPresent int // planning pages served with answers attached
}

func (mc *multiPagePlannerMock) Validate(context.Context, *v2.ConnectorServiceValidateRequest, ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return v2.ConnectorServiceValidateResponse_builder{
		Annotations: annotations.New(v2.SourceCacheCapability_builder{
			Mode: v2.SourceCacheCapability_MODE_READ_WRITE,
		}.Build()),
	}.Build(), nil
}

const plannerPages = 6

func (mc *multiPagePlannerMock) ListGrants(_ context.Context, in *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	if in.GetResource().GetId().GetResourceType() != typeScopedGroupRT.GetId() {
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if !reqAnnos.Contains(&v2.TypeScopedGrants{}) {
		mc.markerMissing = true
	}

	tok := in.GetPageToken()
	if tok == "cursor" {
		mc.cursorServes++
		grants := make([]*v2.Grant, 0, len(mc.groupIDs))
		for _, gid := range mc.groupIDs {
			grants = append(grants, gt.NewGrant(mc.resByID[gid], "member", mc.resByID["user-00"].GetId()))
		}
		return v2.GrantsServiceListGrantsResponse_builder{List: grants}.Build(), nil
	}

	page := 0
	if tok != "" {
		if _, err := fmt.Sscanf(tok, "plan=%d", &page); err != nil {
			return nil, fmt.Errorf("bad token %q", tok)
		}
	}
	scope := fmt.Sprintf("chunks/page-%d", page)

	answersMsg := &v2.SourceCacheLookupAnswers{}
	hasAnswers, err := reqAnnos.Pick(answersMsg)
	if err != nil {
		return nil, err
	}
	if !hasAnswers && reqAnnos.Contains(&v2.SourceCacheLookupOffer{}) {
		mc.asks++
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: annotations.New(sourcecache.AskProto([]sourcecache.Query{
				{RowKind: sourcecache.RowKindGrants, ScopeKey: scope},
			})),
		}.Build(), nil
	}
	if hasAnswers {
		mc.answersPresent++
		answers, answersErr := sourcecache.AnswersFromProto(answersMsg)
		if answersErr != nil {
			return nil, answersErr
		}
		for _, a := range answers {
			if a.ScopeKey == scope {
				mc.answersScope = scope
			}
		}
	}

	// Serve the planning page: no rows; chain or spawn on the last page.
	if page == plannerPages-1 {
		mc.spawns++
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: annotations.New(v2.EnqueuePageTokens_builder{PageTokens: []string{"cursor"}}.Build()),
		}.Build(), nil
	}
	return v2.GrantsServiceListGrantsResponse_builder{
		NextPageToken: fmt.Sprintf("plan=%d", page+1),
	}.Build(), nil
}

func TestSourceCacheContinuation_MultiPagePlannerBouncesPerRequest(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	mc := &multiPagePlannerMock{
		mockConnector: newMockConnector(),
		resByID:       map[string]*v2.Resource{},
	}
	mc.rtDB = append(mc.rtDB, typeScopedGroupRT, userResourceType)
	u, err := rs.NewUserResource("user-00", userResourceType, "user-00", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	mc.AddResource(ctx, u)
	mc.resByID["user-00"] = u
	for i := 0; i < 2; i++ {
		gid := fmt.Sprintf("group-%02d", i)
		g, err := rs.NewGroupResource(gid, typeScopedGroupRT, gid, nil)
		require.NoError(t, err)
		mc.AddResource(ctx, g)
		mc.resByID[gid] = g
		mc.groupIDs = append(mc.groupIDs, gid)
		mc.entDB[gid] = []*v2.Entitlement{mkMemberEnt(g)}
	}

	// Cold: no offer, no asks; the planner walks its pages and spawns.
	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	require.NoError(t, runContinuationSync(ctx, t, mc, sync1, "", tmpDir, 0))
	require.Zero(t, mc.asks)
	require.Equal(t, 1, mc.spawns)
	require.Equal(t, 1, mc.cursorServes)
	grants1 := listGrantsInFile(ctx, t, sync1)
	require.Len(t, grants1, 2)

	// Warm: every planning page asks once — 6 bounces in ONE action,
	// exceeding the per-request cap of 4. Must complete: each page-token
	// advance is a fresh request.
	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	require.NoError(t, runContinuationSync(ctx, t, mc, sync2, sync1, tmpDir, 0),
		"a multi-page planner asking once per page must not trip the per-request bounce cap")
	require.Equal(t, plannerPages, mc.asks, "one ask per planning page")
	require.Equal(t, plannerPages, mc.answersPresent, "every re-invoked planning page carries answers")
	require.NotEmpty(t, mc.answersScope, "answers must cover the asked scope")
	require.False(t, mc.markerMissing, "TypeScopedGrants routing marker must survive on every request, including re-invokes")
	require.Equal(t, 2, mc.spawns, "the asking planner page spawns exactly once per sync")
	require.Equal(t, 2, mc.cursorServes, "spawned cursor executes exactly once per sync")
	requireSameGrantIDs(t, grants1, listGrantsInFile(ctx, t, sync2), "warm planner sync vs cold")
}

// fatEtagLookup finds every scope with the same (large) etag.
type fatEtagLookup struct{ etag string }

func (f fatEtagLookup) Lookup(context.Context, sourcecache.RowKind, string) (sourcecache.Entry, bool, error) {
	return sourcecache.Entry{CacheValidator: f.etag}, true, nil
}

// TestSourceCacheContinuation_AnswerBudgetDegradesToCold pins the answer
// size budget's failure mode: one ask whose FOUND etags total more than
// sourceCacheAnswerBudget. Answers accumulate across bounces and the
// budget only shrinks, so an over-budget answer left absent could never
// be resolved by a re-ask — it must instead be degraded to an explicit
// not-found (the scope goes cold) and the request must complete in one
// answered turn, not livelock into the bounce cap.
func TestSourceCacheContinuation_AnswerBudgetDegradesToCold(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)

	const queryCount = 40
	etag := strings.Repeat("e", 65536) // the wire cap for one etag
	// Mirror the budget arithmetic: every answered query spends its
	// identity bytes (row_kind + scope_key + framing overhead) whether
	// found or degraded; a found answer additionally needs its etag to
	// fit the remaining budget.
	identityCost := len(sourcecache.RowKindGrants) + len("scope-000") + sourceCacheAnswerOverheadBytes
	fitting := 0
	for i, budget := 0, sourceCacheAnswerBudget; i < queryCount; i++ {
		budget -= identityCost
		if len(etag) <= budget {
			budget -= len(etag)
			fitting++
		}
	}
	require.Positive(t, fitting, "test must fit some answers")
	require.Less(t, fitting, queryCount, "test must overflow the budget")

	s := &syncer{
		state: newState(),
		sourceCache: syncerSourceCache{
			prev:   struct{ dotc1z.SourceCacheStore }{}, // non-nil: warm
			lookup: fatEtagLookup{etag: etag},
		},
	}

	queries := make([]sourcecache.Query, 0, queryCount)
	for i := 0; i < queryCount; i++ {
		queries = append(queries, sourcecache.Query{RowKind: sourcecache.RowKindGrants, ScopeKey: fmt.Sprintf("scope-%03d", i)})
	}

	turns := 0
	err = s.withSourceCacheContinuation(ctx, "grants", func(extra annotations.Annotations, attempt int) (listAttempt, error) {
		turns++
		if attempt == 0 {
			return listAttempt{annos: annotations.New(sourcecache.AskProto(queries))}, nil
		}
		answersMsg := &v2.SourceCacheLookupAnswers{}
		hasAnswers, pickErr := extra.Pick(answersMsg)
		require.NoError(t, pickErr)
		require.True(t, hasAnswers)
		answers, convErr := sourcecache.AnswersFromProto(answersMsg)
		require.NoError(t, convErr)
		require.Len(t, answers, queryCount, "every asked query must be answered; an absent answer can never resolve later")
		found := 0
		for _, a := range answers {
			if a.Found {
				found++
			}
		}
		require.Equal(t, fitting, found, "found answers up to the budget")
		return listAttempt{}, nil
	})
	require.NoError(t, err, "an over-budget answer set must degrade to cold, not die at the bounce cap")
	require.Equal(t, 2, turns, "one ask turn + one answered turn")

	stats := s.state.SourceCacheStatsSnapshot()
	require.NotNil(t, stats)
	require.Equal(t, int64(fitting), stats.LookupAnsweredFound)
	require.Equal(t, int64(queryCount-fitting), stats.LookupAnswersTruncated)
	require.Zero(t, stats.LookupAnsweredNotFound, "budget degradation counts as truncated, not as a genuine miss")
}

func TestSourceCacheContinuation_AskWithEnqueuePageTokensFails(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()
	mc := newContinuationMockConnector(t, 1)

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	require.NoError(t, runContinuationSync(ctx, t, mc, sync1, "", tmpDir, 0))

	mc.askWithSpawn = true
	err = runContinuationSync(ctx, t, mc, filepath.Join(tmpDir, "sync2.c1z"), sync1, tmpDir, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no spawned cursors",
		"EnqueuePageTokens alongside an ask is a connector bug and must fail loudly")
}

func TestSourceCacheContinuation_AskWithRowsFails(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()
	mc := newContinuationMockConnector(t, 1)

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	require.NoError(t, runContinuationSync(ctx, t, mc, sync1, "", tmpDir, 0))

	mc.askWithRows = true
	err = runContinuationSync(ctx, t, mc, filepath.Join(tmpDir, "sync2.c1z"), sync1, tmpDir, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must carry ONLY the ask")
}

func TestSourceCacheContinuation_AskWithoutOfferFails(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()
	mc := newContinuationMockConnector(t, 1)
	mc.askNoOffer = true

	// Cold sync: no previous c1z → no offer → the ask is a protocol
	// violation and must fail, not be misread as an empty page.
	err = runContinuationSync(ctx, t, mc, filepath.Join(tmpDir, "sync1.c1z"), "", tmpDir, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no offer")
}

// multiPagePlannerEntitlementsMock mirrors multiPagePlannerMock for
// TypeScopedEntitlements: a multi-page planner that asks once per page
// before spawning a real cursor, asserting the routing marker survives
// every re-invocation.
type multiPagePlannerEntitlementsMock struct {
	*mockConnector

	mu             sync.Mutex
	groupIDs       []string
	resByID        map[string]*v2.Resource
	asks           int
	cursorServes   int
	spawns         int
	markerMissing  bool
	answersScope   string
	answersPresent int
}

func (mc *multiPagePlannerEntitlementsMock) Validate(context.Context, *v2.ConnectorServiceValidateRequest, ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return v2.ConnectorServiceValidateResponse_builder{
		Annotations: annotations.New(v2.SourceCacheCapability_builder{
			Mode: v2.SourceCacheCapability_MODE_READ_WRITE,
		}.Build()),
	}.Build(), nil
}

func (mc *multiPagePlannerEntitlementsMock) ListEntitlements(
	_ context.Context, in *v2.EntitlementsServiceListEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	if in.GetResource().GetId().GetResourceType() != typeScopedEntGroupRT.GetId() {
		return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
	}
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if !reqAnnos.Contains(&v2.TypeScopedEntitlements{}) {
		mc.markerMissing = true
	}

	tok := in.GetPageToken()
	if tok == "cursor" {
		mc.cursorServes++
		ents := make([]*v2.Entitlement, 0, len(mc.groupIDs))
		for _, gid := range mc.groupIDs {
			ents = append(ents, et.NewAssignmentEntitlement(mc.resByID[gid], "member", et.WithGrantableTo(userResourceType)))
		}
		return v2.EntitlementsServiceListEntitlementsResponse_builder{List: ents}.Build(), nil
	}

	page := 0
	if tok != "" {
		if _, err := fmt.Sscanf(tok, "plan=%d", &page); err != nil {
			return nil, fmt.Errorf("bad token %q", tok)
		}
	}
	scope := fmt.Sprintf("ent-chunks/page-%d", page)

	answersMsg := &v2.SourceCacheLookupAnswers{}
	hasAnswers, err := reqAnnos.Pick(answersMsg)
	if err != nil {
		return nil, err
	}
	if !hasAnswers && reqAnnos.Contains(&v2.SourceCacheLookupOffer{}) {
		mc.asks++
		return v2.EntitlementsServiceListEntitlementsResponse_builder{
			Annotations: annotations.New(sourcecache.AskProto([]sourcecache.Query{
				{RowKind: sourcecache.RowKindEntitlements, ScopeKey: scope},
			})),
		}.Build(), nil
	}
	if hasAnswers {
		mc.answersPresent++
		answers, answersErr := sourcecache.AnswersFromProto(answersMsg)
		if answersErr != nil {
			return nil, answersErr
		}
		for _, a := range answers {
			if a.ScopeKey == scope {
				mc.answersScope = scope
			}
		}
	}

	if page == plannerPages-1 {
		mc.spawns++
		return v2.EntitlementsServiceListEntitlementsResponse_builder{
			Annotations: annotations.New(v2.EnqueuePageTokens_builder{PageTokens: []string{"cursor"}}.Build()),
		}.Build(), nil
	}
	return v2.EntitlementsServiceListEntitlementsResponse_builder{
		NextPageToken: fmt.Sprintf("plan=%d", page+1),
	}.Build(), nil
}

func (mc *multiPagePlannerEntitlementsMock) ListGrants(context.Context, *v2.GrantsServiceListGrantsRequest, ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
}

func TestSourceCacheContinuation_MultiPagePlannerEntitlementsBouncesPerRequest(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	mc := &multiPagePlannerEntitlementsMock{
		mockConnector: newMockConnector(),
		resByID:       map[string]*v2.Resource{},
	}
	mc.rtDB = append(mc.rtDB, typeScopedEntGroupRT, userResourceType)
	u, err := rs.NewUserResource("user-00", userResourceType, "user-00", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	mc.AddResource(ctx, u)
	mc.resByID["user-00"] = u
	for i := 0; i < 2; i++ {
		gid := fmt.Sprintf("group-%02d", i)
		g, err := rs.NewGroupResource(gid, typeScopedEntGroupRT, gid, nil)
		require.NoError(t, err)
		mc.AddResource(ctx, g)
		mc.resByID[gid] = g
		mc.groupIDs = append(mc.groupIDs, gid)
	}

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	require.NoError(t, runContinuationSync(ctx, t, mc, sync1, "", tmpDir, 0))
	require.Zero(t, mc.asks)
	require.Equal(t, 1, mc.spawns)
	require.Equal(t, 1, mc.cursorServes)
	ents1 := listEntitlementsInFile(ctx, t, sync1)
	require.Len(t, ents1, 2)

	sync2 := filepath.Join(tmpDir, "sync2.c1z")
	require.NoError(t, runContinuationSync(ctx, t, mc, sync2, sync1, tmpDir, 0),
		"a multi-page entitlements planner asking once per page must not trip the per-request bounce cap")
	require.Equal(t, plannerPages, mc.asks, "one ask per planning page")
	require.Equal(t, plannerPages, mc.answersPresent, "every re-invoked planning page carries answers")
	require.NotEmpty(t, mc.answersScope, "answers must cover the asked scope")
	require.False(t, mc.markerMissing, "TypeScopedEntitlements routing marker must survive on every request, including re-invokes")
	require.Equal(t, 2, mc.spawns, "the asking planner page spawns exactly once per sync")
	require.Equal(t, 2, mc.cursorServes, "spawned cursor executes exactly once per sync")
	requireSameEntitlementIDs(t, ents1, listEntitlementsInFile(ctx, t, sync2), "warm planner sync vs cold")
}
