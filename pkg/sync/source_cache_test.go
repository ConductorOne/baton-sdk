package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// sourceCacheMockConnector emulates a connector against an upstream with
// cheap change detection. Every grants page for a resource has a stable
// scope; the connector looks up the previous validator via the lookup the
// syncer installs (SetSourceCache) and either:
//
//   - miss, or validator != current upstream etag: return fresh rows +
//     SourceCacheRecord (a 200),
//   - hit with matching etag: return no rows + SourceCacheReplay (a 304),
//   - hit in delta mode: return only changed rows + overlay replay with
//     tombstones and a rotated token (a Graph-style delta round).
type sourceCacheMockConnector struct {
	*mockConnector

	mu     sync.Mutex
	lookup sourcecache.Lookup

	// etagByResource is the upstream's CURRENT validator per resource.
	etagByResource map[string]string

	// delta mode: overlay rows + deletions returned on a lookup hit.
	deltaMode       bool
	deltaAdds       map[string][]*v2.Grant
	deltaDeletedIDs map[string][]string
	// deltaDeletedPrincipals are bare principal-id tombstones, emitted on
	// the SourceCacheRecord annotation (the per-page channel) rather than
	// the replay annotation — exercising both proposal A and B paths.
	deltaDeletedPrincipals map[string][]string

	lookupHits   int
	lookupMisses int
}

func newSourceCacheMockConnector() *sourceCacheMockConnector {
	mc := &sourceCacheMockConnector{
		mockConnector:          newMockConnector(),
		etagByResource:         map[string]string{},
		deltaAdds:              map[string][]*v2.Grant{},
		deltaDeletedIDs:        map[string][]string{},
		deltaDeletedPrincipals: map[string][]string{},
	}
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)
	return mc
}

func (mc *sourceCacheMockConnector) SetSourceCache(_ context.Context, lookup sourcecache.Lookup) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if lookup == nil {
		lookup = sourcecache.NoopLookup{}
	}
	mc.lookup = lookup
}

func (mc *sourceCacheMockConnector) Validate(context.Context, *v2.ConnectorServiceValidateRequest, ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return v2.ConnectorServiceValidateResponse_builder{
		Annotations: annotations.New(v2.SourceCacheCapability_builder{
			Mode: v2.SourceCacheCapability_MODE_READ_WRITE,
		}.Build()),
	}.Build(), nil
}

func grantScopeForResource(resourceID string) string {
	return sourcecache.HashScope("mock://grants/" + resourceID)
}

func (mc *sourceCacheMockConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	resourceID := in.GetResource().GetId().GetResource()
	scope := grantScopeForResource(resourceID)

	mc.mu.Lock()
	lookup := mc.lookup
	currentEtag := mc.etagByResource[resourceID]
	deltaMode := mc.deltaMode
	adds := mc.deltaAdds[resourceID]
	deleted := mc.deltaDeletedIDs[resourceID]
	deletedPrincipals := mc.deltaDeletedPrincipals[resourceID]
	mc.mu.Unlock()

	if lookup == nil {
		lookup = sourcecache.NoopLookup{}
	}
	entry, found, err := lookup.Lookup(ctx, sourcecache.RowKindGrants, scope)
	if err != nil {
		return nil, err
	}

	if found && deltaMode {
		// Delta round: replay the base, overlay changes, tombstone
		// removals, rotate the token. Canonical-id tombstones ride the
		// replay annotation; bare principal-id tombstones ride the scope
		// annotation (the per-page channel every page of a round can use).
		mc.mu.Lock()
		mc.lookupHits++
		mc.mu.Unlock()
		return v2.GrantsServiceListGrantsResponse_builder{
			List: adds,
			Annotations: annotations.New(
				v2.SourceCacheReplay_builder{
					ScopeKey:       scope,
					CacheValidator: currentEtag, // new token for this round
					Overlay:        true,
					DeletedIds:     deleted,
				}.Build(),
				v2.SourceCacheRecord_builder{
					ScopeKey:            scope,
					DeletedPrincipalIds: deletedPrincipals,
				}.Build(),
			),
		}.Build(), nil
	}

	if found && entry.CacheValidator == currentEtag {
		// Conditional-request 304: nothing changed upstream.
		mc.mu.Lock()
		mc.lookupHits++
		mc.mu.Unlock()
		return v2.GrantsServiceListGrantsResponse_builder{
			List: []*v2.Grant{},
			Annotations: annotations.New(v2.SourceCacheReplay_builder{
				ScopeKey:       scope,
				CacheValidator: currentEtag,
			}.Build()),
		}.Build(), nil
	}

	// Fresh fetch (miss or changed).
	mc.mu.Lock()
	mc.lookupMisses++
	mc.mu.Unlock()
	return v2.GrantsServiceListGrantsResponse_builder{
		List: mc.grantDB[resourceID],
		Annotations: annotations.New(v2.SourceCacheRecord_builder{
			ScopeKey:       scope,
			CacheValidator: currentEtag,
		}.Build()),
	}.Build(), nil
}

// runSourceCacheSync runs one full sync into a fresh Pebble c1z at path,
// optionally replaying from prevPath.
func runSourceCacheSync(ctx context.Context, t *testing.T, mc *sourceCacheMockConnector, path, prevPath, tmpDir string, extraOpts ...SyncOpt) {
	t.Helper()
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	opts := []SyncOpt{WithConnectorStore(store), WithTmpDir(tmpDir), WithStrictIngestionInvariants()}
	if prevPath != "" {
		opts = append(opts, WithPreviousSyncC1ZPath(prevPath))
	}
	opts = append(opts, extraOpts...)
	syncer, err := NewSyncer(ctx, mc, opts...)
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))
}

// workerCounts are the worker configurations every source-cache scenario
// runs under: 0 (sequential) and 4 (parallel). Parallel sync + replay
// shares one engine across workers, so the scenarios double as races on
// concurrent Replay*/PutSourceCacheEntry batches and expansion arming.
var workerCounts = []int{0, 4}

func workerSyncOpts(workers int) []SyncOpt {
	if workers > 0 {
		return []SyncOpt{WithWorkerCount(workers)}
	}
	return nil
}

// listGrantsInFile reopens a finished Pebble c1z and returns its grants
// keyed by grant ID.
func listGrantsInFile(ctx context.Context, t *testing.T, path string) map[string]*v2.Grant {
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

	out := map[string]*v2.Grant{}
	pageToken := ""
	for {
		resp, err := reopen.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			out[g.GetId()] = g
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return out
}

func sourceCacheTestFixtures(t *testing.T) (*v2.Resource, *v2.Entitlement, *v2.Resource, *v2.Resource) {
	t.Helper()
	group, err := rs.NewGroupResource("g1", groupResourceType, "g1", nil)
	require.NoError(t, err)
	ent := et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	alice, err := rs.NewUserResource("alice", userResourceType, "alice", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	bob, err := rs.NewUserResource("bob", userResourceType, "bob", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)
	return group, ent, alice, bob
}

// TestSourceCache_ConditionalRequestReplay is the GitHub-shape end-to-end
// test: sync 1 stamps rows and records the scope's etag; sync 2's lookup
// hits, the connector answers with a 304-style replay, and the grants are
// carried into the new c1z without the connector emitting any rows.
func TestSourceCache_ConditionalRequestReplay(t *testing.T) {
	for _, workers := range workerCounts {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			runConditionalRequestReplayScenario(t, workerSyncOpts(workers))
		})
	}
}

func runConditionalRequestReplayScenario(t *testing.T, workerOpts []SyncOpt) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	group, ent, alice, bob := sourceCacheTestFixtures(t)
	g1 := gt.NewGrant(group, "member", alice)
	g2 := gt.NewGrant(group, "member", bob)

	mc := newSourceCacheMockConnector()
	mc.AddResource(ctx, group)
	mc.AddResource(ctx, alice)
	mc.AddResource(ctx, bob)
	mc.entDB[group.GetId().GetResource()] = []*v2.Entitlement{ent}
	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{g1, g2}
	mc.etagByResource[group.GetId().GetResource()] = `W/"etag-v1"`

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	sync2 := filepath.Join(tmpDir, "sync2.c1z")

	runSourceCacheSync(ctx, t, mc, sync1, "", tmpDir, workerOpts...)
	require.Zero(t, mc.lookupHits, "sync 1 has no previous sync to hit")

	// Upstream unchanged: sync 2 must replay.
	runSourceCacheSync(ctx, t, mc, sync2, sync1, tmpDir, workerOpts...)
	require.NotZero(t, mc.lookupHits, "sync 2's lookup must hit the etag persisted by sync 1")

	grants := listGrantsInFile(ctx, t, sync2)
	require.Contains(t, grants, g1.GetId())
	require.Contains(t, grants, g2.GetId())

	// And sync 2's file is usable as the NEXT previous sync: manifest
	// entry + stamped rows were carried forward (a third sync replays too).
	sync3 := filepath.Join(tmpDir, "sync3.c1z")
	hitsBefore := mc.lookupHits
	runSourceCacheSync(ctx, t, mc, sync3, sync2, tmpDir, workerOpts...)
	require.Greater(t, mc.lookupHits, hitsBefore, "a replay-only sync must remain usable as the next replay source")
	grants3 := listGrantsInFile(ctx, t, sync3)
	require.Contains(t, grants3, g1.GetId())
	require.Contains(t, grants3, g2.GetId())
}

// TestSourceCache_DeltaOverlayAndDeletes is the Microsoft Graph-shape
// test: the second sync replays the base, upserts changed rows on top,
// applies @removed tombstones, and rotates the token.
func TestSourceCache_DeltaOverlayAndDeletes(t *testing.T) {
	for _, workers := range workerCounts {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			runDeltaOverlayAndDeletesScenario(t, workerSyncOpts(workers))
		})
	}
}

func runDeltaOverlayAndDeletesScenario(t *testing.T, workerOpts []SyncOpt) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	group, ent, alice, bob := sourceCacheTestFixtures(t)
	carol, err := rs.NewUserResource("carol", userResourceType, "carol", nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	require.NoError(t, err)

	g1 := gt.NewGrant(group, "member", alice)
	g2 := gt.NewGrant(group, "member", bob)
	g3 := gt.NewGrant(group, "member", carol)

	mc := newSourceCacheMockConnector()
	mc.AddResource(ctx, group)
	mc.AddResource(ctx, alice)
	mc.AddResource(ctx, bob)
	mc.AddResource(ctx, carol)
	mc.entDB[group.GetId().GetResource()] = []*v2.Entitlement{ent}
	mc.grantDB[group.GetId().GetResource()] = []*v2.Grant{g1, g2}
	mc.etagByResource[group.GetId().GetResource()] = "delta-token-1"

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	sync2 := filepath.Join(tmpDir, "sync2.c1z")

	runSourceCacheSync(ctx, t, mc, sync1, "", tmpDir, workerOpts...)

	// Delta round: bob removed (canonical-id tombstone on the replay
	// annotation), carol added, token rotates.
	mc.mu.Lock()
	mc.deltaMode = true
	mc.etagByResource[group.GetId().GetResource()] = "delta-token-2"
	mc.deltaAdds[group.GetId().GetResource()] = []*v2.Grant{g3}
	mc.deltaDeletedIDs[group.GetId().GetResource()] = []string{g2.GetId()}
	mc.mu.Unlock()

	runSourceCacheSync(ctx, t, mc, sync2, sync1, tmpDir, workerOpts...)
	require.NotZero(t, mc.lookupHits)

	grants := listGrantsInFile(ctx, t, sync2)
	require.Contains(t, grants, g1.GetId(), "replayed base row must survive")
	require.Contains(t, grants, g3.GetId(), "overlay row must be upserted")
	require.NotContains(t, grants, g2.GetId(), "tombstoned row must be deleted")

	// Second delta round: carol removed by BARE PRINCIPAL ID via the
	// scope annotation — no principal type, no canonical-id
	// reconstruction (the Entra @removed shape).
	sync3 := filepath.Join(tmpDir, "sync3.c1z")
	mc.mu.Lock()
	mc.etagByResource[group.GetId().GetResource()] = "delta-token-3"
	mc.deltaAdds[group.GetId().GetResource()] = nil
	mc.deltaDeletedIDs[group.GetId().GetResource()] = nil
	mc.deltaDeletedPrincipals[group.GetId().GetResource()] = []string{"carol"}
	mc.mu.Unlock()

	runSourceCacheSync(ctx, t, mc, sync3, sync2, tmpDir, workerOpts...)

	grants3 := listGrantsInFile(ctx, t, sync3)
	require.Contains(t, grants3, g1.GetId(), "alice's replayed row must survive the principal tombstone")
	require.NotContains(t, grants3, g3.GetId(), "carol's row must die from a bare principal-id tombstone")
	require.NotContains(t, grants3, g2.GetId())
}

// TestSourceCache_ReplayArmsExpansion pins the expansion-arming contract:
// a sync in which EVERY grants page replays must still run grant
// expansion, because replayed rows carry needs_expansion but never pass
// GrantExpandable-annotated rows through the syncer's connector-response
// path (the only other thing that arms the expansion phase).
func TestSourceCache_ReplayArmsExpansion(t *testing.T) {
	for _, workers := range workerCounts {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			runReplayArmsExpansionScenario(t, workerSyncOpts(workers))
		})
	}
}

func runReplayArmsExpansionScenario(t *testing.T, workerOpts []SyncOpt) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()

	mc := newSourceCacheMockConnector()
	mc.rtDB = []*v2.ResourceType{groupResourceType, userResourceType}

	// Standard expansion topology: alice is a member of group2, and
	// group2 is an expandable member of group1 — expansion derives
	// alice's grant on group1's member entitlement.
	group1, group1Ent, err := mc.AddGroup(ctx, "group_1")
	require.NoError(t, err)
	group2, group2Ent, err := mc.AddGroup(ctx, "group_2")
	require.NoError(t, err)
	alice, err := mc.AddUser(ctx, "alice")
	require.NoError(t, err)
	_ = mc.AddGroupMember(ctx, group2, alice)
	_ = mc.AddGroupMember(ctx, group1, group2, group2Ent)

	mc.etagByResource[group1.GetId().GetResource()] = "etag-g1"
	mc.etagByResource[group2.GetId().GetResource()] = "etag-g2"

	derivedGrantID := gt.NewGrant(group1, "member", alice).GetId()

	sync1 := filepath.Join(tmpDir, "sync1.c1z")
	sync2 := filepath.Join(tmpDir, "sync2.c1z")

	runSourceCacheSync(ctx, t, mc, sync1, "", tmpDir, workerOpts...)
	require.Contains(t, listGrantsInFile(ctx, t, sync1), derivedGrantID,
		"sanity: sync 1's expansion must derive alice's group1 grant")

	// Sync 2 replays every grants page (upstream unchanged). Derived
	// grants are never replayed (they carry no scope stamp), so if replay
	// fails to arm expansion, SyncGrantExpansion is skipped and the
	// derived grant vanishes from sync 2.
	runSourceCacheSync(ctx, t, mc, sync2, sync1, tmpDir, workerOpts...)
	require.NotZero(t, mc.lookupHits, "sanity: sync 2 must have replayed")

	grants2 := listGrantsInFile(ctx, t, sync2)
	baseGrantID := gt.NewGrant(group1, "member", group2).GetId()
	require.Contains(t, grants2, baseGrantID, "replayed expandable base grant missing")
	require.Contains(t, grants2, derivedGrantID,
		"sync 2 must run grant expansion over replayed rows: the derived grant is missing, "+
			"meaning SetNeedsExpansion was never armed by the replay path")
	_ = group1Ent
}

// TestSourceCache_ReplayWhileDegradedFails pins the fail-loud half of the
// error contract: a replay annotation arriving with no usable previous
// sync is a hard sync error, never a silent fallback (the connector has
// already skipped row generation).
func TestSourceCache_ReplayWhileDegradedFails(t *testing.T) {
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
	mc.etagByResource[group.GetId().GetResource()] = "etag-v1"

	// Misbehaving connector: fabricate a lookup hit so it emits a replay
	// annotation even though the syncer never installed a lookup with a
	// previous source.
	mc.SetSourceCache(ctx, staticLookup{etag: "etag-v1"})

	store, err := dotc1z.NewStore(ctx, filepath.Join(tmpDir, "out.c1z"),
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	syncer, err := NewSyncer(ctx, &degradedLookupConnector{mc}, WithConnectorStore(store), WithTmpDir(tmpDir))
	require.NoError(t, err)
	err = syncer.Sync(ctx)
	require.Error(t, err, "replay with no previous sync must fail the sync loudly")
	require.Contains(t, err.Error(), "source cache")
	require.NoError(t, syncer.Close(ctx))
}

// stubSourceCacheStore fakes the store surface begin/finish touch, so the
// stats bookkeeping can be pinned without a real engine.
type stubSourceCacheStore struct {
	dotc1z.SourceCacheStore
	replayRows int64
	stamped    []string
}

func (s *stubSourceCacheStore) LookupSourceCacheEntry(context.Context, sourcecache.RowKind, string) (sourcecache.Entry, bool, error) {
	return sourcecache.Entry{CacheValidator: "prev-token"}, true, nil
}

func (s *stubSourceCacheStore) ReplaySourceCache(context.Context, connectorstore.Reader, sourcecache.RowKind, string) (dotc1z.SourceCacheReplayResult, error) {
	return dotc1z.SourceCacheReplayResult{Rows: s.replayRows}, nil
}

func (s *stubSourceCacheStore) PutSourceCacheEntry(_ context.Context, _ sourcecache.RowKind, scopeKey, _ string) error {
	s.stamped = append(s.stamped, scopeKey)
	return nil
}

// TestSourceCacheStats_OverlayRoundBooksOneHit pins two stats contracts:
//
//  1. Replay counters are recorded at FINISH, not begin — a page re-run
//     after failing between replay and finish must not double-count.
//  2. A multi-page overlay round, whose rotated validator legally arrives
//     on the FINAL page's scope annotation (no replay annotation there),
//     books exactly one ScopesReplayed and zero ScopesStamped — not one
//     of each, which would read a 100%-warm delta sync as 50%.
func TestSourceCacheStats_OverlayRoundBooksOneHit(t *testing.T) {
	ctx, err := logging.Init(context.Background())
	require.NoError(t, err)

	store := &stubSourceCacheStore{replayRows: 2}
	s := &syncer{
		state: newState(),
		sourceCache: syncerSourceCache{
			enabled:        true,
			current:        store,
			prev:           store,
			replayedScopes: &replayedScopeSet{},
		},
	}
	scope := sourcecache.HashScope("delta://groups")

	// Page 1: overlay replay, no etag yet (the delta token rotates on the
	// final page). One overlay row rides along.
	_, page1, err := s.beginSourceCachePage(ctx, sourcecache.RowKindGrants,
		annotations.New(v2.SourceCacheReplay_builder{ScopeKey: scope, Overlay: true}.Build()), 1)
	require.NoError(t, err)
	require.True(t, page1.replayed)

	if stats := s.state.SourceCacheStatsSnapshot(); stats != nil {
		require.Zero(t, stats.ScopesReplayed, "replay counters must not be recorded at begin (re-run double-count)")
	}

	require.NoError(t, s.finishSourceCachePage(ctx, page1))

	// Final page of the round: scope annotation with the NEW token, no
	// replay annotation — this is a stamp write, but of the round's own
	// scope, not a cold miss.
	_, page2, err := s.beginSourceCachePage(ctx, sourcecache.RowKindGrants,
		annotations.New(v2.SourceCacheRecord_builder{ScopeKey: scope, CacheValidator: "delta-token-2"}.Build()), 0)
	require.NoError(t, err)
	require.False(t, page2.replayed)
	require.NoError(t, s.finishSourceCachePage(ctx, page2))
	require.Equal(t, []string{scope}, store.stamped, "the rotated validator must still be persisted")

	// A genuinely cold scope still counts as stamped.
	coldScope := sourcecache.HashScope("delta://users")
	_, page3, err := s.beginSourceCachePage(ctx, sourcecache.RowKindGrants,
		annotations.New(v2.SourceCacheRecord_builder{ScopeKey: coldScope, CacheValidator: "etag-1"}.Build()), 3)
	require.NoError(t, err)
	require.NoError(t, s.finishSourceCachePage(ctx, page3))

	stats := s.state.SourceCacheStatsSnapshot()
	require.NotNil(t, stats)
	require.Equal(t, int64(1), stats.ScopesReplayed, "one warm round, one hit")
	require.Equal(t, int64(1), stats.ScopesStamped, "only the cold scope counts as stamped")
	require.Equal(t, int64(2), stats.RowsReplayed[sourcecache.RowKindGrants])
	require.Equal(t, int64(1), stats.OverlayRows)
}

// staticLookup always hits with a fixed etag — used to simulate a
// connector that violates the only-replay-what-you-looked-up invariant.
type staticLookup struct{ etag string }

func (s staticLookup) Lookup(context.Context, sourcecache.RowKind, string) (sourcecache.Entry, bool, error) {
	return sourcecache.Entry{CacheValidator: s.etag}, true, nil
}

// degradedLookupConnector shadows SetSourceCache so the syncer cannot
// overwrite the fabricated lookup: the connector keeps "hitting" while the
// syncer's source-cache read side is degraded.
type degradedLookupConnector struct {
	*sourceCacheMockConnector
}

func (d *degradedLookupConnector) SetSourceCache(context.Context, sourcecache.Lookup) {
	// Deliberately ignore the syncer's install; the mock keeps its
	// fabricated staticLookup.
}
