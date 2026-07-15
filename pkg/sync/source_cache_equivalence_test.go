package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Replay-equivalence differential harness.
//
// THE PROPERTY: for any upstream state, a warm sync (replaying from the
// previous sync's c1z) must produce a c1z semantically identical to a
// cold sync of the same upstream state. This is the contract stated in
// pkg/sourcecache ("a cached sync must reproduce what a full resync
// would produce"), enforced here as a differential test instead of
// relying on humans to remember every response-row side effect.
//
// SHAPE: a fixed, feature-complete topology exercising every feature
// that interacts with replay —
//   - parent/child resources (ChildResourceType scheduling),
//   - grant expansion (GrantExpandable + needs_expansion re-arming),
//   - external-resource matching (MatchID and MatchAll, with the
//     external c1z churning UNDER a 304 of the internal scope — the one
//     shape where a missed re-arm is observable),
//   - InsertResourceGrants (grant-driven resource insertion),
//   - exclusion-group annotations on replayed entitlements,
//   - both delta tombstone channels (canonical grant ids on the replay
//     annotation; bare principal ids on the scope annotation),
//   - 304 rounds, delta-overlay rounds, cold refetch (validator
//     rotation), zero-row scopes, and scopes appearing mid-chain —
// mutated across rounds by a forced schedule (guaranteeing each behavior
// occurs regardless of seed) plus seeded random churn (interleavings).
// The warm side CHAINS: round r replays from round r-1's warm output, so
// decay compounds instead of hiding.
//
// ANTI-VACUITY: the harness fails itself if it did not actually exercise
// what it claims — per-round and per-scenario counters assert 304s,
// overlays, cold refetches, both tombstone channels, child fetches, and
// the presence of expansion-derived, match-transformed, and
// grant-inserted artifacts in the outputs. A trivially-green run is
// treated as a harness bug.
//
// COMPARISON: full semantic diff of both files — resource types,
// resources, entitlements, grants, all fields, annotations normalized by
// sort — read through MULTIPLE paths (primary listings, by-parent
// listings, by-entitlement-resource grant listings) so replay-time index
// synthesis bugs surface, not just primary-row differences.
//
// Also compared, beyond the v2 read surface: the source-cache MANIFEST
// and per-scope INDEX COUNTS of both files. These are invisible to v2
// reads but poison FUTURE syncs' replays — a warm sync could produce
// perfect v2 data while writing wrong stamps or phantom manifest
// entries, and a purely v2-level comparison would pass until the damage
// detonated rounds later. Row listings are also duplicate-checked (the
// deduping snapshot maps would otherwise hide pagination double-yields).
//
// Round 2's warm sync is additionally CRASHED mid-grants-phase (injected
// connector error after other scopes replayed) and resumed into the same
// file, asserting resume-not-restart — so page re-runs over partially
// replayed state (replay idempotency, checkpoint interplay) are part of
// the compared output, every scenario.
//
// EXPLICITLY NOT COVERED HERE (do not mistake green for proof of these):
//   - error-outcome parity (covered by the dedicated exclusion-group
//     parity test in source_cache_sideeffects_test.go);
//   - the ask/answer lookup continuation topology (covered by
//     source_cache_continuation_test.go / _e2e_test.go);
//   - EnqueuePageTokens / type-scoped grants and entitlements
//     (spawn_cursors_test.go, type_scoped_grants_test.go,
//     type_scoped_entitlements_test.go); the team topology below also
//     exercises both type-scoped shapes in the differential chain;
//   - compaction in the warm chain (covered separately by
//     pkg/synccompactor's TestCompactedFileIsColdReplaySource — an import
//     cycle keeps the compactor out of this package; the contract is that
//     compacted artifacts carry NO manifest entries and replay cold);
//   - concurrent syncs; SQLite (source cache is Pebble-only);
//   - delta tombstones combined with external-match annotations (a
//     documented contract limitation — the model deliberately never
//     generates that combination).
//
// The model DELIBERATELY overlaps MatchID and MatchAll on one
// entitlement (both resolving the same principals): the harness's first
// run caught that colliding transformed grants were resolved by store
// iteration order, which differs between cold syncs (iterating source
// grants) and warm syncs (iterating replayed transformed grants).
// processGrantsWithExternalPrincipals now resolves collisions by rule
// specificity; this topology pins that.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	stdsync "sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	pebbleengine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	"github.com/conductorone/baton-sdk/pkg/types"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// ---------------------------------------------------------------------
// Upstream model
// ---------------------------------------------------------------------

var (
	equivOrgRT     = v2.ResourceType_builder{Id: "eq_org", DisplayName: "Org"}.Build()
	equivProjectRT = v2.ResourceType_builder{Id: "eq_project", DisplayName: "Project"}.Build()
	equivRepoRT    = v2.ResourceType_builder{Id: "eq_repo", DisplayName: "Repo"}.Build()
	// equivTeamRT is TYPE-SCOPED for both grants and entitlements: no
	// per-resource pages; one planner call per phase spawns chunk
	// cursors (EnqueuePageTokens), each chunk a scope covering two teams —
	// the Entra/Okta planner shape.
	equivTeamRT = v2.ResourceType_builder{
		Id:          "eq_team",
		DisplayName: "Team",
		Annotations: annotations.New(&v2.TypeScopedGrants{}, &v2.TypeScopedEntitlements{}),
	}.Build()
)

type equivGroup struct {
	id      string
	members map[string]bool

	// version is the grants scope's validator generation, bumped at most
	// ONCE per mutation round (dirty guards it) so a round with several
	// membership changes still reads as exactly one generation behind —
	// the delta-overlay shape. delta* is the coalesced diff between
	// version-1 and version (nil when the group was untouched last
	// round). canonicalTombstones selects which delta channel removals
	// ride: canonical grant ids on the replay annotation, or bare
	// principal ids on the scope annotation.
	version             int
	dirty               bool
	deltaAdds           []string
	deltaRemoves        []string
	canonicalTombstones bool

	// exclusionDefault attaches a (valid, single-member) exclusion-group
	// default annotation to the member entitlement, so replayed
	// entitlement rows carry exclusion state through validation.
	exclusionDefault bool
}

type equivOrg struct {
	id       string
	projects map[string]string // project id -> display name
	// projectsVersion is the per-org project-listing scope generation.
	projectsVersion int
	// repoGrants: repo id -> user id holding "admin". The org's grants
	// page carries InsertResourceGrants; repoVersion is its generation
	// (fresh-on-change, 304 when unchanged — no delta).
	repoGrants  map[string]string
	repoVersion int
}

type equivModel struct {
	users        map[string]bool
	usersVersion int

	orgs        map[string]*equivOrg
	orgsVersion int

	groups        map[string]*equivGroup
	groupsVersion int

	// extGroup's grants page emits ExternalResourceMatchID grants (one
	// per entry of extMatchIDs) plus one ExternalResourceMatchAll(USER)
	// grant. extVersion is that scope's generation. extUsers is the
	// EXTERNAL c1z's content — mutating it WITHOUT bumping extVersion is
	// the divergence trap for the match re-arm path.
	extGroupID  string
	extMatchIDs []string
	extVersion  int
	extUsers    map[string]bool
	extDirty    bool // external c1z must be rebuilt before the next sync

	// Expansion: expandableParent's grants scope permanently contains an
	// expandable membership of expandableChild, so members of the child
	// derive grants on the parent. Churning the child changes expansion
	// output while the parent's scope 304s.
	expandableParent string
	expandableChild  string

	// multiPageGroup's grants scope serves multi-page rounds: fresh
	// fetches AND delta overlays span two pages, with the validator only
	// on the final page and tombstones riding the final page's scope
	// annotation (the page-spanning delta contract).
	multiPageGroup string

	// teams (type-scoped grants): team id -> members. Teams are fixed;
	// only membership churns. teamChunkVersions[i] is chunk i's
	// validator generation (chunk = two teams by sorted order); chunks
	// are fresh-or-304 scopes, no deltas.
	teams             map[string]map[string]bool
	teamChunkVersions []int
}

// teamChunkOf returns the chunk index a team belongs to (two teams per
// chunk, by sorted team id). Teams are never added or removed, so the
// mapping is stable across rounds.
func (m *equivModel) teamChunkOf(teamID string) int {
	ids := sortedKeys(m.teams)
	for i, id := range ids {
		if id == teamID {
			return i / 2
		}
	}
	panic("unknown team " + teamID)
}

func (m *equivModel) teamsInChunk(chunk int) []string {
	ids := sortedKeys(m.teams)
	var out []string
	for i, id := range ids {
		if i/2 == chunk {
			out = append(out, id)
		}
	}
	return out
}

func (m *equivModel) mutateTeam(teamID, uid string, add bool) {
	members := m.teams[teamID]
	if add == members[uid] {
		return
	}
	if add {
		members[uid] = true
	} else {
		delete(members, uid)
	}
	m.teamChunkVersions[m.teamChunkOf(teamID)]++
}

func newEquivModel() *equivModel {
	m := &equivModel{
		users:  map[string]bool{},
		orgs:   map[string]*equivOrg{},
		groups: map[string]*equivGroup{},

		extGroupID:  "gext",
		extMatchIDs: []string{"ext-0", "ext-1"},
		extUsers:    map[string]bool{"ext-0": true, "ext-1": true, "ext-2": true},
		extDirty:    true,

		expandableParent: "g0",
		expandableChild:  "g3",
		multiPageGroup:   "g1",

		teams: map[string]map[string]bool{
			"t0": {"u0": true, "u1": true},
			"t1": {"u2": true},
			"t2": {"u3": true, "u4": true},
			"t3": {"u9": true},
		},
		teamChunkVersions: []int{0, 0},
	}
	for i := 0; i < 10; i++ {
		m.users[fmt.Sprintf("u%d", i)] = true
	}
	m.orgs["org0"] = &equivOrg{
		id:         "org0",
		projects:   map[string]string{"p0": "Project 0", "p1": "Project 1"},
		repoGrants: map[string]string{"repo0": "u0"},
	}
	m.orgs["org1"] = &equivOrg{
		id:         "org1",
		projects:   map[string]string{"p2": "Project 2", "p3": "Project 3"},
		repoGrants: map[string]string{}, // zero-row scope: entry must still persist
	}
	m.groups["g0"] = &equivGroup{id: "g0", members: map[string]bool{"u0": true, "u1": true}, canonicalTombstones: true, exclusionDefault: true}
	m.groups["g1"] = &equivGroup{id: "g1", members: map[string]bool{"u2": true, "u3": true}, exclusionDefault: true}
	m.groups["g2"] = &equivGroup{id: "g2", members: map[string]bool{"u4": true}}
	m.groups["g3"] = &equivGroup{id: "g3", members: map[string]bool{"u5": true, "u6": true}}
	m.groups[m.extGroupID] = &equivGroup{id: m.extGroupID, members: map[string]bool{}}
	return m
}

// clearDeltas resets last round's diffs; called at the start of each
// mutation round so overlays only ever describe one round's changes.
func (m *equivModel) clearDeltas() {
	for _, g := range m.groups {
		g.deltaAdds, g.deltaRemoves = nil, nil
		g.dirty = false
	}
	m.extDirty = false
}

func (g *equivGroup) touch() {
	if !g.dirty {
		g.dirty = true
		g.version++
	}
}

func (m *equivModel) addMember(gid, uid string) {
	g := m.groups[gid]
	if g.members[uid] {
		return
	}
	g.members[uid] = true
	g.touch()
	g.deltaAdds = append(g.deltaAdds, uid)
}

func (m *equivModel) removeMember(gid, uid string) {
	g := m.groups[gid]
	if !g.members[uid] {
		return
	}
	delete(g.members, uid)
	g.touch()
	g.deltaRemoves = append(g.deltaRemoves, uid)
}

func sortedKeys[V any](in map[string]V) []string {
	out := make([]string, 0, len(in))
	for k := range in {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// mutate applies round r's forced schedule plus seeded churn. The forced
// schedule guarantees every replay behavior occurs at least once per
// scenario regardless of seed; see the round comments.
func (m *equivModel) mutate(round int, rng *rand.Rand) {
	m.clearDeltas()
	switch round {
	case 1:
		// Canonical-id tombstone (g0), principal-id tombstone on the
		// MULTI-PAGE overlay group (g1), overlay add (g2), project
		// rename (cold refetch of a resources scope), repo grant add
		// (fresh InsertResourceGrants page), new external user + match
		// (external file rebuild + fresh page).
		m.removeMember("g0", "u1")
		m.removeMember("g1", "u3")
		m.addMember("g1", "u9")
		m.addMember("g2", "u7")
		m.orgs["org0"].projects["p0"] = "Project 0 (renamed)"
		m.orgs["org0"].projectsVersion++
		m.orgs["org0"].repoGrants["repo1"] = "u2"
		m.orgs["org0"].repoVersion++
		m.extUsers["ext-3"] = true
		m.extMatchIDs = append(m.extMatchIDs, "ext-3")
		m.extVersion++
		m.extDirty = true
		// Team churn: chunk 0 refetches, chunk 1 must 304.
		m.mutateTeam("t0", "u5", true)
	case 2:
		// THE DIVERGENCE TRAP: the external c1z changes while the
		// internal match scope's validator does NOT rotate (the internal
		// source grants are unchanged, so the connector legitimately
		// 304s). Cold output reflects the new external set; a warm sync
		// that fails to re-run match processing keeps stale transformed
		// grants. Also: user-set refetch (multi-page fresh), repo grant
		// removal, and expansion input churn under the parent's 304.
		// The RUNNER additionally crashes and resumes this round's warm
		// sync mid-grants-phase.
		delete(m.extUsers, "ext-1")
		m.extDirty = true
		m.users["u10"] = true
		m.usersVersion++
		delete(m.orgs["org0"].repoGrants, "repo1")
		m.orgs["org0"].repoVersion++
		m.addMember(m.expandableChild, "u8")
		m.removeMember(m.expandableChild, "u5")
	case 3:
		// Deletions: a project, a WHOLE group, and a user vanish (their
		// rows must vanish from the warm output exactly as from a cold
		// one — rows only enter a sync via fresh pages or replay, so a
		// leftover here means a replay copied outside its scope). A new
		// group also appears mid-chain (cold scopes while everything
		// else replays), plus seeded churn.
		delete(m.orgs["org0"].projects, "p0")
		m.orgs["org0"].projectsVersion++
		delete(m.groups, "g2")
		m.groupsVersion++
		delete(m.users, "u7") // was only ever a member of g2
		m.usersVersion++
		ng := &equivGroup{id: "g4", members: map[string]bool{"u9": true}, canonicalTombstones: rng.Intn(2) == 0}
		m.groups[ng.id] = ng
		m.mutateTeam("t0", "u5", false)
		m.randomChurn(rng)
	default:
		// Round 4: deliberate no-op — every scope must 304 and the
		// output must STILL match a cold sync (chain-decay probe).
	}
}

func (m *equivModel) randomChurn(rng *rand.Rand) {
	users := sortedKeys(m.users)
	for _, gid := range sortedKeys(m.groups) {
		if gid == m.extGroupID {
			continue // never mix delta churn into the match scope
		}
		g := m.groups[gid]
		switch rng.Intn(3) {
		case 0: // add an absent user
			for _, u := range users {
				if !g.members[u] {
					m.addMember(gid, u)
					break
				}
			}
		case 1: // remove a present member (keep expansion child non-empty)
			members := sortedKeys(g.members)
			if len(members) > 1 {
				m.removeMember(gid, members[rng.Intn(len(members))])
			}
		default: // untouched: must 304
		}
	}
}

// ---------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------

type equivCounters struct {
	pages304            int
	pagesOverlay        int // final (or only) page of an overlay round
	pagesOverlayInterim int // non-final overlay page (no validator yet)
	pagesColdStale      int // lookup hit but validator stale -> full refetch
	pagesFresh          int // lookup miss -> full fetch (final page)
	pagesFreshInterim   int // non-final fresh page (no validator yet)
	tombsCanonical      int
	tombsPrincipal      int
	childPageCalls      int
	injectedFailures    int
	plannerPages        int // type-scoped planning calls (EnqueuePageTokens emitters)
	chunkPages304       int // spawned chunk cursors that replayed
	chunkPagesFresh     int // spawned chunk cursors fetched fresh
	askPages            int // continuation phase-1 responses (lookup asks)
}

// equivConnector serves the model. forceCold ignores the lookup and
// always emits full fresh pages (still scope-annotated — stamps are
// invisible to the comparator), which is exactly what a from-scratch
// resync sees.
type equivConnector struct {
	*mockConnector

	mu        stdsync.Mutex
	model     *equivModel
	lookup    sourcecache.Lookup
	forceCold bool
	// continuation switches the lookup topology: instead of the direct
	// SetSourceCache lookup, lookups are served from request-delivered
	// SourceCacheLookupAnswers and deferred via SourceCacheLookupAsk
	// responses — the single-shot (Lambda) transport, played by hand.
	// The connector must be wrapped in equivContinuationClient so the
	// syncer does not see a SetLookup implementation.
	continuation bool
	counters     equivCounters
	// failNext injects one transient failure per key ("grants:<rid>"),
	// simulating a crash mid-sync; the runner resumes the same file and
	// the resumed output is what gets compared.
	failNext map[string]bool
}

// equivContinuationClient hides the embedded SetSourceCache from the
// syncer's sourcecache.SourceCacheSetter type assertion, making the connector a
// single-shot transport: the syncer attaches SourceCacheLookupOffer to
// requests and the connector answers with asks.
type equivContinuationClient struct {
	*equivConnector
}

// SetSourceCache shadows the embedded method with a different signature
// so equivContinuationClient does NOT satisfy sourcecache.SourceCacheSetter.
func (equivContinuationClient) SetSourceCache() {}

func (c *equivConnector) injectGrantsFailure(resourceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failNext == nil {
		c.failNext = map[string]bool{}
	}
	c.failNext["grants:"+resourceID] = true
}

func newEquivConnector(model *equivModel, forceCold bool) *equivConnector {
	mc := &equivConnector{mockConnector: newMockConnector(), model: model, forceCold: forceCold}
	mc.rtDB = []*v2.ResourceType{equivOrgRT, equivProjectRT, equivRepoRT, equivTeamRT, groupResourceType, userResourceType}
	return mc
}

func (c *equivConnector) SetSourceCache(_ context.Context, lookup sourcecache.Lookup) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lookup = lookup
}

func (c *equivConnector) Validate(context.Context, *v2.ConnectorServiceValidateRequest, ...grpc.CallOption) (*v2.ConnectorServiceValidateResponse, error) {
	return v2.ConnectorServiceValidateResponse_builder{
		Annotations: annotations.New(v2.SourceCacheCapability_builder{
			Mode: v2.SourceCacheCapability_MODE_READ_WRITE,
		}.Build()),
	}.Build(), nil
}

func equivEtag(version int) string { return fmt.Sprintf("v%d", version) }

// revalidate consults the lookup for (kind, scope) and classifies the
// round: fresh (miss), current (304), overlay-eligible (exactly one
// generation behind), or stale (older: full refetch). forceCold always
// reports fresh.
type equivVerdict int

const (
	equivFresh equivVerdict = iota
	equivCurrent
	equivOneBehind
	equivStale
)

// requestLookup resolves the lookup for one request. Direct topology:
// the SetSourceCache-installed lookup. Continuation topology: a
// per-request ContinuationLookup built from the request's answers (nil
// when the request carries neither offer nor answers — cold requests
// must not ask). The returned *ContinuationLookup is non-nil only when
// deferred lookups must be converted into an ask response.
func (c *equivConnector) requestLookup(reqAnnos []*anypb.Any) (sourcecache.Lookup, *sourcecache.ContinuationLookup, error) {
	if !c.continuation {
		c.mu.Lock()
		lookup := c.lookup
		c.mu.Unlock()
		if lookup == nil {
			lookup = sourcecache.NoopLookup{}
		}
		return lookup, nil, nil
	}
	annos := annotations.Annotations(reqAnnos)
	answersMsg := &v2.SourceCacheLookupAnswers{}
	hasAnswers, err := annos.Pick(answersMsg)
	if err != nil {
		return nil, nil, err
	}
	if !hasAnswers && !annos.Contains(&v2.SourceCacheLookupOffer{}) {
		return sourcecache.NoopLookup{}, nil, nil
	}
	var answers []sourcecache.Answer
	if hasAnswers {
		answers, err = sourcecache.AnswersFromProto(answersMsg)
		if err != nil {
			return nil, nil, err
		}
	}
	cl := sourcecache.NewContinuationLookup(answers)
	return cl, cl, nil
}

// askAnnos converts a deferred lookup into the ask response's
// annotations, counting the phase-1 turn.
func (c *equivConnector) askAnnos(cl *sourcecache.ContinuationLookup) annotations.Annotations {
	c.count(func(ct *equivCounters) { ct.askPages++ })
	return annotations.New(sourcecache.AskProto(cl.Asked()))
}

// isDeferred reports whether err is a deferred continuation lookup that
// must become an ask response (only possible when cl is non-nil).
func isDeferred(cl *sourcecache.ContinuationLookup, err error) bool {
	return cl != nil && err != nil && errors.Is(err, sourcecache.ErrLookupDeferred)
}

func (c *equivConnector) revalidate(ctx context.Context, lookup sourcecache.Lookup, kind sourcecache.RowKind, scope string, version int) (equivVerdict, error) {
	if c.forceCold {
		return equivFresh, nil
	}
	entry, found, err := lookup.Lookup(ctx, kind, scope)
	if err != nil {
		return equivFresh, err
	}
	if !found {
		return equivFresh, nil
	}
	switch entry.CacheValidator {
	case equivEtag(version):
		return equivCurrent, nil
	case equivEtag(version - 1):
		return equivOneBehind, nil
	default:
		return equivStale, nil
	}
}

func (c *equivConnector) count(f func(*equivCounters)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f(&c.counters)
}

func (c *equivConnector) snapshotCounters() equivCounters {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counters
}

func equivScopeAnnos(scope string, version int) annotations.Annotations {
	return annotations.New(v2.SourceCacheRecord_builder{
		ScopeKey:       sourcecache.HashScope(scope),
		CacheValidator: equivEtag(version),
	}.Build())
}

func equivReplayAnnos(scope string, version int) annotations.Annotations {
	return annotations.New(v2.SourceCacheReplay_builder{
		ScopeKey:       sourcecache.HashScope(scope),
		CacheValidator: equivEtag(version),
	}.Build())
}

// --- resource builders (deterministic; shared by fresh pages) ---------

func equivOrgResource(id string) *v2.Resource {
	r, err := rs.NewResource("Org "+id, equivOrgRT, id,
		rs.WithAnnotation(v2.ChildResourceType_builder{ResourceTypeId: equivProjectRT.GetId()}.Build()),
		rs.WithAnnotation(&v2.SkipEntitlements{}),
	)
	if err != nil {
		panic(err)
	}
	return r
}

func equivProjectResource(orgID, id, displayName string) *v2.Resource {
	r, err := rs.NewResource(displayName, equivProjectRT, id,
		rs.WithParentResourceID(v2.ResourceId_builder{ResourceType: equivOrgRT.GetId(), Resource: orgID}.Build()),
		rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}),
	)
	if err != nil {
		panic(err)
	}
	return r
}

func equivGroupResource(id string) *v2.Resource {
	r, err := rs.NewGroupResource(id, groupResourceType, id, nil)
	if err != nil {
		panic(err)
	}
	return r
}

func equivUserResource(id string) *v2.Resource {
	r, err := rs.NewUserResource(id, userResourceType, id, nil, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	if err != nil {
		panic(err)
	}
	return r
}

func equivRepoResource(id string) *v2.Resource {
	r, err := rs.NewResource("Repo "+id, equivRepoRT, id, rs.WithAnnotation(&v2.SkipEntitlementsAndGrants{}))
	if err != nil {
		panic(err)
	}
	return r
}

func equivTeamResource(id string) *v2.Resource {
	r, err := rs.NewResource("Team "+id, equivTeamRT, id)
	if err != nil {
		panic(err)
	}
	return r
}

func equivTeamGrant(tid, uid string) *v2.Grant {
	return gt.NewGrant(equivTeamResource(tid), "member",
		v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: uid}.Build())
}

func equivMemberEnt(g *v2.Resource, exclusionDefault bool) *v2.Entitlement {
	ent := et.NewAssignmentEntitlement(g, "member", et.WithGrantableTo(groupResourceType, userResourceType))
	ent.SetSlug("member")
	if exclusionDefault {
		annos := annotations.Annotations(ent.GetAnnotations())
		annos.Update(v2.EntitlementExclusionGroup_builder{
			ExclusionGroupId: "eg-" + g.GetId().GetResource(),
			IsDefault:        true,
		}.Build())
		ent.SetAnnotations(annos)
	}
	return ent
}

func equivMemberGrant(gid, uid string) *v2.Grant {
	return gt.NewGrant(equivGroupResource(gid), "member",
		v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: uid}.Build())
}

// --- list RPCs ---------------------------------------------------------

func (c *equivConnector) ListResources(ctx context.Context, in *v2.ResourcesServiceListResourcesRequest, opts ...grpc.CallOption) (*v2.ResourcesServiceListResourcesResponse, error) {
	lookup, cl, err := c.requestLookup(in.GetAnnotations())
	if err != nil {
		return nil, err
	}
	resp, err := c.listResourcesInner(ctx, lookup, in)
	if isDeferred(cl, err) {
		return v2.ResourcesServiceListResourcesResponse_builder{
			Annotations: c.askAnnos(cl),
		}.Build(), nil
	}
	return resp, err
}

func (c *equivConnector) listResourcesInner(ctx context.Context, lookup sourcecache.Lookup, in *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	c.mu.Lock()
	m := c.model
	switch in.GetResourceTypeId() {
	case equivOrgRT.GetId():
		version := m.orgsVersion
		orgIDs := sortedKeys(m.orgs)
		c.mu.Unlock()
		return c.resourcesPage(ctx, lookup, "res:org", version, func() []*v2.Resource {
			out := make([]*v2.Resource, 0, len(orgIDs))
			for _, id := range orgIDs {
				out = append(out, equivOrgResource(id))
			}
			return out
		})
	case equivProjectRT.GetId():
		if in.GetParentResourceId().GetResource() == "" {
			c.mu.Unlock()
			return v2.ResourcesServiceListResourcesResponse_builder{}.Build(), nil
		}
		orgID := in.GetParentResourceId().GetResource()
		org := m.orgs[orgID]
		if org == nil {
			c.mu.Unlock()
			return v2.ResourcesServiceListResourcesResponse_builder{}.Build(), nil
		}
		version := org.projectsVersion
		projects := make(map[string]string, len(org.projects))
		for k, v := range org.projects {
			projects[k] = v
		}
		c.mu.Unlock()
		c.count(func(ct *equivCounters) { ct.childPageCalls++ })
		return c.resourcesPage(ctx, lookup, "res:project:"+orgID, version, func() []*v2.Resource {
			out := make([]*v2.Resource, 0, len(projects))
			for _, id := range sortedKeys(projects) {
				out = append(out, equivProjectResource(orgID, id, projects[id]))
			}
			return out
		})
	case groupResourceType.GetId():
		version := m.groupsVersion
		groupIDs := sortedKeys(m.groups)
		c.mu.Unlock()
		return c.resourcesPage(ctx, lookup, "res:group", version, func() []*v2.Resource {
			out := make([]*v2.Resource, 0, len(groupIDs))
			for _, id := range groupIDs {
				out = append(out, equivGroupResource(id))
			}
			return out
		})
	case equivTeamRT.GetId():
		// Teams are UNSCOPED: listed fresh every sync (mixes never-scoped
		// pages into an otherwise warm sync).
		teamIDs := sortedKeys(m.teams)
		c.mu.Unlock()
		out := make([]*v2.Resource, 0, len(teamIDs))
		for _, id := range teamIDs {
			out = append(out, equivTeamResource(id))
		}
		return v2.ResourcesServiceListResourcesResponse_builder{List: out}.Build(), nil
	case userResourceType.GetId():
		version := m.usersVersion
		userIDs := sortedKeys(m.users)
		c.mu.Unlock()
		return c.usersPageMulti(ctx, lookup, in.GetPageToken(), version, userIDs)
	default: // repo: insert-only, never listed
		c.mu.Unlock()
		return v2.ResourcesServiceListResourcesResponse_builder{}.Build(), nil
	}
}

// usersPageMulti serves the users scope as a MULTI-PAGE scope: fresh
// fetches span two pages, with the scope annotation on both but the
// validator only on the final page (the interim-page contract). A 304 is
// a single page like any other scope.
func (c *equivConnector) usersPageMulti(ctx context.Context, lookup sourcecache.Lookup, pageToken string, version int, userIDs []string) (*v2.ResourcesServiceListResourcesResponse, error) {
	const scope = "res:user"
	verdict, err := c.revalidate(ctx, lookup, sourcecache.RowKindResources, sourcecache.HashScope(scope), version)
	if err != nil {
		return nil, err
	}
	if verdict == equivCurrent {
		c.count(func(ct *equivCounters) { ct.pages304++ })
		return v2.ResourcesServiceListResourcesResponse_builder{
			Annotations: equivReplayAnnos(scope, version),
		}.Build(), nil
	}
	half := len(userIDs) / 2
	buildRows := func(ids []string) []*v2.Resource {
		out := make([]*v2.Resource, 0, len(ids))
		for _, id := range ids {
			out = append(out, equivUserResource(id))
		}
		return out
	}
	if pageToken == "" {
		c.count(func(ct *equivCounters) { ct.pagesFreshInterim++ })
		return v2.ResourcesServiceListResourcesResponse_builder{
			List: buildRows(userIDs[:half]),
			Annotations: annotations.New(v2.SourceCacheRecord_builder{
				ScopeKey: sourcecache.HashScope(scope),
				// No etag: the validator arrives on the final page.
			}.Build()),
			NextPageToken: "users:2",
		}.Build(), nil
	}
	c.count(func(ct *equivCounters) {
		if verdict == equivFresh {
			ct.pagesFresh++
		} else {
			ct.pagesColdStale++
		}
	})
	return v2.ResourcesServiceListResourcesResponse_builder{
		List:        buildRows(userIDs[half:]),
		Annotations: equivScopeAnnos(scope, version),
	}.Build(), nil
}

// resourcesPage implements the fresh/304/stale protocol for a resources
// scope (no deltas: resources scopes refetch fully on change).
func (c *equivConnector) resourcesPage(ctx context.Context, lookup sourcecache.Lookup, scope string, version int, build func() []*v2.Resource) (*v2.ResourcesServiceListResourcesResponse, error) {
	verdict, err := c.revalidate(ctx, lookup, sourcecache.RowKindResources, sourcecache.HashScope(scope), version)
	if err != nil {
		return nil, err
	}
	if verdict == equivCurrent {
		c.count(func(ct *equivCounters) { ct.pages304++ })
		return v2.ResourcesServiceListResourcesResponse_builder{
			Annotations: equivReplayAnnos(scope, version),
		}.Build(), nil
	}
	c.count(func(ct *equivCounters) {
		if verdict == equivFresh {
			ct.pagesFresh++
		} else {
			ct.pagesColdStale++
		}
	})
	return v2.ResourcesServiceListResourcesResponse_builder{
		List:        build(),
		Annotations: equivScopeAnnos(scope, version),
	}.Build(), nil
}

func (c *equivConnector) ListEntitlements(ctx context.Context, in *v2.EntitlementsServiceListEntitlementsRequest, opts ...grpc.CallOption) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	lookup, cl, err := c.requestLookup(in.GetAnnotations())
	if err != nil {
		return nil, err
	}
	resp, err := c.listEntitlementsInner(ctx, lookup, in)
	if isDeferred(cl, err) {
		return v2.EntitlementsServiceListEntitlementsResponse_builder{
			Annotations: c.askAnnos(cl),
		}.Build(), nil
	}
	return resp, err
}

func (c *equivConnector) listEntitlementsInner(
	ctx context.Context,
	lookup sourcecache.Lookup,
	in *v2.EntitlementsServiceListEntitlementsRequest,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	rid := in.GetResource().GetId()
	if rid.GetResourceType() == equivTeamRT.GetId() {
		// Route on the REQUEST marker, not just the resource type: a
		// syncer bug that drops the TypeScopedEntitlements annotation
		// (e.g. on a continuation bounce or spawned cursor) must fail
		// the harness, not silently keep working.
		reqAnnos := annotations.Annotations(in.GetAnnotations())
		if !reqAnnos.Contains(&v2.TypeScopedEntitlements{}) {
			return nil, fmt.Errorf("team entitlements call missing TypeScopedEntitlements request marker (page token %q)", in.GetPageToken())
		}
		return c.teamEntitlementsPage(ctx, lookup, in.GetPageToken())
	}
	if rid.GetResourceType() != groupResourceType.GetId() {
		return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
	}
	c.mu.Lock()
	g := c.model.groups[rid.GetResource()]
	c.mu.Unlock()
	if g == nil {
		return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
	}
	scope := "ents:" + g.id
	// Entitlement scopes never rotate after creation: warm rounds are
	// pure 304s, which is exactly what keeps exclusion-group state on
	// the replayed path.
	verdict, err := c.revalidate(ctx, lookup, sourcecache.RowKindEntitlements, sourcecache.HashScope(scope), 0)
	if err != nil {
		return nil, err
	}
	if verdict == equivCurrent {
		c.count(func(ct *equivCounters) { ct.pages304++ })
		return v2.EntitlementsServiceListEntitlementsResponse_builder{
			Annotations: equivReplayAnnos(scope, 0),
		}.Build(), nil
	}
	c.count(func(ct *equivCounters) { ct.pagesFresh++ })
	return v2.EntitlementsServiceListEntitlementsResponse_builder{
		List:        []*v2.Entitlement{equivMemberEnt(equivGroupResource(g.id), g.exclusionDefault)},
		Annotations: equivScopeAnnos(scope, 0),
	}.Build(), nil
}

func (c *equivConnector) ListGrants(ctx context.Context, in *v2.GrantsServiceListGrantsRequest, opts ...grpc.CallOption) (*v2.GrantsServiceListGrantsResponse, error) {
	rid := in.GetResource().GetId()
	c.mu.Lock()
	if c.failNext["grants:"+rid.GetResource()] {
		delete(c.failNext, "grants:"+rid.GetResource())
		c.counters.injectedFailures++
		c.mu.Unlock()
		// codes.Internal: NOT in the syncer's retryable set (only
		// Unavailable/DeadlineExceeded are), so the sync dies here —
		// the process-crash analog the resume path exists for.
		return nil, status.Error(codes.Internal, "injected crash: simulated mid-sync failure")
	}
	c.mu.Unlock()
	lookup, cl, err := c.requestLookup(in.GetAnnotations())
	if err != nil {
		return nil, err
	}
	var resp *v2.GrantsServiceListGrantsResponse
	switch rid.GetResourceType() {
	case groupResourceType.GetId():
		resp, err = c.groupGrantsPage(ctx, lookup, rid.GetResource(), in.GetPageToken())
	case equivOrgRT.GetId():
		resp, err = c.orgGrantsPage(ctx, lookup, rid.GetResource())
	case equivTeamRT.GetId():
		// Same marker enforcement as listEntitlementsInner: dropping the
		// TypeScopedGrants request marker must fail the harness.
		reqAnnos := annotations.Annotations(in.GetAnnotations())
		if !reqAnnos.Contains(&v2.TypeScopedGrants{}) {
			return nil, fmt.Errorf("team grants call missing TypeScopedGrants request marker (page token %q)", in.GetPageToken())
		}
		resp, err = c.teamGrantsPage(ctx, lookup, in.GetPageToken())
	default:
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	if isDeferred(cl, err) {
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: c.askAnnos(cl),
		}.Build(), nil
	}
	return resp, err
}

// teamEntitlementsPage mirrors teamGrantsPage for type-scoped
// entitlements: planning EnqueuePageTokens, then one fresh-or-304 scope per
// chunk of two teams. Entitlement rows are stable (one "member" per
// team); version stays 0 so warm rounds are pure 304s after the cold
// fetch — exercising type-scoped entitlement replay in the differential
// chain without coupling to membership churn.
func (c *equivConnector) teamEntitlementsPage(ctx context.Context, lookup sourcecache.Lookup, pageToken string) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	if pageToken == "" {
		c.mu.Lock()
		chunks := len(c.model.teamChunkVersions)
		c.mu.Unlock()
		c.count(func(ct *equivCounters) { ct.plannerPages++ })
		tokens := make([]string, 0, chunks)
		for i := 0; i < chunks; i++ {
			tokens = append(tokens, fmt.Sprintf("ent-chunk:%d", i))
		}
		return v2.EntitlementsServiceListEntitlementsResponse_builder{
			Annotations: annotations.New(v2.EnqueuePageTokens_builder{PageTokens: tokens}.Build()),
		}.Build(), nil
	}
	var chunk int
	if _, err := fmt.Sscanf(pageToken, "ent-chunk:%d", &chunk); err != nil {
		return nil, fmt.Errorf("teamEntitlementsPage: bad page token %q: %w", pageToken, err)
	}
	c.mu.Lock()
	teamIDs := c.model.teamsInChunk(chunk)
	c.mu.Unlock()

	scope := fmt.Sprintf("ents:team-chunk:%d", chunk)
	verdict, err := c.revalidate(ctx, lookup, sourcecache.RowKindEntitlements, sourcecache.HashScope(scope), 0)
	if err != nil {
		return nil, err
	}
	if verdict == equivCurrent {
		c.count(func(ct *equivCounters) { ct.chunkPages304++ })
		return v2.EntitlementsServiceListEntitlementsResponse_builder{
			Annotations: equivReplayAnnos(scope, 0),
		}.Build(), nil
	}
	c.count(func(ct *equivCounters) { ct.chunkPagesFresh++ })
	rows := make([]*v2.Entitlement, 0, len(teamIDs))
	for _, tid := range teamIDs {
		rows = append(rows, equivMemberEnt(equivTeamResource(tid), false))
	}
	return v2.EntitlementsServiceListEntitlementsResponse_builder{
		List:        rows,
		Annotations: equivScopeAnnos(scope, 0),
	}.Build(), nil
}

// teamGrantsPage is the type-scoped planner shape: the planning call
// (empty page token) emits EnqueuePageTokens — one token per chunk of two
// teams — and each spawned cursor serves its chunk as a fresh-or-304
// scope.
func (c *equivConnector) teamGrantsPage(ctx context.Context, lookup sourcecache.Lookup, pageToken string) (*v2.GrantsServiceListGrantsResponse, error) {
	if pageToken == "" {
		c.mu.Lock()
		chunks := len(c.model.teamChunkVersions)
		c.mu.Unlock()
		c.count(func(ct *equivCounters) { ct.plannerPages++ })
		tokens := make([]string, 0, chunks)
		for i := 0; i < chunks; i++ {
			tokens = append(tokens, fmt.Sprintf("chunk:%d", i))
		}
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: annotations.New(v2.EnqueuePageTokens_builder{PageTokens: tokens}.Build()),
		}.Build(), nil
	}
	var chunk int
	if _, err := fmt.Sscanf(pageToken, "chunk:%d", &chunk); err != nil {
		return nil, fmt.Errorf("teamGrantsPage: bad page token %q: %w", pageToken, err)
	}
	c.mu.Lock()
	m := c.model
	version := m.teamChunkVersions[chunk]
	teamIDs := m.teamsInChunk(chunk)
	membersByTeam := make(map[string][]string, len(teamIDs))
	for _, tid := range teamIDs {
		membersByTeam[tid] = sortedKeys(m.teams[tid])
	}
	c.mu.Unlock()

	scope := fmt.Sprintf("grants:team-chunk:%d", chunk)
	verdict, err := c.revalidate(ctx, lookup, sourcecache.RowKindGrants, sourcecache.HashScope(scope), version)
	if err != nil {
		return nil, err
	}
	if verdict == equivCurrent {
		c.count(func(ct *equivCounters) { ct.chunkPages304++ })
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: equivReplayAnnos(scope, version),
		}.Build(), nil
	}
	c.count(func(ct *equivCounters) { ct.chunkPagesFresh++ })
	var rows []*v2.Grant
	for _, tid := range teamIDs {
		for _, uid := range membersByTeam[tid] {
			rows = append(rows, equivTeamGrant(tid, uid))
		}
	}
	return v2.GrantsServiceListGrantsResponse_builder{
		List:        rows,
		Annotations: equivScopeAnnos(scope, version),
	}.Build(), nil
}

// groupGrantsPage: delta-scope protocol for group membership (and the
// external-match group, which is fresh-or-304 only — the model never
// mixes tombstones into it). The multiPageGroup serves its overlay
// rounds across TWO pages: page 1 carries the replay annotation
// (overlay, no validator) plus the first add; page 2 carries the scope
// annotation with the remaining adds, the tombstones, and the NEW
// validator — the page-spanning delta contract.
func (c *equivConnector) groupGrantsPage(ctx context.Context, lookup sourcecache.Lookup, gid string, pageToken string) (*v2.GrantsServiceListGrantsResponse, error) {
	c.mu.Lock()
	m := c.model
	g := m.groups[gid]
	if g == nil {
		c.mu.Unlock()
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	isExt := gid == m.extGroupID
	isExpandableParent := gid == m.expandableParent
	isMultiPage := gid == m.multiPageGroup
	expandableChild := m.expandableChild
	version := g.version
	members := sortedKeys(g.members)
	deltaAdds := append([]string{}, g.deltaAdds...)
	deltaRemoves := append([]string{}, g.deltaRemoves...)
	canonical := g.canonicalTombstones
	extMatchIDs := append([]string{}, m.extMatchIDs...)
	if isExt {
		version = m.extVersion
	}
	c.mu.Unlock()

	scope := "grants:" + gid
	fullRows := func() []*v2.Grant {
		var out []*v2.Grant
		if isExt {
			groupRes := equivGroupResource(gid)
			for _, extID := range extMatchIDs {
				out = append(out, gt.NewGrant(groupRes, "member",
					v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: "placeholder-" + extID}.Build(),
					gt.WithAnnotation(v2.ExternalResourceMatchID_builder{Id: extID}.Build()),
				))
			}
			out = append(out, gt.NewGrant(groupRes, "member",
				v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: "placeholder-all"}.Build(),
				gt.WithAnnotation(v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_USER}.Build()),
			))
			return out
		}
		for _, uid := range members {
			out = append(out, equivMemberGrant(gid, uid))
		}
		if isExpandableParent {
			childRes := equivGroupResource(expandableChild)
			childEnt := equivMemberEnt(childRes, false)
			out = append(out, gt.NewGrant(equivGroupResource(gid), "member", childRes,
				gt.WithAnnotation(v2.GrantExpandable_builder{EntitlementIds: []string{childEnt.GetId()}}.Build()),
			))
		}
		return out
	}

	verdict, err := c.revalidate(ctx, lookup, sourcecache.RowKindGrants, sourcecache.HashScope(scope), version)
	if err != nil {
		return nil, err
	}
	switch verdict {
	case equivCurrent:
		c.count(func(ct *equivCounters) { ct.pages304++ })
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: equivReplayAnnos(scope, version),
		}.Build(), nil
	case equivOneBehind:
		if isExt || (len(deltaAdds) == 0 && len(deltaRemoves) == 0) {
			// No delta journal for this transition: full refetch.
			break
		}
		adds := make([]*v2.Grant, 0, len(deltaAdds))
		for _, uid := range deltaAdds {
			adds = append(adds, equivMemberGrant(gid, uid))
		}
		if isMultiPage {
			if pageToken == "" {
				// Overlay page 1: replay annotation (overlay, NO
				// validator yet) plus the first add.
				c.count(func(ct *equivCounters) { ct.pagesOverlayInterim++ })
				firstAdds := adds
				if len(firstAdds) > 1 {
					firstAdds = adds[:1]
				}
				return v2.GrantsServiceListGrantsResponse_builder{
					List: firstAdds,
					Annotations: annotations.New(v2.SourceCacheReplay_builder{
						ScopeKey: sourcecache.HashScope(scope),
						Overlay:  true,
					}.Build()),
					NextPageToken: "ov:2",
				}.Build(), nil
			}
			// Overlay final page: remaining adds + tombstones + the NEW
			// validator, all on the scope annotation.
			c.count(func(ct *equivCounters) { ct.pagesOverlay++ })
			var restAdds []*v2.Grant
			if len(adds) > 1 {
				restAdds = adds[1:]
			}
			finalAnno := v2.SourceCacheRecord_builder{
				ScopeKey:       sourcecache.HashScope(scope),
				CacheValidator: equivEtag(version),
			}.Build()
			if canonical {
				ids := make([]string, 0, len(deltaRemoves))
				for _, uid := range deltaRemoves {
					ids = append(ids, equivMemberGrant(gid, uid).GetId())
				}
				finalAnno.SetDeletedIds(ids)
				c.count(func(ct *equivCounters) { ct.tombsCanonical += len(ids) })
			} else {
				finalAnno.SetDeletedPrincipalIds(deltaRemoves)
				c.count(func(ct *equivCounters) { ct.tombsPrincipal += len(deltaRemoves) })
			}
			return v2.GrantsServiceListGrantsResponse_builder{
				List:        restAdds,
				Annotations: annotations.New(finalAnno),
			}.Build(), nil
		}
		c.count(func(ct *equivCounters) { ct.pagesOverlay++ })
		replay := v2.SourceCacheReplay_builder{
			ScopeKey:       sourcecache.HashScope(scope),
			CacheValidator: equivEtag(version),
			Overlay:        true,
		}.Build()
		scopeAnno := v2.SourceCacheRecord_builder{
			ScopeKey: sourcecache.HashScope(scope),
		}.Build()
		if canonical {
			ids := make([]string, 0, len(deltaRemoves))
			for _, uid := range deltaRemoves {
				ids = append(ids, equivMemberGrant(gid, uid).GetId())
			}
			replay.SetDeletedIds(ids)
			c.count(func(ct *equivCounters) { ct.tombsCanonical += len(ids) })
		} else {
			scopeAnno.SetDeletedPrincipalIds(deltaRemoves)
			c.count(func(ct *equivCounters) { ct.tombsPrincipal += len(deltaRemoves) })
		}
		return v2.GrantsServiceListGrantsResponse_builder{
			List:        adds,
			Annotations: annotations.New(replay, scopeAnno),
		}.Build(), nil
	default:
	}
	c.count(func(ct *equivCounters) {
		if verdict == equivFresh {
			ct.pagesFresh++
		} else {
			ct.pagesColdStale++
		}
	})
	return v2.GrantsServiceListGrantsResponse_builder{
		List:        fullRows(),
		Annotations: equivScopeAnnos(scope, version),
	}.Build(), nil
}

// orgGrantsPage: the InsertResourceGrants scope (fresh-or-304).
func (c *equivConnector) orgGrantsPage(ctx context.Context, lookup sourcecache.Lookup, orgID string) (*v2.GrantsServiceListGrantsResponse, error) {
	c.mu.Lock()
	org := c.model.orgs[orgID]
	if org == nil {
		c.mu.Unlock()
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	version := org.repoVersion
	repoGrants := make(map[string]string, len(org.repoGrants))
	for k, v := range org.repoGrants {
		repoGrants[k] = v
	}
	c.mu.Unlock()

	scope := "grants:org:" + orgID
	verdict, err := c.revalidate(ctx, lookup, sourcecache.RowKindGrants, sourcecache.HashScope(scope), version)
	if err != nil {
		return nil, err
	}
	if verdict == equivCurrent {
		c.count(func(ct *equivCounters) { ct.pages304++ })
		return v2.GrantsServiceListGrantsResponse_builder{
			Annotations: equivReplayAnnos(scope, version),
		}.Build(), nil
	}
	c.count(func(ct *equivCounters) {
		if verdict == equivFresh {
			ct.pagesFresh++
		} else {
			ct.pagesColdStale++
		}
	})
	rows := make([]*v2.Grant, 0, len(repoGrants))
	for _, repoID := range sortedKeys(repoGrants) {
		rows = append(rows, gt.NewGrant(equivRepoResource(repoID), "admin",
			v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: repoGrants[repoID]}.Build()))
	}
	annos := equivScopeAnnos(scope, version)
	annos.Update(&v2.InsertResourceGrants{})
	return v2.GrantsServiceListGrantsResponse_builder{
		List:        rows,
		Annotations: annos,
	}.Build(), nil
}

// ---------------------------------------------------------------------
// External c1z
// ---------------------------------------------------------------------

// buildExternalC1z materializes the model's external users into a fresh
// c1z (rebuilt whenever extUsers changes; both warm and cold syncs of a
// round read the same file).
func buildExternalC1z(ctx context.Context, t *testing.T, m *equivModel, path, tmpDir string) {
	t.Helper()
	mc := newMockConnector()
	mc.rtDB = append(mc.rtDB, userResourceType, groupResourceType)
	for _, extID := range sortedKeys(m.extUsers) {
		_, err := mc.AddUserProfile(ctx, extID, map[string]any{})
		require.NoError(t, err)
	}
	syncer, err := NewSyncer(ctx, mc, WithC1ZPath(path), WithTmpDir(tmpDir))
	require.NoError(t, err)
	require.NoError(t, syncer.Sync(ctx))
	require.NoError(t, syncer.Close(ctx))
}

// ---------------------------------------------------------------------
// Snapshot + comparison
// ---------------------------------------------------------------------

type c1zSnapshot struct {
	resourceTypes map[string]*v2.ResourceType
	resources     map[string]*v2.Resource // "rt/id"
	entitlements  map[string]*v2.Entitlement
	grants        map[string]*v2.Grant
	// byParent: parent "rt/id" -> child resource keys via the by_parent
	// index read path (cross-index consistency).
	byParent map[string][]string
	// grantsByEntResource: entitlement-resource "rt/id" -> grant ids via
	// the by_entitlement_resource index read path.
	grantsByEntResource map[string][]string
	// manifest ("row_kind\x00scope_key" -> etag) and scopeIndexCounts
	// (row kind -> scope key -> index entries): the source-cache state
	// invisible to v2 reads but load-bearing for the NEXT sync's replay.
	manifest         map[string]string
	scopeIndexCounts map[string]map[string]int
	// syncRuns is the number of sync runs in the file — the
	// crash/resume round asserts exactly one (resume, not restart).
	syncRuns int
}

func resourceKey(id *v2.ResourceId) string { return id.GetResourceType() + "/" + id.GetResource() }

func snapshotC1z(ctx context.Context, t *testing.T, path string) *c1zSnapshot {
	t.Helper()
	reopen, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { _ = reopen.Close(ctx) }()

	syncRun, err := reopen.SyncMeta().LatestFullSync(ctx)
	require.NoError(t, err)
	require.NotNil(t, syncRun)
	require.NoError(t, reopen.SetCurrentSync(ctx, syncRun.ID))

	snap := &c1zSnapshot{
		resourceTypes:       map[string]*v2.ResourceType{},
		resources:           map[string]*v2.Resource{},
		entitlements:        map[string]*v2.Entitlement{},
		grants:              map[string]*v2.Grant{},
		byParent:            map[string][]string{},
		grantsByEntResource: map[string][]string{},
	}

	// Raw row counts per listing: the deduping maps below would silently
	// hide duplicate rows (a pagination double-yield or a replay writing
	// two rows under one identity), so every listing asserts raw == map.
	pageToken := ""
	rawRows := 0
	for {
		resp, err := reopen.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, rt := range resp.GetList() {
			snap.resourceTypes[rt.GetId()] = rt
			rawRows++
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	require.Equal(t, len(snap.resourceTypes), rawRows, "duplicate resource-type rows in %s", path)
	rawRows = 0
	for {
		resp, err := reopen.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, r := range resp.GetList() {
			snap.resources[resourceKey(r.GetId())] = r
			rawRows++
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	require.Equal(t, len(snap.resources), rawRows, "duplicate resource rows in %s", path)
	rawRows = 0
	for {
		resp, err := reopen.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, e := range resp.GetList() {
			snap.entitlements[e.GetId()] = e
			rawRows++
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	require.Equal(t, len(snap.entitlements), rawRows, "duplicate entitlement rows in %s", path)
	rawRows = 0
	for {
		resp, err := reopen.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{PageToken: pageToken}.Build())
		require.NoError(t, err)
		for _, g := range resp.GetList() {
			snap.grants[g.GetId()] = g
			rawRows++
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	require.Equal(t, len(snap.grants), rawRows, "duplicate grant rows in %s", path)

	// Source-cache state below the v2 read surface, plus the sync-run
	// count (crash/resume must resume, never restart).
	engineHolder, ok := any(reopen).(interface{ PebbleEngine() *pebbleengine.Engine })
	require.True(t, ok, "store must expose its pebble engine for source-cache inspection")
	snap.manifest, err = engineHolder.PebbleEngine().SourceCacheManifestSnapshot(ctx)
	require.NoError(t, err)
	snap.scopeIndexCounts, err = engineHolder.PebbleEngine().SourceScopeIndexSnapshot(ctx)
	require.NoError(t, err)
	runLister, ok := any(reopen).(interface {
		ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
	})
	require.True(t, ok, "store must list sync runs")
	runs, _, err := runLister.ListSyncRuns(ctx, "", 100)
	require.NoError(t, err)
	snap.syncRuns = len(runs)

	// Secondary read paths: by-parent resource listings for every parent
	// that has children per the primary rows, and by-entitlement-resource
	// grant listings for every entitlement resource seen in grants.
	parents := map[string]*v2.ResourceId{}
	for _, r := range snap.resources {
		if p := r.GetParentResourceId(); p.GetResource() != "" {
			parents[resourceKey(p)] = p
		}
	}
	for pk, pid := range parents {
		for {
			resp, err := reopen.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
				ResourceTypeId:   "",
				ParentResourceId: pid,
				PageToken:        pageToken,
			}.Build())
			require.NoError(t, err)
			for _, r := range resp.GetList() {
				snap.byParent[pk] = append(snap.byParent[pk], resourceKey(r.GetId()))
			}
			if pageToken = resp.GetNextPageToken(); pageToken == "" {
				break
			}
		}
		sort.Strings(snap.byParent[pk])
	}
	entResources := map[string]*v2.Resource{}
	for _, g := range snap.grants {
		res := g.GetEntitlement().GetResource()
		if res.GetId() != nil {
			entResources[resourceKey(res.GetId())] = res
		}
	}
	for rk, res := range entResources {
		for {
			resp, err := reopen.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
				Resource:  res,
				PageToken: pageToken,
			}.Build())
			require.NoError(t, err)
			for _, g := range resp.GetList() {
				snap.grantsByEntResource[rk] = append(snap.grantsByEntResource[rk], g.GetId())
			}
			if pageToken = resp.GetNextPageToken(); pageToken == "" {
				break
			}
		}
		sort.Strings(snap.grantsByEntResource[rk])
	}
	return snap
}

// normalizeMessage returns a clone with annotations sorted so proto
// equality ignores annotation ordering (the only legitimate ordering
// difference between cold and warm outputs).
func normalizeMessage[T proto.Message](msg T, get func(T) []*anypb.Any, set func(T, []*anypb.Any)) T {
	clone, _ := proto.Clone(msg).(T)
	annos := get(clone)
	if len(annos) > 1 {
		sorted := append([]*anypb.Any{}, annos...)
		sort.Slice(sorted, func(i, j int) bool {
			if sorted[i].GetTypeUrl() != sorted[j].GetTypeUrl() {
				return sorted[i].GetTypeUrl() < sorted[j].GetTypeUrl()
			}
			return string(sorted[i].GetValue()) < string(sorted[j].GetValue())
		})
		set(clone, sorted)
	}
	return clone
}

func diffMaps[T proto.Message](t *testing.T, label string, cold, warm map[string]T, normalize func(T) T) {
	t.Helper()
	for _, k := range sortedKeys(cold) {
		w, ok := warm[k]
		if !ok {
			t.Errorf("replay equivalence: %s %q present in COLD output but MISSING from warm output", label, k)
			continue
		}
		nc, nw := normalize(cold[k]), normalize(w)
		if !proto.Equal(nc, nw) {
			t.Errorf("replay equivalence: %s %q differs\n--- cold ---\n%s\n--- warm ---\n%s",
				label, k, prototext.Format(nc), prototext.Format(nw))
		}
	}
	for _, k := range sortedKeys(warm) {
		if _, ok := cold[k]; !ok {
			t.Errorf("replay equivalence: %s %q present in WARM output but missing from cold output (stale row survived?)", label, k)
		}
	}
}

func requireSnapshotsEqual(t *testing.T, round int, cold, warm *c1zSnapshot) {
	t.Helper()
	t.Logf("round %d: comparing cold (%d res, %d ents, %d grants) vs warm (%d res, %d ents, %d grants)",
		round, len(cold.resources), len(cold.entitlements), len(cold.grants),
		len(warm.resources), len(warm.entitlements), len(warm.grants))

	diffMaps(t, "resource type", cold.resourceTypes, warm.resourceTypes,
		func(m *v2.ResourceType) *v2.ResourceType {
			return normalizeMessage(m, (*v2.ResourceType).GetAnnotations, (*v2.ResourceType).SetAnnotations)
		})
	diffMaps(t, "resource", cold.resources, warm.resources,
		func(m *v2.Resource) *v2.Resource {
			return normalizeMessage(m, (*v2.Resource).GetAnnotations, (*v2.Resource).SetAnnotations)
		})
	diffMaps(t, "entitlement", cold.entitlements, warm.entitlements,
		func(m *v2.Entitlement) *v2.Entitlement {
			return normalizeMessage(m, (*v2.Entitlement).GetAnnotations, (*v2.Entitlement).SetAnnotations)
		})
	diffMaps(t, "grant", cold.grants, warm.grants,
		func(m *v2.Grant) *v2.Grant {
			return normalizeMessage(m, (*v2.Grant).GetAnnotations, (*v2.Grant).SetAnnotations)
		})

	require.Equal(t, cold.byParent, warm.byParent,
		"round %d: by-parent index read path diverged (replay index synthesis?)", round)
	require.Equal(t, cold.grantsByEntResource, warm.grantsByEntResource,
		"round %d: by-entitlement-resource index read path diverged (replay index synthesis?)", round)

	// Below the v2 read surface: both connectors emitted identical scope
	// annotations and validators, so the manifests and scope-index shapes
	// must match. Divergence here is invisible to every check above but
	// poisons the NEXT sync's replay from the warm file.
	require.Equal(t, cold.manifest, warm.manifest,
		"round %d: source-cache manifest diverged (phantom or missing entries poison future replays)", round)
	require.Equal(t, cold.scopeIndexCounts, warm.scopeIndexCounts,
		"round %d: source-cache scope-index shape diverged (stamp divergence poisons future replays)", round)

	if t.Failed() {
		t.Fatalf("round %d: warm sync output is not equivalent to a cold resync of the same upstream state", round)
	}
}

// assertNoStaleResources is the reverse-containment check: every resource
// row of a modeled type must correspond to CURRENT model state (or an
// external principal). The forward diff proves warm == cold; this proves
// neither side is carrying rows the upstream no longer has — the
// deletion-round oracle.
func assertNoStaleResources(t *testing.T, round int, m *equivModel, label string, snap *c1zSnapshot) {
	t.Helper()
	for key := range snap.resources {
		rt, id, _ := strings.Cut(key, "/")
		switch rt {
		case equivOrgRT.GetId():
			require.Contains(t, m.orgs, id, "round %d: %s output holds deleted org %s", round, label, id)
		case equivProjectRT.GetId():
			found := false
			for _, org := range m.orgs {
				if _, ok := org.projects[id]; ok {
					found = true
				}
			}
			require.True(t, found, "round %d: %s output holds deleted project %s", round, label, id)
		case equivRepoRT.GetId():
			found := false
			for _, org := range m.orgs {
				if _, ok := org.repoGrants[id]; ok {
					found = true
				}
			}
			require.True(t, found, "round %d: %s output holds deleted repo %s", round, label, id)
		case groupResourceType.GetId():
			require.Contains(t, m.groups, id, "round %d: %s output holds deleted group %s", round, label, id)
		case equivTeamRT.GetId():
			require.Contains(t, m.teams, id, "round %d: %s output holds unknown team %s", round, label, id)
		case userResourceType.GetId():
			require.True(t, m.users[id] || m.extUsers[id],
				"round %d: %s output holds deleted user %s", round, label, id)
		}
	}
}

// expectedResourceKeys derives the COMPLETE resource set ("rt/id" keys)
// the model implies: users (local + matched external principals), groups,
// orgs, per-org child projects, InsertResourceGrants repos, and teams.
// Exact set equality against the cold output removes the differential
// test's structural blind spot for the resources phase (see
// expectedGrantIDs).
func expectedResourceKeys(m *equivModel) []string {
	set := map[string]struct{}{}
	for uid := range m.users {
		set[userResourceType.GetId()+"/"+uid] = struct{}{}
	}
	// Every external user is matched by the MatchAll(USER) grant, so
	// every one is inserted as a principal resource.
	for extID := range m.extUsers {
		set[userResourceType.GetId()+"/"+extID] = struct{}{}
	}
	for gid := range m.groups {
		set[groupResourceType.GetId()+"/"+gid] = struct{}{}
	}
	for orgID, org := range m.orgs {
		set[equivOrgRT.GetId()+"/"+orgID] = struct{}{}
		for projID := range org.projects {
			set[equivProjectRT.GetId()+"/"+projID] = struct{}{}
		}
		for repoID := range org.repoGrants {
			set[equivRepoRT.GetId()+"/"+repoID] = struct{}{}
		}
	}
	for tid := range m.teams {
		set[equivTeamRT.GetId()+"/"+tid] = struct{}{}
	}
	return sortedKeys(set)
}

// expectedEntitlementIDs derives the COMPLETE entitlement set the model
// implies: one member entitlement per group (including the external-match
// group — its entitlement anchors the transformed grants) and one per
// type-scoped team. Users, projects, and repos are skip-annotated or
// never enumerated; orgs serve no entitlements; expansion and external
// processing create no entitlement rows.
func expectedEntitlementIDs(m *equivModel) []string {
	set := map[string]struct{}{}
	for gid, g := range m.groups {
		set[equivMemberEnt(equivGroupResource(gid), g.exclusionDefault).GetId()] = struct{}{}
	}
	for tid := range m.teams {
		set[equivMemberEnt(equivTeamResource(tid), false).GetId()] = struct{}{}
	}
	return sortedKeys(set)
}

// expectedGrantIDs derives the COMPLETE grant set the model implies:
// group memberships, the expandable membership plus its expansion-derived
// grants, match-transformed external grants, repo (InsertResourceGrants)
// grants, and type-scoped team memberships. Comparing the cold output
// against this — exact set equality, not spot checks — removes the
// differential test's structural blind spot: cold and warm share the
// syncer, so a bug corrupting both identically would otherwise pass.
func expectedGrantIDs(m *equivModel) []string {
	set := map[string]struct{}{}
	for gid, g := range m.groups {
		if gid == m.extGroupID {
			continue
		}
		for uid := range g.members {
			set[equivMemberGrant(gid, uid).GetId()] = struct{}{}
		}
	}
	// The expandable group-to-group membership and its derived grants.
	parentRes := equivGroupResource(m.expandableParent)
	parentEnt := equivMemberEnt(parentRes, false)
	set[gt.NewGrantID(equivGroupResource(m.expandableChild), parentEnt)] = struct{}{}
	for uid := range m.groups[m.expandableChild].members {
		set[gt.NewGrantID(
			v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: uid}.Build(),
			parentEnt)] = struct{}{}
	}
	// Match-transformed grants: MatchAll covers every external user (the
	// MatchID targets are a subset with identical resulting ids).
	extEnt := equivMemberEnt(equivGroupResource(m.extGroupID), false)
	for extID := range m.extUsers {
		set[gt.NewGrantID(
			v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: extID}.Build(),
			extEnt)] = struct{}{}
	}
	for _, org := range m.orgs {
		for repoID, uid := range org.repoGrants {
			set[gt.NewGrant(equivRepoResource(repoID), "admin",
				v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: uid}.Build()).GetId()] = struct{}{}
		}
	}
	for tid, members := range m.teams {
		for uid := range members {
			set[equivTeamGrant(tid, uid).GetId()] = struct{}{}
		}
	}
	return sortedKeys(set)
}

// ---------------------------------------------------------------------
// Coverage guards (anti-vacuity)
// ---------------------------------------------------------------------

// assertColdArtifacts fails unless the cold output actually contains the
// artifacts of every feature the harness claims to cover — a comparison
// against an output that never exercised a feature proves nothing about
// that feature.
func assertColdArtifacts(t *testing.T, round int, m *equivModel, snap *c1zSnapshot) {
	t.Helper()
	for _, orgID := range sortedKeys(m.orgs) {
		for projID := range m.orgs[orgID].projects {
			require.Contains(t, snap.resources, equivProjectRT.GetId()+"/"+projID,
				"round %d: coverage: child project %s must be present", round, projID)
		}
		for repoID := range m.orgs[orgID].repoGrants {
			require.Contains(t, snap.resources, equivRepoRT.GetId()+"/"+repoID,
				"round %d: coverage: InsertResourceGrants repo %s must be present", round, repoID)
		}
	}
	// Expansion-derived grants: every member of the expandable child must
	// hold a derived grant on the parent.
	parentRes := equivGroupResource(m.expandableParent)
	for uid := range m.groups[m.expandableChild].members {
		derived := gt.NewGrantID(
			v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: uid}.Build(),
			equivMemberEnt(parentRes, false))
		require.Contains(t, snap.grants, derived,
			"round %d: coverage: expansion-derived grant for %s must be present", round, uid)
	}
	// Match-transformed grants: MatchAll must cover every external user;
	// MatchID entries must cover exactly the still-existing targets.
	extGroupRes := equivGroupResource(m.extGroupID)
	extEnt := equivMemberEnt(extGroupRes, false)
	for extID := range m.extUsers {
		transformed := gt.NewGrantID(
			v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: extID}.Build(), extEnt)
		require.Contains(t, snap.grants, transformed,
			"round %d: coverage: MatchAll-transformed grant for %s must be present", round, extID)
	}
	for _, extID := range m.extMatchIDs {
		transformed := gt.NewGrantID(
			v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: extID}.Build(), extEnt)
		if m.extUsers[extID] {
			require.Contains(t, snap.grants, transformed,
				"round %d: coverage: MatchID-transformed grant for %s must be present", round, extID)
		} else {
			require.NotContains(t, snap.grants, transformed,
				"round %d: coverage: MatchID grant for REMOVED external user %s must be absent", round, extID)
		}
	}
	// No placeholder principals may survive the transformation.
	for id := range snap.grants {
		require.NotContains(t, id, "placeholder-",
			"round %d: coverage: untransformed placeholder grant leaked into output", round)
	}
	// The independent oracle: the cold output's resource, entitlement,
	// and grant sets must EXACTLY equal the model-derived expectations —
	// the checks that don't trust the syncer code both outputs share.
	require.Equal(t, expectedResourceKeys(m), sortedKeys(snap.resources),
		"round %d: cold output's resource set must equal the model-derived expectation", round)
	require.Equal(t, expectedEntitlementIDs(m), sortedKeys(snap.entitlements),
		"round %d: cold output's entitlement set must equal the model-derived expectation", round)
	require.Equal(t, expectedGrantIDs(m), sortedKeys(snap.grants),
		"round %d: cold output's grant set must equal the model-derived expectation", round)
}

// ---------------------------------------------------------------------
// The harness
// ---------------------------------------------------------------------

const equivRounds = 4

// equivCrashRound is the round whose WARM sync is crashed mid-grants-phase
// (injected connector failure) and resumed into the same file.
const equivCrashRound = 2

// runEquivSync runs one sync into path (creating or RESUMING the file's
// unfinished sync), returning Sync's error. Path-based store creation —
// not WithConnectorStore — so a crashed sync's file can be reopened and
// resumed exactly like production resume.
func runEquivSync(ctx context.Context, t *testing.T, cc types.ConnectorClient, path, prevPath, tmpDir string, extraOpts ...SyncOpt) error {
	t.Helper()
	opts := []SyncOpt{
		WithC1ZPath(path),
		WithStorageEngine(c1zstore.EnginePebble),
		WithTmpDir(tmpDir),
		// Check-only ingestion invariants hard-fail in the harness: a
		// violation must name itself here before the differential
		// comparison would catch its downstream divergence.
		WithStrictIngestionInvariants(),
	}
	if prevPath != "" {
		opts = append(opts, WithPreviousSyncC1ZPath(prevPath))
	}
	opts = append(opts, extraOpts...)
	syncer, err := NewSyncer(ctx, cc, opts...)
	require.NoError(t, err)
	syncErr := syncer.Sync(ctx)
	if syncErr != nil {
		// Close under a canceled context so the unfinished sync's state
		// survives for resumption (same pattern as the resume tests).
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_ = syncer.Close(cctx)
		return syncErr
	}
	require.NoError(t, syncer.Close(ctx))
	return nil
}

func TestSourceCache_ReplayEquivalence(t *testing.T) {
	scenarios := []struct {
		workers      int
		seed         int64
		continuation bool
	}{
		{workers: 0, seed: 1},
		{workers: 0, seed: 2},
		{workers: 4, seed: 3},
		// The single-shot (Lambda) topology: lookups ride the ask/answer
		// continuation instead of a direct SetSourceCache install.
		{workers: 0, seed: 4, continuation: true},
		{workers: 4, seed: 5, continuation: true},
	}
	for _, sc := range scenarios {
		name := fmt.Sprintf("workers=%d/seed=%d", sc.workers, sc.seed)
		if sc.continuation {
			name += "/continuation"
		}
		t.Run(name, func(t *testing.T) {
			runReplayEquivalenceScenario(t, sc.workers, sc.seed, sc.continuation)
		})
	}
}

func runReplayEquivalenceScenario(t *testing.T, workers int, seed int64, continuation bool) {
	ctx, err := logging.Init(t.Context())
	require.NoError(t, err)
	tmpDir := t.TempDir()
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test randomness
	t.Logf("replay-equivalence scenario: workers=%d seed=%d rounds=%d continuation=%v", workers, seed, equivRounds, continuation)

	model := newEquivModel()
	warmConn := newEquivConnector(model, false)
	coldConn := newEquivConnector(model, true)
	warmConn.continuation = continuation
	coldConn.continuation = continuation
	var warmClient types.ConnectorClient = warmConn
	var coldClient types.ConnectorClient = coldConn
	if continuation {
		warmClient = equivContinuationClient{warmConn}
		coldClient = equivContinuationClient{coldConn}
	}

	extPath := filepath.Join(tmpDir, "external-r0.c1z")
	buildExternalC1z(ctx, t, model, extPath, tmpDir)
	model.extDirty = false

	syncOpts := func() []SyncOpt {
		opts := []SyncOpt{WithExternalResourceC1ZPath(extPath)}
		if workers > 0 {
			opts = append(opts, WithWorkerCount(workers))
		}
		return opts
	}

	prevWarm := ""
	for round := 0; round <= equivRounds; round++ {
		if round > 0 {
			model.mutate(round, rng)
			if model.extDirty {
				extPath = filepath.Join(tmpDir, fmt.Sprintf("external-r%d.c1z", round))
				buildExternalC1z(ctx, t, model, extPath, tmpDir)
			}
		}
		warmPath := filepath.Join(tmpDir, fmt.Sprintf("warm-r%d.c1z", round))
		coldPath := filepath.Join(tmpDir, fmt.Sprintf("cold-r%d.c1z", round))

		before := warmConn.snapshotCounters()
		if round == equivCrashRound {
			// Crash the warm sync mid-grants-phase (after other scopes
			// replayed), then resume the SAME file. The resumed output —
			// pages re-run over partially replayed state — is what gets
			// compared.
			warmConn.injectGrantsFailure(model.expandableChild)
			crashErr := runEquivSync(ctx, t, warmClient, warmPath, prevWarm, tmpDir, syncOpts()...)
			require.Error(t, crashErr, "round %d: the injected failure must crash the warm sync", round)
			require.Contains(t, crashErr.Error(), "injected crash")
			require.Equal(t, 1, warmConn.snapshotCounters().injectedFailures-before.injectedFailures,
				"round %d: exactly one injected failure expected", round)
		}
		require.NoError(t, runEquivSync(ctx, t, warmClient, warmPath, prevWarm, tmpDir, syncOpts()...))
		after := warmConn.snapshotCounters()
		if round > 0 {
			require.Positive(t, (after.pages304+after.pagesOverlay)-(before.pages304+before.pagesOverlay),
				"round %d: VACUOUS RUN — the warm sync replayed nothing, so equivalence proves nothing", round)
		}
		require.NoError(t, runEquivSync(ctx, t, coldClient, coldPath, "", tmpDir, syncOpts()...))

		coldSnap := snapshotC1z(ctx, t, coldPath)
		warmSnap := snapshotC1z(ctx, t, warmPath)
		if round == equivCrashRound {
			require.Equal(t, 1, warmSnap.syncRuns,
				"round %d: the crashed warm sync must RESUME (one sync run), not restart", round)
		}
		assertColdArtifacts(t, round, model, coldSnap)
		assertNoStaleResources(t, round, model, "cold", coldSnap)
		assertNoStaleResources(t, round, model, "warm", warmSnap)
		requireSnapshotsEqual(t, round, coldSnap, warmSnap)

		prevWarm = warmPath
	}

	// Scenario-level coverage: every replay behavior must have actually
	// occurred, or this scenario silently tested less than it claims.
	final := warmConn.snapshotCounters()
	require.Positive(t, final.pages304, "coverage: no 304 replay round ever happened")
	require.Positive(t, final.pagesOverlay, "coverage: no delta-overlay round ever happened")
	require.Positive(t, final.pagesOverlayInterim, "coverage: no multi-page overlay round ever happened")
	require.Positive(t, final.pagesColdStale, "coverage: no validator rotation (cold refetch) ever happened")
	require.Positive(t, final.pagesFresh, "coverage: no cold-miss page ever happened")
	require.Positive(t, final.pagesFreshInterim, "coverage: no multi-page fresh fetch ever happened")
	require.Positive(t, final.tombsCanonical, "coverage: canonical-id tombstones never exercised")
	require.Positive(t, final.tombsPrincipal, "coverage: principal-id tombstones never exercised")
	require.Positive(t, final.childPageCalls, "coverage: child resource pages never fetched")
	require.Positive(t, final.injectedFailures, "coverage: the crash/resume round never crashed")
	require.Positive(t, final.plannerPages, "coverage: the type-scoped planner never ran")
	require.Positive(t, final.chunkPages304, "coverage: no spawned chunk cursor ever replayed")
	require.Positive(t, final.chunkPagesFresh, "coverage: no spawned chunk cursor ever fetched fresh")
	if continuation {
		require.Positive(t, final.askPages, "coverage: continuation topology never asked")
	} else {
		require.Zero(t, final.askPages, "harness bug: direct topology must never ask")
	}
	coldFinal := coldConn.snapshotCounters()
	require.Zero(t, coldFinal.pages304+coldFinal.pagesOverlay,
		"harness bug: the cold connector must never replay")
	require.Zero(t, coldFinal.askPages, "harness bug: the cold connector must never ask (no offer without a warm lookup)")
}
