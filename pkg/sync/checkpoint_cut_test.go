package sync //nolint:revive,nolintlint // backwards-compatible package name

// Checkpoint-cut enumeration harness (Option B of the verification plan).
//
// The crash-cut contract — from ANY durable checkpoint plus whatever store
// state exists, a cold resume completes to exactly the right sealed store —
// was previously tested at a handful of hand-picked cut points. This
// harness mechanizes the sweep over a stated space:
//
//   - CHECKPOINT cuts: the sync is killed immediately after its Nth durable
//     checkpoint, for every N the sync produces (checkpointInterval is
//     zeroed so every loop-top checkpoint commits — each one is a cut
//     point). These are quiet-point crashes: token and store agree.
//   - RESPONSE cuts: the sync is killed right after the Mth connector
//     response, for every M — mid-batch, workers in flight, store writes
//     landed PAST the last checkpointed token. These are the adversarial
//     crashes: the resumer must tolerate a store ahead of its token and
//     redo in-flight fan-out work exactly once.
//
// Each cut is followed by a cold resume (fresh store handle, fresh syncer —
// the distributed-execution assumption) under a DIFFERENT worker count than
// the generator, and every third cut gets a double-cut: the first resume is
// itself killed at its first checkpoint before a second resume completes,
// exercising the restored-state re-checkpoint path. The oracle is derived
// ground truth: the topology makes the exact entitlement/grant sets
// computable, so any lost, duplicated, or misrouted cursor is a set
// mismatch. Every attempt also runs the queue-contract audit
// (verifyQueueAudit).
//
// The default sweep is strided to cap runtime; BATON_CUT_SWEEP=full covers
// every cut point.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	native_sync "sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

// errInjectedCut is the cancellation cause for a simulated crash; the
// harness accepts a failed sync only when this cause is present, so a
// genuine bug error cannot masquerade as a cut.
var errInjectedCut = errors.New("checkpoint-cut: injected crash")

// errInjectedExpiry simulates a run-duration expiry: it wraps
// context.DeadlineExceeded so the syncer takes the REAL expiry path —
// which force-writes a checkpoint mid-batch. Those tokens carry spawned
// in-flight actions, the state shape hard cuts never persist (their last
// durable token predates the batch), and historically the shape resume
// bugs hid in.
var errInjectedExpiry = fmt.Errorf("checkpoint-cut: injected expiry: %w", context.DeadlineExceeded)

// cutConnector wraps the deterministic soak connector, counting successful
// list responses and simulating a crash (context cancellation) right after
// the configured one — mid-batch, with in-flight workers and store writes
// ahead of the last checkpoint.
type cutConnector struct {
	*soakConnector
	mu              native_sync.Mutex
	responses       int
	grantsResponses int
	cutAfter        int
	// cutAfterGrants triggers on the Nth type-scoped ListGrants response
	// only — pinning the cut inside the grants phase, the one phase where
	// a topology change across resume cannot manufacture a referential
	// violation (entitlements are complete before grants start).
	cutAfterGrants int
	cut            bool
	cutCause       error
	cancel         context.CancelCauseFunc
}

func (c *cutConnector) observe(grants bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.responses++
	if grants {
		c.grantsResponses++
	}
	hit := (c.cutAfter > 0 && c.responses == c.cutAfter) ||
		(c.cutAfterGrants > 0 && grants && c.grantsResponses == c.cutAfterGrants)
	if hit && c.cancel != nil {
		c.cut = true
		c.cancel(c.cutCause)
	}
}

// stall blocks a call issued after the cut fired until the attempt context
// dies, then fails it — a crashed process's in-flight calls never return.
// This keeps sibling workers in flight at the moment of the crash, so the
// expiry path's forced checkpoint captures a mid-batch stack (spawned
// cursors unfinished) instead of a conveniently drained one.
func (c *cutConnector) stall(ctx context.Context) error {
	c.mu.Lock()
	cut := c.cut
	c.mu.Unlock()
	if !cut {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (c *cutConnector) responseCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.responses
}

func (c *cutConnector) grantsResponseCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.grantsResponses
}

func (c *cutConnector) ListResourceTypes(
	ctx context.Context,
	in *v2.ResourceTypesServiceListResourceTypesRequest,
	opts ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	if err := c.stall(ctx); err != nil {
		return nil, err
	}
	resp, err := c.soakConnector.ListResourceTypes(ctx, in, opts...)
	if err == nil {
		c.observe(false)
	}
	return resp, err
}

func (c *cutConnector) ListResources(
	ctx context.Context,
	in *v2.ResourcesServiceListResourcesRequest,
	opts ...grpc.CallOption,
) (*v2.ResourcesServiceListResourcesResponse, error) {
	if err := c.stall(ctx); err != nil {
		return nil, err
	}
	resp, err := c.soakConnector.ListResources(ctx, in, opts...)
	if err == nil {
		c.observe(false)
	}
	return resp, err
}

func (c *cutConnector) ListEntitlements(
	ctx context.Context,
	in *v2.EntitlementsServiceListEntitlementsRequest,
	opts ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	if err := c.stall(ctx); err != nil {
		return nil, err
	}
	resp, err := c.soakConnector.ListEntitlements(ctx, in, opts...)
	if err == nil {
		c.observe(false)
	}
	return resp, err
}

func (c *cutConnector) ListGrants(
	ctx context.Context,
	in *v2.GrantsServiceListGrantsRequest,
	opts ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	if err := c.stall(ctx); err != nil {
		return nil, err
	}
	resp, err := c.soakConnector.ListGrants(ctx, in, opts...)
	if err == nil {
		reqAnnos := annotations.Annotations(in.GetAnnotations())
		c.observe(reqAnnos.Contains(&v2.TypeScopedGrants{}))
	}
	return resp, err
}

// buildTopoFixture builds a deterministic connector serving the given
// type topologies (no failure injection), plus the exact expected
// entitlement-ID set and the principal user ID — the derived oracle.
func buildTopoFixture(t *testing.T, topos map[string]*soakTypeTopology) (*soakConnector, map[string]struct{}, string) {
	t.Helper()

	userType := v2.ResourceType_builder{
		Id:          "user",
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
	}.Build()
	user, err := rs.NewUserResource("user-1", userType, "User 1", nil)
	require.NoError(t, err)

	connector := &soakConnector{
		mockConnector: newMockConnector(),
		topos:         make(map[string]*soakTypeTopology),
		entsByToken:   make(map[string]map[string]*v2.Entitlement),
		grantsByTok:   make(map[string]map[string]*v2.Grant),
		failedOnce:    make(map[string]struct{}),
	}
	connector.rtDB = append(connector.rtDB, userType)
	connector.resourceDB["user"] = append(connector.resourceDB["user"], user)

	expectedEntIDs := make(map[string]struct{})
	for typeID, topo := range topos {
		groupType := v2.ResourceType_builder{
			Id:          typeID,
			DisplayName: typeID,
			Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
			Annotations: annotations.New(&v2.TypeScopedEntitlements{}, &v2.TypeScopedGrants{}),
		}.Build()
		connector.rtDB = append(connector.rtDB, groupType)
		connector.topos[typeID] = topo

		resources := make([]*v2.Resource, 0, 2)
		for ri := 0; ri < 2; ri++ {
			res, err := rs.NewGroupResource(fmt.Sprintf("%s-res%d", typeID, ri), groupType, fmt.Sprintf("%s res %d", typeID, ri), nil)
			require.NoError(t, err)
			resources = append(resources, res)
			connector.resourceDB[typeID] = append(connector.resourceDB[typeID], res)
		}

		connector.entsByToken[typeID] = make(map[string]*v2.Entitlement, len(topo.tokens))
		connector.grantsByTok[typeID] = make(map[string]*v2.Grant, len(topo.tokens))
		for i, token := range topo.tokens {
			res := resources[i%len(resources)]
			slug := "m-" + token
			ent := et.NewAssignmentEntitlement(res, slug, et.WithGrantableTo(userType))
			grant := gt.NewGrant(res, slug, user.GetId())
			connector.entsByToken[typeID][token] = ent
			connector.grantsByTok[typeID][token] = grant
			expectedEntIDs[ent.GetId()] = struct{}{}
		}
	}
	return connector, expectedEntIDs, user.GetId().GetResource()
}

// withTopos clones the connector with alternate topologies over the same
// token universe (shared payload maps) — "the API's answers changed".
func withTopos(base *soakConnector, topos map[string]*soakTypeTopology) *soakConnector {
	return &soakConnector{
		mockConnector: base.mockConnector,
		topos:         topos,
		entsByToken:   base.entsByToken,
		grantsByTok:   base.grantsByTok,
		failedOnce:    make(map[string]struct{}),
	}
}

// buildCutFixture hand-builds a deterministic two-type fan-out topology
// covering the interesting shapes: planner-spawned siblings, a planner
// continuation, chained continuations, spawns from spawned cursors, and a
// deep chain. Every token is reachable exactly once, so the expected
// payload sets are exact. No failure injection — cuts are the only
// disturbance under test.
func buildCutFixture(t *testing.T) (*soakConnector, map[string]struct{}, string) {
	t.Helper()
	return buildTopoFixture(t, map[string]*soakTypeTopology{
		// Type A: wide — planner spawns two siblings and continues;
		// spawned cursors spawn and continue in turn.
		"groupA": {
			plannerChildren: []string{"a0", "a1"},
			plannerNext:     "a2",
			nodes: map[string]*soakNode{
				"a0": {children: []string{"a3", "a4"}, next: "a5"},
				"a1": {next: "a6"},
				"a2": {children: []string{"a7"}},
				"a3": {}, "a4": {},
				"a5": {children: []string{"a8"}},
				"a6": {next: "a9"},
				"a7": {}, "a8": {}, "a9": {},
			},
			tokens:         []string{"a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"},
			poisonedEnts:   map[string]bool{},
			poisonedGrants: map[string]bool{},
		},
		// Type B: deep — a single spawned cursor heads a continuation
		// chain that fans out near the bottom.
		"groupB": {
			plannerChildren: []string{"b0"},
			nodes: map[string]*soakNode{
				"b0": {next: "b1"},
				"b1": {next: "b2"},
				"b2": {children: []string{"b3", "b4", "b5"}},
				"b3": {next: "b6"},
				"b4": {children: []string{"b7"}},
				"b5": {}, "b6": {}, "b7": {},
			},
			tokens:         []string{"b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7"},
			poisonedEnts:   map[string]bool{},
			poisonedGrants: map[string]bool{},
		},
	})
}

type cutAttemptResult struct {
	completed       bool
	checkpoints     int
	responses       int
	grantsResponses int
	// spawnedTokens counts durable checkpoint tokens that carried a
	// spawned in-flight action — the state shape resume bugs hide in.
	// The sweep meta-asserts it reaches that shape at least once, so the
	// harness cannot silently degrade into sweeping only trivial cuts.
	spawnedTokens int
}

// cutSpec configures one attempt: which cut trigger fires (checkpoint
// count, total response count, or grants-phase response count; zero =
// disabled) and the crash flavor. cause selects a hard cut
// (errInjectedCut — nothing further is written) or an expiry
// (errInjectedExpiry — the syncer's deadline path force-checkpoints the
// mid-batch stack before exiting). cause must be non-nil even for no-cut
// attempts so a genuine failure can never be mistaken for an injected one.
type cutSpec struct {
	workers        int
	checkpoint     int
	response       int
	grantsResponse int
	cause          error
}

// runCutAttempt runs one sync attempt against the shared fixture on the
// c1z at path, simulating a crash per spec. A fresh store handle and
// syncer per attempt simulate the cold restart of a different process.
func runCutAttempt(
	t *testing.T,
	base *soakConnector,
	c1zPath string,
	tmpDir string,
	spec cutSpec,
) cutAttemptResult {
	t.Helper()
	require.NotNil(t, spec.cause, "cutSpec.cause must always be set")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	connector := &cutConnector{
		soakConnector:  base,
		cutAfter:       spec.response,
		cutAfterGrants: spec.grantsResponse,
		cutCause:       spec.cause,
		cancel:         cancel,
	}

	store, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)

	s, err := NewSyncer(ctx, connector,
		WithConnectorStore(store),
		WithTmpDir(tmpDir),
		WithWorkerCount(spec.workers),
	)
	require.NoError(t, err)
	sc, ok := s.(*syncer)
	require.True(t, ok)
	// Every loop-top checkpoint commits durably: each one is a cut point.
	sc.checkpointInterval = 0
	checkpoints := 0
	spawnedTokens := 0
	sc.testCheckpointHook = func(token string) {
		checkpoints++
		if strings.Contains(token, `"spawned":true`) {
			spawnedTokens++
		}
		if spec.checkpoint > 0 && checkpoints == spec.checkpoint {
			cancel(spec.cause)
		}
	}
	audit := attachQueueAudit(t, s)

	syncErr := s.Sync(ctx)
	require.NoError(t, s.Close(context.Background()))
	verifyQueueAudit(t, audit)

	if syncErr != nil {
		// A failed attempt is only acceptable when WE crashed it; any
		// other failure is a real bug the harness must not absorb.
		require.ErrorIs(t, context.Cause(ctx), spec.cause,
			"sync failed without an injected cut (spec %+v): %v", spec, syncErr)
	}
	return cutAttemptResult{
		completed:       syncErr == nil,
		checkpoints:     checkpoints,
		responses:       connector.responseCount(),
		grantsResponses: connector.grantsResponseCount(),
		spawnedTokens:   spawnedTokens,
	}
}

// verifyCutStore opens the sealed c1z read-only and asserts the stored
// entitlement and grant sets match the topology-derived expectation
// exactly — the derived-ground-truth oracle.
func verifyCutStore(t *testing.T, c1zPath, tmpDir string, expectedEntIDs map[string]struct{}, userID string) {
	t.Helper()
	ctx := context.Background()
	store, err := dotc1z.NewStore(ctx, c1zPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()

	gotEntIDs := make(map[string]struct{})
	pageToken := ""
	for {
		resp, err := store.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, ent := range resp.GetList() {
			gotEntIDs[ent.GetId()] = struct{}{}
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	require.Equal(t, expectedEntIDs, gotEntIDs, "stored entitlement set diverged from topology")

	gotGrantEnts := make(map[string]struct{})
	grantCount := 0
	pageToken = ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, grant := range resp.GetList() {
			grantCount++
			gotGrantEnts[grant.GetEntitlement().GetId()] = struct{}{}
			require.Equal(t, userID, grant.GetPrincipal().GetId().GetResource())
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	require.Equal(t, expectedEntIDs, gotGrantEnts, "stored grant set diverged from topology")
	require.Equal(t, len(expectedEntIDs), grantCount, "duplicate grants stored")
}

// enumerateCutPoints strides a 1..total sweep down to at most limit points
// (always including 1 and total). BATON_CUT_SWEEP=full disables the cap.
func enumerateCutPoints(total, limit int) []int {
	if total <= 0 {
		return nil
	}
	if os.Getenv("BATON_CUT_SWEEP") == "full" || total <= limit {
		out := make([]int, 0, total)
		for i := 1; i <= total; i++ {
			out = append(out, i)
		}
		return out
	}
	out := make([]int, 0, limit)
	seen := map[int]bool{}
	for i := 0; i < limit; i++ {
		// Even spread over [1, total], endpoints included.
		p := 1 + i*(total-1)/(limit-1)
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

func TestCheckpointCutEnumeration(t *testing.T) {
	tmpDir := t.TempDir()
	base, expectedEntIDs, userID := buildCutFixture(t)

	// Baseline: an uninterrupted run measures the cut space (K durable
	// checkpoints, M connector responses) and sanity-checks the oracle.
	baselinePath := filepath.Join(tmpDir, "baseline.c1z")
	baseline := runCutAttempt(t, base, baselinePath, tmpDir, cutSpec{workers: 4, cause: errInjectedCut})
	require.True(t, baseline.completed, "baseline sync must complete")
	verifyCutStore(t, baselinePath, tmpDir, expectedEntIDs, userID)
	t.Logf("cut space: %d durable checkpoints, %d connector responses (BATON_CUT_SWEEP=full for the whole space)",
		baseline.checkpoints, baseline.responses)

	type cut struct {
		name       string
		checkpoint int
		response   int
		cause      error
	}
	var cuts []cut
	for _, n := range enumerateCutPoints(baseline.checkpoints, 16) {
		cuts = append(cuts, cut{name: fmt.Sprintf("checkpoint-%02d", n), checkpoint: n, cause: errInjectedCut})
	}
	for _, m := range enumerateCutPoints(baseline.responses, 16) {
		cuts = append(cuts, cut{name: fmt.Sprintf("response-%02d", m), response: m, cause: errInjectedCut})
	}
	// Expiry cuts take the run-duration deadline path, which force-writes
	// a checkpoint of the MID-BATCH stack (spawned cursors in flight)
	// before exiting — the token shape hard cuts never persist, and the
	// one resume bugs have historically hidden in.
	for _, m := range enumerateCutPoints(baseline.responses, 12) {
		cuts = append(cuts, cut{name: fmt.Sprintf("expire-%02d", m), response: m, cause: errInjectedExpiry})
	}

	// midBatchTokens counts, across the sweep, the durable tokens that
	// carried spawned in-flight actions. If it stays zero the sweep never
	// reached the state shape the expire mode exists for, and the harness
	// is vacuous — fail loudly instead of passing quietly.
	midBatchTokens := 0
	for i, c := range cuts {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(tmpDir, fmt.Sprintf("cut-%s.c1z", c.name))
			r := runCutAttempt(t, base, path, tmpDir, cutSpec{workers: 4, checkpoint: c.checkpoint, response: c.response, cause: c.cause})
			midBatchTokens += r.spawnedTokens
			if r.completed {
				// The cut landed past the end of this run's schedule
				// (parallel timing varies run to run); the completed
				// store must still be exactly right.
				verifyCutStore(t, path, tmpDir, expectedEntIDs, userID)
				return
			}

			// Cold resume under a different worker count than the
			// generator. Every third cut double-cuts: the first resume
			// is itself crashed at its first checkpoint, exercising the
			// restored-state re-checkpoint path before a final resume.
			resumeWorkers := []int{1, 7}[i%2]
			if i%3 == 0 {
				rr := runCutAttempt(t, base, path, tmpDir, cutSpec{workers: resumeWorkers, checkpoint: 1, cause: errInjectedCut})
				if rr.completed {
					verifyCutStore(t, path, tmpDir, expectedEntIDs, userID)
					return
				}
			}
			final := runCutAttempt(t, base, path, tmpDir, cutSpec{workers: resumeWorkers, cause: errInjectedCut})
			require.True(t, final.completed, "resume after cut %s did not complete", c.name)
			midBatchTokens += final.spawnedTokens
			verifyCutStore(t, path, tmpDir, expectedEntIDs, userID)
		})
	}

	require.Positive(t, midBatchTokens,
		"no durable token in the whole sweep carried a spawned in-flight action; the expire mode is not reaching mid-batch state and the harness has gone vacuous")
	require.Zero(t, base.perResourceCalls, "type-scoped types must never receive per-resource calls")
}
