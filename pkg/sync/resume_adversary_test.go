package sync //nolint:revive,nolintlint // backwards-compatible package name

// Adversarial protocol behaviors for the fan-out scheduler (Option C of
// the verification plan): connectors whose answers are legal but
// inconvenient — re-mentioning cursors that were already spawned (DAG
// discovery, or answers that shifted across a crash), and transient
// failures that must retry in place.
//
// The re-mention family pins a specific bug class: treating a re-mention
// as a fatal duplicate is DETERMINISTIC on every retry, so one shifted
// answer after a crash would wedge a sync permanently — at every worker
// count, including the default of one (all batch ops run through the
// parallel queue; workerCount only sets the worker pool size). The
// contract under test: identical spawned work is admitted exactly once
// per process, re-mentions are skipped idempotently and loudly, and
// syncs always terminate with the exact payload set.

import (
	"context"
	"fmt"
	"path/filepath"
	native_sync "sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// mutualMentionTopo is a legal-but-adversarial answer shape: w1 spawns
// both w2 and w3, and w2/w3 each ALSO mention the other — a DAG-with-a-
// cycle in the discovery graph. Every payload must land exactly once and
// the sync must terminate in both scheduler modes.
func mutualMentionTopo() map[string]*soakTypeTopology {
	return map[string]*soakTypeTopology{
		"groupW": {
			plannerChildren: []string{"w1"},
			nodes: map[string]*soakNode{
				"w1": {children: []string{"w2", "w3"}},
				"w2": {children: []string{"w3"}},
				"w3": {children: []string{"w2"}},
			},
			tokens:         []string{"w1", "w2", "w3"},
			poisonedEnts:   map[string]bool{},
			poisonedGrants: map[string]bool{},
		},
	}
}

func TestReMentionedSpawnsTerminateIdempotently(t *testing.T) {
	// workers=1 is the DEFAULT config (still queue-scheduled, single
	// worker); workers=4 adds real concurrency. Pre-fix, both treated
	// the re-mention as a fatal duplicate and failed the sync —
	// deterministically, on every retry.
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			tmpDir := t.TempDir()
			base, expectedEntIDs, userID := buildTopoFixture(t, mutualMentionTopo())
			path := filepath.Join(tmpDir, "mutual.c1z")
			r := runCutAttempt(t, base, path, tmpDir, cutSpec{workers: workers, cause: errInjectedCut})
			require.True(t, r.completed, "a mutual-mention topology must terminate")
			verifyCutStore(t, path, tmpDir, expectedEntIDs, userID)
		})
	}
}

// TestStateSkipsReAdmittedSpawnIdentity pins the state-level re-mention
// guard at its own scope: the parallel queue's dedup set lives for ONE
// batch, so an identity finished in an earlier batch is invisible to it —
// the process-lifetime spawnedAdmitted set is what terminates mutual
// mentions that span batches. The guard must hold across completion
// (never pruned on finish) and reset with a fresh state (rebuilt from the
// surviving stack on resume).
func TestStateSkipsReAdmittedSpawnIdentity(t *testing.T) {
	ctx := t.Context()
	st := newEmptySchedulerState(t)
	spawn := Action{Op: SyncGrantsOp, ResourceTypeID: "group", PageToken: "shard-1", Spawned: true, TypeScoped: true}
	parent := st.pushAction(ctx, Action{Op: SyncGrantsOp, ResourceTypeID: "group", TypeScoped: true, PageToken: "root"})

	pushed, err := st.transitionAction(ctx, parent, "root-2", []Action{spawn})
	require.NoError(t, err)
	require.Len(t, pushed, 1, "first admission goes through")

	// Re-mention while the original is still in flight: skipped.
	pushed2, err := st.transitionAction(ctx, parent, "root-3", []Action{spawn})
	require.NoError(t, err)
	require.Empty(t, pushed2, "re-mentioned in-flight spawn must be skipped")

	// Finish the original, then re-mention again — the batch-scoped queue
	// would have forgotten it by the next batch; the state must not.
	st.FinishAction(ctx, pushed[0])
	pushed3, err := st.transitionAction(ctx, parent, "root-4", []Action{spawn})
	require.NoError(t, err)
	require.Empty(t, pushed3, "re-mentioned COMPLETED spawn must stay skipped for the life of the process")

	// A distinct identity is unaffected.
	other := Action{Op: SyncGrantsOp, ResourceTypeID: "group", PageToken: "shard-2", Spawned: true, TypeScoped: true}
	pushed4, err := st.transitionAction(ctx, parent, "root-5", []Action{other})
	require.NoError(t, err)
	require.Len(t, pushed4, 1)

	// Resume semantics: the guard rebuilds from the SURVIVING stack.
	// shard-2 is still in flight (survives, stays skippable); shard-1
	// completed (amnesia — re-doable once, idempotently).
	token, err := st.Marshal()
	require.NoError(t, err)
	resumed := newState()
	require.NoError(t, resumed.Unmarshal(token))
	restoredParent := resumed.Current()
	require.NotNil(t, restoredParent)
	require.Equal(t, "shard-2", restoredParent.PageToken)

	rePushed, err := resumed.transitionAction(ctx, restoredParent, "", []Action{spawn})
	require.NoError(t, err)
	require.Len(t, rePushed, 1, "a spawn completed before the crash is re-doable after resume (idempotent redo, set re-accumulates)")
	rePushed2, err := resumed.transitionAction(ctx, rePushed[0], "", []Action{spawn})
	require.NoError(t, err)
	require.Empty(t, rePushed2, "and the re-accumulated set terminates cycles post-resume")
}

// rewiredCutTopologies is the changed-answers variant of the cut
// fixture's graphs: the SAME token universe for both types, reattached
// along completely different spawn/continuation edges. Any cursor that
// persisted in a checkpoint can be re-mentioned by the new graph.
func rewiredCutTopologies() map[string]*soakTypeTopology {
	return map[string]*soakTypeTopology{
		"groupA": {
			plannerChildren: []string{"a9", "a8"},
			plannerNext:     "a7",
			nodes: map[string]*soakNode{
				"a9": {children: []string{"a0", "a1"}},
				"a8": {next: "a2"},
				"a7": {children: []string{"a3"}},
				"a0": {next: "a4"},
				"a1": {children: []string{"a5"}},
				"a2": {next: "a6"},
				"a3": {}, "a4": {}, "a5": {}, "a6": {},
			},
			tokens:         []string{"a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"},
			poisonedEnts:   map[string]bool{},
			poisonedGrants: map[string]bool{},
		},
		"groupB": {
			plannerChildren: []string{"b7", "b6"},
			nodes: map[string]*soakNode{
				"b7": {next: "b5"},
				"b6": {children: []string{"b4", "b3"}},
				"b5": {children: []string{"b2"}},
				"b4": {next: "b1"},
				"b3": {children: []string{"b0"}},
				"b2": {}, "b1": {}, "b0": {},
			},
			tokens:         []string{"b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7"},
			poisonedEnts:   map[string]bool{},
			poisonedGrants: map[string]bool{},
		},
	}
}

// verifyChangedAnswerStore asserts the contract that survives an API
// whose answers shifted across a crash: the entitlement set is exactly
// complete (the entitlements phase finished before the first grants-phase
// cut), and the grant set is duplicate-free and referentially closed —
// every grant references a stored entitlement and the right principal.
// Grant COMPLETENESS under changed answers is explicitly not promised:
// data collected at two moments legitimately mixes.
func verifyChangedAnswerStore(t *testing.T, c1zPath, tmpDir string, expectedEntIDs map[string]struct{}, userID string) {
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
	require.Equal(t, expectedEntIDs, gotEntIDs,
		"entitlements completed before the grants-phase cut and must be exactly complete")

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
			entID := grant.GetEntitlement().GetId()
			gotGrantEnts[entID] = struct{}{}
			require.Contains(t, expectedEntIDs, entID, "grant references an entitlement outside the token universe")
			require.Equal(t, userID, grant.GetPrincipal().GetId().GetResource())
		}
		if pageToken = resp.GetNextPageToken(); pageToken == "" {
			break
		}
	}
	require.Equal(t, len(gotGrantEnts), grantCount, "duplicate grants stored")
}

// TestResumeAcrossChangedAnswersTerminates sweeps expiry cuts across the
// grants phase (the forced checkpoint persists mid-batch spawned cursors)
// and resumes every one against the REWIRED topology — the API's answers
// changed while the sync was down. Pre-fix, a re-mention of a persisted
// cursor was a fatal duplicate, deterministic on every retry: the sync
// could never complete again. The contract: every resume terminates, and
// the store honors the changed-answer guarantees (see
// verifyChangedAnswerStore).
func TestResumeAcrossChangedAnswersTerminates(t *testing.T) {
	tmpDir := t.TempDir()
	base, expectedEntIDs, userID := buildCutFixture(t)
	rewired := withTopos(base, rewiredCutTopologies())

	baselinePath := filepath.Join(tmpDir, "baseline.c1z")
	baseline := runCutAttempt(t, base, baselinePath, tmpDir, cutSpec{workers: 4, cause: errInjectedCut})
	require.True(t, baseline.completed, "baseline sync must complete")
	require.Positive(t, baseline.grantsResponses)

	midBatchTokens := 0
	for i, m := range enumerateCutPoints(baseline.grantsResponses, 10) {
		t.Run(fmt.Sprintf("grants-cut-%02d", m), func(t *testing.T) {
			path := filepath.Join(tmpDir, fmt.Sprintf("changed-%02d.c1z", m))
			r := runCutAttempt(t, base, path, tmpDir, cutSpec{workers: 4, grantsResponse: m, cause: errInjectedExpiry})
			midBatchTokens += r.spawnedTokens
			if r.completed {
				verifyCutStore(t, path, tmpDir, expectedEntIDs, userID)
				return
			}
			resumeWorkers := []int{1, 5}[i%2]
			final := runCutAttempt(t, rewired, path, tmpDir, cutSpec{workers: resumeWorkers, cause: errInjectedCut})
			require.True(t, final.completed,
				"resume under changed answers must terminate (grants cut %d, workers %d)", m, resumeWorkers)
			verifyChangedAnswerStore(t, path, tmpDir, expectedEntIDs, userID)
		})
	}
	require.Positive(t, midBatchTokens,
		"no expiry checkpoint carried a spawned in-flight cursor; the sweep is not reaching the changed-answer state shape")
}

// flakyOnceConnector fails the FIRST type-scoped grants call for one
// token with a retryable gRPC code, then serves normally.
type flakyOnceConnector struct {
	*soakConnector
	mu    native_sync.Mutex
	token string
	fired bool
	hits  int
}

func (c *flakyOnceConnector) ListGrants(
	ctx context.Context,
	in *v2.GrantsServiceListGrantsRequest,
	opts ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	reqAnnos := annotations.Annotations(in.GetAnnotations())
	if reqAnnos.Contains(&v2.TypeScopedGrants{}) && in.GetPageToken() == c.token {
		c.mu.Lock()
		fired := c.fired
		c.fired = true
		c.hits++
		c.mu.Unlock()
		if !fired {
			return nil, status.Error(codes.Unavailable, "injected transient unavailability")
		}
	}
	return c.soakConnector.ListGrants(ctx, in, opts...)
}

// TestParallelWorkerRetriesUnavailableInPlace pins the in-batch retry
// path: a retryable failure (codes.Unavailable) on a spawned cursor must
// retry IN PLACE — same worker, same action, no batch abort, no resume —
// and the failed attempt must contribute nothing (its spawns were never
// committed). One Sync call completes with the exact payload set.
func TestParallelWorkerRetriesUnavailableInPlace(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()
	base, expectedEntIDs, userID := buildTopoFixture(t, map[string]*soakTypeTopology{
		"groupU": {
			plannerChildren: []string{"u1", "u2"},
			nodes: map[string]*soakNode{
				// u2 — the flaky cursor — spawns u3, so the retry must
				// also prove the failed attempt admitted nothing.
				"u1": {},
				"u2": {children: []string{"u3"}},
				"u3": {},
			},
			tokens:         []string{"u1", "u2", "u3"},
			poisonedEnts:   map[string]bool{},
			poisonedGrants: map[string]bool{},
		},
	})
	connector := &flakyOnceConnector{soakConnector: base, token: "u2"}

	path := filepath.Join(tmpDir, "flaky.c1z")
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	s, err := NewSyncer(ctx, connector,
		WithConnectorStore(store),
		WithTmpDir(tmpDir),
		WithWorkerCount(3),
	)
	require.NoError(t, err)
	audit := attachQueueAudit(t, s)

	require.NoError(t, s.Sync(ctx), "a retryable failure must be absorbed in place, not fail the sync")
	require.NoError(t, s.Close(ctx))
	verifyQueueAudit(t, audit)

	connector.mu.Lock()
	hits := connector.hits
	connector.mu.Unlock()
	require.GreaterOrEqual(t, hits, 2, "the flaky cursor must have been retried after the injected failure")
	verifyCutStore(t, path, tmpDir, expectedEntIDs, userID)
}
