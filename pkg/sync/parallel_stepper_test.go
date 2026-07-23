package sync //nolint:revive,nolintlint // backwards-compatible package name

// Bounded-exhaustive interleaving stepper for parallelActionQueue (Option
// A of the verification plan).
//
// Every queue operation executes under one mutex, so a concurrent
// schedule is exactly an ORDERING of critical sections — and executing
// enabled operations one at a time against the REAL queue is a faithful
// execution of that schedule. The stepper explores ALL reachable
// orderings of a scenario by depth-first search: each tree node is a
// prefix of operations, each branch an enabled next operation, with
// visited-state pruning (queue state + worker states; workers are
// symmetric, so states are sorted) and replay from scratch per node.
//
// Worker lifecycle mirrors syncParallel's worker loop exactly:
//
//	idle -next()-> holding -transition()xN-> finished -done()-> idle
//	                  \-(connector failure or transition error)-> failed
//	                     -done()-> failedDone -abort()-> exited
//	idle -next()==false-> exited
//
// At every node the stepper checks liveness (some operation enabled, or
// every worker exited — a stuck node is the outstanding-accounting bug
// class) and queue sanity (outstanding never negative). At every terminal
// it runs the full Option D audit contract (verifyQueueAudit): exactly-
// once, admission-before-execution, no-commit-after-abort, dedup
// soundness, accounting, and full drain on clean completion. Silent
// violations in ANY schedule of the scenario space fail the test.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// stepPage is one connector response for a cursor: a continuation token
// (empty = final page) and spawned sibling tokens.
type stepPage struct {
	next     string
	children []string
}

// stepBehavior is the deterministic behavior of one cursor. fail models
// the connector call failing (non-retryable): no transition happens and
// the worker takes the done→abort→exit path. Tokens without an entry
// behave as leaves (one final page, no spawns).
type stepBehavior struct {
	pages []stepPage
	fail  bool
}

type stepScenario struct {
	name      string
	seeds     []string
	workers   int
	behaviors map[string]stepBehavior
	// cleanReachable declares whether any schedule can finish without an
	// abort (false when a seed always fails). Used to sanity-check the
	// explored space.
	cleanReachable bool
}

const (
	wIdle = iota
	wHolding
	wFinished
	wFailed
	wFailedDone
	wExited
)

type stepWorker struct {
	phase  int
	action *Action
	page   int
}

// stepRun is one replayed execution: the real queue plus worker states.
type stepRun struct {
	sc      *stepScenario
	queue   *parallelActionQueue
	audit   *queueAudit
	workers []*stepWorker
	nextID  int
	failed  bool // any worker took the failure path (batchErr != nil)
}

func newStepRun(sc *stepScenario) *stepRun {
	r := &stepRun{sc: sc}
	seeds := make([]*Action, 0, len(sc.seeds))
	for _, tok := range sc.seeds {
		r.nextID++
		seeds = append(seeds, &Action{
			ID:             fmt.Sprintf("s%03d", r.nextID),
			Op:             SyncGrantsOp,
			ResourceTypeID: "group",
			ResourceID:     "res-1",
			PageToken:      tok,
			Spawned:        true,
			TypeScoped:     true,
		})
	}
	r.queue = newParallelActionQueue(seeds)
	r.audit = &queueAudit{}
	r.queue.attachAudit(r.audit, SyncGrantsOp, 1)
	r.workers = make([]*stepWorker, sc.workers)
	for i := range r.workers {
		r.workers[i] = &stepWorker{phase: wIdle}
	}
	return r
}

func (r *stepRun) behavior(tok string) stepBehavior {
	if b, ok := r.sc.behaviors[tok]; ok {
		return b
	}
	return stepBehavior{pages: []stepPage{{}}}
}

// enabledOps lists every worker operation that can run without blocking.
// Op encoding: worker index only — each worker phase has exactly one next
// operation, so the phase determines what runs.
func (r *stepRun) enabledOps() []int {
	var ops []int
	for i, w := range r.workers {
		switch w.phase {
		case wExited:
			continue
		case wIdle:
			// next() blocks while !aborted && drained-head && outstanding>0.
			q := r.queue
			q.mu.Lock()
			blocked := !q.aborted && q.head == len(q.actions) && q.outstanding > 0
			q.mu.Unlock()
			if !blocked {
				ops = append(ops, i)
			}
		default:
			// transition/done/abort never block.
			ops = append(ops, i)
		}
	}
	return ops
}

// step executes worker i's next operation against the real queue,
// mirroring syncParallel's worker loop:
//
//	go func() {
//	    for { action, ok := next(); if !ok return
//	          r := syncOneAction(...)          // pages: transition per page
//	          done(); if r.err != nil { abort(); return } }
//	}()
func (r *stepRun) step(t *testing.T, i int) {
	w := r.workers[i]
	switch w.phase {
	case wIdle:
		action, ok := r.queue.next()
		if !ok {
			w.phase = wExited
			return
		}
		w.action = action
		w.page = 0
		if r.behavior(action.PageToken).fail {
			// The connector call fails before any transition.
			w.phase = wFailed
			return
		}
		w.phase = wHolding
	case wHolding:
		b := r.behavior(w.action.PageToken)
		require.Less(t, w.page, len(b.pages), "scenario bug: cursor %q ran out of pages", w.action.PageToken)
		page := b.pages[w.page]
		children := make([]Action, 0, len(page.children))
		for _, tok := range page.children {
			children = append(children, Action{
				Op:             SyncGrantsOp,
				ResourceTypeID: "group",
				ResourceID:     "res-1",
				PageToken:      tok,
				Spawned:        true,
				TypeScoped:     true,
			})
		}
		var committedNext string
		err := r.queue.transition(context.Background(), SyncGrantsOp, w.action, page.next, children,
			func(effectiveNext string, admitted []Action) ([]*Action, error) {
				committedNext = effectiveNext
				pushed := make([]*Action, 0, len(admitted))
				for j := range admitted {
					child := admitted[j]
					r.nextID++
					child.ID = fmt.Sprintf("c%03d", r.nextID)
					pushed = append(pushed, &child)
				}
				return pushed, nil
			})
		switch {
		case err != nil:
			// Post-abort rejection (or a protocol violation): the real
			// worker surfaces the error from f and takes the failure
			// path.
			w.phase = wFailed
		case committedNext == "":
			w.phase = wFinished
		default:
			w.page++
		}
	case wFinished:
		r.queue.done()
		w.phase = wIdle
		w.action = nil
	case wFailed:
		r.queue.done()
		r.failed = true
		w.phase = wFailedDone
	case wFailedDone:
		r.queue.abort()
		w.phase = wExited
	default:
		t.Fatalf("step on worker in phase %d", w.phase)
	}
}

// signature canonicalizes the full future-relevant state: real queue
// internals plus sorted worker states (workers are symmetric).
func (r *stepRun) signature() string {
	q := r.queue
	q.mu.Lock()
	head := fmt.Sprintf("o%d|a%v", q.outstanding, q.aborted)
	pending := make([]string, 0, len(q.actions)-q.head)
	for _, action := range q.actions[q.head:] {
		if action != nil {
			pending = append(pending, action.PageToken)
		}
	}
	seen := make([]string, 0, len(q.seen))
	for k := range q.seen {
		seen = append(seen, fmt.Sprintf("%x", k[:8]))
	}
	q.mu.Unlock()
	sort.Strings(seen)

	ws := make([]string, 0, len(r.workers))
	for _, w := range r.workers {
		switch w.phase {
		case wHolding:
			ws = append(ws, fmt.Sprintf("h:%s:%d", w.action.PageToken, w.page))
		case wFinished:
			ws = append(ws, "f:"+w.action.PageToken)
		case wFailed:
			ws = append(ws, "x:"+w.action.PageToken)
		default:
			ws = append(ws, fmt.Sprintf("p%d", w.phase))
		}
	}
	sort.Strings(ws)
	return strings.Join([]string{head, strings.Join(pending, ","), strings.Join(seen, ","), strings.Join(ws, ",")}, "|")
}

func (r *stepRun) sane(t *testing.T) {
	q := r.queue
	q.mu.Lock()
	outstanding, head, actions := q.outstanding, q.head, len(q.actions)
	q.mu.Unlock()
	require.GreaterOrEqual(t, outstanding, 0, "outstanding went negative")
	require.LessOrEqual(t, head, actions, "queue head ran past the buffer")
}

type stepStats struct {
	nodes          int
	terminals      int
	cleanTerminals int
}

const stepNodeCap = 500_000

// exploreScenario DFS-explores every schedule of the scenario, replaying
// the operation prefix from scratch at each node (the queue is rebuilt
// deterministically), pruning by state signature.
func exploreScenario(t *testing.T, sc *stepScenario) stepStats {
	t.Helper()
	stats := stepStats{}
	visited := map[string]bool{}

	var dfs func(prefix []int)
	dfs = func(prefix []int) {
		stats.nodes++
		require.Less(t, stats.nodes, stepNodeCap, "scenario state space exceeded the node cap")

		// Replay the prefix on a fresh run.
		r := newStepRun(sc)
		for _, op := range prefix {
			r.step(t, op)
			r.sane(t)
		}

		sig := r.signature()
		if visited[sig] {
			return
		}
		visited[sig] = true

		ops := r.enabledOps()
		if len(ops) == 0 {
			// Liveness: a node with no enabled operations must be a
			// fully exited terminal — anything else is the stuck-batch
			// bug class (broken outstanding accounting).
			for _, w := range r.workers {
				require.Equal(t, wExited, w.phase,
					"schedule %v deadlocked: worker stuck in phase %d with no enabled operations", prefix, w.phase)
			}
			stats.terminals++
			clean := !r.failed
			if clean {
				stats.cleanTerminals++
				q := r.queue
				q.mu.Lock()
				outstanding := q.outstanding
				q.mu.Unlock()
				require.Zero(t, outstanding, "clean terminal with nonzero outstanding")
			}
			// Full queue-contract audit over this schedule's event log.
			r.audit.record(queueAuditEvent{kind: auditBatchEnd, batch: 1, clean: clean})
			verifyQueueAudit(t, r.audit)
			return
		}
		for _, op := range ops {
			dfs(append(append([]int(nil), prefix...), op))
		}
	}
	dfs(nil)
	require.Positive(t, stats.terminals, "scenario explored no terminal states")
	if sc.cleanReachable {
		require.Positive(t, stats.cleanTerminals, "scenario declared clean-reachable but no clean terminal was found")
	}
	return stats
}

func TestParallelQueueExhaustiveInterleavings(t *testing.T) {
	scenarios := []*stepScenario{
		{
			// Fan-out chain: the planner-ish seed spawns two, one spawns
			// a grandchild. Exactly-once and full drain across every
			// dequeue/commit/done ordering.
			name:    "fanout-chain",
			seeds:   []string{"A"},
			workers: 2,
			behaviors: map[string]stepBehavior{
				"A": {pages: []stepPage{{children: []string{"B", "C"}}}},
				"B": {pages: []stepPage{{children: []string{"D"}}}},
			},
			cleanReachable: true,
		},
		{
			// Pagination interleaved with spawns: A pages twice,
			// spawning on both pages, while B runs alongside.
			name:    "pagination-with-spawns",
			seeds:   []string{"A", "B"},
			workers: 2,
			behaviors: map[string]stepBehavior{
				"A": {pages: []stepPage{
					{next: "A#2", children: []string{"C"}},
					{children: []string{"E"}},
				}},
			},
			cleanReachable: true,
		},
		{
			// Failure mid-fan-out: B always fails, aborting the batch at
			// every possible point relative to A's spawning — the
			// no-commit-after-abort and accounting-on-abort space.
			name:    "failure-aborts-batch",
			seeds:   []string{"A", "B", "C"},
			workers: 2,
			behaviors: map[string]stepBehavior{
				"A": {pages: []stepPage{{children: []string{"D"}}}},
				"B": {fail: true},
			},
			cleanReachable: false,
		},
		{
			// Both workers fail concurrently: done/abort/exit windows of
			// the two failure paths interleave; abort must be idempotent
			// and accounting exact.
			name:    "double-failure",
			seeds:   []string{"A", "B"},
			workers: 2,
			behaviors: map[string]stepBehavior{
				"A": {fail: true},
				"B": {fail: true},
			},
			cleanReachable: false,
		},
		{
			// Re-mention: A re-mentions seed B (skip, admitted once) and
			// spawns fresh C; the mutual pair B<->C also re-mention each
			// other — the cycle must terminate in every schedule.
			name:    "re-mention-and-cycle",
			seeds:   []string{"A", "B"},
			workers: 2,
			behaviors: map[string]stepBehavior{
				"A": {pages: []stepPage{{children: []string{"B", "C"}}}},
				"B": {pages: []stepPage{{children: []string{"C"}}}},
				"C": {pages: []stepPage{{children: []string{"B"}}}},
			},
			cleanReachable: true,
		},
		{
			// Continuation re-convergence: A's next page token is seed
			// H's identity. In every schedule A must finish (not error,
			// not walk H's pages itself) and H must run exactly once.
			name:    "continuation-reconvergence",
			seeds:   []string{"A", "H"},
			workers: 2,
			behaviors: map[string]stepBehavior{
				"A": {pages: []stepPage{{next: "H", children: []string{"C"}}}},
				"H": {pages: []stepPage{{children: []string{"D"}}}},
			},
			cleanReachable: true,
		},
		{
			// Three workers over the widest scenario: more concurrent
			// windows between dequeue, commit, done, and abort.
			name:    "three-workers-wide",
			seeds:   []string{"A", "B"},
			workers: 3,
			behaviors: map[string]stepBehavior{
				"A": {pages: []stepPage{{next: "A#2", children: []string{"C", "D"}}, {}}},
				"B": {pages: []stepPage{{children: []string{"E"}}}},
				"E": {fail: true},
			},
			cleanReachable: false,
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			stats := exploreScenario(t, sc)
			t.Logf("explored %d nodes, %d distinct terminal states (%d clean)", stats.nodes, stats.terminals, stats.cleanTerminals)
			// Non-vacuity: the space must be genuinely combinatorial.
			// (The smallest scenario, double-failure, legitimately has a
			// 19-node space: two seeds, both failing, two workers.)
			require.Greater(t, stats.nodes, 15, "suspiciously small schedule space — stepper may have gone vacuous")
		})
	}
}
