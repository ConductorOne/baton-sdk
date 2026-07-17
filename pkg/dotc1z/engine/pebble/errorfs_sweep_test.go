package pebble

// Fault-injection sweep for the engine's crash/error contract, over
// pebble's errorfs + crashable MemFS.
//
// THE CONTRACT UNDER TEST: for ANY write/fsync failure surfaced to the
// caller, followed by a crash (the handle is hard-discarded — no clean
// close mutates state after the failure), the store must reopen as one
// of:
//
//   (a) UNFINISHED — the sync is discoverable via
//       LatestUnfinishedSyncRecord and resumes to completion (page
//       replay where the model calls for it + a re-run EndSync ends
//       green), after which the content oracle passes; or
//   (b) FINISHED and content-complete — the failure hit after the
//       finished verdict was durable, and everything the sync wrote is
//       readable (primaries AND indexes).
//
// Anything else — finished-but-incomplete (a lying artifact),
// unfinished-but-unresumable — is a real durability-ordering bug.
//
// Mechanics:
//   - The engine runs over errorfs wrapping vfs.NewCrashableMem(). The
//     WithVFS threading routes ALL SSTs the DB ever reads (staged
//     deferred-index/digest/import SSTs included) through that FS;
//     spill-chunk scratch is engine-private OS IO and deliberately
//     outside the fault domain.
//   - The injector fails the k-th write-class op — and in the window
//     sweep every write after it (a dying disk does not recover; this
//     also keeps post-failure background writes from healing the crash
//     image). The whole-sync soak fails once instead; see
//     failFromInjector.failOnce for why.
//   - pebble escalates some failures (WAL commit, MANIFEST sync) to
//     Logger.Fatalf, assuming it exits the process. The fatalGate
//     records the fatal, signals the harness, and parks the calling
//     goroutine — a wedged process, observed from outside. Such an
//     engine is deliberately leaked (its lock state is undefined);
//     the shared small cache keeps that cheap.
//   - The "crash" is MemFS.CrashClone cut BEFORE any Close of the
//     failed engine: only fsynced data is guaranteed into the clone
//     (UnsyncedDataPercent=0 — the strictest image). Verification
//     opens a clean engine (no errorfs) over the clone.
//
// Sweep shape (per the stack plan): the EndSync-window sweep runs in
// default CI and is exhaustive — injection arms when EndSync is called
// and k self-calibrates upward until an armed run completes without
// injecting anything, which proves the window was covered end-to-end.
// The whole-sync randomized sweep (seeded failure points, strict
// zero-unsynced-survival crash images) is env-gated behind BATON_SOAK.
//
// Out of scope for this harness:
//   - The v3 envelope save (os-level IO in the dotc1z store layer's
//     Close path) — the engine FS never sees it.
//   - IO failures inside an EndSync whose pages are still
//     memtable-resident: those ingests ride pebble's flushable-ingest
//     path (handleIngestAsFlushable), where WAL rotation treats any
//     failure as a raw panic(err) whose unwind trips runtime lock
//     faults — pebble is crash-only about that path, and no in-process
//     recover can contain it. In production that failure IS a process
//     crash, and its recovery contract (durable WAL prefix + resume)
//     is exactly what the window sweep and the soak's CrashClone
//     images already pin. Modeling the failure itself needs subprocess
//     isolation — a candidate follow-up, not PR-1 scope.

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// failFromInjector injects errorfs.ErrInjected into the k-th
// write-class op after arming — and, unless failOnce is set, every
// write-class op after it (a dying disk does not recover, and nothing
// may heal the crash image). Reads are never failed. Remove/RemoveAll
// are never failed either: pebble's obsolete-file cleanup escalates
// removal failures to a raw panic(err) on a background goroutine,
// which no logger hook can contain — and file removal is never
// durability-critical for the contract under test. Implements
// errorfs.Injector.
type failFromInjector struct {
	// failOnce limits the injection to the single k-th op. The
	// whole-sync soak needs this: fail-forever poisons background
	// flushes and cleanup with cascading failures pebble escalates to
	// raw panics. One transient failure per run still exercises every
	// surfaced-error path.
	failOnce bool

	armed     atomic.Bool
	remaining atomic.Int64
	injected  atomic.Int64
}

func (f *failFromInjector) String() string { return "(FailFrom k on writes)" }

func (f *failFromInjector) MaybeError(op errorfs.Op) error {
	if !f.armed.Load() {
		return nil
	}
	if op.Kind.ReadOrWrite() != errorfs.OpIsWrite {
		return nil
	}
	if op.Kind == errorfs.OpRemove || op.Kind == errorfs.OpRemoveAll {
		return nil
	}
	if n := f.remaining.Add(-1); n < 0 {
		if f.failOnce && n < -1 {
			return nil
		}
		f.injected.Add(1)
		return errorfs.ErrInjected
	}
	return nil
}

// arm starts failing at the k-th write-class op from now.
func (f *failFromInjector) arm(k int64) {
	f.remaining.Store(k)
	f.injected.Store(0)
	f.armed.Store(true)
}

func (f *failFromInjector) disarm() { f.armed.Store(false) }

// fatalGate replaces pebble's Fatalf (os.Exit) for injected engines.
// Pebble escalates to Fatalf for failures it cannot unwind — a failed
// WAL commit on the CALLER's goroutine, a failed MANIFEST sync on a
// BACKGROUND flush goroutine — so neither os.Exit (kills the test
// binary) nor panic (unrecoverable on pebble's own goroutines) works.
// Instead Fatalf records the message, signals the harness, and PARKS
// the calling goroutine forever: exactly what a wedged process looks
// like from the outside. The harness treats a fired gate as the crash
// and leaks the horked engine (its goroutines and small memtable —
// the workload never grows past the initial arena) for the remainder
// of the test binary.
type fatalGate struct {
	discardPebbleLogger
	once sync.Once
	ch   chan struct{}
	msg  atomic.Pointer[string]
}

func newFatalGate() *fatalGate { return &fatalGate{ch: make(chan struct{})} }

func (g *fatalGate) Fatalf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	g.msg.Store(&msg)
	g.once.Do(func() { close(g.ch) })
	select {} // park: the engine is crashed from the harness's viewpoint
}

func (g *fatalGate) err() error {
	if m := g.msg.Load(); m != nil {
		return fmt.Errorf("pebble fatal: %s", *m)
	}
	return fmt.Errorf("pebble fatal")
}

func withFatalGate(g *fatalGate) Option {
	return func(o *Options) { o.pebbleLogger = g }
}

// panicOnFatalLogger is for the sweep's NON-injected engines (baseline
// build, crash-image verification): no failure is expected there, so a
// pebble fatal is a genuine test failure and must fail FAST, not park.
// A panic on the test goroutine fails the test with a stack; on a
// background goroutine it kills the binary with the same stack —
// either beats a parked gate nobody selects on (a CI hang).
type panicOnFatalLogger struct{ discardPebbleLogger }

func (panicOnFatalLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf("pebble fatal on a non-injected sweep engine: "+format, args...))
}

func withPanicOnFatalLogger() Option {
	return func(o *Options) { o.pebbleLogger = panicOnFatalLogger{} }
}

// sweepWorkload is the fixed mini-sync the sweeps write and verify.
// Small, but it crosses every EndSync phase: the expanded-grant page
// arms the deferred by_principal rebuild (plain PutGrants writes
// by_principal inline; only expanded writes defer), and the digest
// index is on by default — so the sealed window covers staging-SST
// writes, IngestAndExcise, the digest fold, the stats sidecar, the
// ended_at stamp, and the durability flush.
type sweepWorkload struct {
	principals []string
	perPrinc   int // connector grants per principal (distinct entitlements)
}

// expandedEntID is the entitlement the workload's expanded-grant page
// targets (one expanded grant per principal).
const sweepExpandedEnt = "ent-expanded"

func defaultSweepWorkload() sweepWorkload {
	return sweepWorkload{
		principals: []string{"alice", "bob", "carol", "dave", "erin"},
		perPrinc:   3,
	}
}

// write replays the workload's pages into a bound sync. Idempotent
// (puts are upserts) — resume paths call it again on the reopened
// store, mirroring the connector-replay resume model.
func (w sweepWorkload) write(ctx context.Context, a *Adapter) error {
	if err := a.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "app"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build(),
	); err != nil {
		return err
	}
	resources := []*v2.Resource{
		v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
		}.Build(),
	}
	for _, p := range w.principals {
		resources = append(resources, v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: p}.Build(),
		}.Build())
	}
	if err := a.PutResources(ctx, resources...); err != nil {
		return err
	}
	ents := make([]*v2.Entitlement, 0, w.perPrinc+1)
	for i := 0; i < w.perPrinc; i++ {
		ents = append(ents, v2.Entitlement_builder{
			Id: canonicalTestEntID(fmt.Sprintf("ent-%02d", i)),
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
			}.Build(),
		}.Build())
	}
	ents = append(ents, v2.Entitlement_builder{
		Id: canonicalTestEntID(sweepExpandedEnt),
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
		}.Build(),
	}.Build())
	if err := a.PutEntitlements(ctx, ents...); err != nil {
		return err
	}
	// Two pages of grants, like a paginated connector.
	var gs []*v2.Grant
	for _, p := range w.principals {
		for i := 0; i < w.perPrinc; i++ {
			gs = append(gs, mkV2Grant("", fmt.Sprintf("ent-%02d", i), "user", p))
		}
	}
	half := len(gs) / 2
	if err := a.PutGrants(ctx, gs[:half]...); err != nil {
		return err
	}
	if err := a.PutGrants(ctx, gs[half:]...); err != nil {
		return err
	}
	// One expanded-grant page: takes the deferred-index write path
	// (StoreExpandedGrants), arming the EndSync by_principal rebuild —
	// the phase a plain PutGrants-only workload would never exercise.
	var expanded []*v2.Grant
	for _, p := range w.principals {
		expanded = append(expanded, mkV2Grant("", sweepExpandedEnt, "user", p))
	}
	return a.Grants().StoreExpandedGrants(ctx, expanded...)
}

// verifyComplete is the content oracle: every workload row is readable
// through the primary keyspaces AND the by_principal index on a
// finished store.
func (w sweepWorkload) verifyComplete(ctx context.Context, t *testing.T, e *Engine, syncID string, label string) {
	t.Helper()
	rec, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoErrorf(t, err, "%s: sync run record must exist", label)
	require.NotNilf(t, rec.GetEndedAt(), "%s: sync must be finished", label)

	a := NewAdapter(e)
	require.NoErrorf(t, a.SetCurrentSync(ctx, syncID), "%s: bind sync for reads", label)

	resp, err := a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoErrorf(t, err, "%s: ListGrants", label)
	require.Lenf(t, resp.GetList(), len(w.principals)*(w.perPrinc+1), "%s: grant count", label)

	rresp, err := a.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{}.Build())
	require.NoErrorf(t, err, "%s: ListResources", label)
	require.Lenf(t, rresp.GetList(), 1+len(w.principals), "%s: resource count", label)

	eresp, err := a.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoErrorf(t, err, "%s: ListEntitlements", label)
	require.Lenf(t, eresp.GetList(), w.perPrinc+1, "%s: entitlement count", label)

	// The by_principal index is rebuilt inside the EndSync window — the
	// exact artifact a torn deferred build would corrupt or lose.
	for _, p := range w.principals {
		n := 0
		require.NoErrorf(t, e.IterateGrantsByPrincipal(ctx, "user", p, func(*v3.GrantRecord) bool {
			n++
			return true
		}), "%s: IterateGrantsByPrincipal(%s)", label, p)
		require.Equalf(t, w.perPrinc+1, n, "%s: by_principal must serve %s", label, p)
	}
}

// crashStepResult reports how one injected run surfaced its failure.
type crashStepResult struct {
	err      error // the surfaced failure (nil = the run completed)
	fatal    bool  // pebble Logger.Fatalf fired (engine horked, leaked)
	injected int64 // ops the injector actually failed
	image    *vfs.MemFS
}

// runInjected runs body with the injector armed at k over base
// (crashable). The body runs on its own goroutine because a pebble
// fatal parks the goroutine it fires on — possibly one the body is
// waiting on — so the harness selects on body-completion OR the fatal
// gate. The crash clone is cut at the failure point BEFORE any close
// can heal the FS; a fatal (or a close that itself wedges) leaks the
// engine, which is the point: a crashed process closes nothing.
func runInjected(base *vfs.MemFS, inj *failFromInjector, e *Engine, gate *fatalGate, k int64, body func() error) crashStepResult {
	var res crashStepResult
	inj.arm(k)
	done := make(chan error, 1)
	go func() { done <- body() }()
	select {
	case err := <-done:
		res.err = err
	case <-gate.ch:
		res.fatal = true
		res.err = gate.err()
	}
	// Snapshot the injection count the moment the body resolves — it
	// answers "did the body's window inject?", which the window sweep's
	// termination condition depends on; later background injections
	// must not pollute it.
	res.injected = inj.injected.Load()
	// The crash image: only fsynced state survives. Cut while the
	// injector is STILL ARMED and before Close — a background goroutine
	// completing a write in a disarm-to-clone gap would heal state the
	// image must not contain. Background injections in the read-to-clone
	// gap only make the image more crashed, never healed — the safe
	// direction. (CrashClone blocks FS mutations for the copy, so the
	// image is an atomic cut even with engine goroutines still running;
	// the clone runs on the base MemFS, not through the errorfs wrapper,
	// so arming doesn't affect the cut itself.)
	res.image = base.CrashClone(vfs.CrashCloneCfg{})
	inj.disarm()
	if res.fatal {
		return res
	}
	// Close on a goroutine gated the same way: teardown can hit its own
	// late fatal (a background flush observing the earlier injection).
	closed := make(chan struct{})
	go func() { _ = e.Close(); close(closed) }()
	select {
	case <-closed:
	case <-gate.ch:
		res.fatal = true // the run's outcome stands; the engine leaks
	}
	return res
}

// verifyCrashImage opens a clean engine over the crash clone and
// asserts the reopen dichotomy, resuming when unfinished.
// replayPages selects the resume model: false = the pages were already
// durable before the window (EndSync sweep), true = replay them (whole
// sync soak).
func verifyCrashImage(ctx context.Context, t *testing.T, w sweepWorkload, image *vfs.MemFS, cache *pebble.Cache, syncID string, replayPages bool, label string) {
	t.Helper()
	e, err := Open(ctx, "sweep-db", WithVFS(image), WithSharedCache(cache), withPanicOnFatalLogger())
	require.NoErrorf(t, err, "%s: the crash image must reopen", label)
	defer func() { _ = e.Close() }()

	rec, recErr := e.GetSyncRunRecord(ctx, syncID)
	if recErr != nil {
		require.ErrorIsf(t, recErr, pebble.ErrNotFound, "%s: sync-run lookup may only miss cleanly", label)
	}

	switch {
	case recErr == nil && rec.GetEndedAt() != nil:
		// (b) finished: everything the sync wrote must have been durable
		// before the stamp — a finished-but-incomplete image is the lying
		// artifact this harness exists to catch.
		w.verifyComplete(ctx, t, e, syncID, label+" (finished image)")
	case recErr == nil:
		// (a) unfinished: must be discoverable and resume to completion.
		unfinished, err := e.LatestUnfinishedSyncRecord(ctx, nil)
		require.NoErrorf(t, err, "%s: LatestUnfinishedSyncRecord", label)
		require.NotNilf(t, unfinished, "%s: an unstamped sync must be discoverable as unfinished (resumable)", label)
		require.Equalf(t, syncID, unfinished.GetSyncId(), "%s: the unfinished sync must be ours", label)

		a := NewAdapter(e)
		require.NoErrorf(t, a.SetCurrentSync(ctx, syncID), "%s: rebind for resume", label)
		if replayPages {
			require.NoErrorf(t, w.write(ctx, a), "%s: resume page replay", label)
		}
		require.NoErrorf(t, a.EndSync(ctx), "%s: resumed EndSync must converge", label)
		w.verifyComplete(ctx, t, e, syncID, label+" (resumed image)")
	default:
		// (c) the record never became durable. Legal only when the crash
		// predates the sync-run record's fsync — possible in the whole-sync
		// soak, impossible in the EndSync sweep (the baseline had it
		// durable). The image must support a fresh sync from scratch.
		require.Truef(t, replayPages, "%s: the sync-run record vanished from a baseline that had it durable", label)
		a := NewAdapter(e)
		newID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		require.NoErrorf(t, err, "%s: restart after empty image", label)
		require.NoErrorf(t, w.write(ctx, a), "%s: restart page write", label)
		require.NoErrorf(t, a.EndSync(ctx), "%s: restart EndSync", label)
		w.verifyComplete(ctx, t, e, newID, label+" (restarted image)")
	}
}

// buildSweepBaseline writes the workload into a fresh crashable MemFS
// and closes the engine cleanly, leaving a fully synced pre-EndSync
// image with the deferred-index marker durably armed. Returns the FS
// and the open sync's id.
func buildSweepBaseline(ctx context.Context, t *testing.T, w sweepWorkload, cache *pebble.Cache) (*vfs.MemFS, string) {
	t.Helper()
	fs := vfs.NewCrashableMem()
	e, err := Open(ctx, "sweep-db", WithVFS(fs), WithSharedCache(cache), withPanicOnFatalLogger())
	require.NoError(t, err)
	a := NewAdapter(e)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, w.write(ctx, a))
	require.True(t, e.deferredIdxPending.Load(), "grant writes must arm the deferred index rebuild")
	require.NoError(t, e.Close())
	return fs, syncID
}

// TestErrorFSEndSyncWindowSweep exhaustively sweeps write-op failure
// points across the EndSync window: for k = 0, 1, 2, ... the k-th
// write-class op after EndSync begins (and every one after it) fails,
// the FS is crash-cloned at the failure, and the clone must satisfy
// the reopen dichotomy. The sweep self-terminates when an armed run
// completes without injecting anything — proof the whole window was
// covered.
func TestErrorFSEndSyncWindowSweep(t *testing.T) {
	ctx := context.Background()
	w := defaultSweepWorkload()
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()

	baseline, syncID := buildSweepBaseline(ctx, t, w, cache)

	const maxK = 5000
	covered := false
	var k int64
	for k = 0; !covered; k++ {
		require.Less(t, k, int64(maxK), "EndSync window exceeded %d write ops; sweep runaway", maxK)

		stepFS := baseline.CrashClone(vfs.CrashCloneCfg{})
		inj := &failFromInjector{}
		efs := errorfs.Wrap(stepFS, inj)

		gate := newFatalGate()
		e, err := Open(ctx, "sweep-db", WithVFS(efs), WithSharedCache(cache), withFatalGate(gate))
		require.NoErrorf(t, err, "k=%d: open over injected FS (disarmed)", k)
		a := NewAdapter(e)
		require.NoError(t, a.SetCurrentSync(ctx, syncID))

		res := runInjected(stepFS, inj, e, gate, k, func() error {
			return a.EndSync(ctx)
		})

		label := fmt.Sprintf("k=%d", k)
		if res.err == nil {
			// The run completed. If nothing was injected, k walked past the
			// last write op of the window — coverage proven. (A nonzero
			// injected count with a green run means the injection only hit
			// post-completion background work; verify and keep sweeping.)
			verifyCrashImage(ctx, t, w, res.image, cache, syncID, false, label+" (clean run)")
			covered = res.injected == 0
			continue
		}
		require.Greaterf(t, res.injected, int64(0), "k=%d: EndSync failed with the injector armed but nothing injected: %v", k, res.err)
		verifyCrashImage(ctx, t, w, res.image, cache, syncID, false, label)
	}
	t.Logf("EndSync window covered: %d write-op failure points swept", k-1)
}

// TestErrorFSWholeSyncRandomSweepSoak randomizes the failure point
// across the whole sync (open → pages → EndSync) with the strictest
// crash image (no unsynced survival). Env-gated like the other soaks.
func TestErrorFSWholeSyncRandomSweepSoak(t *testing.T) {
	if os.Getenv("BATON_SOAK") == "" {
		t.Skip("set BATON_SOAK=1 to run the whole-sync errorfs sweep")
	}
	ctx := context.Background()
	w := defaultSweepWorkload()
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()

	for seed := int64(1); seed <= 60; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic seeded sweep, not cryptography
			k := int64(rng.Intn(400))

			fs := vfs.NewCrashableMem()
			inj := &failFromInjector{failOnce: true}
			efs := errorfs.Wrap(fs, inj)

			gate := newFatalGate()
			e, err := Open(ctx, "sweep-db", WithVFS(efs), WithSharedCache(cache), withFatalGate(gate))
			require.NoError(t, err, "open before arming")
			a := NewAdapter(e)

			var syncID string
			res := runInjected(fs, inj, e, gate, k, func() error {
				var err error
				syncID, err = a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
				if err != nil {
					return err
				}
				if err := w.write(ctx, a); err != nil {
					return err
				}
				// Disarm before EndSync: the soak owns the OPEN + PAGES
				// phase (undurable, memtable-resident state — the fresh-sync
				// crash surface the window sweep's durable baseline can't
				// reach). EndSync itself is the window sweep's territory —
				// and with pages still in the memtable, EndSync's ingests
				// take pebble's flushable-ingest path, where ANY injected
				// failure is a raw panic(err) whose unwind faults the
				// runtime (crash-only by design; not recoverable in-process;
				// see the coverage-gap note in the file header).
				inj.disarm()
				return a.EndSync(ctx)
			})

			label := fmt.Sprintf("seed=%d k=%d", seed, k)
			if res.err == nil {
				// Failure point beyond the sync's op count (or only
				// post-completion background work was hit): the finished
				// artifact was flushed, so the strict crash image must
				// verify complete.
				require.NotEmpty(t, syncID)
				verifyCrashImage(ctx, t, w, res.image, cache, syncID, true, label+" (clean run)")
				return
			}
			if syncID == "" {
				// StartNewSync itself failed; the image owes nothing beyond
				// reopening clean and supporting a fresh sync.
				verifyEmptyImageRestarts(ctx, t, w, res.image, cache, label+" (pre-sync)")
				return
			}
			verifyCrashImage(ctx, t, w, res.image, cache, syncID, true, label)
		})
	}
}

// verifyEmptyImageRestarts asserts a crash image with no sync at all
// still opens and supports a fresh, complete sync.
func verifyEmptyImageRestarts(ctx context.Context, t *testing.T, w sweepWorkload, image *vfs.MemFS, cache *pebble.Cache, label string) {
	t.Helper()
	e, err := Open(ctx, "sweep-db", WithVFS(image), WithSharedCache(cache), withPanicOnFatalLogger())
	require.NoErrorf(t, err, "%s: empty crash image must reopen", label)
	defer func() { _ = e.Close() }()
	a := NewAdapter(e)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoErrorf(t, err, "%s: restart", label)
	require.NoErrorf(t, w.write(ctx, a), "%s: restart write", label)
	require.NoErrorf(t, a.EndSync(ctx), "%s: restart EndSync", label)
	w.verifyComplete(ctx, t, e, syncID, label+" (restarted)")
}
