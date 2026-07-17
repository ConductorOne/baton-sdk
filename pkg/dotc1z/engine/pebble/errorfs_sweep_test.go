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
	"runtime"
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

// skipOnWindowsMemFS skips MemFS-backed tests on Windows: the engine
// mirrors os.MkdirTemp staging paths onto the engine FS
// (prepareStagingDir), and MemFS only understands "/" separators —
// backslash host paths would collapse into flat names with undefined
// semantics. WithVFS+MemFS is a test-only configuration; see the
// portability note on prepareStagingDir.
func skipOnWindowsMemFS(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("MemFS-backed engine tests assume '/' path separators; see prepareStagingDir portability note")
	}
}

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

// expectedGrantIDs is the exact external-id set the workload implies:
// perPrinc connector grants plus one expanded grant per principal.
func (w sweepWorkload) expectedGrantIDs() []string {
	var ids []string
	for _, p := range w.principals {
		for i := 0; i < w.perPrinc; i++ {
			ids = append(ids, canonicalTestGrantID(fmt.Sprintf("ent-%02d", i), "user", p))
		}
		ids = append(ids, canonicalTestGrantID(sweepExpandedEnt, "user", p))
	}
	return ids
}

// expectedEntIDsPerPrincipal is the exact entitlement-id set every
// principal's by_principal slice must serve.
func (w sweepWorkload) expectedEntIDsPerPrincipal() []string {
	var ids []string
	for i := 0; i < w.perPrinc; i++ {
		ids = append(ids, canonicalTestEntID(fmt.Sprintf("ent-%02d", i)))
	}
	return append(ids, canonicalTestEntID(sweepExpandedEnt))
}

// verifyComplete is the content oracle: the EXACT workload row sets —
// not just counts — are readable through the primary keyspaces AND the
// by_principal index on a finished store. Set equality is what makes
// "resumes to completion" mean CORRECT completion: a resume that
// duplicated, dropped, or cross-wired rows while keeping counts
// plausible fails here.
//
// requireDigests: whether the digest oracle must find digest state
// PRESENT (see verifyDigests) — true whenever the finishing EndSync
// ran without injection (resumed/restarted/uninjected images), false
// for images whose EndSync completed under an armed injector.
// Production policy is technically weaker — EndSync downgrades ANY
// digest build failure to a loud full drop and still finishes — but
// on this harness's clean engines (MemFS, no injector) a soft digest
// failure has no legitimate cause, so the oracle deliberately fails
// CLOSED on it rather than shrugging it off as a legal drop.
func (w sweepWorkload) verifyComplete(ctx context.Context, t *testing.T, e *Engine, syncID string, requireDigests bool, label string) {
	t.Helper()
	rec, err := e.GetSyncRunRecord(ctx, syncID)
	require.NoErrorf(t, err, "%s: sync run record must exist", label)
	require.NotNilf(t, rec.GetEndedAt(), "%s: sync must be finished", label)

	a := NewAdapter(e)
	require.NoErrorf(t, a.SetCurrentSync(ctx, syncID), "%s: bind sync for reads", label)

	resp, err := a.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoErrorf(t, err, "%s: ListGrants", label)
	gotGrantIDs := make([]string, 0, len(resp.GetList()))
	for _, g := range resp.GetList() {
		gotGrantIDs = append(gotGrantIDs, g.GetId())
	}
	require.ElementsMatchf(t, w.expectedGrantIDs(), gotGrantIDs, "%s: grant id set", label)

	rresp, err := a.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{}.Build())
	require.NoErrorf(t, err, "%s: ListResources", label)
	require.Lenf(t, rresp.GetList(), 1+len(w.principals), "%s: resource count", label)

	eresp, err := a.ListEntitlements(ctx, v2.EntitlementsServiceListEntitlementsRequest_builder{}.Build())
	require.NoErrorf(t, err, "%s: ListEntitlements", label)
	require.Lenf(t, eresp.GetList(), w.perPrinc+1, "%s: entitlement count", label)

	// The by_principal index is rebuilt inside the EndSync window — the
	// exact artifact a torn deferred build would corrupt or lose. Each
	// principal's slice must serve exactly its entitlement set, with
	// records whose refs actually point at the principal being asked
	// about (a cross-wired index entry fails the principal check).
	wantEnts := w.expectedEntIDsPerPrincipal()
	for _, p := range w.principals {
		var gotEnts []string
		require.NoErrorf(t, e.IterateGrantsByPrincipal(ctx, "user", p, func(r *v3.GrantRecord) bool {
			require.Equalf(t, "user", r.GetPrincipal().GetResourceTypeId(), "%s: %s index row principal type", label, p)
			require.Equalf(t, p, r.GetPrincipal().GetResourceId(), "%s: %s index row principal id", label, p)
			gotEnts = append(gotEnts, r.GetEntitlement().GetEntitlementId())
			return true
		}), "%s: IterateGrantsByPrincipal(%s)", label, p)
		require.ElementsMatchf(t, wantEnts, gotEnts, "%s: by_principal entitlement set for %s", label, p)
	}

	w.verifyDigests(ctx, t, e, requireDigests, label)
}

// verifyDigests is the digest half of the content oracle: on a
// FINISHED store the seal-time build must have left every digest
// artifact present AND exact. Three independently derived
// representations have to agree, per entitlement:
//
//  1. the fold of content hashes recomputed from the PRIMARY grant
//     records (ground truth — touches no digest state at all);
//  2. the stored per-entitlement digest root (present-means-exact);
//  3. the on-demand fold of the grant hash index
//     (ComputeEntitlementBucketDigest) — pinning the index itself,
//     which lives and dies with the digest nodes.
//
// Then the stored whole-file global root must equal the XOR/count fold
// of the entitlement roots. A crash image whose resume produced
// primaries without digests, digests without the hash index, or roots
// over torn index ranges fails one of these legs; verifying rows alone
// would have called such an image complete (review finding, edge/
// resume round).
//
// Digests are an OPTIONAL artifact with a present-means-exact
// contract: a mid-seal build failure is downgraded to a loud full
// drop and EndSync still finishes (see RepairMissingGrantDigests), so
// a finished injected image may legitimately carry NO digest state.
// The oracle therefore branches on the global root: absent → every
// entitlement root must be absent too (the safe "recalculate" state —
// a partial presence would lie to the repair fast path); present →
// the full three-way triangulation. Presence is still hard-required
// where it must hold: clean-run and resumed images end with an
// uninjected digest build. The one thing this can never accept is
// present-but-wrong — the digest lie.
func (w sweepWorkload) verifyDigests(ctx context.Context, t *testing.T, e *Engine, requireDigests bool, label string) {
	t.Helper()

	groot, gok, err := e.GetGrantDigestGlobalRoot(ctx)
	require.NoErrorf(t, err, "%s: GetGrantDigestGlobalRoot", label)
	if !gok {
		require.Falsef(t, requireDigests, "%s: digest state absent on an image whose finishing EndSync ran uninjected — the build cannot legally have dropped", label)
		// Consistent absence: no entitlement root may survive a drop.
		for _, entID := range w.expectedEntIDsPerPrincipal() {
			id := entitlementIdentityFromParts("app", "github", entID)
			_, ok, err := e.GetEntitlementDigestRoot(ctx, id)
			require.NoErrorf(t, err, "%s: GetEntitlementDigestRoot(%s)", label, entID)
			require.Falsef(t, ok, "%s: global digest root absent but %s kept a root — partial digest state lies to the repair fast path", label, entID)
		}
		t.Logf("%s: digest state absent (legal drop); consistency verified", label)
		return
	}

	type fold struct {
		xor   [hashLen]byte
		count int64
	}
	primary := map[string]*fold{}
	require.NoErrorf(t, e.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		h, err := grantContentHashForRecord(r)
		require.NoErrorf(t, err, "%s: content hash for %s", label, r.GetExternalId())
		gid, err := grantIdentityFromRecord(r)
		require.NoErrorf(t, err, "%s: identity for %s", label, r.GetExternalId())
		part := digestPartitionForEntitlement(gid.entitlement)
		f := primary[part]
		if f == nil {
			f = &fold{}
			primary[part] = f
		}
		xorInto(f.xor[:], h)
		f.count++
		return true
	}), "%s: IterateGrants for digest oracle", label)

	var globalXor [hashLen]byte
	var globalCount int64
	for _, entID := range w.expectedEntIDsPerPrincipal() {
		id := entitlementIdentityFromParts("app", "github", entID)
		part := digestPartitionForEntitlement(id)

		root, ok, err := e.GetEntitlementDigestRoot(ctx, id)
		require.NoErrorf(t, err, "%s: GetEntitlementDigestRoot(%s)", label, entID)
		require.Truef(t, ok, "%s: finished store must have a digest root for %s (present-means-exact)", label, entID)

		pf := primary[part]
		require.NotNilf(t, pf, "%s: no primary grants found for %s", label, entID)
		require.Equalf(t, pf.count, root.Count, "%s: stored root count vs primary fold for %s", label, entID)
		require.Equalf(t, pf.xor[:], root.Hash, "%s: stored root hash vs primary fold for %s", label, entID)

		idxHash, idxCount, err := e.ComputeEntitlementBucketDigest(ctx, id, DigestBucket{})
		require.NoErrorf(t, err, "%s: ComputeEntitlementBucketDigest(%s)", label, entID)
		require.Equalf(t, root.Count, idxCount, "%s: hash-index fold count vs stored root for %s", label, entID)
		require.Equalf(t, root.Hash, idxHash, "%s: hash-index fold hash vs stored root for %s", label, entID)

		xorInto(globalXor[:], root.Hash)
		globalCount += root.Count
	}
	// Every grant-bearing partition must have been one of the expected
	// entitlements — an unexpected partition means cross-wired identity.
	require.Lenf(t, primary, len(w.expectedEntIDsPerPrincipal()), "%s: grant-bearing partition set", label)

	require.Equalf(t, globalCount, groot.Count, "%s: global root count vs entitlement-root fold", label)
	require.Equalf(t, globalXor[:], groot.Hash, "%s: global root hash vs entitlement-root fold", label)
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

// crashImageOutcome names which arm of the reopen dichotomy a crash
// image landed in — reported by the sweeps so the injection coverage
// is visible in the test log, not just implied by a green run.
type crashImageOutcome string

const (
	outcomeFinishedComplete   crashImageOutcome = "finished-and-complete"
	outcomeUnfinishedResumed  crashImageOutcome = "unfinished-resumed-to-completion"
	outcomeNoSyncRunRestarted crashImageOutcome = "no-sync-run-restarted"
)

// verifyCrashImage opens a clean engine over the crash clone and
// asserts the reopen dichotomy, resuming when unfinished. Returns the
// outcome arm for the caller's coverage accounting.
// replayPages selects the resume model: false = the pages were already
// durable before the window (EndSync sweep), true = replay them (whole
// sync soak).
func verifyCrashImage(ctx context.Context, t *testing.T, w sweepWorkload, image *vfs.MemFS, cache *pebble.Cache, syncID string, replayPages bool, label string) crashImageOutcome {
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
		w.verifyComplete(ctx, t, e, syncID, false, label+" (finished image)")
		return outcomeFinishedComplete
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
		w.verifyComplete(ctx, t, e, syncID, true, label+" (resumed image)")
		return outcomeUnfinishedResumed
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
		w.verifyComplete(ctx, t, e, newID, true, label+" (restarted image)")
		return outcomeNoSyncRunRestarted
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
	skipOnWindowsMemFS(t)
	ctx := context.Background()
	w := defaultSweepWorkload()
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()

	baseline, syncID := buildSweepBaseline(ctx, t, w, cache)

	const maxK = 5000
	covered := false
	var k, injectedRuns, fatalRuns int64
	outcomes := map[crashImageOutcome]int64{}
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
		if res.injected > 0 {
			injectedRuns++
		}
		if res.fatal {
			fatalRuns++
		}

		label := fmt.Sprintf("k=%d", k)
		if res.err == nil {
			// The run completed. If nothing was injected, k walked past the
			// last write op of the window — coverage proven. (A nonzero
			// injected count with a green run means the injection only hit
			// post-completion background work; verify and keep sweeping.)
			outcomes[verifyCrashImage(ctx, t, w, res.image, cache, syncID, false, label+" (clean run)")]++
			covered = res.injected == 0
			continue
		}
		require.Greaterf(t, res.injected, int64(0), "k=%d: EndSync failed with the injector armed but nothing injected: %v", k, res.err)
		outcomes[verifyCrashImage(ctx, t, w, res.image, cache, syncID, false, label)]++
	}
	// The coverage evidence, not just a green run: every k below the
	// terminator must have injected a fault, and both arms of the reopen
	// dichotomy must be populated (all-finished would mean the injections
	// never landed before the durability boundary; all-unfinished would
	// mean the window's tail — post-stamp failures — was never reached).
	require.Equalf(t, k-1, injectedRuns,
		"every failure point below the terminator must inject (swept %d, injected %d)", k-1, injectedRuns)
	require.Positivef(t, outcomes[outcomeUnfinishedResumed],
		"no injection landed before the finished verdict; the sweep isn't reaching the window's body: %v", outcomes)
	require.Positivef(t, outcomes[outcomeFinishedComplete],
		"no injection landed after the finished verdict; the sweep isn't reaching the window's tail: %v", outcomes)
	t.Logf("EndSync window covered: %d write-op failure points, all injected (%d pebble-fatal); crash-image outcomes: %v",
		k-1, fatalRuns, outcomes)
}

// TestErrorFSWholeSyncRandomSweepSoak randomizes the failure point
// across the whole sync (open → pages → EndSync) with the strictest
// crash image (no unsynced survival). Env-gated like the other soaks.
func TestErrorFSWholeSyncRandomSweepSoak(t *testing.T) {
	skipOnWindowsMemFS(t)
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

// TestEndSyncStampDurabilityCarriesPages pins the WAL-prefix-
// durability mechanism ITSELF: the finished stamp's pebble.Sync
// commit must harden every earlier NoSync page commit (sequential
// WAL). Isolation is the point — the default workload's EndSync
// performs OTHER Sync commits before the stamp (digest-pending
// marker arm/clear, deferred-marker clear), any of which would
// harden the pages and make the pin vacuous (review finding, delta
// round). So this variant strips them all: digests disabled, plain
// inline grants only (no deferred marker), pages committed NoSync
// with no clean close — leaving the ended_at stamp as the ONLY Sync
// between the pages and the crash cut, which the endSyncPreFlushHook seam
// takes immediately after the stamp (before the stats key's own
// Sync). A strict image (UnsyncedDataPercent=0) that reads finished
// but lost rows means the stamp's durability stopped carrying the
// pages — the finished-but-incomplete lying artifact.
func TestEndSyncStampDurabilityCarriesPages(t *testing.T) {
	skipOnWindowsMemFS(t)
	ctx := context.Background()
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()

	fs := vfs.NewCrashableMem()
	e, err := Open(ctx, "sweep-db",
		WithVFS(fs), WithSharedCache(cache), withPanicOnFatalLogger(),
		WithGrantDigestIndex(false))
	require.NoError(t, err)
	a := NewAdapter(e)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Inline-only pages: PutGrants writes by_principal inline and never
	// arms the deferred marker, so endSyncFinalize runs NO marker
	// clears and (digests off) no digest build — nothing Syncs between
	// these NoSync commits and the stamp.
	principals := []string{"alice", "bob", "carol"}
	require.NoError(t, a.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "app"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build()))
	require.NoError(t, a.PutEntitlements(ctx, v2.Entitlement_builder{
		Id: canonicalTestEntID("ent-00"),
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
		}.Build(),
	}.Build()))
	var gs []*v2.Grant
	for _, p := range principals {
		gs = append(gs, mkV2Grant("", "ent-00", "user", p))
	}
	require.NoError(t, a.PutGrants(ctx, gs...))
	require.False(t, e.deferredIdxPending.Load(), "inline-only pages must not arm the deferred marker (isolation precondition)")

	var image *vfs.MemFS
	e.test.endSyncPreFlushHook = func() {
		// The stamp is the only Sync since the pages; cut here.
		image = fs.CrashClone(vfs.CrashCloneCfg{})
	}
	require.NoError(t, a.EndSync(ctx))
	require.NotNil(t, image, "pre-flush hook must have fired")
	require.NoError(t, e.Close())

	ve, err := Open(ctx, "sweep-db", WithVFS(image), WithSharedCache(cache), withPanicOnFatalLogger(), WithGrantDigestIndex(false))
	require.NoError(t, err, "stamp-window crash image must reopen")
	defer func() { _ = ve.Close() }()
	rec, err := ve.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err, "the Sync-committed stamp must be in the crash image")
	require.NotNil(t, rec.GetEndedAt(), "the image must read as finished")

	va := NewAdapter(ve)
	require.NoError(t, va.SetCurrentSync(ctx, syncID))
	resp, err := va.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{}.Build())
	require.NoError(t, err)
	var wantIDs []string
	for _, p := range principals {
		wantIDs = append(wantIDs, canonicalTestGrantID("ent-00", "user", p))
	}
	gotIDs := make([]string, 0, len(resp.GetList()))
	for _, g := range resp.GetList() {
		gotIDs = append(gotIDs, g.GetId())
	}
	require.ElementsMatch(t, wantIDs, gotIDs,
		"finished image lost NoSync page rows: the stamp's Sync no longer carries earlier commits (WAL prefix durability broken)")
	for _, p := range principals {
		n := 0
		require.NoError(t, ve.IterateGrantsByPrincipal(ctx, "user", p, func(*v3.GrantRecord) bool {
			n++
			return true
		}))
		require.Equalf(t, 1, n, "by_principal for %s in the stamp-window image", p)
	}
}

// TestEndSyncStampWindowImageComplete is the full-default-workload
// companion: it cuts a strict crash image at the same post-stamp
// hook and requires dichotomy conformance — the image reads finished,
// so it must be content-complete, digests included. Unlike the
// isolated variant above it makes NO claim about WHICH Sync hardened
// the pages (the default EndSync has marker Syncs before the stamp);
// it pins the caller-visible contract at this window for the
// deferred+digest path.
func TestEndSyncStampWindowImageComplete(t *testing.T) {
	skipOnWindowsMemFS(t)
	ctx := context.Background()
	w := defaultSweepWorkload()
	cache := pebble.NewCache(8 << 20)
	defer cache.Unref()

	fs := vfs.NewCrashableMem()
	e, err := Open(ctx, "sweep-db", WithVFS(fs), WithSharedCache(cache), withPanicOnFatalLogger())
	require.NoError(t, err)
	a := NewAdapter(e)
	syncID, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, w.write(ctx, a))

	var image *vfs.MemFS
	e.test.endSyncPreFlushHook = func() {
		image = fs.CrashClone(vfs.CrashCloneCfg{})
	}
	require.NoError(t, a.EndSync(ctx))
	require.NotNil(t, image, "pre-flush hook must have fired")
	require.NoError(t, e.Close())

	ve, err := Open(ctx, "sweep-db", WithVFS(image), WithSharedCache(cache), withPanicOnFatalLogger())
	require.NoError(t, err, "stamp-window crash image must reopen")
	defer func() { _ = ve.Close() }()
	rec, err := ve.GetSyncRunRecord(ctx, syncID)
	require.NoError(t, err, "the Sync-committed stamp must be in the crash image")
	require.NotNil(t, rec.GetEndedAt(), "the image must read as finished")
	w.verifyComplete(ctx, t, ve, syncID, true, "stamp-window image")
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
	w.verifyComplete(ctx, t, e, syncID, true, label+" (restarted)")
}
