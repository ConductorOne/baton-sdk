package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// Source-cache replay orchestration (ADVANCED FUNCTIONALITY — see
// pkg/sourcecache for the connector-facing contract and
// docs/verification/sync-replay-6b/plan.md for the frozen behavioral
// contract this file implements).
//
// A connector opts in by attaching SourceCacheCapability MODE_READ_WRITE to
// its Validate response. The syncer then decides warm vs cold: warm installs
// a lookup backed by the previous sync artifact's source-cache manifest;
// every degradation installs NoopLookup so a well-behaved connector
// cold-fetches. The lookup is delivered through the connector client at sync
// start and cleared at sync end so a late RPC cannot read stale state.

// parseSourceCacheCapability extracts the SourceCacheCapability annotation
// from a Validate response. Absent or unparsable annotations mean the
// capability is not declared (unparsable additionally warns): source-cache
// handling stays off and the connector runs a plain cold sync.
func parseSourceCacheCapability(ctx context.Context, annos annotations.Annotations) *v2.SourceCacheCapability {
	capability := &v2.SourceCacheCapability{}
	ok, err := annos.Pick(capability)
	if err != nil {
		ctxzap.Extract(ctx).Warn("failed to parse SourceCacheCapability from Validate response; treating as not declared", zap.Error(err))
		return nil
	}
	if !ok {
		return nil
	}
	return capability
}

// sourceCacheEnabled reports whether this sync's connector declared
// SourceCacheCapability MODE_READ_WRITE and this sync's shape supports
// source-cache handling. Only then are SourceCacheRecord /
// SourceCacheReplay annotations honored (produce side); the consume side
// (warm lookup) additionally requires a usable previous artifact.
//
// Source-cache handling is untargeted-FULL-sync only (CO-6b-003): a
// partial, resources-only, or targeted sync intentionally collects less
// than the full inventory, so its pages must not stamp rows or publish
// validators, and its lookup must stay cold — a warm replay copy would
// import a whole scope's previous rows into a store that never selected
// them.
func (s *syncer) sourceCacheEnabled() bool {
	if s.sourceCacheCapability.GetMode() != v2.SourceCacheCapability_MODE_READ_WRITE {
		return false
	}
	return s.syncType == connectorstore.SyncTypeFull && len(s.targetedSyncResources) == 0
}

// sourceCacheEntryReader is the slice of the previous store's source-cache
// surface the warm lookup needs (satisfied by dotc1z.SourceCacheStore).
type sourceCacheEntryReader interface {
	LookupSourceCacheEntry(ctx context.Context, kind sourcecache.RowKind, scopeKey string) (sourcecache.Entry, bool, error)
}

// sourceCacheCompatReader is the slice of a store's source-cache surface the
// consume-side compat gate (G7) needs; sourceCacheCompatWriter adds the
// produce side (satisfied by dotc1z.SourceCacheStore).
type sourceCacheCompatReader interface {
	GetSourceCacheCompat(ctx context.Context) (sourcecache.CompatKey, bool, error)
}

type sourceCacheCompatWriter interface {
	sourceCacheCompatReader
	PutSourceCacheCompat(ctx context.Context, compat sourcecache.CompatKey) error
}

// sourceCacheSelectionFingerprint digests this sync's selection shape: a
// sync that intentionally collects less (resource-type filter, skip flags)
// must never serve as the replay base for one that collects more. Frozen
// canonical form (plan B4):
//
//	v1|types=<comma-joined sorted resource-type filter>|skipEG=<bool>|skipG=<bool>
//
// hashed as lowercase-hex sha256. The empty filter (all types) canonicalizes
// to an empty types segment. skipEG never appears on a FULL sync (it flips
// the sync type), but it shapes THIS sync's consume decision, so it stays in
// the canonical string rather than relying on that coupling.
func (s *syncer) sourceCacheSelectionFingerprint() string {
	types := slices.Clone(s.syncResourceTypes)
	slices.Sort(types)
	canonical := fmt.Sprintf("v1|types=%s|skipEG=%t|skipG=%t", strings.Join(types, ","), s.skipEntitlementsAndGrants, s.skipGrants)
	digest := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(digest[:])
}

// computeSourceCacheCompatKey builds the current sync's replay-compatibility
// key (plan B4). Connector components are taken verbatim from the declared
// capability; empty strings are legitimate values that match only empty.
func (s *syncer) computeSourceCacheCompatKey() sourcecache.CompatKey {
	return sourcecache.CompatKey{
		ConnectorCacheGeneration:     s.sourceCacheCapability.GetCacheGeneration(),
		ConnectorConfigFingerprint:   s.sourceCacheCapability.GetConfigFingerprint(),
		SDKMaterializationGeneration: sourcecache.MaterializationPolicyGeneration,
		SyncSelectionFingerprint:     s.sourceCacheSelectionFingerprint(),
	}
}

// ensureSourceCacheCompatRecord runs the produce-side compat lifecycle
// (plan B4): called once per sync attempt — including resumes — when the
// capability is MODE_READ_WRITE, after Validate and before the first list
// action. First attempt writes the record; a resume that recomputes the
// same key is an idempotent no-op.
//
// A resume that recomputes a DIFFERENT key means the connector or its
// configuration changed mid-sync and the artifact's cached rows are
// mixed-generation: the original record stays (it described the rows
// already recorded), the sync is marked replay-blocked through the
// ingest-quality reason flags so this artifact cannot seed the next sync,
// and drifted=true tells install to degrade this sync's own lookup to
// NoopLookup for the remainder.
//
// Errors are reads/writes against the sync's OWN store and fail the sync,
// matching every other current-store write on the sync path.
func (s *syncer) ensureSourceCacheCompatRecord(ctx context.Context, store sourceCacheCompatWriter) (bool, error) {
	computed := s.computeSourceCacheCompatKey()
	existing, found, err := store.GetSourceCacheCompat(ctx)
	if err != nil {
		return false, fmt.Errorf("source cache: read current sync compat record: %w", err)
	}
	if found {
		if existing == computed {
			return false, nil
		}
		s.ingestFilterStats.blockReplay(ingestQualityReasonCompatDriftOnResume)
		ctxzap.Extract(ctx).Warn(
			"source-cache compat key changed across resume attempts; this sync's rows are mixed-generation — "+
				"degrading to cold lookups and blocking this artifact as a future replay source",
			zap.Bool("cache_generation_drifted", existing.ConnectorCacheGeneration != computed.ConnectorCacheGeneration),
			zap.Bool("config_fingerprint_drifted", existing.ConnectorConfigFingerprint != computed.ConnectorConfigFingerprint),
			zap.Bool("materialization_generation_drifted", existing.SDKMaterializationGeneration != computed.SDKMaterializationGeneration),
			zap.Bool("selection_fingerprint_drifted", existing.SyncSelectionFingerprint != computed.SyncSelectionFingerprint),
		)
		return true, nil
	}
	if err := store.PutSourceCacheCompat(ctx, computed); err != nil {
		return false, fmt.Errorf("source cache: write compat record: %w", err)
	}
	return false, nil
}

// previousSyncSourceCacheLookup is the warm sourcecache.Lookup installed for
// a sync whose previous artifact passed every consume-side gate. Hits are
// reported through onHit so replay provenance (same-sync validator origin)
// can be enforced when a SourceCacheReplay annotation arrives.
type previousSyncSourceCacheLookup struct {
	prev sourceCacheEntryReader
	// onHit records a lookup hit and its validator for provenance
	// enforcement. May be nil.
	onHit func(rowKind sourcecache.RowKind, scopeKey string, cacheValidator string)
	// onConsult, when non-nil, observes every cleanly resolved lookup
	// (hit or miss) for the sync-trace audit. Purely observational.
	onConsult func(rowKind sourcecache.RowKind, scopeKey string)
}

var _ sourcecache.Lookup = (*previousSyncSourceCacheLookup)(nil)

// LookupPreviousSourceCache resolves a scope's previous-sync validator.
// Contract (pkg/sourcecache): internal read errors that leave fresh fetch
// available are misses, never connector-call failures — a miss means "fetch
// cold", which is always safe. Poisoned scopes already read as misses at the
// store layer. Invalid connector-supplied arguments are logged and read as
// misses for the same reason.
func (p *previousSyncSourceCacheLookup) LookupPreviousSourceCache(
	ctx context.Context,
	rowKind sourcecache.RowKind,
	scopeKey string,
) (sourcecache.Entry, bool, error) {
	l := ctxzap.Extract(ctx)
	if err := sourcecache.ValidateRowKind(rowKind); err != nil {
		l.Warn("source-cache lookup with invalid row kind; treating as miss",
			zap.String("row_kind", string(rowKind)),
			zap.Error(err),
		)
		return sourcecache.Entry{}, false, nil
	}
	if err := sourcecache.ValidateScopeKey(scopeKey); err != nil {
		l.Warn("source-cache lookup with invalid scope key; treating as miss",
			zap.String("row_kind", string(rowKind)),
			zap.Error(err),
		)
		return sourcecache.Entry{}, false, nil
	}
	entry, found, err := p.prev.LookupSourceCacheEntry(ctx, rowKind, scopeKey)
	if err != nil {
		l.Warn("source-cache lookup read failed; treating as miss",
			zap.String("row_kind", string(rowKind)),
			zap.String("scope_key", scopeKey),
			zap.Error(err),
		)
		return sourcecache.Entry{}, false, nil
	}
	if found && p.onHit != nil {
		p.onHit(rowKind, scopeKey, entry.CacheValidator)
	}
	if p.onConsult != nil {
		p.onConsult(rowKind, scopeKey)
	}
	return entry, found, nil
}

// sourceCacheWarmStore evaluates the install-time consume-side gates and
// returns the previous artifact's lookup surface when ALL pass. NewSyncer
// already gated artifact provenance (present, Pebble, latest finished sync
// FULL and non-compacted) before setting previousSyncReader; this adds the
// capability gate (G6) and the compat byte-match gate (G7). Every
// degradation warns with the failing condition and returns false: the sync
// proceeds cold, never errors.
func (s *syncer) sourceCacheWarmStore(ctx context.Context) (sourceCacheEntryReader, bool) {
	if !s.sourceCacheEnabled() {
		return nil, false
	}
	l := ctxzap.Extract(ctx)
	if s.previousSyncReader == nil {
		l.Warn("source-cache capability declared but no usable previous sync artifact; syncing cold")
		return nil, false
	}
	reader, ok := s.previousSyncReader.(sourceCacheEntryReader)
	if !ok {
		l.Warn("source-cache capability declared but previous sync store exposes no source-cache surface; syncing cold")
		return nil, false
	}
	// G7 — the previous artifact's stored compat key must byte-match the
	// current sync's computed key on every field. Absent or unreadable
	// records are mismatches: compatibility is declared, never inferred.
	compatReader, ok := s.previousSyncReader.(sourceCacheCompatReader)
	if !ok {
		l.Warn("source-cache capability declared but previous sync store exposes no compat record surface; syncing cold")
		return nil, false
	}
	prevCompat, found, err := compatReader.GetSourceCacheCompat(ctx)
	if err != nil {
		l.Warn("failed reading previous artifact's source-cache compat record; syncing cold", zap.Error(err))
		return nil, false
	}
	if !found {
		l.Warn("previous artifact carries no source-cache compat record; syncing cold")
		return nil, false
	}
	computed := s.computeSourceCacheCompatKey()
	if prevCompat != computed {
		l.Warn("previous artifact's source-cache compat key does not match this sync; syncing cold",
			zap.Bool("cache_generation_match", prevCompat.ConnectorCacheGeneration == computed.ConnectorCacheGeneration),
			zap.Bool("config_fingerprint_match", prevCompat.ConnectorConfigFingerprint == computed.ConnectorConfigFingerprint),
			zap.Bool("materialization_generation_match", prevCompat.SDKMaterializationGeneration == computed.SDKMaterializationGeneration),
			zap.Bool("selection_fingerprint_match", prevCompat.SyncSelectionFingerprint == computed.SyncSelectionFingerprint),
		)
		return nil, false
	}
	return reader, true
}

// installSourceCacheLookup runs the per-attempt source-cache install
// sequence: the produce-side compat lifecycle (write/verify the current
// store's compat record — plan B4), then lookup delivery to the connector
// (warm lookup when every consume gate passes, nil otherwise — the builder
// substitutes NoopLookup). It returns the teardown that clears the
// connector-held lookup at sync end. Delivery is unconditional when the
// transport supports it so a long-lived connector server never carries a
// prior sync's lookup into this one.
//
// The returned error is always a current-store failure (compat record
// read/write); every consume-side gate failure degrades to cold instead.
func (s *syncer) installSourceCacheLookup(ctx context.Context) (func(), error) {
	l := ctxzap.Extract(ctx)

	// Produce side first: when the connector is capable and the store can
	// record replay state, the compat record must exist before any list
	// action — even on syncs that end up consuming cold. A capable
	// connector on a store without the source-cache surface runs as if the
	// capability were absent (replay is Pebble-only by design).
	s.sourceCacheStore = nil
	compatDrifted := false
	if s.sourceCacheEnabled() {
		store, ok := s.store.(dotc1z.SourceCacheStore)
		if !ok {
			l.Warn("source-cache capability declared but this sync's store has no source-cache surface; source-cache handling disabled")
		} else {
			var err error
			compatDrifted, err = s.ensureSourceCacheCompatRecord(ctx, store)
			if err != nil {
				return nil, err
			}
			s.sourceCacheStore = store
		}
	} else if store, ok := s.store.(dotc1z.SourceCacheStore); ok {
		// Capability withdrawn or sync shape changed across resume
		// attempts (CO-6b-003): a fresh sync's store can never hold a
		// compat record (replacement syncs excise the family), so finding
		// one here means a PRIOR ATTEMPT ran with source-cache enabled and
		// this attempt does not. The rows recorded before the cut carry
		// stamps and possibly manifest entries whose recording conditions
		// this attempt no longer declares — the same mixed-generation
		// hazard as compat drift, so it gets the same treatment: the
		// artifact is blocked as a future replay source. Read failures
		// block too (fail-closed: unknown produce history is untrusted).
		if s.sourceCacheCapability.GetMode() == v2.SourceCacheCapability_MODE_READ_WRITE {
			l.Warn("source-cache capability declared but this sync is not an untargeted FULL sync; source-cache handling disabled")
		}
		_, found, err := store.GetSourceCacheCompat(ctx)
		if err != nil || found {
			s.ingestFilterStats.blockReplay(ingestQualityReasonCompatDriftOnResume)
			l.Warn("source-cache produce state exists from a prior attempt but this attempt runs without source-cache handling; "+
				"blocking this artifact as a future replay source",
				zap.Bool("compat_record_found", found),
				zap.Error(err),
			)
		}
	}

	setter, canDeliver := s.connector.(sourcecache.SetLookup)
	if !canDeliver {
		if s.sourceCacheEnabled() {
			l.Warn("source-cache capability declared but the connector client cannot receive a lookup; syncing cold")
		}
		return func() {}, nil
	}
	// A client can satisfy SetLookup structurally while wrapping a
	// transport that cannot forward an interface value (the runner's
	// subprocess wrapper, CO-6b-001). Delivery into the void must not
	// count as warm: the connector would consult NoopLookup while the
	// syncer believed a warm lookup was live.
	if probe, ok := s.connector.(sourcecache.LookupDeliverabilityProbe); ok && !probe.SourceCacheLookupDeliverable() {
		if s.sourceCacheEnabled() {
			l.Warn("source-cache capability declared but the connector transport cannot deliver a lookup " +
				"(subprocess connectors have no in-process backchannel, CO-6b-001); syncing cold")
		}
		return func() {}, nil
	}

	var lookup sourcecache.Lookup
	// Compat drift on resume forces the remainder of the sync cold even
	// when every consume gate would pass: the drifted capability describes
	// a different connector than the one whose validators the previous
	// artifact recorded (plan B4).
	if s.sourceCacheStore != nil && !compatDrifted {
		if warmStore, warm := s.sourceCacheWarmStore(ctx); warm {
			lookup = &previousSyncSourceCacheLookup{
				prev:  warmStore,
				onHit: s.recordSourceCacheHit,
				onConsult: func(rowKind sourcecache.RowKind, scopeKey string) {
					s.testSyncTraceAudit.record(syncTraceConsult, string(rowKind), scopeKey)
				},
			}
		}
	}
	setter.SetSourceCache(ctx, lookup)
	if lookup != nil {
		// Warm only after delivery actually happened; this flag is the
		// provenance gate replay enforcement reads (plan B5, CO-6b-003).
		s.sourceCacheWarm = true
		l.Info("source-cache replay lookup installed (warm)")
	}

	// The teardown must run even when the sync exits because ctx was
	// canceled; the cleared state is what protects the NEXT sync.
	teardownCtx := context.WithoutCancel(ctx)
	return func() {
		s.sourceCacheWarm = false
		setter.SetSourceCache(teardownCtx, nil)
	}, nil
}

// recordSourceCacheHit records a warm-lookup hit and its validator for
// same-sync replay provenance. Recording is durable (checkpointed with the
// sync token) so a planning call's verdicts survive interrupt/resume; see
// the replay handling for enforcement, including the validator binding
// against the current replay base.
func (s *syncer) recordSourceCacheHit(rowKind sourcecache.RowKind, scopeKey string, cacheValidator string) {
	s.state.RecordSourceCacheHit(rowKind, scopeKey, cacheValidator)
}

// sourceCacheScopeLock returns the mutex serializing page application for
// one (rowKind, scopeKey) — every scoped page (record or replay) holds it
// from beforeUpserts through afterUpserts (CO-6b-006). Locks are per-syncer
// and never reclaimed: the map is bounded by the number of distinct scoped
// (rowKind, scopeKey) pairs in one sync.
func (s *syncer) sourceCacheScopeLock(rowKind sourcecache.RowKind, scopeKey string) *sync.Mutex {
	key := sourceCacheScopeKey(rowKind, scopeKey)
	if mu, ok := s.sourceCacheScopeLocks.Load(key); ok {
		return mu.(*sync.Mutex)
	}
	mu, _ := s.sourceCacheScopeLocks.LoadOrStore(key, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

// sourceCacheScopeKey is the map key for per-(rowKind, scopeKey) syncer
// state (scope locks, the attempt-local grounded set).
func sourceCacheScopeKey(rowKind sourcecache.RowKind, scopeKey string) string {
	return string(rowKind) + "\x00" + scopeKey
}

// === Page handling (plan B3 fresh pages, B5 replay pages) ===

// sourceCachePageOps drives one list-response page's source-cache work.
// The three collection handlers call, in order:
//
//	ops, err := s.sourceCachePageOps(ctx, rowKind, respAnnos, pageRows)
//	defer ops.release()      // scope-lock backstop for error paths
//	ops.beforeUpserts(ctx)   // scope lock + replay copy (B5), before row puts
//	ops.stampCtx(ctx)        // ctx for the page's CONNECTOR-row puts only
//	ops.afterUpserts(ctx)    // tombstones, then validator publish; unlocks
//
// A nil *sourceCachePageOps (page without honored annotations) no-ops all
// four, keeping the handlers' happy path annotation-free.
type sourceCachePageOps struct {
	s        *syncer
	rowKind  sourcecache.RowKind
	scopeKey string
	record   *v2.SourceCacheRecord
	replay   *v2.SourceCacheReplay
	pageRows int
	// held is the scope lock acquired by beforeUpserts and released by
	// afterUpserts (or by the handler's deferred release() on error paths
	// between the two). See beforeUpserts for why EVERY scoped page holds
	// it, not just replay pages.
	held *sync.Mutex
}

// release unlocks the scope lock if this page still holds it. Idempotent
// and nil-safe: handlers defer it unconditionally so an error between
// beforeUpserts and afterUpserts cannot leak the lock (a leaked scope lock
// would deadlock every later page for the scope, including the action's
// own retry).
func (o *sourceCachePageOps) release() {
	if o == nil || o.held == nil {
		return
	}
	o.held.Unlock()
	o.held = nil
}

// sourceCachePageOps parses and gates the page's source-cache annotations.
// Row kind comes from the RPC, never the annotation (proto contract).
//
//   - Capability not MODE_READ_WRITE: annotations are ignored wholesale
//     (proto contract, B1). Returns nil ops.
//   - Capability declared but the store has no source-cache surface: a
//     record is ignored with a warn (nothing can be stamped — cold-sync
//     behavior); a replay is a loud cold failure, because the connector
//     skipped row generation and there is nothing to fall back to.
//   - Honored annotations with invalid shapes — unparsable annotation,
//     invalid scope key, two different scope keys on one page (a page's
//     rows can be stamped with only one scope), or principal tombstones on
//     an entitlements page — fail the sync loudly with a cold verdict (B3).
func (s *syncer) sourceCachePageOps(
	ctx context.Context,
	rowKind sourcecache.RowKind,
	annos annotations.Annotations,
	pageRows int,
) (*sourceCachePageOps, error) {
	record := &v2.SourceCacheRecord{}
	hasRecord, recordErr := annos.Pick(record)
	replay := &v2.SourceCacheReplay{}
	hasReplay, replayErr := annos.Pick(replay)

	if !s.sourceCacheEnabled() {
		if hasRecord || hasReplay || recordErr != nil || replayErr != nil {
			ctxzap.Extract(ctx).Debug("source-cache annotations on page ignored: capability not declared MODE_READ_WRITE")
		}
		return nil, nil
	}
	if recordErr != nil {
		return nil, newReplayIntegrityError(ReplayVerdictCold, rowKind, "",
			fmt.Errorf("unparsable SourceCacheRecord annotation: %w", recordErr))
	}
	if replayErr != nil {
		return nil, newReplayIntegrityError(ReplayVerdictCold, rowKind, "",
			fmt.Errorf("unparsable SourceCacheReplay annotation: %w", replayErr))
	}
	if !hasRecord && !hasReplay {
		return nil, nil
	}
	// Duplicate same-type annotations would silently collapse to the first
	// (Pick returns the first match), letting a second scope bypass the
	// one-scope-per-page rule below. Reject them loudly (CO-6b-003).
	var recordCount, replayCount int
	for _, a := range annos {
		switch {
		case a.MessageIs(record):
			recordCount++
		case a.MessageIs(replay):
			replayCount++
		}
	}
	if recordCount > 1 || replayCount > 1 {
		return nil, newReplayIntegrityError(ReplayVerdictCold, rowKind, "",
			fmt.Errorf("page carries %d SourceCacheRecord and %d SourceCacheReplay annotations: at most one of each is defined",
				recordCount, replayCount))
	}
	if s.sourceCacheStore == nil {
		if hasReplay {
			return nil, newReplayIntegrityError(ReplayVerdictCold, rowKind, replay.GetScopeKey(),
				fmt.Errorf("SourceCacheReplay on a store with no source-cache surface: the connector skipped row generation and there is nothing to fall back to"))
		}
		ctxzap.Extract(ctx).Warn("SourceCacheRecord ignored: this sync's store has no source-cache surface",
			zap.String("row_kind", string(rowKind)),
			zap.String("scope_key", record.GetScopeKey()),
		)
		return nil, nil
	}

	ops := &sourceCachePageOps{s: s, rowKind: rowKind, pageRows: pageRows}
	if hasReplay {
		if err := sourcecache.ValidateScopeKey(replay.GetScopeKey()); err != nil {
			return nil, newReplayIntegrityError(ReplayVerdictCold, rowKind, replay.GetScopeKey(),
				fmt.Errorf("SourceCacheReplay scope key: %w", err))
		}
		ops.replay = replay
		ops.scopeKey = replay.GetScopeKey()
	}
	if hasRecord {
		if err := sourcecache.ValidateScopeKey(record.GetScopeKey()); err != nil {
			return nil, newReplayIntegrityError(ReplayVerdictCold, rowKind, record.GetScopeKey(),
				fmt.Errorf("SourceCacheRecord scope key: %w", err))
		}
		if hasReplay && record.GetScopeKey() != replay.GetScopeKey() {
			return nil, newReplayIntegrityError(ReplayVerdictCold, rowKind, record.GetScopeKey(),
				fmt.Errorf("page carries SourceCacheRecord scope %q and SourceCacheReplay scope %q: a page's rows can be stamped with only one scope",
					record.GetScopeKey(), replay.GetScopeKey()))
		}
		ops.record = record
		ops.scopeKey = record.GetScopeKey()
	}
	if rowKind == sourcecache.RowKindEntitlements &&
		(len(record.GetDeletedPrincipalIds()) > 0 || len(replay.GetDeletedPrincipalIds()) > 0) {
		return nil, newReplayIntegrityError(ReplayVerdictCold, rowKind, ops.scopeKey,
			fmt.Errorf("deleted_principal_ids on an entitlements page: the proto defines no semantics for it"))
	}
	return ops, nil
}

// warnIgnoredSourceCacheAnnotations covers list surfaces where source-cache
// annotations are registered exclusions (static entitlements, B3): the
// annotations are ignored with a warn instead of being honored or failing.
func warnIgnoredSourceCacheAnnotations(ctx context.Context, surface string, annos annotations.Annotations) {
	if !annos.ContainsAny(&v2.SourceCacheRecord{}, &v2.SourceCacheReplay{}) {
		return
	}
	ctxzap.Extract(ctx).Warn("source-cache annotations are not supported on this surface; ignored",
		zap.String("surface", surface),
	)
}

// stampCtx returns the context for the page's connector-row puts. Rows of a
// scope-annotated page (fresh or replay-overlay) are stamped with the scope
// via sourcecache.WithScope; the Pebble adapter picks the scope up at write
// time. SDK-derived writes (expansion output, reconciliation, sub-resource
// or related-resource fetches) must NOT go through this context.
func (o *sourceCachePageOps) stampCtx(ctx context.Context) context.Context {
	if o == nil || o.scopeKey == "" {
		return ctx
	}
	return sourcecache.WithScope(ctx, o.scopeKey)
}

// beforeUpserts acquires the page's scope lock and, for replay pages, runs
// the replay copy (B5) before the page's row puts, enforcing same-sync
// provenance and once-per-scope idempotence.
//
// EVERY scoped page — record-only included — holds the scope's lock from
// here through afterUpserts (CO-6b-003, extended per re-review N1): at
// worker counts above one, two pages carrying replay annotations for the
// same scope could otherwise both observe "not yet replayed" and both run
// the REPLACEMENT copy — the second wiping overlay rows the first already
// applied; and a record-only page's row puts, tombstones, and manifest
// publish could interleave with another action's in-progress replacement
// copy for the same scope, which deletes the scope's rows before copying
// the base — silently wiping the fresh rows or publishing a validator over
// an incomplete scope. Handlers must defer release() so error paths
// between beforeUpserts and afterUpserts cannot leak the lock.
func (o *sourceCachePageOps) beforeUpserts(ctx context.Context) error {
	if o == nil {
		return nil
	}
	mu := o.s.sourceCacheScopeLock(o.rowKind, o.scopeKey)
	mu.Lock()
	o.held = mu
	if o.replay == nil {
		// Record-only page: ground the replacement listing before its
		// first write this attempt.
		return o.groundRecordScope(ctx)
	}
	l := ctxzap.Extract(ctx)
	// The warm flag gates every replay this attempt (CO-6b-003): the
	// checkpointed hit-set can authorize a handed-off replay verdict only
	// while the ATTEMPT that drains it still has the warm lookup installed.
	// A resume whose gates degraded to cold (compat drift, withdrawn
	// previous artifact) must not honor hits recorded by an earlier attempt
	// against a base this attempt never re-validated.
	//
	// Operational contract for these cold verdicts until Phase 6c: nothing
	// in-tree consumes ReplayVerdictCold yet, and the offending replay
	// verdict may live in a checkpointed cursor, so a sync degraded
	// mid-flight fails identically on every resume — deterministically and
	// loudly, like any deterministic connector error — until the caller's
	// retry policy abandons the unfinished sync and starts a fresh (cold)
	// one. 6c's runner ladder consumes the verdict to make that fallback
	// automatic.
	if !o.s.sourceCacheWarm {
		return newReplayIntegrityError(ReplayVerdictCold, o.rowKind, o.scopeKey,
			fmt.Errorf("SourceCacheReplay while this attempt's lookup is not warm: the replay base was not re-validated by this attempt's consume gates"))
	}
	// Same-sync provenance: the replayed scope's validator must have come
	// from THIS sync's lookup (possibly a prior attempt of it — the hit-set
	// is checkpoint-durable). This is also what rejects every replay while
	// the lookup is NoopLookup: a cold sync records no hits.
	hitValidator, hasHit := o.s.state.SourceCacheHitValidator(o.rowKind, o.scopeKey)
	if !hasHit {
		return newReplayIntegrityError(ReplayVerdictCold, o.rowKind, o.scopeKey,
			fmt.Errorf("SourceCacheReplay for a scope with no lookup hit recorded this sync: the validator did not originate from this sync's lookup"))
	}
	if o.s.state.SourceCacheReplayed(o.rowKind, o.scopeKey) {
		// Duplicate page / lost-response retry: the copy already ran this
		// sync. Replay is replacement, so re-running it would also wipe
		// overlay rows upserted since. Skip the copy; apply the page's
		// upserts/tombstones normally.
		l.Debug("source-cache replay already completed for scope this sync; skipping copy",
			zap.String("row_kind", string(o.rowKind)),
			zap.String("scope_key", o.scopeKey),
		)
	} else {
		if o.s.previousSyncReader == nil {
			return newReplayIntegrityError(ReplayVerdictCold, o.rowKind, o.scopeKey,
				fmt.Errorf("no previous sync artifact available to replay from"))
		}
		// Bind the hit to the CURRENT replay base. The eligibility gates
		// cannot distinguish two artifacts from the same connector and
		// config (identical compat keys), so a previous artifact swapped
		// between attempts — service-mode spare replaced by a
		// rollback/restore — passes every gate while its rows for this
		// scope may predate the state the connector actually revalidated.
		// The recorded validator is the one the connector's verdict was
		// computed against; the base we copy from must still publish
		// exactly it. Mismatch, absence, and read failure are all cold:
		// the copy's provenance cannot be established.
		reader, ok := o.s.previousSyncReader.(sourceCacheEntryReader)
		if !ok {
			return newReplayIntegrityError(ReplayVerdictCold, o.rowKind, o.scopeKey,
				fmt.Errorf("previous sync artifact exposes no source-cache surface to verify the replay base against the recorded hit"))
		}
		baseEntry, found, err := reader.LookupSourceCacheEntry(ctx, o.rowKind, o.scopeKey)
		if err != nil {
			return newReplayIntegrityError(ReplayVerdictCold, o.rowKind, o.scopeKey,
				fmt.Errorf("reading the replay base's manifest entry to verify the recorded hit: %w", err))
		}
		if !found {
			return newReplayIntegrityError(ReplayVerdictCold, o.rowKind, o.scopeKey,
				fmt.Errorf("replay base has no manifest entry for this scope: the recorded hit came from a different artifact"))
		}
		if baseEntry.CacheValidator != hitValidator {
			return newReplayIntegrityError(ReplayVerdictCold, o.rowKind, o.scopeKey,
				fmt.Errorf("replay base's validator does not match the one this sync's lookup returned: the previous artifact changed between attempts"))
		}
		res, err := o.s.sourceCacheStore.ReplaySourceCache(ctx, o.s.previousSyncReader, o.rowKind, o.scopeKey)
		if err != nil {
			return newReplayIntegrityError(replayCopyVerdict(err), o.rowKind, o.scopeKey,
				fmt.Errorf("replay copy: %w", err))
		}
		// Trace-audit the unit's legs in the store's contractual
		// clear-then-copy order (TRACE_BRIDGE.md unit expansion).
		o.s.testSyncTraceAudit.record(syncTraceClear, string(o.rowKind), o.scopeKey)
		o.s.testSyncTraceAudit.record(syncTraceReplay, string(o.rowKind), o.scopeKey)
		if res.NeedsExpansion && !o.s.dontExpandGrants {
			o.s.state.SetNeedsExpansion()
		}
		o.s.state.MarkSourceCacheReplayed(o.rowKind, o.scopeKey)
		l.Debug("source-cache replay copied previous sync's scope rows",
			zap.String("row_kind", string(o.rowKind)),
			zap.String("scope_key", o.scopeKey),
			zap.Int64("rows", res.Rows),
		)
	}
	// The scope's base is established this attempt (the copy ran, or a
	// committed copy was observed via the replayed-set): record pages
	// later in this attempt must not re-ground over it.
	o.s.sourceCacheScopeGrounded.Store(sourceCacheScopeKey(o.rowKind, o.scopeKey), struct{}{})
	if !o.replay.GetOverlay() && o.pageRows > 0 {
		// TRANSITIONAL tolerance (proto contract, pins 6a C34): a
		// non-overlay replay page must carry no rows. Warn and apply them
		// with overlay semantics; this hardens to an error later.
		l.Warn("SourceCacheReplay page with overlay=false carries rows; applying them as overlay upserts (transitional tolerance)",
			zap.String("row_kind", string(o.rowKind)),
			zap.String("scope_key", o.scopeKey),
			zap.Int("rows", o.pageRows),
		)
	}
	return nil
}

// groundRecordScope grounds a record round's replacement semantics: before
// the round's first write to a scope this attempt, a partition holding rows
// that no completed round published is cleared. Un-published rows are
// un-attributed debris from a crashed attempt — most dangerously a replay
// copy whose round never published before a cut, after which upstream moved
// and the resume's consult missed (the verdict-flip path). Composing the
// record round's fresh listing with that debris seals a phantom union under
// the fresh validator, which the NEXT sync's consult validates clean and
// replays forward — the non-self-healing direction. This is the walker
// model's scenario-1 family (formal/walker/CALIBRATION.md, tc1c flavor),
// witnessed against this code by
// TestChaosSourceCacheRecordFlipOverReplayDebris.
//
// The rule fires once per scope per attempt (the grounded set is volatile
// by design — a resume re-decides from the durable facts) and skips scopes
// with a manifest entry: a published entry means a completed round owns the
// partition's rows, so later record pages accumulate exactly as before.
// The caller holds the scope lock. Clearing an empty partition is a no-op,
// so the common born-empty case costs one manifest lookup.
func (o *sourceCachePageOps) groundRecordScope(ctx context.Context) error {
	key := sourceCacheScopeKey(o.rowKind, o.scopeKey)
	if _, done := o.s.sourceCacheScopeGrounded.Load(key); done {
		return nil
	}
	_, published, err := o.s.sourceCacheStore.LookupSourceCacheEntry(ctx, o.rowKind, o.scopeKey)
	if err != nil {
		return newReplayIntegrityError(ReplayVerdictWarm, o.rowKind, o.scopeKey,
			fmt.Errorf("record grounding: reading this sync's manifest entry: %w", err))
	}
	if !published {
		deleted, err := o.s.sourceCacheStore.ClearSourceCacheScope(ctx, o.rowKind, o.scopeKey)
		if err != nil {
			return newReplayIntegrityError(ReplayVerdictWarm, o.rowKind, o.scopeKey,
				fmt.Errorf("record grounding: clearing un-attributed rows: %w", err))
		}
		o.s.testSyncTraceAudit.record(syncTraceClear, string(o.rowKind), o.scopeKey)
		if deleted > 0 {
			ctxzap.Extract(ctx).Warn("record grounding cleared un-attributed rows from a prior attempt",
				zap.String("row_kind", string(o.rowKind)),
				zap.String("scope_key", o.scopeKey),
				zap.Int64("rows", deleted),
			)
		}
	}
	o.s.sourceCacheScopeGrounded.Store(key, struct{}{})
	return nil
}

// replayCopyVerdict classifies a ReplaySourceCache failure per plan B7:
// destination-commit failures past preflight and interruptions are warm
// (the replay decision was sound; retry may succeed warm); everything else
// — preflight/source integrity, eligibility, ambiguous store errors — is
// cold, fail-closed. Only the ERROR CHAIN classifies: ambient context state
// must not promote (CO-6b-003) — in parallel mode a sibling action's
// failure cancels the batch context, and a genuine source-integrity error
// racing that cancellation would otherwise read as warm, the exact
// fail-open B7's cold default exists to prevent.
func replayCopyVerdict(err error) ReplayVerdict {
	if errors.Is(err, dotc1z.ErrSourceCacheReplayDestination) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return ReplayVerdictWarm
	}
	return ReplayVerdictCold
}

// wrapPageRowPutError classifies a failure writing THIS page's own rows on
// a scope-annotated page. Plan B7 names overlay/record upsert write errors
// as destination-side failures after a sound decision: warm. Nil ops (page
// without honored annotations) and nil errors pass through untouched, so
// the handlers can wrap unconditionally.
func (o *sourceCachePageOps) wrapPageRowPutError(err error) error {
	if o == nil || err == nil || o.scopeKey == "" {
		return err
	}
	return newReplayIntegrityError(ReplayVerdictWarm, o.rowKind, o.scopeKey,
		fmt.Errorf("page row upserts: %w", err))
}

// afterUpserts applies the page's tombstones (after its rows commit — B3's
// within-page order, closing 6a C29's orchestration half) and then
// publishes the scope's manifest entry when the page supplied a validator.
// Write failures here are destination-side after a sound decision: warm.
// Deterministic input-shape failures (a malformed tombstone id the store
// can never resolve) are cold — retrying them warm cannot succeed
// (CO-6b-003).
func (o *sourceCachePageOps) afterUpserts(ctx context.Context) error {
	if o == nil {
		return nil
	}
	defer o.release()
	if o.pageRows > 0 {
		// The page's row puts committed between beforeUpserts and here.
		o.s.testSyncTraceAudit.record(syncTraceUpsert, string(o.rowKind), o.scopeKey)
	}
	// Replay tombstones precede record tombstones (the replay annotation
	// describes the base; the record describes this page), each canonical
	// before principal-scoped.
	canonical := append(append([]string{}, o.replay.GetDeletedIds()...), o.record.GetDeletedIds()...)
	if o.rowKind == sourcecache.RowKindResources {
		// The store contract requires Baton resource BIDs for resource
		// tombstones; a malformed id is a connector bug that fails
		// deterministically before any write, so it must not read warm.
		for _, id := range canonical {
			if !strings.HasPrefix(id, "bid:r:") {
				return newReplayIntegrityError(ReplayVerdictCold, o.rowKind, o.scopeKey,
					fmt.Errorf("resource tombstone %q is not a Baton resource BID (bid:r:...)", id))
			}
		}
	}
	if len(canonical) > 0 {
		if err := o.s.sourceCacheStore.DeleteSourceCacheRows(ctx, o.rowKind, o.scopeKey, canonical); err != nil {
			return newReplayIntegrityError(ReplayVerdictWarm, o.rowKind, o.scopeKey,
				fmt.Errorf("canonical-id tombstones: %w", err))
		}
		o.s.testSyncTraceAudit.record(syncTraceDelete, string(o.rowKind), o.scopeKey)
	}
	principals := append(append([]string{}, o.replay.GetDeletedPrincipalIds()...), o.record.GetDeletedPrincipalIds()...)
	if len(principals) > 0 {
		if _, err := o.s.sourceCacheStore.DeleteSourceCacheRowsInScope(ctx, o.rowKind, o.scopeKey, principals); err != nil {
			return newReplayIntegrityError(ReplayVerdictWarm, o.rowKind, o.scopeKey,
				fmt.Errorf("principal-scoped tombstones: %w", err))
		}
		o.s.testSyncTraceAudit.record(syncTraceDelete, string(o.rowKind), o.scopeKey)
	}
	// Validator publish AFTER the page's rows and tombstones: a failed
	// page can't leave a phantom manifest entry vouching for rows that
	// never landed. The record's validator wins over the replay's (a delta
	// round's final record carries the NEW token); interim empty
	// validators publish nothing (B3 — a scope whose round never supplies
	// one is a miss next sync).
	validator := o.record.GetCacheValidator()
	if validator == "" {
		validator = o.replay.GetCacheValidator()
	}
	if validator != "" {
		if err := o.s.sourceCacheStore.PutSourceCacheEntry(ctx, o.rowKind, o.scopeKey, validator); err != nil {
			return newReplayIntegrityError(ReplayVerdictWarm, o.rowKind, o.scopeKey,
				fmt.Errorf("manifest publish: %w", err))
		}
		o.s.testSyncTraceAudit.record(syncTracePublish, string(o.rowKind), o.scopeKey)
	}
	return nil
}
