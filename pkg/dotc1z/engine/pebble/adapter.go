package pebble

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// This file is the Engine's connectorstore face: the sync lifecycle
// (StartNewSync/ResumeSync/CheckpointSync/EndSync) and the v2
// writer/reader surface C1 + the syncer call. It translates v2 wire
// types ↔ v3 record types via translate_v2.go and routes Put/Get/List
// into the per-record-type methods. Historically this was a separate
// Adapter type wrapping the Engine; PR 2.6 dissolved that layer — the
// Engine implements the surface directly, and the only sync-lifecycle
// state is the engine's own currentSync binding plus the durable
// SyncRunRecord (read on demand, like the SQLite engine's row-backed
// reads). Adapter survives as a type alias for compatibility.

// Adapter is the Engine, by its historical name. The wrapping type
// dissolved when the layer collapsed; the alias keeps existing
// constructors and fixtures compiling. New code should use *Engine
// directly.
//
// DELIBERATE BREAK vs the old wrapper: a bare Adapter/Engine is NOT a
// connectorstore.Writer anymore. The old type carried Close(ctx); the
// Engine's teardown is Close() (no ctx), and a Go alias cannot carry
// both signatures. The supported Writer — before and after — is the
// pkg/dotc1z store (dotc1z.NewStore with the Pebble engine), whose
// Close(ctx) owns the envelope save the bare engine never performed;
// a bare handle "closing" a c1z without saving it was a trap, not a
// contract. Every other Writer method remains on the Engine, so code
// holding a concrete *Adapter compiles untouched except Close(ctx)
// call sites, which drop the ctx.
type Adapter = Engine

// NewAdapter is a compatibility constructor from the dissolved-layer
// era: the "adapter" IS the engine now.
func NewAdapter(e *Engine) *Adapter { return e }

// Compile-time checks for the optional connectorstore capabilities
// that SQLite's *C1File also exposes. (The full connectorstore.Writer
// contract is asserted on pkg/dotc1z's store via c1zstore.Store — see
// the Adapter alias doc for why the bare engine is deliberately not a
// Writer.)
var (
	_ connectorstore.LatestFinishedSyncIDFetcher  = (*Engine)(nil)
	_ connectorstore.DBSizeProvider               = (*Engine)(nil)
	_ connectorstore.EntitlementGrantDigestReader = (*Engine)(nil)
)

// === sync lifecycle ===

// StartNewSync creates a new sync_run record under a freshly-minted
// sync_id. Returns the new sync_id.
func (e *Engine) StartNewSync(ctx context.Context, syncType connectorstore.SyncType, parentSyncID string) (string, error) {
	return e.startNewSync(ctx, syncType, "", parentSyncID)
}

// StartNewSyncWithID is StartNewSync but adopts the caller-supplied
// syncID instead of minting one. It exists for conversion/compaction
// (e.g. ToPebble) that must preserve the source sync's identity so the
// produced file's sync_id matches the snapshot it was derived from.
// syncID must be non-empty.
func (e *Engine) StartNewSyncWithID(ctx context.Context, syncType connectorstore.SyncType, syncID, parentSyncID string) (string, error) {
	if syncID == "" {
		return "", errors.New("StartNewSyncWithID: empty syncID")
	}
	return e.startNewSync(ctx, syncType, syncID, parentSyncID)
}

// startNewSync opens a new sync. An empty syncID mints a fresh ksuid;
// a non-empty syncID is adopted verbatim (the single-sync file holds
// exactly one sync, so any prior data is wiped first regardless).
func (e *Engine) startNewSync(ctx context.Context, syncType connectorstore.SyncType, syncID, parentSyncID string) (string, error) {
	if syncID == "" {
		syncID = ksuid.New().String()
	}
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	// Single-sync contract: a v3 Pebble c1z holds exactly one sync.
	// Keys carry no sync_id, so if a prior sync's data is present we
	// must wipe it before starting — otherwise records the new sync
	// doesn't overwrite would linger as orphans under identical keys.
	// This also restores the empty-by-construction invariant that the
	// MarkFreshSync skip-Get fast path depends on. A fresh engine
	// (the common ToPebble/compaction case) has no sync-run and skips
	// the wipe.
	if existed, err := e.hasSyncRun(); err != nil {
		return "", err
	} else if existed {
		if err := e.ResetForNewSync(ctx); err != nil {
			return "", err
		}
	}
	// MarkFreshSync flips the engine into the perf-fast write path:
	// pebble.NoSync per commit, skip read-before-write index cleanup.
	// EndSync calls EndFreshSync to flush + fsync once at the end.
	if err := e.MarkFreshSync(syncID); err != nil {
		return "", err
	}
	rec := v3.SyncRunRecord_builder{
		SyncId:       syncID,
		Type:         v2SyncTypeToV3(syncType),
		ParentSyncId: parentSyncID,
		StartedAt:    timestamppb.New(time.Now()),
	}.Build()
	if err := e.PutSyncRunRecord(ctx, rec); err != nil {
		e.clearCurrentSync()
		return "", err
	}
	return syncID, nil
}

// ResumeSync attaches to an existing sync_run by id. Returns the
// caller-provided id if it matches an existing record. The persisted
// checkpoint token needs no rehydration step: CurrentSyncStep reads
// the SyncRunRecord on demand, so a resumed sync reports the on-disk
// SyncToken by construction (the pre-2.6 Adapter cached it and had to
// reload carefully — a stale/empty cache made the syncer's FSM restart
// from InitOp every activity window).
func (e *Engine) ResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, error) {
	if syncID == "" {
		return "", errors.New("pebble.ResumeSync: empty syncID")
	}
	if _, err := e.GetSyncRunRecord(ctx, syncID); err != nil {
		return "", c1zstore.AdaptNotFound(fmt.Errorf("ResumeSync: lookup: %w", err), pebble.ErrNotFound)
	}
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	if err := e.bindCurrentSync(syncID); err != nil {
		return "", err
	}
	return syncID, nil
}

// StartOrResumeSync resumes an existing sync if one is resumable, else
// starts a new sync. Returns (id, started_new, err).
//
// Resume precedence mirrors SQLite's StartOrResumeSync→ResumeSync
// cascade (pkg/dotc1z/sync_runs.go):
//
//  1. A caller-supplied syncID that names an existing sync_run.
//  2. With an empty syncID, the latest in-progress (not-yet-ended)
//     sync of the requested type started within the last week — the
//     same predicate as Engine.LatestUnfinishedSyncRecord. This is the
//     path the syncer drives across activity windows: it calls
//     StartOrResumeSync(ctx, syncType, "") with no id, expecting an
//     interrupted sync to resume where it checkpointed. Without this,
//     every window started a brand-new sync (wiping prior data via
//     ResetForNewSync and resetting the FSM to InitOp), so any sync
//     exceeding one window never finished.
//
// Only when nothing is resumable do we start a new sync.
func (e *Engine) StartOrResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, bool, error) {
	if syncID != "" {
		if _, err := e.GetSyncRunRecord(ctx, syncID); err == nil {
			id, err := e.ResumeSync(ctx, syncType, syncID)
			return id, false, err
		}
	} else if rec, err := e.LatestUnfinishedSyncRecord(ctx, syncTypeFilterFromConnectorstore(syncType)); err != nil {
		return "", false, err
	} else if rec != nil {
		id, err := e.ResumeSync(ctx, syncType, rec.GetSyncId())
		return id, false, err
	}
	id, err := e.StartNewSync(ctx, syncType, "")
	return id, true, err
}

// SetCurrentSync rebinds the engine's current sync without creating a
// new SyncRunRecord. Used by callers that previously called
// StartNewSync/ResumeSync. A missing record is legal (the legitimate
// "no checkpoint yet" case — CurrentSyncStep will report ""); any
// other read failure propagates rather than silently binding a sync
// whose step can't be read (SQLite's SetCurrentSync likewise returns
// its getSync error).
func (e *Engine) SetCurrentSync(ctx context.Context, syncID string) error {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	if _, err := e.GetSyncRunRecord(ctx, syncID); err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	return e.bindCurrentSync(syncID)
}

// CurrentSyncStep returns the current sync's step string, or "". Read
// from the durable SyncRunRecord on every call, exactly like SQLite's
// CurrentSyncStep re-reads the sync_runs row — there is no in-memory
// step cache to go stale (the old Adapter cached it and grew
// rehydration logic at every rebind to compensate).
//
// Holds lifecycleMu so the id read and the record read are one
// snapshot with respect to the lifecycle transitions — without it, an
// interleaved EndSync/SetCurrentSync between the two reads could
// return a token for a sync the engine is no longer bound to (the old
// Adapter's mutex gave the same guarantee over its cache; review
// finding, final round).
func (e *Engine) CurrentSyncStep(ctx context.Context) (string, error) {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return "", nil
	}
	rec, err := e.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return "", nil
		}
		return "", err
	}
	return rec.GetSyncToken(), nil
}

// CheckpointSync persists a step token to the open sync's record.
func (e *Engine) CheckpointSync(ctx context.Context, syncToken string) error {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return errors.New("CheckpointSync: no open sync")
	}
	existing, err := e.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return err
	}
	updated := v3.SyncRunRecord_builder{
		SyncId:       existing.GetSyncId(),
		Type:         existing.GetType(),
		ParentSyncId: existing.GetParentSyncId(),
		StartedAt:    existing.GetStartedAt(),
		EndedAt:      existing.GetEndedAt(),
		SyncToken:    syncToken,
	}.Build()
	return e.PutSyncRunRecord(ctx, updated)
}

// EndSync stamps the open sync_run's ended_at and detaches it. After
// EndSync, the engine has no current sync; SetCurrentSync or
// StartNewSync are required for further writes. The binding itself is
// cleared inside the finalize tail (EndFreshSync), so success leaves
// no lifecycle state to reset here.
func (e *Engine) EndSync(ctx context.Context) error {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return errors.New("EndSync: no open sync")
	}
	existing, err := e.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return err
	}
	// The sync's writes are done. From here to save/close the store only
	// runs the deferred index build, the stats sidecar, and the durability
	// flush — automatic compactions in that window are incremental
	// level-by-level rewrites that compete with those phases for CPU and IO,
	// so stop granting new ones. The deferred index build below consolidates
	// the grant keyspace itself (its scan is teed into a flat rebuild that
	// replaces the range via IngestAndExcise), so the saved artifact ships
	// with near-zero compaction debt without running the compactor.
	// Seal BEFORE finalize, not after: the seal must cover the deferred
	// index build and the pending-marker clear, or a straggler record
	// writer that was blocked on writeMu could commit in the gap between
	// them — a row present in the primary keyspace but permanently missing
	// from by_principal, in the saved artifact. Finalize's own steps run
	// on AllowSealed paths; sync-run metadata stamps remain allowed.
	// Sealing also pauses compactions for the EndSync-to-close window.
	e.seal()
	if err := e.endSyncFinalize(ctx, existing); err != nil {
		// On failure the sync stays bound and the caller may keep writing
		// (or retry EndSync later): leave the sealed state and resume
		// compactions, or L0 would accumulate until pebble stalls writes at
		// L0StopWritesThreshold with nothing left to resume the scheduler.
		e.unseal()
		return err
	}
	return nil
}

// endSyncFinalize runs the sealed tail of EndSync: the deferred index
// build, the ended_at stamp, the stats sidecar, and the durability flush.
// Runs with the engine SEALED (see EndSync) — every write below goes
// through an AllowSealed path. Split out so EndSync can unseal on failure.
func (e *Engine) endSyncFinalize(ctx context.Context, existing *v3.SyncRunRecord) error {
	// Build the deferred by_principal index BEFORE stamping ended_at (an
	// interrupted build must leave the sync visibly unfinished so a resume
	// re-runs EndSync and the rebuild — the pending marker is durable, see
	// markDeferredIdxPending) and BEFORE the stats sidecar (the build's
	// full grant scan also accumulates the grant portion of the stats via
	// stashDeferredGrantStats, letting PersistSyncStats skip a second
	// O(grants) pass over the keyspace).
	if e.db.DeferredIdxPending() {
		if err := e.BuildDeferredGrantIndexes(ctx); err != nil {
			return fmt.Errorf("EndSync: build deferred grant indexes: %w", err)
		}
		if err := e.clearDeferredIdxPending(); err != nil {
			return fmt.Errorf("EndSync: clear deferred index marker: %w", err)
		}
	} else if e.GrantDigestIndexEnabled() {
		// The deferred pass didn't run (no grant went through the
		// deferred index paths — inline-index writes like PutGrantRecords
		// never arm the marker). RepairMissingGrantDigests reduces to a
		// full BuildGrantDigests-equivalent scan for the common case (a
		// brand-new sync always has grantDigestsPresent false, since
		// ResetForNewSync excised the digest keyspace at StartNewSync) —
		// no behavior change there. It only does LESS work when this
		// EndSync is a second call on an already-digested sync that was
		// rebound via SetCurrentSync rather than started fresh (grant
		// expansion's own follow-up sync is exactly this shape): trusting
		// a still-valid whole-file root outright, or rebuilding only the
		// entitlements invalidated since the prior seal, instead of
		// rescanning every grant in the file again. Build failures are
		// downgraded to a loud digest-state drop inside; an error
		// surfacing here (cancellation, drop failure) is fatal.
		if err := e.RepairMissingGrantDigests(ctx); err != nil {
			return fmt.Errorf("EndSync: repair grant digests: %w", err)
		}
	}
	updated := v3.SyncRunRecord_builder{
		SyncId:       existing.GetSyncId(),
		Type:         existing.GetType(),
		ParentSyncId: existing.GetParentSyncId(),
		StartedAt:    existing.GetStartedAt(),
		EndedAt:      timestamppb.Now(),
		SyncToken:    existing.GetSyncToken(),
	}.Build()
	if err := e.PutSyncRunRecord(ctx, updated); err != nil {
		return err
	}
	if e.test.endSyncPreFlushHook != nil {
		e.test.endSyncPreFlushHook()
	}
	// Populate the stats sidecar BEFORE the durability flush. Stats
	// is engine-meta keyspace, committed pebble.Sync in
	// writeSyncStats; the EndFreshSync flush below then bounds reopen
	// WAL-replay cost for everything. Failures here are non-fatal — Stats() falls back to legacy
	// O(N) iteration on a missing sidecar. NOTE: there is currently
	// no on-Open backfill (the indexMigrations registry is
	// intentionally empty), so a missing sidecar stays missing and
	// every Stats() call pays the iteration cost. We log a warning so
	// the failure is visible in production telemetry but don't fail
	// the sync end on stats-sidecar trouble.
	if err := e.PersistSyncStats(ctx, existing.GetSyncId()); err != nil {
		ctxzap.Extract(ctx).Warn("pebble: persist sync stats sidecar failed; Stats() will fall back to O(N) iteration for this file",
			zap.String("sync_id", existing.GetSyncId()),
			zap.Error(err),
		)
	}
	// Single flush + WAL fsync at sync end. This is the counterpart to
	// MarkFreshSync at StartNewSync; after it returns all of the sync's
	// writes are SST-durable and the WAL is fsynced. Note the ordering
	// above is CRASH-SAFE even though the pages were NoSync: every
	// pebble.Sync commit in the finalize sequence (marker clears, the
	// ended_at stamp, the stats key) rides pebble's sequential WAL, so
	// each fsync also hardens every earlier NoSync page commit — a
	// crash image can hold the finished verdict only if it also holds
	// the pages. Pinned by TestEndSyncStampDurabilityCarriesPages
	// (isolated: the stamp is the ONLY Sync between the pages and the
	// crash cut) and TestEndSyncStampWindowImageComplete (the full
	// default workload at the same cut). This flush's job is bounding
	// reopen WAL-replay cost and hardening the NoSync case
	// (WithDurability(DurabilityNoSync)), where the stamp itself was
	// not synced.
	return e.EndFreshSync(ctx)
}

// === writes ===

// PutGrants writes a batch of grants in a single Pebble batch. v2 is
// translated to v3 first; the engine then commits the whole batch
// with one fsync (or NoSync during a fresh sync — see MarkFreshSync).
//
// The translation uses per-shard arenas (grantTranslateArena) so the
// 3 × N proto-struct allocations from V2GrantToV3's builder pattern
// collapse to 3 slice allocations per shard. For large fresh-sync
// writes this substantially reduces GC scan pressure during the
// engine's parallel build phase.
//
// The translation itself runs in parallel across translateShards
// workers when the input is large enough — protobuf Get/Set methods
// on the underlying v2.Grant and v3 arena structs are thread-safe for
// read+arena-private-write access patterns. Each worker writes to a
// disjoint range of the records slice and uses its own arena, so no
// shared mutable state across workers.
func (e *Engine) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	records := translateGrants(syncID, grants)
	if err := e.PutGrantRecords(ctx, records...); err != nil {
		return fmt.Errorf("PutGrants: %w", err)
	}
	return nil
}

// UnsafePutUniqueGrants writes grants on the trusted-import path: records
// are encoded in parallel and written unconditionally, with no read-before-write
// and no dedup pass. Do not use it for live connector output. The destination
// sync must be fresh, and the caller MUST guarantee each external_id appears at
// most once across the whole sync (not just within this batch). Live connector
// writes should use PutGrants.
func (e *Engine) UnsafePutUniqueGrants(ctx context.Context, grants ...*v2.Grant) error {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	records := translateGrants(syncID, grants)
	if err := e.UnsafePutUniqueGrantRecords(ctx, records...); err != nil {
		return fmt.Errorf("UnsafePutUniqueGrants: %w", err)
	}
	return nil
}

// translateGrants converts v2 grants to v3 records, stamping discovered_at
// where unset. The translation uses per-shard arenas (grantTranslateArena) so
// the 3 × N proto-struct allocations from V2GrantToV3's builder pattern
// collapse to 3 slice allocations per shard, and runs in parallel across
// translateShards workers when the input is large enough — protobuf Get/Set on
// the underlying v2.Grant and v3 arena structs are thread-safe for
// read+arena-private-write patterns, and each worker owns a disjoint range of
// the records slice and its own arena.
func translateGrants(syncID string, grants []*v2.Grant) []*v3.GrantRecord {
	now := timestamppb.Now()

	const translateMinPerShard = 1024
	const translateShards = 4
	shards := translateShards
	if n := len(grants) / translateMinPerShard; n < shards {
		shards = n
	}
	if shards < 2 {
		return translateGrantsSerial(syncID, grants, nil, now)
	}

	// Parallel path: shard workers each translate their range into a
	// private arena and write into their owned slot of records.
	records := make([]*v3.GrantRecord, len(grants))
	chunkSize := (len(grants) + shards - 1) / shards
	var wg sync.WaitGroup
	wg.Add(shards)
	for s := 0; s < shards; s++ {
		start := s * chunkSize
		end := start + chunkSize
		if end > len(grants) {
			end = len(grants)
		}
		go func(start, end int) {
			defer wg.Done()
			arena := newGrantTranslateArena(end - start)
			for i := start; i < end; i++ {
				g := grants[i]
				if g == nil {
					continue
				}
				rec := arena.translateV2Grant(syncID, g)
				if rec == nil {
					continue
				}
				if rec.GetDiscoveredAt() == nil {
					rec.SetDiscoveredAt(now)
				}
				records[i] = rec
			}
		}(start, end)
	}
	wg.Wait()

	// Compact: drop nil slots from skipped grants. Usually len(records)
	// equals len(grants) when no input was nil.
	compact := records[:0]
	for _, r := range records {
		if r != nil {
			compact = append(compact, r)
		}
	}
	return compact
}

// translateGrantsSerial is the single-goroutine translate path: one
// arena, one pass. Used by translateGrants for small batches and by
// callers that are already running on parallel lanes (the bulk import's
// grant shards), where nested fan-out would just oversubscribe a shared
// host. discoveredAt optionally supplies a per-record discovery time
// aligned by index with grants; nil (or short) falls back to now.
func translateGrantsSerial(syncID string, grants []*v2.Grant, discoveredAt []*timestamppb.Timestamp, now *timestamppb.Timestamp) []*v3.GrantRecord {
	arena := newGrantTranslateArena(len(grants))
	records := make([]*v3.GrantRecord, 0, len(grants))
	for i, g := range grants {
		if g == nil {
			continue
		}
		rec := arena.translateV2Grant(syncID, g)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(discoveredAtOrNow(discoveredAt, i, now))
		}
		records = append(records, rec)
	}
	return records
}

// PutResourceTypes writes a batch of resource types in a single
// Pebble batch.
func (e *Engine) PutResourceTypes(ctx context.Context, rts ...*v2.ResourceType) error {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	records := make([]*v3.ResourceTypeRecord, 0, len(rts))
	now := timestamppb.Now()
	for _, rt := range rts {
		if rt == nil {
			continue
		}
		rec := V2ResourceTypeToV3(syncID, rt)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(now)
		}
		records = append(records, rec)
	}
	if err := e.PutResourceTypeRecords(ctx, records...); err != nil {
		return fmt.Errorf("PutResourceTypes: %w", err)
	}
	return nil
}

// PutResources writes a batch of resources in a single Pebble batch.
func (e *Engine) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	records := make([]*v3.ResourceRecord, 0, len(resources))
	now := timestamppb.Now()
	for _, r := range resources {
		if r == nil {
			continue
		}
		rec := V2ResourceToV3(syncID, r)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(now)
		}
		records = append(records, rec)
	}
	if err := e.PutResourceRecords(ctx, records...); err != nil {
		return fmt.Errorf("PutResources: %w", err)
	}
	return nil
}

// PutEntitlements writes a batch of entitlements in a single Pebble batch.
func (e *Engine) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	records := make([]*v3.EntitlementRecord, 0, len(entitlements))
	now := timestamppb.Now()
	for _, e := range entitlements {
		if e == nil {
			continue
		}
		rec := V2EntitlementToV3(syncID, e)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(now)
		}
		records = append(records, rec)
	}
	if err := e.PutEntitlementRecords(ctx, records...); err != nil {
		return fmt.Errorf("PutEntitlements: %w", err)
	}
	return nil
}

// DeleteGrant removes a grant by its raw public id, resolved through the
// bare-id lookup edge. Callers holding the full grant should prefer
// DeleteGrantByRefs, which needs no id-string resolution.
func (e *Engine) DeleteGrant(ctx context.Context, grantID string) error {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	return e.DeleteGrantRecord(ctx, grantID)
}

// DeleteGrantByRefs removes a grant addressed by the structured refs of the
// supplied v2 grant — the exact delete path, no lossy id string involved.
// Incomplete refs are an error, never a fallback to bare-id resolution:
// this is a sync-internal surface, and string resolution is reserved for
// interactive/CLI edges (see lookup.go). A grant whose refs cannot derive
// an identity could not have been stored in the first place, so there is
// nothing a string could correctly address here.
func (e *Engine) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	rec := V2GrantToV3(syncID, grant)
	if _, err := grantIdentityFromRecord(rec); err != nil {
		return fmt.Errorf("DeleteGrantByRefs: grant %q: %w", grant.GetId(), err)
	}
	return e.DeleteGrantByIdentityRefs(ctx, rec)
}

// PutAsset writes a single asset row. assetRef carries the
// (resource_type, resource_id) pair we use as the external_id —
// joined with a "/" separator since the engine's AssetRecord PK is
// (sync_id, external_id).
func (e *Engine) PutAsset(ctx context.Context, assetRef *v2.AssetRef, contentType string, data []byte) error {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	if assetRef == nil {
		return errors.New("PutAsset: nil assetRef")
	}
	externalID := assetRef.GetId()
	if externalID == "" {
		return errors.New("PutAsset: empty assetRef.Id")
	}
	rec := v3.AssetRecord_builder{
		SyncId:       syncID,
		ExternalId:   externalID,
		ContentType:  contentType,
		Data:         data,
		DiscoveredAt: timestamppb.Now(),
	}.Build()
	return e.PutAssetRecord(ctx, rec)
}

// Cleanup on the bare Engine is a no-op. The real Pebble
// retention policy lives on pkg/dotc1z's Pebble store wrapper
// (pebble_store.go) — it needs access to caller-supplied options
// (SyncLimit, SkipCleanup) that the engine itself doesn't track,
// plus the dirty-flag plumbing on the wrapper.
//
// Callers that open through dotc1z.NewStore(..., WithEngine(EnginePebble))
// get the real Cleanup; callers that use a bare Engine (unit tests,
// embedding) silently get retention=disabled. Kept so the engine
// serves the writer-shaped call sites that predate the layer collapse
// (the bare engine is deliberately NOT a full connectorstore.Writer —
// see the Adapter alias doc).
func (e *Engine) Cleanup(ctx context.Context) error { return nil }

// === GetAsset ===

// GetAsset returns the (content_type, data-reader) for the given
// asset. The returned reader is backed by a bytes.Reader over the
// fully-materialized blob.
func (e *Engine) GetAsset(ctx context.Context, req *v2.AssetServiceGetAssetRequest) (string, io.Reader, error) {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return "", nil, ErrNoCurrentSync
	}
	if req == nil || req.GetAsset() == nil {
		return "", nil, errors.New("GetAsset: nil request")
	}
	rec, err := e.GetAssetRecord(ctx, req.GetAsset().GetId())
	if err != nil {
		return "", nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	return rec.GetContentType(), &bytesReader{b: rec.GetData()}, nil
}

// === read service surface ===
//
// ListGrants / ListResources / etc. The connectorstore.Reader
// interface embeds these as gRPC ServiceServer interfaces; the
// adapter implements the most-called paths directly and leaves the
// rest to the embedded Unimplemented* stubs.

// ListGrants returns up to page_size grants on the active sync.
// Pagination matches the SQLite engine's semantics:
//   - page_size == 0 || page_size > MaxPageSize → DefaultPageSize (10000)
//   - page_token is opaque base64; pass nextPageToken back verbatim
//   - filter by req.Resource — the entitlement-side resource of each
//     grant — when set; uses primary grant entitlement-resource prefixes. This
//     matches SQLite's `listGrantsGeneric` which filters on
//     grants.resource_id / resource_type_id (the entitlement's
//     resource columns). Callers who want to filter by principal
//     should use ListGrantsForPrincipal instead.
func (e *Engine) ListGrants(ctx context.Context, req *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	syncID, err := e.resolveActiveSync(ctx, req.GetActiveSyncId(), req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	limit := clampPageSize(req.GetPageSize())
	cursor := req.GetPageToken()
	var records []*v3.GrantRecord
	var nextCursor string
	if r := req.GetResource(); r != nil && r.GetId() != nil {
		records, nextCursor, err = e.PaginateGrantsByEntitlementResource(ctx,
			r.GetId().GetResourceType(), r.GetId().GetResource(), cursor, limit)
	} else {
		records, nextCursor, err = e.PaginateGrants(ctx, cursor, limit)
	}
	if err != nil {
		return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	out := make([]*v2.Grant, 0, len(records))
	for _, rec := range records {
		out = append(out, V3GrantToV2(rec))
	}
	return v2.GrantsServiceListGrantsResponse_builder{
		List:          out,
		NextPageToken: nextCursor,
	}.Build(), nil
}

// ListResources returns up to page_size resources, optionally filtered
// by parent resource id and/or resource_type_id. Pagination matches
// SQLite (see ListGrants).
//
// Note on the resource_type_id filter: when parent is also set, the
// by_parent index is used and we post-filter by resource_type_id (the
// index doesn't carry resource_type_id in the lookup prefix). When
// only resource_type_id is set, we still iterate the full primary
// range and post-filter — adding a by_resource_type index is a
// future-work item if this path becomes hot.
func (e *Engine) ListResources(ctx context.Context, req *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	syncID, err := e.resolveActiveSync(ctx, req.GetActiveSyncId(), req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	limit := clampPageSize(req.GetPageSize())
	cursor := req.GetPageToken()
	rtFilter := req.GetResourceTypeId()
	parent := req.GetParentResourceId()
	useParent := parent != nil && parent.GetResource() != ""

	// cursorFor returns the engine cursor for rec under the path
	// this call is iterating — primary keyspace for the unfiltered
	// case, by_parent index for the parent-scoped case. We need
	// per-record cursors because a post-filter break at len(out) ==
	// limit may leave matching records unconsumed in the engine
	// page; emitting the engine's end-of-page cursor would skip
	// them on the next call.
	cursorFor := func(rec *v3.ResourceRecord) string {
		if useParent {
			return encodeCursor(rawdb.EncodeResourceByParentIndexKey(
				parent.GetResourceType(), parent.GetResource(),
				rec.GetResourceTypeId(), rec.GetResourceId(),
			))
		}
		return encodeCursor(encodeResourceKey(rec.GetResourceTypeId(), rec.GetResourceId()))
	}

	out := make([]*v2.Resource, 0, limit)
	var nextCursor string
	for len(out) < limit {
		pageLimit := limit - len(out)
		// Over-fetch a little when post-filtering so a sparse hit rate
		// doesn't force a tail of extra round-trips. 4x is the cap; if
		// rtFilter is empty we skip the over-fetch entirely.
		fetchLimit := pageLimit
		if rtFilter != "" {
			fetchLimit = pageLimit * 4
			if fetchLimit > MaxPageSize {
				fetchLimit = MaxPageSize
			}
		}
		var records []*v3.ResourceRecord
		var err error
		if useParent {
			records, nextCursor, err = e.PaginateResourcesByParent(ctx,
				parent.GetResourceType(), parent.GetResource(), cursor, fetchLimit)
		} else {
			records, nextCursor, err = e.PaginateResources(ctx, cursor, fetchLimit)
		}
		if err != nil {
			return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
		}
		brokeEarly := false
		for _, rec := range records {
			if rtFilter != "" && rec.GetResourceTypeId() != rtFilter {
				continue
			}

			out = append(out, V3ResourceToV2(rec))
			if len(out) == limit {
				// Override the engine's end-of-page cursor with
				// THIS record's cursor so the next page resumes
				// strictly after this record.
				nextCursor = cursorFor(rec)
				brokeEarly = true
				break
			}
		}
		if brokeEarly {
			break
		}
		if nextCursor == "" || len(records) == 0 {
			break
		}
		cursor = nextCursor
	}
	return v2.ResourcesServiceListResourcesResponse_builder{
		List:          out,
		NextPageToken: nextCursor,
	}.Build(), nil
}

// ListResourceTypes returns up to page_size resource_types. Pagination
// matches SQLite (see ListGrants).
func (e *Engine) ListResourceTypes(ctx context.Context, req *v2.ResourceTypesServiceListResourceTypesRequest) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	syncID, err := e.resolveActiveSync(ctx, req.GetActiveSyncId(), req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	limit := clampPageSize(req.GetPageSize())
	records, nextCursor, err := e.PaginateResourceTypes(ctx, req.GetPageToken(), limit)
	if err != nil {
		return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	out := make([]*v2.ResourceType, 0, len(records))
	for _, rec := range records {
		out = append(out, V3ResourceTypeToV2(rec))
	}
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{
		List:          out,
		NextPageToken: nextCursor,
	}.Build(), nil
}

// ListEntitlements returns up to page_size entitlements, optionally
// filtered by Resource (resource_type_id, resource_id). Pagination
// matches SQLite (see ListGrants).
func (e *Engine) ListEntitlements(ctx context.Context, req *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	syncID, err := e.resolveActiveSync(ctx, req.GetActiveSyncId(), req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	limit := clampPageSize(req.GetPageSize())
	cursor := req.GetPageToken()
	var records []*v3.EntitlementRecord
	var nextCursor string
	if r := req.GetResource(); r != nil && r.GetId() != nil {
		records, nextCursor, err = e.PaginateEntitlementsByResource(ctx,
			r.GetId().GetResourceType(), r.GetId().GetResource(), cursor, limit)
	} else {
		records, nextCursor, err = e.PaginateEntitlements(ctx, cursor, limit)
	}
	if err != nil {
		return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	out := make([]*v2.Entitlement, 0, len(records))
	for _, rec := range records {
		out = append(out, V3EntitlementToV2(rec))
	}
	return v2.EntitlementsServiceListEntitlementsResponse_builder{
		List:          out,
		NextPageToken: nextCursor,
	}.Build(), nil
}

// ListStaticEntitlements is the always-empty counterpart to
// ListEntitlements that some connectors expose for static (compile-
// time-known) entitlements. C1File returns an empty list; the
// Pebble adapter mirrors that contract.
func (e *Engine) ListStaticEntitlements(
	_ context.Context,
	_ *v2.EntitlementsServiceListStaticEntitlementsRequest,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{
		List:          []*v2.Entitlement{},
		NextPageToken: "",
	}.Build(), nil
}

// === reader_v2 surface ===

// GetGrant fetches a single grant by ID.
func (e *Engine) GetGrant(ctx context.Context, req *reader_v2.GrantsReaderServiceGetGrantRequest) (*reader_v2.GrantsReaderServiceGetGrantResponse, error) {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	rec, err := e.GetGrantRecord(ctx, req.GetGrantId())
	if err != nil {
		return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	return reader_v2.GrantsReaderServiceGetGrantResponse_builder{
		Grant: V3GrantToV2(rec),
	}.Build(), nil
}

// LatestFinishedSyncID returns the most-recently-finished sync ID of
// the given type. Implements connectorstore.LatestFinishedSyncIDFetcher.
// Delegates to Engine.LatestFinishedSyncRecord; see that method for
// the predicate + tiebreaker contract.
func (e *Engine) LatestFinishedSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error) {
	latest, err := e.LatestFinishedSyncRecord(ctx, syncTypeFilterFromConnectorstore(syncType))
	if err != nil {
		return "", err
	}
	if latest == nil {
		return "", nil
	}
	return latest.GetSyncId(), nil
}

// syncTypeFilterFromConnectorstore returns a predicate that matches
// sync_runs whose v3 type corresponds to the given connectorstore
// SyncType. SyncTypeAny returns nil (no filter), matching the
// Engine.LatestFinishedSyncRecord contract.
func syncTypeFilterFromConnectorstore(t connectorstore.SyncType) func(v3.SyncType) bool {
	if t == connectorstore.SyncTypeAny {
		return nil
	}
	want := v2SyncTypeToV3(t)
	return func(got v3.SyncType) bool { return got == want }
}

// Metadata describes the storage backing this engine. Always reports
// the v3 format; PayloadEncoding is set by the writer at envelope time
// and is not directly visible here — pkg/dotc1z's Pebble store wrapper
// (pebble_store.go) overrides this method to fill PayloadEncoding
// from its configured value.
//
// Strings are inlined rather than referencing dotc1z constants
// because this subpackage is imported by dotc1z, so the reverse
// import would cycle. The values match c1zstore.EnginePebble.String()
// and dotc1z.C1ZFormatV3.String() — see connectorstore.StoreMetadata
// docs for the canonical value list.
func (e *Engine) Metadata() connectorstore.StoreMetadata {
	return connectorstore.StoreMetadata{
		Engine: "pebble",
		Format: "v3",
	}
}

// === helpers ===

// resolveActiveSync picks the sync_id a List* read should scope to.
//
// Precedence:
//
//  1. req.ActiveSyncId — explicit top-level override on the proto
//     request. Pebble-specific (SQLite ignores this field today);
//     kept first so existing callers that wired it continue to win.
//  2. Everything resolveActiveSyncForReader resolves: the
//     c1zpb.SyncDetails annotation, then the adapter's current sync,
//     then the most-recent finished sync.
//
// Returns ("", nil) when no sync resolves. A malformed SyncDetails
// annotation surfaces as a non-nil error so callers don't silently
// fall through to the wrong sync.
func (e *Engine) resolveActiveSync(ctx context.Context, reqSyncID string, annos []*anypb.Any) (string, error) {
	if reqSyncID != "" {
		return reqSyncID, nil
	}
	return e.resolveActiveSyncForReader(ctx, annos)
}

// v2SyncTypeToV3 maps the connectorstore.SyncType string to the v3
// SyncType enum.
func v2SyncTypeToV3(t connectorstore.SyncType) v3.SyncType {
	switch t {
	case connectorstore.SyncTypeFull:
		return v3.SyncType_SYNC_TYPE_FULL
	case connectorstore.SyncTypePartial:
		return v3.SyncType_SYNC_TYPE_PARTIAL
	case connectorstore.SyncTypeResourcesOnly:
		return v3.SyncType_SYNC_TYPE_RESOURCES_ONLY
	case connectorstore.SyncTypePartialUpserts:
		return v3.SyncType_SYNC_TYPE_PARTIAL_UPSERTS
	case connectorstore.SyncTypePartialDeletions:
		return v3.SyncType_SYNC_TYPE_PARTIAL_DELETIONS
	default:
		return v3.SyncType_SYNC_TYPE_UNSPECIFIED
	}
}

// bytesReader is a tiny io.Reader over a []byte that doesn't pull in
// the bytes package's full Reader machinery. (We do this to keep the
// adapter's import set minimal — the bytes package is unused
// elsewhere in this file.)
type bytesReader struct {
	b []byte
	i int
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
