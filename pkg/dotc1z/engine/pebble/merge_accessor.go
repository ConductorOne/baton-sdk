package pebble

import (
	"context"
	"errors"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// FoldBatch and MergeDB are exported aliases for the choke point's
// fold-exempt batch and the narrowed compactor handle (internal/
// rawdb). The synccompactor/pebble package — the choke point's one
// sanctioned external client, via Engine.DB() — needs to NAME these
// types in struct fields and signatures, which the internal-package
// import fence forbids; an alias is referable without the import.
//
// MergeDB (= rawdb.MergeView) carries reads, LSM stats, the bulk
// range/ingest ops, and the fold-exempt batch — and nothing else:
// typed record staging (NewRecordBatch) and the session/meta/digest
// write families are not on it, so the documented DB() exemption
// cannot quietly grow into a second engine write path that bypasses
// the lifecycle barrier. It is deliberately a CONCRETE struct, not an
// interface over *rawdb.DB — an interface's dynamic type would let a
// caller recover the omitted write families with a structural type
// assertion (review finding, delta round). UnsafeForTesting stays
// reachable for test fixtures; its testing.Testing() runtime gate
// makes it inert in production.
type (
	FoldBatch = rawdb.FoldBatch
	MergeDB   = rawdb.MergeView
)

// engineAccessor is implemented by *Adapter and by pkg/dotc1z's Pebble
// store wrapper (which embeds *Adapter and overrides the method with a
// nil-safe version).
type engineAccessor interface {
	PebbleEngine() *Engine
}

// PebbleEngine returns the underlying *Engine. Nil-safe so AsEngine can
// probe arbitrary writers.
func (a *Adapter) PebbleEngine() *Engine {
	if a == nil {
		return nil
	}
	return a.engine
}

// AsEngine recovers the underlying *Engine from a connectorstore.Writer
// produced by dotc1z.NewStore for the Pebble engine. NewStore returns a
// wrapper that embeds *Adapter; a bare *Adapter is also accepted for
// callers that construct one directly. Returns (nil, false) for any
// non-Pebble store, so a caller can branch on the engine without
// importing internal types.
func AsEngine(w connectorstore.Writer) (*Engine, bool) {
	if a, ok := w.(engineAccessor); ok {
		if e := a.PebbleEngine(); e != nil {
			return e, true
		}
	}
	return nil, false
}

// LogCompactionMetrics logs a snapshot of the LSM's compaction and ingest
// counters, labeled with the caller's phase name. Diagnostic only: used to
// attribute wall time lost to background compactions triggered by large
// ingests (e.g. the synthesized-grant layer segments) during benchmark runs.
func (e *Engine) LogCompactionMetrics(ctx context.Context, phase string) {
	if e == nil || e.db == nil {
		return
	}
	m := e.db.Metrics()
	ctxzap.Extract(ctx).Info("pebble compaction metrics",
		zap.String("phase", phase),
		zap.Int64("compact_count", m.Compact.Count),
		zap.Duration("compact_duration_total", m.Compact.Duration),
		zap.Uint64("compact_estimated_debt_bytes", m.Compact.EstimatedDebt),
		zap.Int64("compact_in_progress", m.Compact.NumInProgress),
		zap.Int64("compact_in_progress_bytes", m.Compact.InProgressBytes),
		zap.Uint64("ingest_count", m.Ingest.Count),
		zap.Int64("flush_count", m.Flush.Count),
		zap.Int("read_amp", m.ReadAmp()),
	)
}

// ReadSyncStatsRecord returns the raw stats sidecar record for a sync,
// or (nil, nil) when no sidecar exists. Exposed for synccompactor
// tests that compare merge-accumulated stats against a recompute.
func ReadSyncStatsRecord(ctx context.Context, e *Engine, syncID string) (*v3.SyncStatsRecord, error) {
	if e == nil {
		return nil, nil
	}
	return e.readSyncStats(ctx, syncID)
}

// SyncStatsFromRecord converts the engine-internal stats sidecar shape
// into the public reader stats type used by C1ZStore APIs and compactor
// planning.
func SyncStatsFromRecord(stats *v3.SyncStatsRecord) *reader_v2.SyncStats {
	if stats == nil {
		return nil
	}
	return reader_v2.SyncStats_builder{
		ResourceTypes:              stats.GetResourceTypes(),
		Resources:                  stats.GetResources(),
		Entitlements:               stats.GetEntitlements(),
		Grants:                     stats.GetGrants(),
		Assets:                     stats.GetAssets(),
		ResourcesByResourceType:    stats.GetResourcesByResourceType(),
		EntitlementsByResourceType: stats.GetEntitlementsByResourceType(),
		GrantsByResourceType:       stats.GetGrantsByEntitlementResourceType(),
		StepDurationsMs:            stats.GetStepDurationsMs(),
		ConnectorCallStats:         storageCallStatsToReader(stats.GetConnectorCallStats()),
		SessionStoreStats:          storageCallStatsToReader(stats.GetSessionStoreStats()),
	}.Build()
}

func storageCallStatsToReader(in map[string]*v3.CallStat) map[string]*reader_v2.CallStat {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*reader_v2.CallStat, len(in))
	for k, v := range in {
		if v == nil {
			continue
		}
		out[k] = reader_v2.CallStat_builder{
			Count:    v.GetCount(),
			TotalMs:  v.GetTotalMs(),
			MaxMs:    v.GetMaxMs(),
			Errors:   v.GetErrors(),
			Timeouts: v.GetTimeouts(),
		}.Build()
	}
	return out
}

func CachedSyncStats(ctx context.Context, e *Engine, syncID string) (*reader_v2.SyncStats, bool, error) {
	if e == nil {
		return nil, false, nil
	}
	stats, err := e.readSyncStats(ctx, syncID)
	if err != nil {
		return nil, false, err
	}
	if stats == nil {
		return nil, false, nil
	}
	return SyncStatsFromRecord(stats), true, nil
}

// engineOnlyCloser and fixtureNormalizer are implemented by
// pkg/dotc1z's Pebble store wrapper (pebbleStore). The wrapper lives in
// dotc1z, which imports this package, so the engine side dispatches on
// interfaces instead of the concrete type.
type engineOnlyCloser interface {
	CloseEngineOnly() error
}

type fixtureNormalizer interface {
	NormalizeForFixtureSave(ctx context.Context, syncID string) error
}

type storeDirtyMarker interface {
	MarkDirty()
}

type foldDeadBytesAdder interface {
	AddFoldDeadBytes(n int64)
}

// AddFoldDeadBytes bumps a registered store's cumulative fold-waste
// counter (persisted as the envelope manifest's fold_dead_bytes at
// save). Called by the fold compactor with the exact raw bytes its
// merge shadowed in the base keyspace; the compactor's auto cutover
// later reads the counter from the envelope header to force a rebuild
// once waste crosses its threshold. Returns false when w is not a
// registered Pebble store.
func AddFoldDeadBytes(w connectorstore.Writer, n int64) bool {
	s, ok := w.(foldDeadBytesAdder)
	if !ok || s == nil {
		return false
	}
	s.AddFoldDeadBytes(n)
	return true
}

// MarkStoreDirty flips a registered store's dirty bit so Close drives
// the save → checkpoint → envelope path. Engine-level writes (raw
// batches, SST ingest, direct Put*Records) bypass the registered
// store's markDirty wrappers; merge tooling that mutates the engine
// directly calls this once so the mutations are persisted at Close.
// Returns false when w is not a registered Pebble store.
func MarkStoreDirty(w connectorstore.Writer) bool {
	s, ok := w.(storeDirtyMarker)
	if !ok || s == nil {
		return false
	}
	s.MarkDirty()
	return true
}

// CloseEngineOnly closes the Pebble engine inside a registered store without
// removing that store's unpacked temp directory. This is useful when a caller
// owns a parent temp directory and wants one bulk cleanup after closing many
// read-only source stores.
func CloseEngineOnly(w connectorstore.Writer) error {
	switch s := w.(type) {
	case engineOnlyCloser:
		return s.CloseEngineOnly()
	case *Adapter:
		if s == nil || s.engine == nil {
			return nil
		}
		return s.engine.Close()
	default:
		return nil
	}
}

// NormalizeForFixtureSave flushes and compacts one sync in a Pebble
// store, then marks the wrapper dirty so Close writes a fresh c1z envelope.
// This is intentionally narrow: benchmark fixtures should not measure WAL
// replay or un-compacted LSM shape left over from fixture generation.
func NormalizeForFixtureSave(ctx context.Context, w connectorstore.Writer, syncID string) error {
	s, ok := w.(fixtureNormalizer)
	if !ok {
		return errors.New("pebble NormalizeForFixtureSave: writer is not a pebble store wrapper")
	}
	return s.NormalizeForFixtureSave(ctx, syncID)
}

func ResourceTypeRecordKey(externalID string) []byte {
	return encodeResourceTypeKey(externalID)
}

func ResourceRecordKey(resourceTypeID string, resourceID string) []byte {
	return encodeResourceKey(resourceTypeID, resourceID)
}

func EntitlementRecordKey(externalID string) []byte {
	return encodeEntitlementKey(externalID)
}

func GrantRecordKey(externalID string) []byte {
	return encodeGrantKey(externalID)
}

func EntitlementRecordIdentityKey(r *v3.EntitlementRecord) string {
	id, err := entitlementIdentityFromRecord(r)
	if err != nil {
		return r.GetExternalId()
	}
	return string(encodeEntitlementIdentityKey(id))
}

func GrantRecordIdentityKey(r *v3.GrantRecord) string {
	id, err := grantIdentityFromRecord(r)
	if err != nil {
		return r.GetExternalId()
	}
	return string(encodeGrantIdentityKey(id))
}

func ResourceIndexKeys(r *v3.ResourceRecord) [][]byte {
	parent := r.GetParent()
	if parent == nil || parent.GetResourceId() == "" {
		return nil
	}
	return [][]byte{encodeResourceByParentIndexKey(parent.GetResourceTypeId(), parent.GetResourceId(), r.GetResourceTypeId(), r.GetResourceId())}
}

func ForEachResourceIndexKey(r *v3.ResourceRecord, yield func([]byte) error) error {
	parent := r.GetParent()
	if parent == nil || parent.GetResourceId() == "" {
		return nil
	}
	return yield(encodeResourceByParentIndexKey(parent.GetResourceTypeId(), parent.GetResourceId(), r.GetResourceTypeId(), r.GetResourceId()))
}

func ForEachResourceIndexKeyRaw(parentRT string, parentID string, resourceTypeID string, resourceID string, yield func([]byte) error) error {
	if parentID == "" {
		return nil
	}
	return yield(encodeResourceByParentIndexKey(parentRT, parentID, resourceTypeID, resourceID))
}

// Byte-slice appenders let merge/overlay code build index keys from borrowed
// protobuf string bytes. Avoiding intermediate string conversion was the last
// large allocation drop in the same-size syncs=50 overlay profile (~2.4M to
// ~0.8M allocs/op, combined with scratch reuse).
func AppendResourceIndexKeyRawBytes(dst []byte, parentRT []byte, parentID []byte, resourceTypeID []byte, resourceID []byte) []byte {
	dst = append(dst, versionV3, typeIndex, idxResourceByParent)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, parentRT)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, parentID)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, resourceTypeID)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleBytes(dst, resourceID)
}

func EntitlementIndexKeys(r *v3.EntitlementRecord) [][]byte {
	return nil
}

func ForEachEntitlementIndexKey(r *v3.EntitlementRecord, yield func([]byte) error) error {
	return nil
}

func ForEachEntitlementIndexKeyRaw(resourceRT string, resourceID string, externalID string, yield func([]byte) error) error {
	return nil
}

func GrantIndexKeys(r *v3.GrantRecord) [][]byte {
	return grantIndexKeys(r)
}

func ForEachGrantIndexKey(r *v3.GrantRecord, yield func([]byte) error) error {
	for _, key := range grantIndexKeys(r) {
		if err := yield(key); err != nil {
			return err
		}
	}
	return nil
}

func ForEachGrantIndexKeyRaw(
	entitlementRT string,
	entitlementResourceID string,
	entitlementID string,
	principalRT string,
	principalID string,
	externalID string,
	needsExpansion bool,
	yield func([]byte) error,
) error {
	if entitlementRT == "" || entitlementResourceID == "" || entitlementID == "" || principalRT == "" || principalID == "" {
		return nil
	}
	id := grantIdentity{
		entitlement:     entitlementIdentityFromParts(entitlementRT, entitlementResourceID, entitlementID),
		principalTypeID: principalRT,
		principalID:     principalID,
	}
	if err := yield(encodeGrantByPrincipalIdentityIndexKey(id)); err != nil {
		return err
	}
	if needsExpansion {
		if err := yield(encodeGrantByNeedsExpansionIdentityIndexKey(id)); err != nil {
			return err
		}
	}
	return nil
}
