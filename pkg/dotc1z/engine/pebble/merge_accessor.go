package pebble

import (
	"context"
	"errors"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
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
// planning. The public type intentionally has no assets field, so assets
// remain available only on the sidecar record.
func SyncStatsFromRecord(stats *v3.SyncStatsRecord) *reader_v2.SyncStats {
	if stats == nil {
		return nil
	}
	return reader_v2.SyncStats_builder{
		ResourceTypes:           stats.GetResourceTypes(),
		Resources:               stats.GetResources(),
		Entitlements:            stats.GetEntitlements(),
		Grants:                  stats.GetGrants(),
		ResourcesByResourceType: stats.GetResourcesByResourceType(),
		GrantsByResourceType:    stats.GetGrantsByEntitlementResourceType(),
	}.Build()
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

// CloseEngineOnly closes the Pebble engine inside a store wrapper without
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

func ResourceTypeRecordKey(syncIDBytes []byte, externalID string) []byte {
	return encodeResourceTypeKey(syncIDBytes, externalID)
}

func ResourceRecordKey(syncIDBytes []byte, resourceTypeID string, resourceID string) []byte {
	return encodeResourceKey(syncIDBytes, resourceTypeID, resourceID)
}

func EntitlementRecordKey(syncIDBytes []byte, externalID string) []byte {
	return encodeEntitlementKey(syncIDBytes, externalID)
}

func GrantRecordKey(syncIDBytes []byte, externalID string) []byte {
	return encodeGrantKey(syncIDBytes, externalID)
}

func ResourceIndexKeys(syncIDBytes []byte, r *v3.ResourceRecord) [][]byte {
	parent := r.GetParent()
	if parent == nil || parent.GetResourceId() == "" {
		return nil
	}
	return [][]byte{encodeResourceByParentIndexKey(syncIDBytes, parent.GetResourceTypeId(), parent.GetResourceId(), r.GetResourceTypeId(), r.GetResourceId())}
}

func ForEachResourceIndexKey(syncIDBytes []byte, r *v3.ResourceRecord, yield func([]byte) error) error {
	parent := r.GetParent()
	if parent == nil || parent.GetResourceId() == "" {
		return nil
	}
	return yield(encodeResourceByParentIndexKey(syncIDBytes, parent.GetResourceTypeId(), parent.GetResourceId(), r.GetResourceTypeId(), r.GetResourceId()))
}

func ForEachResourceIndexKeyRaw(syncIDBytes []byte, parentRT string, parentID string, resourceTypeID string, resourceID string, yield func([]byte) error) error {
	if parentID == "" {
		return nil
	}
	return yield(encodeResourceByParentIndexKey(syncIDBytes, parentRT, parentID, resourceTypeID, resourceID))
}

// Byte-slice appenders let merge/overlay code build index keys from borrowed
// protobuf string bytes. Avoiding intermediate string conversion was the last
// large allocation drop in the same-size syncs=50 overlay profile (~2.4M to
// ~0.8M allocs/op, combined with scratch reuse).
func AppendResourceIndexKeyRawBytes(dst []byte, syncIDBytes []byte, parentRT []byte, parentID []byte, resourceTypeID []byte, resourceID []byte) []byte {
	dst = append(dst, versionV3, typeIndex, idxResourceByParent)
	dst = append(dst, syncIDBytes...)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, parentRT)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, parentID)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, resourceTypeID)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleBytes(dst, resourceID)
}

func EntitlementIndexKeys(syncIDBytes []byte, r *v3.EntitlementRecord) [][]byte {
	res := r.GetResource()
	if res == nil || res.GetResourceId() == "" {
		return nil
	}
	return [][]byte{encodeEntitlementByResourceIndexKey(syncIDBytes, res.GetResourceTypeId(), res.GetResourceId(), r.GetExternalId())}
}

func ForEachEntitlementIndexKey(syncIDBytes []byte, r *v3.EntitlementRecord, yield func([]byte) error) error {
	res := r.GetResource()
	if res == nil || res.GetResourceId() == "" {
		return nil
	}
	return yield(encodeEntitlementByResourceIndexKey(syncIDBytes, res.GetResourceTypeId(), res.GetResourceId(), r.GetExternalId()))
}

func ForEachEntitlementIndexKeyRaw(syncIDBytes []byte, resourceRT string, resourceID string, externalID string, yield func([]byte) error) error {
	if resourceID == "" {
		return nil
	}
	return yield(encodeEntitlementByResourceIndexKey(syncIDBytes, resourceRT, resourceID, externalID))
}

func AppendEntitlementIndexKeyRawBytes(dst []byte, syncIDBytes []byte, resourceRT []byte, resourceID []byte, externalID []byte) []byte {
	dst = append(dst, versionV3, typeIndex, idxEntitlementByResource)
	dst = append(dst, syncIDBytes...)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, resourceRT)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, resourceID)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleBytes(dst, externalID)
}

func GrantIndexKeys(syncIDBytes []byte, r *v3.GrantRecord) [][]byte {
	return grantIndexKeys(syncIDBytes, r)
}

func ForEachGrantIndexKey(syncIDBytes []byte, r *v3.GrantRecord, yield func([]byte) error) error {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	ext := r.GetExternalId()
	if ent != nil && princ != nil {
		if err := yield(encodeGrantByEntitlementIndexKey(syncIDBytes, ent.GetEntitlementId(), princ.GetResourceTypeId(), princ.GetResourceId(), ext)); err != nil {
			return err
		}
	}
	if ent != nil && ent.GetResourceId() != "" {
		if err := yield(encodeGrantByEntitlementResourceIndexKey(syncIDBytes, ent.GetResourceTypeId(), ent.GetResourceId(), ext)); err != nil {
			return err
		}
	}
	if princ != nil {
		if err := yield(encodeGrantByPrincipalIndexKey(syncIDBytes, princ.GetResourceTypeId(), princ.GetResourceId(), ext)); err != nil {
			return err
		}
		if err := yield(encodeGrantByPrincipalResourceTypeIndexKey(syncIDBytes, princ.GetResourceTypeId(), ext)); err != nil {
			return err
		}
	}
	if r.GetNeedsExpansion() {
		if err := yield(encodeGrantByNeedsExpansionIndexKey(syncIDBytes, ext)); err != nil {
			return err
		}
	}
	return nil
}

func ForEachGrantIndexKeyRaw(
	syncIDBytes []byte,
	entitlementRT string,
	entitlementResourceID string,
	entitlementID string,
	principalRT string,
	principalID string,
	externalID string,
	needsExpansion bool,
	yield func([]byte) error,
) error {
	if entitlementID != "" && principalRT != "" && principalID != "" {
		if err := yield(encodeGrantByEntitlementIndexKey(syncIDBytes, entitlementID, principalRT, principalID, externalID)); err != nil {
			return err
		}
	}
	if entitlementResourceID != "" {
		if err := yield(encodeGrantByEntitlementResourceIndexKey(syncIDBytes, entitlementRT, entitlementResourceID, externalID)); err != nil {
			return err
		}
	}
	if principalRT != "" && principalID != "" {
		if err := yield(encodeGrantByPrincipalIndexKey(syncIDBytes, principalRT, principalID, externalID)); err != nil {
			return err
		}
		if err := yield(encodeGrantByPrincipalResourceTypeIndexKey(syncIDBytes, principalRT, externalID)); err != nil {
			return err
		}
	}
	if needsExpansion {
		if err := yield(encodeGrantByNeedsExpansionIndexKey(syncIDBytes, externalID)); err != nil {
			return err
		}
	}
	return nil
}

func AppendGrantByEntitlementIndexKeyRawBytes(dst []byte, syncIDBytes []byte, entitlementID []byte, principalRT []byte, principalID []byte, externalID []byte) []byte {
	dst = append(dst, versionV3, typeIndex, idxGrantByEntitlement)
	dst = append(dst, syncIDBytes...)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, entitlementID)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, principalRT)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, principalID)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleBytes(dst, externalID)
}

func AppendGrantByEntitlementResourceIndexKeyRawBytes(dst []byte, syncIDBytes []byte, entitlementRT []byte, entitlementResourceID []byte, externalID []byte) []byte {
	dst = append(dst, versionV3, typeIndex, idxGrantByEntitlementResource)
	dst = append(dst, syncIDBytes...)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, entitlementRT)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, entitlementResourceID)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleBytes(dst, externalID)
}

func AppendGrantByPrincipalIndexKeyRawBytes(dst []byte, syncIDBytes []byte, principalRT []byte, principalID []byte, externalID []byte) []byte {
	dst = append(dst, versionV3, typeIndex, idxGrantByPrincipal)
	dst = append(dst, syncIDBytes...)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, principalRT)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, principalID)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleBytes(dst, externalID)
}

func AppendGrantByPrincipalResourceTypeIndexKeyRawBytes(dst []byte, syncIDBytes []byte, principalRT []byte, externalID []byte) []byte {
	dst = append(dst, versionV3, typeIndex, idxGrantByPrincipalResourceType)
	dst = append(dst, syncIDBytes...)
	dst = codec.AppendTupleSeparator(dst)
	dst = codec.AppendTupleBytes(dst, principalRT)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleBytes(dst, externalID)
}

func AppendGrantByNeedsExpansionIndexKeyRawBytes(dst []byte, syncIDBytes []byte, externalID []byte) []byte {
	dst = append(dst, versionV3, typeIndex, idxGrantByNeedsExpansion)
	dst = append(dst, syncIDBytes...)
	dst = codec.AppendTupleSeparator(dst)
	return codec.AppendTupleBytes(dst, externalID)
}
