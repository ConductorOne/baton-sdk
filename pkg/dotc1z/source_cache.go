package dotc1z

import (
	"context"
	"errors"
	"fmt"

	cdbpebble "github.com/cockroachdb/pebble/v2"

	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// SourceCacheReplayResult reports what one scope's replay copied.
type SourceCacheReplayResult = pebble.SourceCacheReplayResult

// SourceCacheStore is the optional store capability backing source-cache
// replay (see proto/c1/connector/v2/annotation_source_cache.proto). It is
// implemented ONLY by the Pebble engine; the syncer type-asserts for it and
// treats a store without it as "source cache unsupported" (no-op lookup,
// no replay). It is deliberately NOT part of c1zstore.Store.
type SourceCacheStore interface {
	// LookupSourceCacheEntry returns this store's manifest entry for
	// (kind, scopeKey). Backs the connector-facing lookup when this
	// store is the previous sync.
	LookupSourceCacheEntry(ctx context.Context, kind sourcecache.RowKind, scopeKey string) (sourcecache.Entry, bool, error)

	// PutSourceCacheEntry writes the current sync's manifest entry for
	// (kind, scopeKey). Zero-row scopes still get entries.
	PutSourceCacheEntry(ctx context.Context, kind sourcecache.RowKind, scopeKey string, etag string) error

	// ReplaySourceCache copies every row stamped with scopeKey from prev
	// (the previous sync's store, opened read-only) into this store. prev
	// must be a Pebble store. Does NOT write the manifest entry — the
	// caller writes it after the scope's overlay/deletes complete, so a
	// failed replay can't leave a phantom hit for the next sync.
	ReplaySourceCache(ctx context.Context, prev connectorstore.Reader, kind sourcecache.RowKind, scopeKey string) (SourceCacheReplayResult, error)

	// DeleteSourceCacheRows removes rows by public canonical ID from the
	// current sync, after replay + overlay (delta-query tombstones).
	// ID formats per kind: grants and entitlements use their canonical
	// IDs; resources use Baton resource BIDs ("bid:r:...").
	//
	// Grant resolution is BOUNDED: candidate probing only, never the
	// O(all grants) stored-external-id scan. Grants stored under
	// connector-custom ids are unreachable here (delete no-ops); such
	// connectors use DeleteSourceCacheRowsInScope instead.
	DeleteSourceCacheRows(ctx context.Context, kind sourcecache.RowKind, ids []string) error

	// DeleteSourceCacheRowsInScope removes rows stamped with scopeKey by
	// bare object id — grants by principal id (no principal type, no
	// canonical-id reconstruction), resources by resource id (any type).
	// One index scan of the scope per call; a page's tombstones are
	// batched into one call. Ids with no matching rows are no-ops.
	// Not supported for entitlements.
	DeleteSourceCacheRowsInScope(ctx context.Context, kind sourcecache.RowKind, scopeKey string, ids []string) (int64, error)

	// DeleteSourceCacheGrantsByIDInScope removes grant rows stamped with
	// scopeKey whose STORED grant id is in ids — works for
	// connector-custom grant-id shapes that the global bounded delete
	// cannot resolve, and stays bounded by the scope's row count. Ids with
	// no matching rows are no-ops.
	DeleteSourceCacheGrantsByIDInScope(ctx context.Context, scopeKey string, ids []string) (int64, error)
}

var _ SourceCacheStore = (*pebbleStore)(nil)

// IngestFactStore is the optional store capability backing the syncer's
// ingestion invariants (see docs/tasks/source-cache-ingestion-invariants.md):
// dense row facts tracked as existence bits, and the ordered-key
// inspection reads the referential invariant uses. Pebble-only; the
// syncer degrades per-invariant when the store lacks it.
type IngestFactStore interface {
	// HasExternalMatchGrants reports whether the open sync holds at
	// least one grant carrying an ExternalResourceMatch* annotation —
	// the store-derived truth behind HasExternalResourcesGrants.
	HasExternalMatchGrants(ctx context.Context) (bool, error)

	// ForEachDistinctGrantEntitlementResource visits each distinct
	// entitlement resource referenced by any grant: one seek per
	// distinct resource, never O(grants).
	ForEachDistinctGrantEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error

	// GrantsForEntResourceCarryInsertFact reports whether any grant
	// under the entitlement resource carries InsertResourceGrants.
	// Value-reading; reserved for dangling-reference probes.
	GrantsForEntResourceCarryInsertFact(ctx context.Context, resourceTypeID, resourceID string) (bool, error)

	// HasResourceRecord reports whether a resource row exists.
	HasResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (bool, error)
}

var _ IngestFactStore = (*pebbleStore)(nil)

func (s *pebbleStore) HasExternalMatchGrants(ctx context.Context) (bool, error) {
	return s.engine.HasExternalMatchGrants(), nil
}

func (s *pebbleStore) ForEachDistinctGrantEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error {
	return s.engine.ForEachDistinctGrantEntitlementResource(ctx, visit)
}

func (s *pebbleStore) GrantsForEntResourceCarryInsertFact(ctx context.Context, resourceTypeID, resourceID string) (bool, error) {
	return s.engine.GrantsForEntResourceCarryInsertFact(ctx, resourceTypeID, resourceID)
}

func (s *pebbleStore) HasResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (bool, error) {
	return s.engine.HasResourceRecord(ctx, resourceTypeID, resourceID)
}

// sourceCacheEngine recovers the Pebble engine from an arbitrary store,
// nil-safe. Mirrors pebble.AsEngine but accepts any value so the syncer
// can probe its previous-sync reader without caring about its static type.
func sourceCacheEngine(store any) (*pebble.Engine, bool) {
	a, ok := store.(interface{ PebbleEngine() *pebble.Engine })
	if !ok {
		return nil, false
	}
	e := a.PebbleEngine()
	return e, e != nil
}

func (s *pebbleStore) LookupSourceCacheEntry(ctx context.Context, kind sourcecache.RowKind, scopeKey string) (sourcecache.Entry, bool, error) {
	if err := sourcecache.ValidateRowKind(kind); err != nil {
		return sourcecache.Entry{}, false, err
	}
	rec, err := s.engine.GetSourceCacheEntry(ctx, string(kind), scopeKey)
	if err != nil {
		if errors.Is(err, cdbpebble.ErrNotFound) {
			return sourcecache.Entry{}, false, nil
		}
		return sourcecache.Entry{}, false, err
	}
	return sourcecache.Entry{
		CacheValidator: rec.GetCacheValidator(),
		DiscoveredAt:   rec.GetDiscoveredAt().AsTime(),
	}, true, nil
}

func (s *pebbleStore) PutSourceCacheEntry(ctx context.Context, kind sourcecache.RowKind, scopeKey string, etag string) error {
	if err := sourcecache.ValidateRowKind(kind); err != nil {
		return err
	}
	return s.markDirty(s.engine.PutSourceCacheEntry(ctx, string(kind), scopeKey, etag))
}

func (s *pebbleStore) ReplaySourceCache(ctx context.Context, prev connectorstore.Reader, kind sourcecache.RowKind, scopeKey string) (SourceCacheReplayResult, error) {
	prevEngine, ok := sourceCacheEngine(prev)
	if !ok {
		return SourceCacheReplayResult{}, errors.New("source cache replay: previous sync store is not a pebble store")
	}
	var res SourceCacheReplayResult
	var err error
	switch kind {
	case sourcecache.RowKindResources:
		res, err = s.engine.ReplaySourceCacheResources(ctx, prevEngine, scopeKey)
	case sourcecache.RowKindEntitlements:
		res, err = s.engine.ReplaySourceCacheEntitlements(ctx, prevEngine, scopeKey)
	case sourcecache.RowKindGrants:
		res, err = s.engine.ReplaySourceCacheGrants(ctx, prevEngine, scopeKey)
	default:
		return SourceCacheReplayResult{}, fmt.Errorf("source cache replay: invalid row kind %q", kind)
	}
	if err != nil {
		return SourceCacheReplayResult{}, err
	}
	if res.Rows > 0 {
		s.MarkDirty()
	}
	return res, nil
}

// DeleteSourceCacheRows deletes delta tombstones by public id string.
//
// NOTE on the bare-id lookup safety contract (engine/pebble/lookup.go):
// sync paths normally must not resolve grants by string. This path is a
// deliberate, narrow exception: tombstone ids are strings the connector
// itself emitted for these rows, volumes are delta-sized (not O(rows)),
// and resolution keeps the exactly-one rule — an ambiguous id fails the
// sync loudly rather than guessing a delete, which matches the
// source-cache replay-phase error policy.
func (s *pebbleStore) DeleteSourceCacheRows(ctx context.Context, kind sourcecache.RowKind, ids []string) error {
	for _, id := range ids {
		switch kind {
		case sourcecache.RowKindGrants:
			if err := s.markDirty(s.engine.DeleteGrantRecordBounded(ctx, id)); err != nil {
				return fmt.Errorf("source cache delete grant %q: %w", id, err)
			}
		case sourcecache.RowKindEntitlements:
			if err := s.markDirty(s.engine.DeleteEntitlementRecord(ctx, id)); err != nil {
				return fmt.Errorf("source cache delete entitlement %q: %w", id, err)
			}
		case sourcecache.RowKindResources:
			r, err := bid.ParseResourceBid(id)
			if err != nil {
				return fmt.Errorf("source cache delete resource: invalid resource bid %q: %w", id, err)
			}
			rid := r.GetId()
			if err := s.markDirty(s.engine.DeleteResourceRecord(ctx, rid.GetResourceType(), rid.GetResource())); err != nil {
				return fmt.Errorf("source cache delete resource %q: %w", id, err)
			}
		default:
			return fmt.Errorf("source cache delete: invalid row kind %q", kind)
		}
	}
	return nil
}

func (s *pebbleStore) DeleteSourceCacheGrantsByIDInScope(ctx context.Context, scopeKey string, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	idSet := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	deleted, err := s.engine.DeleteGrantsByExternalIDsInScope(ctx, scopeKey, idSet)
	if err != nil {
		return 0, fmt.Errorf("source cache grant-id delete for scope %q: %w", scopeKey, err)
	}
	if deleted > 0 {
		s.MarkDirty()
	}
	return deleted, nil
}

func (s *pebbleStore) DeleteSourceCacheRowsInScope(ctx context.Context, kind sourcecache.RowKind, scopeKey string, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	idSet := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	var deleted int64
	var err error
	switch kind {
	case sourcecache.RowKindGrants:
		deleted, err = s.engine.DeleteGrantsByPrincipalsInScope(ctx, scopeKey, idSet)
	case sourcecache.RowKindResources:
		deleted, err = s.engine.DeleteResourcesByIDsInScope(ctx, scopeKey, idSet)
	case sourcecache.RowKindEntitlements:
		return 0, fmt.Errorf("source cache scoped delete: not supported for entitlements")
	default:
		return 0, fmt.Errorf("source cache scoped delete: invalid row kind %q", kind)
	}
	if err != nil {
		return 0, fmt.Errorf("source cache scoped delete for scope %q: %w", scopeKey, err)
	}
	if deleted > 0 {
		s.MarkDirty()
	}
	return deleted, nil
}
