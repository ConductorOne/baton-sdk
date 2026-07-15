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
	PutSourceCacheEntry(ctx context.Context, kind sourcecache.RowKind, scopeKey string, cacheValidator string) error

	// ReplaySourceCache copies every row stamped with scopeKey from prev
	// (the previous sync's store, opened read-only) into this store. prev
	// must be a Pebble store. Does NOT write the manifest entry — the
	// caller writes it after the scope's overlay/deletes complete, so a
	// failed replay can't leave a phantom hit for the next sync.
	ReplaySourceCache(ctx context.Context, prev connectorstore.Reader, kind sourcecache.RowKind, scopeKey string) (SourceCacheReplayResult, error)

	// ApplyTombstones applies one page's delta tombstones for a scope
	// after replay + overlay, returning the number of rows deleted.
	//
	//   - deletedIDs are public canonical IDs (grant/entitlement IDs, or
	//     resource BIDs "bid:r:..."). Grants resolve WITHIN the scope's
	//     own rows by stored id, so connector-custom grant-id shapes work
	//     and the cost stays bounded by the scope's size; entitlements
	//     and resources resolve by bounded id probing (never an O(all
	//     rows) scan).
	//   - deletedPrincipalIDs are bare object ids: grants delete every
	//     row in the scope whose principal id matches (no principal
	//     type, no canonical-id reconstruction), resources by resource
	//     id (any type). Not supported for entitlements.
	//
	// Ids with no matching rows are no-ops. One scope-index scan per
	// call; a page's tombstones are batched into one call.
	ApplyTombstones(ctx context.Context, kind sourcecache.RowKind, scopeKey string, deletedIDs, deletedPrincipalIDs []string) (int64, error)

	// SourceCacheOrphanScopes returns, per row kind, scope keys present
	// in the by_source_scope indexes with no manifest entry — an
	// invariant violation at a sealed sync's quiesce point (ingestion
	// invariant I6): the orphaned stamps would poison a future sync's
	// replay.
	SourceCacheOrphanScopes(ctx context.Context) (map[string][]string, error)
}

var _ SourceCacheStore = (*pebbleStore)(nil)

// SourceCacheInspector is the read-only inspection surface for tooling
// (the `baton source-cache` audit command) — typed access to the
// manifest, the per-scope index counts, and the I6 orphan check, without
// reaching for the raw engine.
type SourceCacheInspector interface {
	// SourceCacheManifestSnapshot returns every manifest entry as
	// "row_kind\x00scope_key" -> cache validator.
	SourceCacheManifestSnapshot(ctx context.Context) (map[string]string, error)
	// SourceScopeIndexSnapshot returns, per row kind, scope_key -> the
	// number of by_source_scope index entries.
	SourceScopeIndexSnapshot(ctx context.Context) (map[string]map[string]int, error)
	// SourceCacheOrphanScopes: see SourceCacheStore.
	SourceCacheOrphanScopes(ctx context.Context) (map[string][]string, error)
	// LookupSourceCacheEntry: see SourceCacheStore.
	LookupSourceCacheEntry(ctx context.Context, kind sourcecache.RowKind, scopeKey string) (sourcecache.Entry, bool, error)
}

var _ SourceCacheInspector = (*pebbleStore)(nil)

func (s *pebbleStore) SourceCacheManifestSnapshot(ctx context.Context) (map[string]string, error) {
	return s.engine.SourceCacheManifestSnapshot(ctx)
}

func (s *pebbleStore) SourceScopeIndexSnapshot(ctx context.Context) (map[string]map[string]int, error) {
	return s.engine.SourceScopeIndexSnapshot(ctx)
}

// IngestInvariantStore is the optional store capability backing the
// syncer's ingestion invariants (see
// docs/tasks/source-cache-ingestion-invariants.md): dense row facts
// tracked as existence bits, the ordered-key scans the referential
// invariants (I3/I7/I8/I9) ride, and the drop/repair mechanics their
// default-mode verdicts apply. Pebble-only; the syncer degrades
// per-invariant when the store lacks it (SQLite artifacts are never
// scanned or mutated).
type IngestInvariantStore interface {
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

	// GrantsForEntitlementAllCarryInsertFact reports whether every grant
	// under the entitlement identity carries InsertResourceGrants — the
	// fail-fast side of I8's per-grant exemption. Value-reading;
	// reserved for dangling-reference probes.
	GrantsForEntitlementAllCarryInsertFact(ctx context.Context, entitlementID, entResourceTypeID, entResourceID string) (bool, error)

	// HasResourceRecord reports whether a resource row exists.
	HasResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (bool, error)

	// ForEachDistinctEntitlementResource visits each distinct resource
	// referenced by any entitlement row: one seek per distinct
	// resource, never O(entitlements).
	ForEachDistinctEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error

	// ForEachDanglingGrantEntitlement visits each distinct entitlement
	// referenced by grants that has no entitlement row: one seek plus
	// one point probe per distinct entitlement, never O(grants).
	ForEachDanglingGrantEntitlement(ctx context.Context, visit func(entitlementID, resourceTypeID, resourceID string) error) error

	// EnsureGrantIndexes forces a pending deferred grant-index build to
	// run now, making by_principal complete for the dangling-principal
	// scan. Near-free: the same build would run at EndSync.
	EnsureGrantIndexes(ctx context.Context) error

	// ForEachDanglingGrantPrincipal visits each distinct principal
	// referenced by grants that has no resource row. matchAnnotatedOnly
	// reports that every grant of the principal is an unprocessed
	// ExternalResourceMatch* carrier (exempt shape); carrierGrants is
	// the per-grant count of that population.
	ForEachDanglingGrantPrincipal(ctx context.Context, visit func(principalRT, principalID string, matchAnnotatedOnly bool, carrierGrants int64) error) error

	// DeleteEntitlementsForResource drops every entitlement row under a
	// resource; returns the count and up to maxIDs deleted ids
	// (maxIDs <= 0 collects none — callers pass a shrinking budget).
	DeleteEntitlementsForResource(ctx context.Context, resourceTypeID, resourceID string, maxIDs int) (int64, []string, error)

	// DeleteGrantsForEntitlement drops every grant row under one
	// entitlement identity except InsertResourceGrants carriers (the
	// machinery-owned shape); returns (deleted, skippedInsertFact).
	DeleteGrantsForEntitlement(ctx context.Context, entitlementID, entResourceTypeID, entResourceID string) (int64, int64, error)

	// DeleteGrantsForPrincipal drops every grant of one principal except
	// ExternalResourceMatch* carriers; returns (deleted, skipped).
	DeleteGrantsForPrincipal(ctx context.Context, principalRT, principalID string) (int64, int64, error)
}

var _ IngestInvariantStore = (*pebbleStore)(nil)

func (s *pebbleStore) HasExternalMatchGrants(ctx context.Context) (bool, error) {
	return s.engine.HasExternalMatchGrants(), nil
}

func (s *pebbleStore) ForEachDistinctGrantEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error {
	return s.engine.ForEachDistinctGrantEntitlementResource(ctx, visit)
}

func (s *pebbleStore) GrantsForEntResourceCarryInsertFact(ctx context.Context, resourceTypeID, resourceID string) (bool, error) {
	return s.engine.GrantsForEntResourceCarryInsertFact(ctx, resourceTypeID, resourceID)
}

func (s *pebbleStore) GrantsForEntitlementAllCarryInsertFact(ctx context.Context, entitlementID, entResourceTypeID, entResourceID string) (bool, error) {
	return s.engine.GrantsForEntitlementAllCarryInsertFact(ctx, entitlementID, entResourceTypeID, entResourceID)
}

func (s *pebbleStore) HasResourceRecord(ctx context.Context, resourceTypeID, resourceID string) (bool, error) {
	return s.engine.HasResourceRecord(ctx, resourceTypeID, resourceID)
}

func (s *pebbleStore) ForEachDistinctEntitlementResource(ctx context.Context, visit func(resourceTypeID, resourceID string) error) error {
	return s.engine.ForEachDistinctEntitlementResource(ctx, visit)
}

func (s *pebbleStore) ForEachDanglingGrantEntitlement(ctx context.Context, visit func(entitlementID, resourceTypeID, resourceID string) error) error {
	return s.engine.ForEachDanglingGrantEntitlement(ctx, visit)
}

func (s *pebbleStore) EnsureGrantIndexes(ctx context.Context) error {
	return s.engine.EnsureGrantIndexes(ctx)
}

func (s *pebbleStore) ForEachDanglingGrantPrincipal(ctx context.Context, visit func(principalRT, principalID string, matchAnnotatedOnly bool, carrierGrants int64) error) error {
	return s.engine.ForEachDanglingGrantPrincipal(ctx, visit)
}

func (s *pebbleStore) DeleteEntitlementsForResource(ctx context.Context, resourceTypeID, resourceID string, maxIDs int) (int64, []string, error) {
	n, ids, err := s.engine.DeleteEntitlementsForResource(ctx, resourceTypeID, resourceID, maxIDs)
	if n > 0 {
		s.MarkDirty()
	}
	return n, ids, err
}

func (s *pebbleStore) DeleteGrantsForEntitlement(ctx context.Context, entitlementID, entResourceTypeID, entResourceID string) (int64, int64, error) {
	n, skipped, err := s.engine.DeleteGrantsForEntitlement(ctx, entitlementID, entResourceTypeID, entResourceID)
	if n > 0 {
		s.MarkDirty()
	}
	return n, skipped, err
}

func (s *pebbleStore) DeleteGrantsForPrincipal(ctx context.Context, principalRT, principalID string) (int64, int64, error) {
	n, skipped, err := s.engine.DeleteGrantsForPrincipal(ctx, principalRT, principalID)
	if n > 0 {
		s.MarkDirty()
	}
	return n, skipped, err
}

func (s *pebbleStore) SourceCacheOrphanScopes(ctx context.Context) (map[string][]string, error) {
	return s.engine.SourceCacheOrphanScopes(ctx)
}

// RepairRelatedResource copies one resource row from prev (the previous
// sync's store) into the current sync — ingestion invariant I3's repair
// arm for grant-inserted resources lost by a replay-path failure.
// Returns false when prev holds no such row (repair impossible).
func (s *pebbleStore) RepairRelatedResource(ctx context.Context, prev connectorstore.Reader, resourceTypeID, resourceID string) (bool, error) {
	prevEngine, ok := sourceCacheEngine(prev)
	if !ok {
		return false, errors.New("related-resource repair: previous sync store is not a pebble store")
	}
	copied, err := s.engine.CopyResourceRowFrom(ctx, prevEngine, resourceTypeID, resourceID)
	if err != nil {
		return false, err
	}
	if copied {
		s.MarkDirty()
	}
	return copied, nil
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

func (s *pebbleStore) PutSourceCacheEntry(ctx context.Context, kind sourcecache.RowKind, scopeKey string, cacheValidator string) error {
	if err := sourcecache.ValidateRowKind(kind); err != nil {
		return err
	}
	return s.markDirty(s.engine.PutSourceCacheEntry(ctx, string(kind), scopeKey, cacheValidator))
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

// ApplyTombstones applies one page's delta tombstones; see the interface
// doc for id semantics. Every delete path returns its row count so the
// syncer's tombstone accounting is symmetric across kinds.
//
// NOTE on the bare-id lookup safety contract (engine/pebble/lookup.go):
// sync paths normally must not resolve rows by string. Canonical-id
// tombstones are a deliberate, narrow exception: the ids are strings the
// connector itself emitted for these rows, volumes are delta-sized (not
// O(rows)), and resolution keeps the exactly-one rule — an ambiguous id
// fails the sync loudly rather than guessing a delete. Grant tombstones
// avoid string resolution entirely by matching stored ids within the
// scope's own rows.
func (s *pebbleStore) ApplyTombstones(ctx context.Context, kind sourcecache.RowKind, scopeKey string, deletedIDs, deletedPrincipalIDs []string) (int64, error) {
	var total int64

	if len(deletedIDs) > 0 {
		switch kind {
		case sourcecache.RowKindGrants:
			idSet := make(map[string]struct{}, len(deletedIDs))
			for _, id := range deletedIDs {
				idSet[id] = struct{}{}
			}
			deleted, err := s.engine.DeleteGrantsByExternalIDsInScope(ctx, scopeKey, idSet)
			if err != nil {
				return total, fmt.Errorf("source cache grant-id delete for scope %q: %w", scopeKey, err)
			}
			total += deleted
		case sourcecache.RowKindEntitlements:
			for _, id := range deletedIDs {
				deleted, err := s.engine.DeleteEntitlementRecord(ctx, id)
				if err != nil {
					return total, fmt.Errorf("source cache delete entitlement %q: %w", id, err)
				}
				if deleted {
					total++
				}
			}
		case sourcecache.RowKindResources:
			for _, id := range deletedIDs {
				r, err := bid.ParseResourceBid(id)
				if err != nil {
					return total, fmt.Errorf("source cache delete resource: invalid resource bid %q: %w", id, err)
				}
				rid := r.GetId()
				deleted, err := s.engine.DeleteResourceRecord(ctx, rid.GetResourceType(), rid.GetResource())
				if err != nil {
					return total, fmt.Errorf("source cache delete resource %q: %w", id, err)
				}
				if deleted {
					total++
				}
			}
		default:
			return total, fmt.Errorf("source cache delete: invalid row kind %q", kind)
		}
	}

	if len(deletedPrincipalIDs) > 0 {
		idSet := make(map[string]struct{}, len(deletedPrincipalIDs))
		for _, id := range deletedPrincipalIDs {
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
			return total, fmt.Errorf("source cache scoped delete: not supported for entitlements")
		default:
			return total, fmt.Errorf("source cache scoped delete: invalid row kind %q", kind)
		}
		if err != nil {
			return total, fmt.Errorf("source cache scoped delete for scope %q: %w", scopeKey, err)
		}
		total += deleted
	}

	if total > 0 {
		s.MarkDirty()
	}
	return total, nil
}
