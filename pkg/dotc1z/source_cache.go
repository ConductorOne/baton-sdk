package dotc1z

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	cdbpebble "github.com/cockroachdb/pebble/v2"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/bid"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// SourceCacheReplayResult reports what one scope's replay copied.
type SourceCacheReplayResult = pebble.SourceCacheReplayResult

type sourceCacheStoreTestSeams struct {
	// afterEngineReplay injects a wrapper-level error after the engine has
	// committed replay work.
	afterEngineReplay func() error

	// beforeEngineMutation runs after the public wrapper owns closeMu and
	// marks dirty, immediately before entering an engine mutation.
	beforeEngineMutation func()

	// beforeCloseLock runs immediately before Close attempts to acquire
	// closeMu, allowing tests to prove it contends with an active wrapper.
	beforeCloseLock func()
}

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
	// batched into one call. Deletes commit in bounded chunks; on error,
	// the returned count reports rows already committed and retry converges.
	// Ids with no matching rows are no-ops.
	// Not supported for entitlements.
	DeleteSourceCacheRowsInScope(ctx context.Context, kind sourcecache.RowKind, scopeKey string, ids []string) (int64, error)

	// DeleteSourceCacheGrantsByIDInScope removes grant rows stamped with
	// scopeKey whose STORED grant id is in ids — works for
	// connector-custom grant-id shapes that the global bounded delete
	// cannot resolve, and stays bounded by the scope's row count. Ids with
	// no matching rows are no-ops. Deletes commit in bounded chunks; on
	// error, the returned count reports rows already committed.
	DeleteSourceCacheGrantsByIDInScope(ctx context.Context, scopeKey string, ids []string) (int64, error)
}

var _ SourceCacheStore = (*pebbleStore)(nil)

func (s *pebbleStore) beginSourceCacheMutation() (func(), error) {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil, pebble.ErrEngineClosing
	}
	// Hold closeMu until the engine call and dirty transition are complete.
	// Close cannot checkpoint between these two halves of one public mutation.
	s.dirty = true
	if s.sourceCacheTest.beforeEngineMutation != nil {
		s.sourceCacheTest.beforeEngineMutation()
	}
	return s.closeMu.Unlock, nil
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

func validateReplaySourceEligible(ctx context.Context, previous *pebble.Engine) error {
	run, err := previous.LatestFinishedSyncRecord(ctx, nil)
	if err != nil {
		return fmt.Errorf("source cache replay: read previous sync lifecycle: %w", err)
	}
	if run == nil {
		return errors.New("source cache replay: previous artifact sync is not finished")
	}
	if run.GetType() != v3.SyncType_SYNC_TYPE_FULL {
		return fmt.Errorf("source cache replay: previous artifact sync type %s is not replay-eligible", run.GetType())
	}
	if run.GetCompacted() {
		return errors.New("source cache replay: compacted artifacts are not replay-eligible")
	}
	return nil
}

func sameSourceCacheArtifact(current *pebbleStore, previous connectorstore.Reader) bool {
	prev, ok := previous.(*pebbleStore)
	if !ok {
		return false
	}
	currentPath, currentErr := filepath.Abs(current.outputFilePath)
	previousPath, previousErr := filepath.Abs(prev.outputFilePath)
	if currentErr == nil && previousErr == nil && filepath.Clean(currentPath) == filepath.Clean(previousPath) {
		return true
	}
	currentInfo, currentErr := os.Stat(current.outputFilePath)
	previousInfo, previousErr := os.Stat(prev.outputFilePath)
	return currentErr == nil && previousErr == nil && os.SameFile(currentInfo, previousInfo)
}

func (s *pebbleStore) LookupSourceCacheEntry(ctx context.Context, kind sourcecache.RowKind, scopeKey string) (sourcecache.Entry, bool, error) {
	if err := sourcecache.ValidateRowKind(kind); err != nil {
		return sourcecache.Entry{}, false, err
	}
	if err := sourcecache.ValidateScopeKey(scopeKey); err != nil {
		return sourcecache.Entry{}, false, err
	}
	rec, err := s.GetSourceCacheEntry(ctx, string(kind), scopeKey)
	if err != nil {
		if errors.Is(err, cdbpebble.ErrNotFound) {
			return sourcecache.Entry{}, false, nil
		}
		return sourcecache.Entry{}, false, err
	}
	if rec.GetInvalidated() || rec.GetCacheValidator() == "" {
		return sourcecache.Entry{}, false, nil
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
	if err := sourcecache.ValidateScopeKey(scopeKey); err != nil {
		return err
	}
	if cacheValidator == "" {
		return errors.New("source cache manifest: cache validator is required")
	}
	done, err := s.beginSourceCacheMutation()
	if err != nil {
		return err
	}
	defer done()
	return s.Engine.PutSourceCacheEntry(ctx, string(kind), scopeKey, cacheValidator)
}

func (s *pebbleStore) ReplaySourceCache(ctx context.Context, prev connectorstore.Reader, kind sourcecache.RowKind, scopeKey string) (SourceCacheReplayResult, error) {
	if err := sourcecache.ValidateRowKind(kind); err != nil {
		return SourceCacheReplayResult{}, err
	}
	if err := sourcecache.ValidateScopeKey(scopeKey); err != nil {
		return SourceCacheReplayResult{}, err
	}
	if sameSourceCacheArtifact(s, prev) {
		return SourceCacheReplayResult{}, errors.New("source cache replay: previous and current stores use the same artifact")
	}
	prevEngine, ok := sourceCacheEngine(prev)
	if !ok {
		return SourceCacheReplayResult{}, errors.New("source cache replay: previous sync store is not a pebble store")
	}
	if prevEngine == s.Engine {
		return SourceCacheReplayResult{}, errors.New("source cache replay: previous and current stores are the same")
	}
	if err := validateReplaySourceEligible(ctx, prevEngine); err != nil {
		return SourceCacheReplayResult{}, err
	}
	entry, err := prevEngine.GetSourceCacheEntry(ctx, string(kind), scopeKey)
	if err != nil {
		if errors.Is(err, cdbpebble.ErrNotFound) {
			return SourceCacheReplayResult{}, fmt.Errorf(
				"source cache replay: no manifest for row kind %q and scope %q: %w",
				kind,
				scopeKey,
				cdbpebble.ErrNotFound,
			)
		}
		return SourceCacheReplayResult{}, fmt.Errorf("source cache replay: read previous manifest: %w", err)
	}
	if entry.GetInvalidated() {
		return SourceCacheReplayResult{}, fmt.Errorf("source cache replay: manifest for row kind %q and scope %q is invalidated", kind, scopeKey)
	}
	if entry.GetCacheValidator() == "" {
		return SourceCacheReplayResult{}, fmt.Errorf("source cache replay: manifest for row kind %q and scope %q has no validator", kind, scopeKey)
	}
	// Replay is replacement, not append: the engine may clear destination rows
	// or commit one or more bounded chunks before returning zero rows or an error.
	// Serialize dirty marking and the engine mutation against Close so a checkpoint
	// cannot cut between them.
	done, err := s.beginSourceCacheMutation()
	if err != nil {
		return SourceCacheReplayResult{}, err
	}
	defer done()
	var res SourceCacheReplayResult
	switch kind {
	case sourcecache.RowKindResources:
		res, err = s.ReplaySourceCacheResources(ctx, prevEngine, scopeKey)
	case sourcecache.RowKindEntitlements:
		res, err = s.ReplaySourceCacheEntitlements(ctx, prevEngine, scopeKey)
	case sourcecache.RowKindGrants:
		res, err = s.ReplaySourceCacheGrants(ctx, prevEngine, scopeKey)
	default:
		return SourceCacheReplayResult{}, fmt.Errorf("source cache replay: invalid row kind %q", kind)
	}
	if err != nil {
		return SourceCacheReplayResult{}, err
	}
	if s.sourceCacheTest.afterEngineReplay != nil {
		if err := s.sourceCacheTest.afterEngineReplay(); err != nil {
			return SourceCacheReplayResult{}, err
		}
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
	if err := sourcecache.ValidateRowKind(kind); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	if kind == sourcecache.RowKindResources {
		resources := make([]*v2.Resource, len(ids))
		for i, id := range ids {
			r, err := bid.ParseResourceBid(id)
			if err != nil {
				return fmt.Errorf("source cache delete resource: invalid resource bid %q: %w", id, err)
			}
			resources[i] = r
		}
		done, err := s.beginSourceCacheMutation()
		if err != nil {
			return err
		}
		defer done()
		for i, r := range resources {
			rid := r.GetId()
			if err := s.Engine.DeleteResourceRecord(ctx, rid.GetResourceType(), rid.GetResource()); err != nil {
				return fmt.Errorf("source cache delete resource %q: %w", ids[i], err)
			}
		}
		return nil
	}
	done, err := s.beginSourceCacheMutation()
	if err != nil {
		return err
	}
	defer done()
	switch kind {
	case sourcecache.RowKindGrants:
		if err := s.DeleteGrantRecordsBounded(ctx, ids); err != nil {
			return fmt.Errorf("source cache delete grants: %w", err)
		}
	case sourcecache.RowKindEntitlements:
		if err := s.DeleteEntitlementRecords(ctx, ids); err != nil {
			return fmt.Errorf("source cache delete entitlements: %w", err)
		}
	case sourcecache.RowKindResources:
		return errors.New("source cache delete resource: internal dispatch error")
	}
	return nil
}

func (s *pebbleStore) DeleteSourceCacheGrantsByIDInScope(ctx context.Context, scopeKey string, ids []string) (int64, error) {
	if err := sourcecache.ValidateScopeKey(scopeKey); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	idSet := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	done, err := s.beginSourceCacheMutation()
	if err != nil {
		return 0, err
	}
	defer done()
	deleted, err := s.DeleteGrantsByExternalIDsInScope(ctx, scopeKey, idSet)
	if err != nil {
		return deleted, fmt.Errorf("source cache grant-id delete for scope %q: %w", scopeKey, err)
	}
	return deleted, nil
}

func (s *pebbleStore) DeleteSourceCacheRowsInScope(ctx context.Context, kind sourcecache.RowKind, scopeKey string, ids []string) (int64, error) {
	if err := sourcecache.ValidateRowKind(kind); err != nil {
		return 0, err
	}
	if err := sourcecache.ValidateScopeKey(scopeKey); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	idSet := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}
	if kind == sourcecache.RowKindEntitlements {
		return 0, fmt.Errorf("source cache scoped delete: not supported for entitlements")
	}
	done, err := s.beginSourceCacheMutation()
	if err != nil {
		return 0, err
	}
	defer done()
	var deleted int64
	// A matching orphan scope index is a durable mutation even though no
	// primary row contributes to the returned deletion count.
	switch kind {
	case sourcecache.RowKindGrants:
		deleted, err = s.DeleteGrantsByPrincipalsInScope(ctx, scopeKey, idSet)
	case sourcecache.RowKindResources:
		deleted, err = s.DeleteResourcesByIDsInScope(ctx, scopeKey, idSet)
	case sourcecache.RowKindEntitlements:
		return 0, fmt.Errorf("source cache scoped delete: not supported for entitlements")
	}
	if err != nil {
		return deleted, fmt.Errorf("source cache scoped delete for scope %q: %w", scopeKey, err)
	}
	return deleted, nil
}
