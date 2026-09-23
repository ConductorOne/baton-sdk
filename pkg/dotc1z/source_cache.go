package dotc1z

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	cdbpebble "github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
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
//
// Advanced: this interface exists for the SDK's replay orchestration, not
// for direct use. The correctness obligations (preflight, scope poisoning,
// compat validation) live in the callers; see pkg/sourcecache for the
// connector-facing contract.
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
	//
	// On error, the result's Rows reports rows whose bounded intermediate
	// commits landed (retry converges; matching the delete siblings
	// below), and NeedsExpansion may overreport a staged-but-uncommitted
	// row — the safe direction, since arming expansion is idempotent and
	// add-only.
	ReplaySourceCache(ctx context.Context, prev connectorstore.Reader, kind sourcecache.RowKind, scopeKey string) (SourceCacheReplayResult, error)

	// DeleteSourceCacheRows removes rows from the current sync after replay
	// and this page's upserts (delta-query tombstones), by structured
	// identity. Absent refs are no-ops. Returns the rows deleted.
	//
	// scopeKey is the scope whose delta reported the tombstones. Removing
	// its own rows is the legitimate shrink flow; removing a row stamped
	// with any OTHER scope is a row-partition violation that durably
	// poisons the stamped scope (CO-015 — it becomes a lookup miss and is
	// refused as a replay source).
	//
	// Principals delete every grant in the scope whose principal matches:
	// one index scan of the scope per call. Deletes commit in bounded
	// chunks; deletion is idempotent, so on error the returned count
	// reports rows already committed and retry converges.
	DeleteSourceCacheRows(ctx context.Context, kind sourcecache.RowKind, scopeKey string, t sourcecache.Tombstones) (int64, error)
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
	// A poisoned scope reads as a MISS (CO-015): this store observed a
	// row-partition violation against it, so its stamped row set no
	// longer matches what the validator vouches for. Reporting a miss
	// makes the scope re-fetch cold and converge; reporting a hit would
	// send orchestration into a replay that preflight hard-refuses.
	poisoned, err := s.SourceCachePoisoned(ctx, string(kind), scopeKey)
	if err != nil {
		return sourcecache.Entry{}, false, err
	}
	if poisoned {
		return sourcecache.Entry{}, false, nil
	}
	return sourcecache.Entry{
		CacheValidator: rec.GetCacheValidator(),
		DiscoveredAt:   rec.GetDiscoveredAt().AsTime(),
	}, true, nil
}

func (s *pebbleStore) PutSourceCacheEntry(ctx context.Context, kind sourcecache.RowKind, scopeKey string, cacheValidator string) error {
	if err := s.writeHook(ctx, "PutSourceCacheEntry"); err != nil {
		return err
	}
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
	if err := s.writeHook(ctx, "ReplaySourceCache"); err != nil {
		return SourceCacheReplayResult{}, err
	}
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
		// Committed progress rides the error: bounded intermediate chunks
		// may have landed before the failure, and the engine result
		// reports exactly those rows — same contract as the delete
		// siblings on this interface. Zeroing it here would make the
		// engine-level contract unreachable from the only surface replay
		// orchestration uses.
		return res, err
	}
	if s.sourceCacheTest.afterEngineReplay != nil {
		if err := s.sourceCacheTest.afterEngineReplay(); err != nil {
			return res, err
		}
	}
	return res, nil
}

func (s *pebbleStore) DeleteSourceCacheRows(ctx context.Context, kind sourcecache.RowKind, scopeKey string, t sourcecache.Tombstones) (int64, error) {
	if err := s.writeHook(ctx, "DeleteSourceCacheRows"); err != nil {
		return 0, err
	}
	if err := t.ValidateKind(kind); err != nil {
		return 0, err
	}
	if err := sourcecache.ValidateScopeKey(scopeKey); err != nil {
		return 0, err
	}
	if t.Empty() {
		return 0, nil
	}
	done, err := s.beginSourceCacheMutation()
	if err != nil {
		return 0, err
	}
	defer done()
	switch kind {
	case sourcecache.RowKindResources:
		n, err := s.DeleteResourceRecordsBounded(ctx, t.Resources, scopeKey)
		if err != nil {
			return n, fmt.Errorf("source cache delete resources for scope %q: %w", scopeKey, err)
		}
		return n, nil
	case sourcecache.RowKindEntitlements:
		n, err := s.DeleteEntitlementRecordsByRef(ctx, t.Entitlements, scopeKey)
		if err != nil {
			return n, fmt.Errorf("source cache delete entitlements for scope %q: %w", scopeKey, err)
		}
		return n, nil
	case sourcecache.RowKindGrants:
		byRef, err := s.DeleteGrantRecordsByRef(ctx, t.Grants, scopeKey)
		if err != nil {
			return byRef, fmt.Errorf("source cache delete grants for scope %q: %w", scopeKey, err)
		}
		// A matching orphan scope index is a durable mutation even though
		// no primary row contributes to the returned count.
		byPrincipal, err := s.DeleteGrantsByPrincipalsInScope(ctx, scopeKey, t.Principals)
		if err != nil {
			return byRef + byPrincipal, fmt.Errorf("source cache delete grants by principal for scope %q: %w", scopeKey, err)
		}
		return byRef + byPrincipal, nil
	}
	return 0, fmt.Errorf("source cache delete: unknown row kind %q", kind)
}
