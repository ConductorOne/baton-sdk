package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"fmt"
	stdsync "sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// Source-cache replay, syncer side. See
// proto/c1/connector/v2/annotation_source_cache.proto for the contract.
//
// Setup degrades, replay fails loudly. Any setup problem (capability
// absent, store engine unsupported, no usable previous sync) installs the
// no-op lookup: the connector never sees a previous validator, never gets
// a conditional-request hit, and therefore never emits SourceCacheReplay
// — which is what makes it safe to treat a replay annotation arriving
// while degraded as a hard error (the connector already skipped row
// generation; there is nothing to fall back to).

// syncerSourceCache is the per-sync source-cache state resolved by
// configureSourceCache.
//
// Write side and read side enable independently: the FIRST sync of a chain
// has no previous sync but must still stamp rows and write manifest
// entries, or the second sync would have nothing to replay. enabled covers
// the write side (capability declared + current store supports it); prev
// is non-nil only when a usable previous sync exists (read side — lookup
// hits and replay).
type syncerSourceCache struct {
	enabled bool
	// current is the writable output store's source-cache capability.
	current dotc1z.SourceCacheStore
	// prev is the previous sync's lookup/replay source (read-only). Nil
	// when no usable previous sync exists; lookups then miss and replay
	// annotations are hard errors.
	prev dotc1z.SourceCacheStore
	// prevReader is the same store as prev, typed for ReplaySourceCache.
	prevReader connectorstore.Reader
	// lookup is the connector-facing lookup built from prev (NoopLookup
	// when prev is nil). The syncer also uses it to answer lookup asks
	// from connectors on single-shot transports (the ask/answer
	// continuation); both paths see identical results by construction.
	lookup sourcecache.Lookup
	// replayedScopes tracks scopes replayed this sync whose validator may
	// arrive on a LATER page (multi-page overlay rounds), so the final
	// page's manifest write is not also counted as a fresh stamp — one
	// warm delta round would otherwise book as both a hit and a miss in
	// the replay ratio. Only rounds that can span pages are recorded
	// (overlay, or replay without an inline etag), keeping the set tiny:
	// single-page 304 rounds never enter it. Pointer so copying the
	// per-sync struct shares the set; guarded internally for parallel
	// workers. Best-effort across resume (in-memory), stats-only.
	replayedScopes *replayedScopeSet
}

type replayedScopeSet struct {
	mu stdsync.Mutex
	m  map[string]struct{}
}

func (c *syncerSourceCache) markScopeReplayed(kind sourcecache.RowKind, scopeKey string) {
	if c.replayedScopes == nil {
		return
	}
	c.replayedScopes.mu.Lock()
	defer c.replayedScopes.mu.Unlock()
	if c.replayedScopes.m == nil {
		c.replayedScopes.m = make(map[string]struct{})
	}
	c.replayedScopes.m[string(kind)+"\x00"+scopeKey] = struct{}{}
}

func (c *syncerSourceCache) scopeReplayedThisSync(kind sourcecache.RowKind, scopeKey string) bool {
	if c.replayedScopes == nil {
		return false
	}
	c.replayedScopes.mu.Lock()
	defer c.replayedScopes.mu.Unlock()
	_, ok := c.replayedScopes.m[string(kind)+"\x00"+scopeKey]
	return ok
}

// prevStoreLookup adapts the previous store's manifest to the
// connector-facing Lookup. Mid-sync read errors are logged once and
// treated as misses: at lookup time the connector can still fetch fresh,
// so degrading beats failing the sync.
type prevStoreLookup struct {
	prev    dotc1z.SourceCacheStore
	logOnce *stdsync.Once
}

var _ sourcecache.Lookup = prevStoreLookup{}

func (p prevStoreLookup) Lookup(ctx context.Context, kind sourcecache.RowKind, scopeKey string) (sourcecache.Entry, bool, error) {
	entry, found, err := p.prev.LookupSourceCacheEntry(ctx, kind, scopeKey)
	if err != nil {
		p.logOnce.Do(func() {
			ctxzap.Extract(ctx).Warn("source cache lookup failed; treating as miss", zap.Error(err))
		})
		return sourcecache.Entry{}, false, nil //nolint:nilerr // intentional: a failed lookup degrades to a miss (connector fetches fresh) rather than failing the connector call
	}
	return entry, found, nil
}

// configureSourceCache resolves per-sync source-cache state from the
// connector's Validate response and installs the connector-facing lookup.
// Called once per Sync, after Validate.
func (s *syncer) configureSourceCache(ctx context.Context, resp *v2.ConnectorServiceValidateResponse) error {
	l := ctxzap.Extract(ctx)
	s.sourceCache = syncerSourceCache{}

	setLookup, canSetLookup := s.connector.(sourcecache.SetLookup)
	degrade := func(reason string) error {
		if canSetLookup {
			setLookup.SetSourceCache(ctx, sourcecache.NoopLookup{})
		}
		if reason != "" {
			l.Info("source cache disabled", zap.String("reason", reason))
		}
		return nil
	}

	capability := &v2.SourceCacheCapability{}
	annos := annotations.Annotations(resp.GetAnnotations())
	ok, err := annos.Pick(capability)
	if err != nil {
		return fmt.Errorf("error parsing source cache capability annotation: %w", err)
	}
	if !ok || capability.GetMode() != v2.SourceCacheCapability_MODE_READ_WRITE {
		// The common case; stay quiet.
		return degrade("")
	}
	if s.syncType != connectorstore.SyncTypeFull {
		// Replay is FULL-sync-only. A partial/targeted sync writes a
		// targeted subset; replaying a whole scope could resurrect rows
		// outside the targets, and the partial-sync related-resource
		// fetch (ShouldFetchRelatedResources) only runs over response
		// rows, so replayed grants could reference resources the partial
		// store never received. Degrading installs the no-op lookup:
		// every scope misses and the connector fetches fresh —
		// cold-correct, and partial syncs are small by construction.
		return degrade(fmt.Sprintf("source cache replay is only supported on full syncs (sync type %q)", s.syncType))
	}
	current, ok := s.store.(dotc1z.SourceCacheStore)
	if !ok {
		return degrade("storage engine does not support source cache")
	}

	// Write side enabled: rows produced under a scope get stamped and
	// manifest entries get written, so this sync is usable as the NEXT
	// sync's replay source even when this one has nothing to replay from.
	s.sourceCache = syncerSourceCache{enabled: true, current: current, replayedScopes: &replayedScopeSet{}}

	// Read side: a usable previous sync makes lookups hit and replay legal.
	var readReason string
	if s.previousSyncReader == nil {
		readReason = "no previous-sync c1z configured"
	} else if prev, ok := s.previousSyncReader.(dotc1z.SourceCacheStore); !ok {
		readReason = "previous-sync store engine does not support source cache"
	} else if reason, err := previousSyncReplayUnusableReason(ctx, s.previousSyncReader); err != nil {
		return fmt.Errorf("error reading previous sync's run metadata: %w", err)
	} else if reason != "" {
		// The explicit metadata gate, independent of the manifest's
		// contents (c1zstore.SyncRun.UsableAsReplaySource): compacted
		// artifacts are keep-newer merges no input's validators describe,
		// and partial/derived syncs are subsets whose rows do not cover
		// the scopes any validator vouches for. Those paths also leave no
		// manifest to hit (belt); the run-record gate is the contract
		// (suspenders) — "what kind of sync is this" answers "can it be
		// used for replay" without inspecting keyspaces.
		readReason = reason
	} else {
		s.sourceCache.prev = prev
		s.sourceCache.prevReader = s.previousSyncReader
	}

	lookup := sourcecache.Lookup(sourcecache.NoopLookup{})
	if s.sourceCache.prev != nil {
		lookup = prevStoreLookup{prev: s.sourceCache.prev, logOnce: &stdsync.Once{}}
	}
	s.sourceCache.lookup = lookup
	if canSetLookup {
		setLookup.SetSourceCache(ctx, lookup)
	} else {
		// The connector declared the capability but the client offers no
		// way to deliver lookups. Its own lookup stays no-op, so every
		// scope misses and no replay annotations can legally arrive.
		l.Warn("source cache capability declared but connector client cannot receive lookups")
	}
	l.Info("source cache enabled",
		zap.Bool("replay_available", s.sourceCache.prev != nil),
		zap.String("replay_unavailable_reason", readReason),
	)
	return nil
}

// previousSyncReplayUnusableReason inspects the previous-sync reader's
// newest finished run record against the replay-source predicate
// (c1zstore.SyncRun.UsableAsReplaySource) and returns a human-readable
// reason when it fails, or "" when the sync qualifies. A reader without a
// SyncMeta surface degrades to "" — the manifest-based guards still
// apply — but a readable record is authoritative.
func previousSyncReplayUnusableReason(ctx context.Context, reader connectorstore.Reader) (string, error) {
	metaHolder, ok := reader.(interface{ SyncMeta() c1zstore.SyncMeta })
	if !ok {
		return "", nil
	}
	run, err := metaHolder.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	if err != nil {
		return "", err
	}
	switch {
	case run == nil:
		return "previous-sync c1z has no finished sync run", nil
	case run.UsableAsReplaySource():
		return "", nil
	case run.Compacted:
		return "previous sync is a compaction artifact; upstream validators do not describe its merged contents", nil
	default:
		return fmt.Sprintf("previous sync is type %q, not a full sync; its rows do not cover the scopes any validator vouches for", run.Type), nil
	}
}

// clearSourceCacheLookup detaches the per-sync lookup so a late RPC from
// the connector cannot read a store the syncer no longer owns.
func (s *syncer) clearSourceCacheLookup(ctx context.Context) {
	if setLookup, ok := s.connector.(sourcecache.SetLookup); ok {
		setLookup.SetSourceCache(ctx, nil)
	}
}

// sourceCachePage carries one list response's source-cache instructions
// from beginSourceCachePage (before rows are written) to
// finishSourceCachePage (after rows are written).
type sourceCachePage struct {
	kind     sourcecache.RowKind
	scopeKey string
	etag     string
	// replayed reports that beginSourceCachePage copied the previous
	// sync's rows for this scope into the current sync BEFORE the page's
	// own rows commit. Consumers that dedupe against "already synced this
	// sync" state (the resources path) must not skip this page's rows:
	// they are the overlay, and the already-present row is the stale
	// replayed base they exist to overwrite.
	replayed bool
	// replayRows / overlayRows carry the replay's copy count and the
	// page's own row count from begin to finish, where the stats are
	// recorded — recording at begin would double-count a page that is
	// re-run after failing between replay and finish.
	replayRows  int64
	overlayRows int
	// deletedIDs are canonical-id tombstones (grant/entitlement ids,
	// resource BIDs); deletedPrincipalIDs are bare-object-id tombstones
	// applied scope-relatively. Both may arrive on any page of a scope
	// (replay annotation on the first page, scope annotation on every
	// page) and apply after the page's rows commit.
	deletedIDs          []string
	deletedPrincipalIDs []string
}

// beginSourceCachePage inspects a list response's annotations, performs
// any requested replay, and returns the context to write the page's rows
// under (stamped with the scope when one is present). A nil page means the
// response carried no source-cache instructions.
//
// rowCount is the number of rows in the response; a non-overlay replay
// that also returned rows gets a warning (the rows are upserted anyway).
func (s *syncer) beginSourceCachePage(
	ctx context.Context,
	kind sourcecache.RowKind,
	respAnnos annotations.Annotations,
	rowCount int,
) (context.Context, *sourceCachePage, error) {
	replay := &v2.SourceCacheReplay{}
	hasReplay, err := respAnnos.Pick(replay)
	if err != nil {
		return ctx, nil, fmt.Errorf("source cache: error parsing replay annotation: %w", err)
	}
	scope := &v2.SourceCacheRecord{}
	hasScope, err := respAnnos.Pick(scope)
	if err != nil {
		return ctx, nil, fmt.Errorf("source cache: error parsing scope annotation: %w", err)
	}
	if !hasReplay && !hasScope {
		return ctx, nil, nil
	}

	if !s.sourceCache.enabled {
		if hasReplay {
			// The connector skipped row generation expecting a replay; with
			// source cache disabled there is nothing to replay from. This is
			// a connector bug (replay for a scope it never got a lookup hit
			// on), not a degradable condition.
			return ctx, nil, fmt.Errorf("source cache: connector requested replay for scope %q but source cache is disabled", replay.GetScopeKey())
		}
		// Scope annotations without the capability handshake are ignored.
		return ctx, nil, nil
	}

	page := &sourceCachePage{kind: kind}
	switch {
	case hasReplay && hasScope:
		if replay.GetScopeKey() != scope.GetScopeKey() {
			return ctx, nil, fmt.Errorf("source cache: replay scope %q and page scope %q disagree", replay.GetScopeKey(), scope.GetScopeKey())
		}
		page.scopeKey = replay.GetScopeKey()
	case hasReplay:
		page.scopeKey = replay.GetScopeKey()
	default:
		page.scopeKey = scope.GetScopeKey()
	}
	if err := sourcecache.ValidateScopeKey(page.scopeKey); err != nil {
		return ctx, nil, fmt.Errorf("source cache: %w", err)
	}
	// Prefer the scope annotation's etag (the freshest validator on
	// overlay pages); fall back to the replay's.
	page.etag = scope.GetCacheValidator()
	if page.etag == "" {
		page.etag = replay.GetCacheValidator()
	}
	// Tombstones may ride either annotation — the replay annotation on a
	// round's first page, the scope annotation on every page (so a
	// multi-page delta round never buffers deletions). Fresh slices: an
	// append onto the proto's returned slice could write into its backing
	// array when it has spare capacity.
	page.deletedIDs = append(append([]string{}, replay.GetDeletedIds()...), scope.GetDeletedIds()...)
	page.deletedPrincipalIDs = append(append([]string{}, replay.GetDeletedPrincipalIds()...), scope.GetDeletedPrincipalIds()...)

	if hasReplay {
		if s.sourceCache.prev == nil {
			// Same invariant violation as the disabled case: the connector
			// can only have gotten a lookup hit if a previous source exists.
			return ctx, nil, fmt.Errorf("source cache: connector requested replay for scope %q but no previous sync is available", replay.GetScopeKey())
		}
		page.replayed = true
		if !replay.GetOverlay() && rowCount > 0 {
			// The contract says a 304-style replay page is empty, but rows
			// arriving here are more data, not less — upsert them on top of
			// the replayed base (overlay semantics) rather than failing the
			// sync. Kept lenient while the model is proven against real
			// providers.
			ctxzap.Extract(ctx).Warn("source cache: non-overlay replay returned rows; treating them as an overlay",
				zap.String("scope_key", page.scopeKey),
				zap.Int("rows", rowCount),
			)
		}
		// Advisory check: a well-behaved connector only replays a scope
		// whose validator came from this sync's lookup, so a missing
		// previous manifest entry is suspicious — but not by itself data
		// loss (the stamped rows may still exist, e.g. a partially carried
		// file). The hard error below is reserved for a replay that
		// produces nothing.
		_, entryFound, err := s.sourceCache.prev.LookupSourceCacheEntry(ctx, kind, page.scopeKey)
		if err != nil {
			return ctx, nil, fmt.Errorf("source cache: error reading previous manifest for scope %q: %w", page.scopeKey, err)
		}
		if !entryFound {
			ctxzap.Extract(ctx).Warn("source cache: replay requested for scope with no previous manifest entry",
				zap.String("scope_key", page.scopeKey))
		}
		replayStart := time.Now()
		res, err := s.sourceCache.current.ReplaySourceCache(ctx, s.sourceCache.prevReader, kind, page.scopeKey)
		s.state.AddStepDuration("source_cache_replay", time.Since(replayStart))
		if err != nil {
			return ctx, nil, fmt.Errorf("source cache: replay for scope %q failed: %w", page.scopeKey, err)
		}
		// Stats are stashed on the page and recorded in
		// finishSourceCachePage so a page re-run after failing between
		// replay and finish doesn't double-count.
		page.replayRows = res.Rows
		page.overlayRows = rowCount
		if replay.GetOverlay() || page.etag == "" {
			// This replay round can span pages: the validator arrives on
			// a LATER page's scope annotation (delta rounds), and that
			// page must count toward ScopesReplayed's scope, not as a
			// freshly stamped one — see finishSourceCachePage.
			s.sourceCache.markScopeReplayed(kind, page.scopeKey)
		}
		if res.Rows == 0 && !entryFound {
			// The connector skipped row generation expecting a base that
			// does not exist anywhere in the previous file — this sync
			// would silently drop the scope's rows.
			return ctx, nil, fmt.Errorf("source cache: replay for scope %q found no previous rows and no manifest entry; the connector replayed a scope it never looked up", page.scopeKey)
		}
		// Empty-vs-dropped discrimination via the previous file's scope
		// index: a legitimately empty scope has NO index entries
		// (StaleSkipped == 0, fine to proceed); a scope whose index says
		// rows existed but none carried a matching value stamp means the
		// rows were rewritten without index cleanup — proceeding would
		// silently drop the scope's contents from this sync.
		if res.Rows == 0 && res.StaleSkipped > 0 {
			return ctx, nil, fmt.Errorf(
				"source cache: replay for scope %q copied no rows but the previous file's scope index holds %d entries "+
					"whose rows no longer carry this scope's stamp; refusing to silently drop the scope",
				page.scopeKey, res.StaleSkipped)
		}
		if res.StaleSkipped > 0 {
			// Partial staleness: some rows copied, some index entries were
			// dead. Not data loss by itself (the live rows carried their
			// true stamp), but a signal a writer skipped index cleanup.
			ctxzap.Extract(ctx).Warn("source cache: replay skipped stale scope-index entries",
				zap.String("scope_key", page.scopeKey),
				zap.Int64("rows", res.Rows),
				zap.Int64("stale_skipped", res.StaleSkipped),
			)
		}
		// Side effects for replayed rows are STORE-DERIVED, not armed
		// here — the engine maintains the needs_expansion index and the
		// external-match existence bit for replayed rows exactly as for
		// fresh ones, and the ingestion invariants read the store at
		// their consuming seams (expansion: the PendingExpansion probe at
		// SyncGrantExpansionOp; external match: the fact probe at
		// SyncExternalResourcesOp; exclusion groups: the post-collection
		// keyspace validation). See
		// docs/tasks/source-cache-ingestion-invariants.md. The one
		// replay-carried side effect is child scheduling, which cannot be
		// derived after the fact (a zero-row child listing leaves no
		// store evidence), so replay carries the parents' child types:
		if kind == sourcecache.RowKindResources {
			for _, parent := range res.ChildResources {
				s.pushChildResourceActions(ctx, parent.ChildTypeIDs, parent.ResourceTypeID, parent.ResourceID)
			}
		}
		ctxzap.Extract(ctx).Debug("source cache replayed scope",
			zap.String("row_kind", string(kind)),
			zap.String("scope_key", page.scopeKey),
			zap.Int64("rows", res.Rows),
			zap.Int("deleted_ids", len(page.deletedIDs)),
			zap.Int("deleted_principal_ids", len(page.deletedPrincipalIDs)),
		)
		if s.testSourceCacheHaltHook != nil {
			if err := s.testSourceCacheHaltHook("replay-copied", page.scopeKey); err != nil {
				return ctx, nil, err
			}
		}
	}

	return sourcecache.WithScope(ctx, page.scopeKey), page, nil
}

// finishSourceCachePage runs after the page's rows committed: applies
// delta tombstones and, when the page carried a validator, writes the
// current sync's manifest entry. The entry write is last so a failed page
// can never leave a phantom hit for the next sync.
func (s *syncer) finishSourceCachePage(ctx context.Context, page *sourceCachePage) error {
	if page == nil {
		return nil
	}
	if s.testSourceCacheHaltHook != nil {
		if err := s.testSourceCacheHaltHook("rows-committed", page.scopeKey); err != nil {
			return err
		}
	}
	var rowsDeleted int64
	if len(page.deletedIDs)+len(page.deletedPrincipalIDs) > 0 {
		tombstoneStart := time.Now()
		defer func() {
			s.state.AddStepDuration("source_cache_tombstones", time.Since(tombstoneStart))
		}()
	}
	if len(page.deletedIDs) > 0 {
		if page.kind == sourcecache.RowKindGrants {
			// Grant-id tombstones resolve within the scope's own rows so
			// connector-custom grant-id shapes (unreachable by the global
			// bounded delete) work, and the cost stays bounded by the
			// scope's size.
			deleted, err := s.sourceCache.current.DeleteSourceCacheGrantsByIDInScope(ctx, page.scopeKey, page.deletedIDs)
			if err != nil {
				return fmt.Errorf("source cache: error applying grant deletions for scope %q: %w", page.scopeKey, err)
			}
			rowsDeleted += deleted
			ctxzap.Extract(ctx).Debug("source cache applied grant-id deletions",
				zap.String("scope_key", page.scopeKey),
				zap.Int("tombstones", len(page.deletedIDs)),
				zap.Int64("rows_deleted", deleted),
			)
		} else if err := s.sourceCache.current.DeleteSourceCacheRows(ctx, page.kind, page.deletedIDs); err != nil {
			return fmt.Errorf("source cache: error applying deletions for scope %q: %w", page.scopeKey, err)
		}
	}
	if len(page.deletedPrincipalIDs) > 0 {
		deleted, err := s.sourceCache.current.DeleteSourceCacheRowsInScope(ctx, page.kind, page.scopeKey, page.deletedPrincipalIDs)
		if err != nil {
			return fmt.Errorf("source cache: error applying scoped deletions for scope %q: %w", page.scopeKey, err)
		}
		rowsDeleted += deleted
		ctxzap.Extract(ctx).Debug("source cache applied scoped deletions",
			zap.String("row_kind", string(page.kind)),
			zap.String("scope_key", page.scopeKey),
			zap.Int("tombstones", len(page.deletedPrincipalIDs)),
			zap.Int64("rows_deleted", deleted),
		)
	}
	if s.testSourceCacheHaltHook != nil {
		if err := s.testSourceCacheHaltHook("tombstones-applied", page.scopeKey); err != nil {
			return err
		}
	}
	if page.etag != "" {
		if err := s.sourceCache.current.PutSourceCacheEntry(ctx, page.kind, page.scopeKey, page.etag); err != nil {
			return fmt.Errorf("source cache: error writing manifest entry for scope %q: %w", page.scopeKey, err)
		}
		if s.testSourceCacheHaltHook != nil {
			if err := s.testSourceCacheHaltHook("manifest-written", page.scopeKey); err != nil {
				return err
			}
		}
	}

	stats := SourceCacheStats{
		TombstoneIDs:        int64(len(page.deletedIDs)),
		TombstonePrincipals: int64(len(page.deletedPrincipalIDs)),
		RowsDeleted:         rowsDeleted,
	}
	if page.replayed {
		// Recorded here rather than in beginSourceCachePage so a page
		// re-run after failing between replay and finish doesn't
		// double-count the scope.
		stats.ScopesReplayed = 1
		stats.RowsReplayed = map[string]int64{string(page.kind): page.replayRows}
		stats.OverlayRows = int64(page.overlayRows)
	}
	if page.etag != "" && !page.replayed && !s.sourceCache.scopeReplayedThisSync(page.kind, page.scopeKey) {
		// A cold/fresh page that persisted a validator: the denominator
		// of the replay hit ratio. The replayed-scope check keeps a
		// multi-page overlay round — whose new validator legally arrives
		// on the final page, without a replay annotation — from booking
		// its own hit as a miss too.
		stats.ScopesStamped = 1
	}
	if stats.TombstoneIDs > 0 || stats.TombstonePrincipals > 0 || stats.RowsDeleted > 0 || stats.ScopesStamped > 0 || page.replayed {
		s.state.AddSourceCacheStats(stats)
	}
	return nil
}
