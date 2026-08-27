package c1zsanitize

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// recordSink is where sanitizeSync's copy loops write the four record
// families. It is exactly the method subset of connectorstore.Writer those
// loops use, so a sqlite destination satisfies it as-is (upserting Put*
// path); a pebble destination substitutes bulkImportSink so the same
// transform loops feed the engine's bulk-import fast path instead.
type recordSink interface {
	PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error
	PutResources(ctx context.Context, resources ...*v2.Resource) error
	PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error
	PutGrants(ctx context.Context, grants ...*v2.Grant) error
}

// bulkImportSink adapts a pebble BulkSyncImport to recordSink. The bulk
// contract (fresh sync, nothing else writes until Finish) holds on the
// sanitize path by construction: StartNewSync marked the destination sync
// fresh, resumable+pebble is rejected up front so every checkpoint call is
// a no-op, and assets are copied only after finish() has ingested.
//
// Semantics vs. the Put* path: Put* upserts, the bulk path does not. A
// source sync carrying two records with the same sanitized external id now
// fails the import (resource types/resources/entitlements) or folds with a
// warning (grants) instead of silently last-write-wins. A valid c1z has
// unique external ids per sync, so this only surfaces on corrupt input.
//
// Grants flow through a single shard: the sanitizer's grant loop is
// sequential, so shard fan-out would add nothing.
type bulkImportSink struct {
	eng    *pebble.Engine
	bi     *pebble.BulkSyncImport
	shard  *pebble.BulkGrantShard
	syncID string

	// finished gates abort(): after a successful finish() there is nothing
	// to tear down, while any earlier exit (including a failed Finish) must
	// abort to discard staged spill files and the partial ingest.
	finished bool
}

// startBulkImportSink opens a bulk import on the destination's current
// fresh sync. tmpDir stages spill files ("" = system temp dir).
func startBulkImportSink(ctx context.Context, eng *pebble.Engine, syncID string, tmpDir string) (*bulkImportSink, error) {
	bi, err := eng.StartBulkSyncImport(ctx, syncID, tmpDir)
	if err != nil {
		return nil, fmt.Errorf("start bulk import: %w", err)
	}
	shard, err := bi.NewGrantShard()
	if err != nil {
		bi.Abort()
		return nil, fmt.Errorf("open grant shard: %w", err)
	}
	return &bulkImportSink{eng: eng, bi: bi, shard: shard, syncID: syncID}, nil
}

func (s *bulkImportSink) PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error {
	// copyResourceTypes writes the full set once, sorted by output id,
	// which is the sorted-by-external-id arrival AddResourceTypes requires.
	return s.bi.AddResourceTypes(ctx, resourceTypes...)
}

func (s *bulkImportSink) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	return s.bi.AddResources(ctx, resources...)
}

func (s *bulkImportSink) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	return s.bi.AddEntitlements(ctx, entitlements...)
}

func (s *bulkImportSink) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.shard.AddGrants(ctx, grants...)
}

// finish seals the grant shard and ingests the import. After it returns
// nil the destination sync holds every record the sink received and the
// writer path (PutAsset, EndSync) may be used again.
func (s *bulkImportSink) finish(ctx context.Context) error {
	s.shard.Close()
	if err := s.bi.Finish(ctx); err != nil {
		return err
	}
	s.finished = true
	return nil
}

// abort discards the import unless finish succeeded. Deferred by
// sanitizeSync so any error exit between start and finish tears down
// staged spill files and the partial ingest.
func (s *bulkImportSink) abort() {
	if s.finished {
		return
	}
	s.bi.Abort()
}

// stashStats hands the import's record counts (plus the asset count, which
// rides outside the import) to the engine as the sync's stats sidecar, so
// EndSync persists stats directly instead of re-scanning the freshly
// ingested keyspaces.
func (s *bulkImportSink) stashStats(assetCount int64) {
	rec := s.bi.ComputedStats()
	rec.SetAssets(assetCount)
	s.eng.StashComputedSyncStats(s.syncID, rec)
}
