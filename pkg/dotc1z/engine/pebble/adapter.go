package pebble

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Adapter wraps an *Engine and implements connectorstore.Writer
// (which embeds connectorstore.Reader) — the surface C1 + the syncer
// call against the v3 Pebble engine. Translates v2 wire types ↔ v3
// record types via translate_v2.go and routes Put/Get/List into the
// engine's per-record-type methods.
//
// The adapter implements the common connectorstore writer and reader
// paths directly. gRPC methods outside that surface fall through to
// the embedded UnimplementedXxxServer stubs.
//
// Adapter is goroutine-safe modulo Close — concurrent reads + writes
// are fine, but caller must serialize Close against other calls.
type Adapter struct {
	engine *Engine

	// embedded Unimplemented stubs for the gRPC service surfaces we
	// implement partially. Each implemented method overrides the stub.
	v2.UnimplementedResourceTypesServiceServer
	reader_v2.UnimplementedResourceTypesReaderServiceServer
	v2.UnimplementedResourcesServiceServer
	reader_v2.UnimplementedResourcesReaderServiceServer
	v2.UnimplementedEntitlementsServiceServer
	reader_v2.UnimplementedEntitlementsReaderServiceServer
	v2.UnimplementedGrantsServiceServer
	reader_v2.UnimplementedGrantsReaderServiceServer
	reader_v2.UnimplementedSyncsReaderServiceServer

	mu      sync.Mutex
	current syncRunState
}

// syncRunState tracks the currently-open sync. The connectorstore
// interface treats sync IDs as opaque strings; we store the full
// SyncRunRecord shape so EndSync / CheckpointSync can update the
// sync_runs record.
type syncRunState struct {
	syncID    string
	syncType  v3.SyncType
	parentID  string
	step      string
	startedAt time.Time
}

// NewAdapter wraps an Engine. The engine must remain alive for the
// lifetime of the adapter.
func NewAdapter(e *Engine) *Adapter {
	return &Adapter{engine: e}
}

// Compile-time checks for the full Writer interface and the optional
// connectorstore capabilities that SQLite's *C1File also exposes.
var (
	_ connectorstore.Writer                      = (*Adapter)(nil)
	_ connectorstore.LatestFinishedSyncIDFetcher = (*Adapter)(nil)
	_ connectorstore.DBSizeProvider              = (*Adapter)(nil)
)

// === sync lifecycle ===

// StartNewSync creates a new sync_run record. Returns the new sync_id.
func (a *Adapter) StartNewSync(ctx context.Context, syncType connectorstore.SyncType, parentSyncID string) (string, error) {
	syncID := ksuid.New().String()
	a.mu.Lock()
	defer a.mu.Unlock()
	// MarkFreshSync flips the engine into the perf-fast write path:
	// pebble.NoSync per commit, skip read-before-write index cleanup.
	// EndSync calls EndFreshSync to flush + fsync once at the end.
	if err := a.engine.MarkFreshSync(syncID); err != nil {
		return "", err
	}
	a.current = syncRunState{
		syncID:    syncID,
		syncType:  v2SyncTypeToV3(syncType),
		parentID:  parentSyncID,
		startedAt: time.Now(),
	}
	rec := v3.SyncRunRecord_builder{
		SyncId:       syncID,
		Type:         a.current.syncType,
		ParentSyncId: parentSyncID,
		StartedAt:    timestamppb.New(a.current.startedAt),
	}.Build()
	if err := a.engine.PutSyncRunRecord(ctx, rec); err != nil {
		return "", err
	}
	return syncID, nil
}

// ResumeSync attaches to an existing sync_run by id. Returns the
// caller-provided id if it matches an existing record.
func (a *Adapter) ResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, error) {
	if syncID == "" {
		return "", errors.New("adapter.ResumeSync: empty syncID")
	}
	existing, err := a.engine.GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return "", fmt.Errorf("ResumeSync: lookup: %w", err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.engine.SetCurrentSync(syncID); err != nil {
		return "", err
	}
	a.current = syncRunState{
		syncID:    syncID,
		syncType:  existing.GetType(),
		parentID:  existing.GetParentSyncId(),
		startedAt: existing.GetStartedAt().AsTime(),
	}
	return syncID, nil
}

// StartOrResumeSync resumes if syncID-like state exists for the given
// type, else starts a new sync. Returns (id, started_new, err).
func (a *Adapter) StartOrResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, bool, error) {
	if syncID != "" {
		if _, err := a.engine.GetSyncRunRecord(ctx, syncID); err == nil {
			id, err := a.ResumeSync(ctx, syncType, syncID)
			return id, false, err
		}
	}
	id, err := a.StartNewSync(ctx, syncType, "")
	return id, true, err
}

// SetCurrentSync rebinds the engine's current sync without creating a
// new SyncRunRecord. Used by callers that previously called
// StartNewSync/ResumeSync.
func (a *Adapter) SetCurrentSync(ctx context.Context, syncID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.engine.SetCurrentSync(syncID); err != nil {
		return err
	}
	a.current.syncID = syncID
	return nil
}

// CurrentSyncStep returns the current sync's step string, or "".
func (a *Adapter) CurrentSyncStep(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current.step, nil
}

// CheckpointSync persists a step token to the open sync's record.
func (a *Adapter) CheckpointSync(ctx context.Context, syncToken string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current.syncID == "" {
		return errors.New("CheckpointSync: no open sync")
	}
	a.current.step = syncToken
	existing, err := a.engine.GetSyncRunRecord(ctx, a.current.syncID)
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
	return a.engine.PutSyncRunRecord(ctx, updated)
}

// EndSync stamps the open sync_run's ended_at and detaches it. After
// EndSync, the adapter has no current sync; SetCurrentSync or
// StartNewSync are required for further writes.
func (a *Adapter) EndSync(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.current.syncID == "" {
		return errors.New("EndSync: no open sync")
	}
	existing, err := a.engine.GetSyncRunRecord(ctx, a.current.syncID)
	if err != nil {
		return err
	}
	updated := v3.SyncRunRecord_builder{
		SyncId:       existing.GetSyncId(),
		Type:         existing.GetType(),
		ParentSyncId: existing.GetParentSyncId(),
		StartedAt:    existing.GetStartedAt(),
		EndedAt:      timestamppb.Now(),
		SyncToken:    existing.GetSyncToken(),
	}.Build()
	if err := a.engine.PutSyncRunRecord(ctx, updated); err != nil {
		return err
	}
	// Populate the stats sidecar BEFORE the durability flush. Stats
	// is engine-meta keyspace; the EndFreshSync flush below covers
	// the WAL fsync for both the sync_run record and the stats key.
	// Failures here are non-fatal — Stats() falls back to legacy
	// iteration on a missing sidecar, and the on-Open migration
	// framework will backfill next time the file opens. We log a
	// warning so the failure is visible in production telemetry but
	// don't fail the sync end on stats-sidecar trouble.
	if err := a.engine.PersistSyncStats(ctx, existing.GetSyncId()); err != nil {
		ctxzap.Extract(ctx).Warn("pebble: persist sync stats sidecar failed; Stats() will fall back to O(N) iteration until the next Open backfills it",
			zap.String("sync_id", existing.GetSyncId()),
			zap.Error(err),
		)
	}
	// Single flush + WAL fsync at sync end. This is the durability
	// boundary — counterpart to MarkFreshSync at StartNewSync. After
	// this returns, all writes from the sync are on disk.
	if err := a.engine.EndFreshSync(ctx); err != nil {
		return err
	}
	a.current = syncRunState{}
	return nil
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
func (a *Adapter) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	syncID := a.currentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	records := translateGrants(syncID, grants)
	if err := a.engine.PutGrantRecords(ctx, records...); err != nil {
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
func (a *Adapter) UnsafePutUniqueGrants(ctx context.Context, grants ...*v2.Grant) error {
	syncID := a.currentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	records := translateGrants(syncID, grants)
	if err := a.engine.UnsafePutUniqueGrantRecords(ctx, records...); err != nil {
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
		// Serial path: one arena, one pass.
		arena := newGrantTranslateArena(len(grants))
		records := make([]*v3.GrantRecord, 0, len(grants))
		for _, g := range grants {
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
			records = append(records, rec)
		}
		return records
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

// PutResourceTypes writes a batch of resource types in a single
// Pebble batch.
func (a *Adapter) PutResourceTypes(ctx context.Context, rts ...*v2.ResourceType) error {
	syncID := a.currentSyncID()
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
	if err := a.engine.PutResourceTypeRecords(ctx, records...); err != nil {
		return fmt.Errorf("PutResourceTypes: %w", err)
	}
	return nil
}

// PutResources writes a batch of resources in a single Pebble batch.
func (a *Adapter) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	syncID := a.currentSyncID()
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
	if err := a.engine.PutResourceRecords(ctx, records...); err != nil {
		return fmt.Errorf("PutResources: %w", err)
	}
	return nil
}

// PutEntitlements writes a batch of entitlements in a single Pebble batch.
func (a *Adapter) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	syncID := a.currentSyncID()
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
	if err := a.engine.PutEntitlementRecords(ctx, records...); err != nil {
		return fmt.Errorf("PutEntitlements: %w", err)
	}
	return nil
}

// DeleteGrant removes a grant by id.
func (a *Adapter) DeleteGrant(ctx context.Context, grantID string) error {
	syncID := a.currentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	return a.engine.DeleteGrantRecord(ctx, syncID, grantID)
}

// PutAsset writes a single asset row. assetRef carries the
// (resource_type, resource_id) pair we use as the external_id —
// joined with a "/" separator since the engine's AssetRecord PK is
// (sync_id, external_id).
func (a *Adapter) PutAsset(ctx context.Context, assetRef *v2.AssetRef, contentType string, data []byte) error {
	syncID := a.currentSyncID()
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
	return a.engine.PutAssetRecord(ctx, rec)
}

// Cleanup on the bare Adapter is a no-op. The real Pebble
// retention policy lives on pkg/dotc1z's Pebble store wrapper
// (pebble_store.go) — it needs access to caller-supplied options
// (SyncLimit, SkipCleanup) that the engine itself doesn't track,
// plus the dirty-flag plumbing on the wrapper.
//
// Callers that open through dotc1z.NewStore(..., WithEngine(EnginePebble))
// get the real Cleanup; callers that build a bare Adapter (unit
// tests, embedding) silently get retention=disabled. The method is
// kept on the Adapter only to satisfy the connectorstore.Writer
// interface contract regardless of how the adapter was constructed.
func (a *Adapter) Cleanup(ctx context.Context) error { return nil }

// Close shuts down the engine. After Close, all methods return errors.
func (a *Adapter) Close(ctx context.Context) error {
	return a.engine.Close()
}

// === GetAsset ===

// GetAsset returns the (content_type, data-reader) for the given
// asset. The returned reader is backed by a bytes.Reader over the
// fully-materialized blob.
func (a *Adapter) GetAsset(ctx context.Context, req *v2.AssetServiceGetAssetRequest) (string, io.Reader, error) {
	syncID := a.currentSyncID()
	if syncID == "" {
		return "", nil, ErrNoCurrentSync
	}
	if req == nil || req.GetAsset() == nil {
		return "", nil, errors.New("GetAsset: nil request")
	}
	rec, err := a.engine.GetAssetRecord(ctx, syncID, req.GetAsset().GetId())
	if err != nil {
		return "", nil, adaptNotFound(err)
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
//     grant — when set; uses the by_entitlement_resource index. This
//     matches SQLite's `listGrantsGeneric` which filters on
//     grants.resource_id / resource_type_id (the entitlement's
//     resource columns). Callers who want to filter by principal
//     should use ListGrantsForPrincipal instead.
func (a *Adapter) ListGrants(ctx context.Context, req *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	syncID, err := a.resolveActiveSync(ctx, req.GetActiveSyncId(), req.GetAnnotations())
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
		records, nextCursor, err = a.engine.PaginateGrantsByEntitlementResource(ctx, syncID,
			r.GetId().GetResourceType(), r.GetId().GetResource(), cursor, limit)
	} else {
		records, nextCursor, err = a.engine.PaginateGrantsBySync(ctx, syncID, cursor, limit)
	}
	if err != nil {
		return nil, err
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
func (a *Adapter) ListResources(ctx context.Context, req *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	syncID, err := a.resolveActiveSync(ctx, req.GetActiveSyncId(), req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return nil, err
	}
	limit := clampPageSize(req.GetPageSize())
	cursor := req.GetPageToken()
	rtFilter := req.GetResourceTypeId()
	parent := req.GetParentResourceId()
	useParent := parent != nil && parent.GetResource() != ""

	// Trait filter (RFC §B2): pre-resolve the resource_type set so
	// the post-filter only admits resources whose RT carries the
	// trait. resource_types is small (O(10-100)) so the up-front
	// scan is negligible compared to the resources scan we save.
	var traitRTs map[string]struct{}
	if t := req.GetTrait(); t != v2.ResourceType_TRAIT_UNSPECIFIED {
		ids, err := a.resourceTypeIDsWithTrait(ctx, syncID, t)
		if err != nil {
			return nil, err
		}
		if len(ids) == 0 {
			return v2.ResourcesServiceListResourcesResponse_builder{}.Build(), nil
		}
		traitRTs = make(map[string]struct{}, len(ids))
		for _, id := range ids {
			traitRTs[id] = struct{}{}
		}
		// If an explicit RT filter is set, intersect.
		if rtFilter != "" {
			if _, ok := traitRTs[rtFilter]; !ok {
				return v2.ResourcesServiceListResourcesResponse_builder{}.Build(), nil
			}
		}
	}

	// cursorFor returns the engine cursor for rec under the path
	// this call is iterating — primary keyspace for the unfiltered
	// case, by_parent index for the parent-scoped case. We need
	// per-record cursors because a post-filter break at len(out) ==
	// limit may leave matching records unconsumed in the engine
	// page; emitting the engine's end-of-page cursor would skip
	// them on the next call.
	cursorFor := func(rec *v3.ResourceRecord) string {
		if useParent {
			return encodeCursor(encodeResourceByParentIndexKey(
				idBytes,
				parent.GetResourceType(), parent.GetResource(),
				rec.GetResourceTypeId(), rec.GetResourceId(),
			))
		}
		return encodeCursor(encodeResourceKey(idBytes, rec.GetResourceTypeId(), rec.GetResourceId()))
	}

	out := make([]*v2.Resource, 0, limit)
	var nextCursor string
	for len(out) < limit {
		pageLimit := limit - len(out)
		// Over-fetch a little when post-filtering so a sparse hit rate
		// doesn't force a tail of extra round-trips. 4x is the cap; if
		// rtFilter is empty we skip the over-fetch entirely.
		fetchLimit := pageLimit
		if rtFilter != "" || traitRTs != nil {
			fetchLimit = pageLimit * 4
			if fetchLimit > MaxPageSize {
				fetchLimit = MaxPageSize
			}
		}
		var records []*v3.ResourceRecord
		var err error
		if useParent {
			records, nextCursor, err = a.engine.PaginateResourcesByParent(ctx, syncID,
				parent.GetResourceType(), parent.GetResource(), cursor, fetchLimit)
		} else {
			records, nextCursor, err = a.engine.PaginateResourcesBySync(ctx, syncID, cursor, fetchLimit)
		}
		if err != nil {
			return nil, err
		}
		brokeEarly := false
		for _, rec := range records {
			if rtFilter != "" && rec.GetResourceTypeId() != rtFilter {
				continue
			}
			if traitRTs != nil {
				if _, ok := traitRTs[rec.GetResourceTypeId()]; !ok {
					continue
				}
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
func (a *Adapter) ListResourceTypes(ctx context.Context, req *v2.ResourceTypesServiceListResourceTypesRequest) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	syncID, err := a.resolveActiveSync(ctx, req.GetActiveSyncId(), req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	limit := clampPageSize(req.GetPageSize())
	records, nextCursor, err := a.engine.PaginateResourceTypesBySync(ctx, syncID, req.GetPageToken(), limit)
	if err != nil {
		return nil, err
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
func (a *Adapter) ListEntitlements(ctx context.Context, req *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	syncID, err := a.resolveActiveSync(ctx, req.GetActiveSyncId(), req.GetAnnotations())
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
		records, nextCursor, err = a.engine.PaginateEntitlementsByResource(ctx, syncID,
			r.GetId().GetResourceType(), r.GetId().GetResource(), cursor, limit)
	} else {
		records, nextCursor, err = a.engine.PaginateEntitlementsBySync(ctx, syncID, cursor, limit)
	}
	if err != nil {
		return nil, err
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
func (a *Adapter) ListStaticEntitlements(
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
func (a *Adapter) GetGrant(ctx context.Context, req *reader_v2.GrantsReaderServiceGetGrantRequest) (*reader_v2.GrantsReaderServiceGetGrantResponse, error) {
	syncID := a.currentSyncID()
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	rec, err := a.engine.GetGrantRecord(ctx, syncID, req.GetGrantId())
	if err != nil {
		return nil, adaptNotFound(err)
	}
	return reader_v2.GrantsReaderServiceGetGrantResponse_builder{
		Grant: V3GrantToV2(rec),
	}.Build(), nil
}

// LatestFinishedSyncID returns the most-recently-finished sync ID of
// the given type. Implements connectorstore.LatestFinishedSyncIDFetcher.
// Delegates to Engine.LatestFinishedSyncRecord; see that method for
// the predicate + tiebreaker contract.
func (a *Adapter) LatestFinishedSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error) {
	latest, err := a.engine.LatestFinishedSyncRecord(ctx, syncTypeFilterFromConnectorstore(syncType))
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

// CurrentDBSizeBytes returns the current uncompressed Pebble working-set size
// on disk. Implements connectorstore.DBSizeProvider for progress logging
// parity with the SQLite-backed *dotc1z.C1File.
func (a *Adapter) CurrentDBSizeBytes() (int64, error) {
	return a.engine.CurrentDBSizeBytes()
}

// Metadata describes the storage backing this adapter. The Pebble
// adapter always reports the v3 format; PayloadEncoding is set by
// the writer at envelope time and is not directly visible on the
// Adapter itself — pkg/dotc1z's Pebble store wrapper
// (pebble_store.go) overrides this method to fill PayloadEncoding
// from its configured value.
//
// Strings are inlined rather than referencing dotc1z constants
// because this subpackage is imported by dotc1z, so the reverse
// import would cycle. The values match dotc1z.EnginePebble.String()
// and dotc1z.C1ZFormatV3.String() — see connectorstore.StoreMetadata
// docs for the canonical value list.
func (a *Adapter) Metadata() connectorstore.StoreMetadata {
	return connectorstore.StoreMetadata{
		Engine: "pebble",
		Format: "v3",
	}
}

// === helpers ===

// currentSyncID returns the adapter's current sync id under the
// adapter's lock.
func (a *Adapter) currentSyncID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current.syncID
}

// CurrentSyncID returns the adapter's current sync id, or "" when no
// sync is active. Used by pkg/dotc1z's Pebble store to drive the
// retention policy at Cleanup.
func (a *Adapter) CurrentSyncID() string {
	return a.currentSyncID()
}

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
func (a *Adapter) resolveActiveSync(ctx context.Context, reqSyncID string, annos []*anypb.Any) (string, error) {
	if reqSyncID != "" {
		return reqSyncID, nil
	}
	return a.resolveActiveSyncForReader(ctx, annos)
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
