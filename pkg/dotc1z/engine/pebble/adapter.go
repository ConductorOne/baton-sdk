package pebble

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
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
	now := timestamppb.Now()

	// Parallel translation. Below the threshold, single-goroutine
	// translation avoids goroutine setup cost on small calls.
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
		if err := a.engine.PutGrantRecords(ctx, records...); err != nil {
			return fmt.Errorf("PutGrants: %w", err)
		}
		return nil
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
	if err := a.engine.PutGrantRecords(ctx, compact...); err != nil {
		return fmt.Errorf("PutGrants: %w", err)
	}
	return nil
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

// Cleanup is a no-op for the Pebble engine; callers historically used
// it on the SQLite engine to drop temp tables. Kept to satisfy the
// connectorstore.Writer interface.
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
//   - filter by req.Resource (= principal) when set; uses by_principal index
func (a *Adapter) ListGrants(ctx context.Context, req *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	syncID := a.resolveActiveSync(req.GetActiveSyncId())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	limit := clampPageSize(req.GetPageSize())
	cursor := req.GetPageToken()
	var records []*v3.GrantRecord
	var nextCursor string
	var err error
	if r := req.GetResource(); r != nil && r.GetId() != nil {
		records, nextCursor, err = a.engine.PaginateGrantsByPrincipal(ctx, syncID,
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
	syncID := a.resolveActiveSync(req.GetActiveSyncId())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	limit := clampPageSize(req.GetPageSize())
	cursor := req.GetPageToken()
	rtFilter := req.GetResourceTypeId()

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
		if p := req.GetParentResourceId(); p != nil && p.GetResource() != "" {
			records, nextCursor, err = a.engine.PaginateResourcesByParent(ctx, syncID,
				p.GetResourceType(), p.GetResource(), cursor, fetchLimit)
		} else {
			records, nextCursor, err = a.engine.PaginateResourcesBySync(ctx, syncID, cursor, fetchLimit)
		}
		if err != nil {
			return nil, err
		}
		for _, rec := range records {
			if rtFilter != "" && rec.GetResourceTypeId() != rtFilter {
				continue
			}
			out = append(out, V3ResourceToV2(rec))
			if len(out) == limit {
				break
			}
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
	syncID := a.resolveActiveSync(req.GetActiveSyncId())
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
	syncID := a.resolveActiveSync(req.GetActiveSyncId())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	limit := clampPageSize(req.GetPageSize())
	cursor := req.GetPageToken()
	var records []*v3.EntitlementRecord
	var nextCursor string
	var err error
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
func (a *Adapter) LatestFinishedSyncID(ctx context.Context, syncType connectorstore.SyncType) (string, error) {
	var latest *v3.SyncRunRecord
	if err := a.engine.IterateAllSyncRuns(ctx, func(rec *v3.SyncRunRecord) bool {
		if rec.GetEndedAt() == nil {
			return true
		}
		if syncType != connectorstore.SyncTypeAny &&
			v2SyncTypeToV3(syncType) != rec.GetType() {
			return true
		}
		if latest == nil || rec.GetEndedAt().AsTime().After(latest.GetEndedAt().AsTime()) {
			latest = rec
		}
		return true
	}); err != nil {
		return "", err
	}
	if latest == nil {
		return "", nil
	}
	return latest.GetSyncId(), nil
}

// CurrentDBSizeBytes returns the current uncompressed Pebble working-set size
// on disk. Implements connectorstore.DBSizeProvider for progress logging
// parity with the SQLite-backed *dotc1z.C1File.
func (a *Adapter) CurrentDBSizeBytes() (int64, error) {
	return a.engine.CurrentDBSizeBytes()
}

// === helpers ===

// currentSyncID returns the adapter's current sync id under the
// adapter's lock.
func (a *Adapter) currentSyncID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.current.syncID
}

// resolveActiveSync uses the request's ActiveSyncId override if set,
// else the adapter's current sync.
func (a *Adapter) resolveActiveSync(reqSyncID string) string {
	if reqSyncID != "" {
		return reqSyncID
	}
	return a.currentSyncID()
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
