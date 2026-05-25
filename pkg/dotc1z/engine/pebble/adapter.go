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
	if err := a.engine.SetCurrentSync(syncID); err != nil {
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
	a.current = syncRunState{}
	return nil
}

// === writes ===

// PutGrants writes a batch of grants. Each grant is translated v2→v3
// and stored via the engine's PutGrantRecord (which handles index
// maintenance). Atomicity: each grant is its own batch — there is no
// cross-grant transaction.
func (a *Adapter) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	syncID := a.currentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	for _, g := range grants {
		if g == nil {
			continue
		}
		rec := V2GrantToV3(syncID, g)
		if rec == nil {
			continue
		}
		// Stamp discovered_at if the caller didn't.
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(timestamppb.Now())
		}
		if err := a.engine.PutGrantRecord(ctx, rec); err != nil {
			return fmt.Errorf("PutGrants: %w", err)
		}
	}
	return nil
}

// PutResourceTypes writes a batch of resource types.
func (a *Adapter) PutResourceTypes(ctx context.Context, rts ...*v2.ResourceType) error {
	syncID := a.currentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	for _, rt := range rts {
		if rt == nil {
			continue
		}
		rec := V2ResourceTypeToV3(syncID, rt)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(timestamppb.Now())
		}
		if err := a.engine.PutResourceTypeRecord(ctx, rec); err != nil {
			return fmt.Errorf("PutResourceTypes: %w", err)
		}
	}
	return nil
}

// PutResources writes a batch of resources.
func (a *Adapter) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	syncID := a.currentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	for _, r := range resources {
		if r == nil {
			continue
		}
		rec := V2ResourceToV3(syncID, r)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(timestamppb.Now())
		}
		if err := a.engine.PutResourceRecord(ctx, rec); err != nil {
			return fmt.Errorf("PutResources: %w", err)
		}
	}
	return nil
}

// PutEntitlements writes a batch of entitlements.
func (a *Adapter) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	syncID := a.currentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	for _, e := range entitlements {
		if e == nil {
			continue
		}
		rec := V2EntitlementToV3(syncID, e)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(timestamppb.Now())
		}
		if err := a.engine.PutEntitlementRecord(ctx, rec); err != nil {
			return fmt.Errorf("PutEntitlements: %w", err)
		}
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
		return "", nil, err
	}
	return rec.GetContentType(), &bytesReader{b: rec.GetData()}, nil
}

// === read service surface ===
//
// ListGrants / ListResources / etc. The connectorstore.Reader
// interface embeds these as gRPC ServiceServer interfaces; the
// adapter implements the most-called paths directly and leaves the
// rest to the embedded Unimplemented* stubs.

// ListGrants returns all grants on the active sync, optionally
// filtered by Resource (= principal) when req.Resource is set.
func (a *Adapter) ListGrants(ctx context.Context, req *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	syncID := a.resolveActiveSync(req.GetActiveSyncId())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	out := make([]*v2.Grant, 0)
	var iter func(yield func(*v3.GrantRecord) bool) error
	if r := req.GetResource(); r != nil && r.GetId() != nil {
		// Filter by principal.
		iter = func(yield func(*v3.GrantRecord) bool) error {
			return a.engine.IterateGrantsByPrincipal(ctx, syncID,
				r.GetId().GetResourceType(), r.GetId().GetResource(), yield)
		}
	} else {
		iter = func(yield func(*v3.GrantRecord) bool) error {
			return a.engine.IterateGrantsBySync(ctx, syncID, yield)
		}
	}
	if err := iter(func(rec *v3.GrantRecord) bool {
		out = append(out, V3GrantToV2(rec))
		return true
	}); err != nil {
		return nil, err
	}
	return v2.GrantsServiceListGrantsResponse_builder{List: out}.Build(), nil
}

// ListResources returns all resources on the active sync,
// optionally filtered by parent resource id.
func (a *Adapter) ListResources(ctx context.Context, req *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	syncID := a.resolveActiveSync(req.GetActiveSyncId())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	out := make([]*v2.Resource, 0)
	var iter func(yield func(*v3.ResourceRecord) bool) error
	if p := req.GetParentResourceId(); p != nil && p.GetResource() != "" {
		iter = func(yield func(*v3.ResourceRecord) bool) error {
			return a.engine.IterateResourcesByParent(ctx, syncID,
				p.GetResourceType(), p.GetResource(), yield)
		}
	} else {
		iter = func(yield func(*v3.ResourceRecord) bool) error {
			return a.engine.IterateResourcesBySync(ctx, syncID, yield)
		}
	}
	if err := iter(func(rec *v3.ResourceRecord) bool {
		// Only include resources of the requested type if specified.
		if rtID := req.GetResourceTypeId(); rtID != "" && rec.GetResourceTypeId() != rtID {
			return true
		}
		out = append(out, V3ResourceToV2(rec))
		return true
	}); err != nil {
		return nil, err
	}
	return v2.ResourcesServiceListResourcesResponse_builder{List: out}.Build(), nil
}

// ListResourceTypes returns all resource_types on the active sync.
func (a *Adapter) ListResourceTypes(ctx context.Context, req *v2.ResourceTypesServiceListResourceTypesRequest) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	syncID := a.resolveActiveSync(req.GetActiveSyncId())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	out := make([]*v2.ResourceType, 0)
	if err := a.engine.IterateResourceTypesBySync(ctx, syncID, func(rec *v3.ResourceTypeRecord) bool {
		out = append(out, V3ResourceTypeToV2(rec))
		return true
	}); err != nil {
		return nil, err
	}
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{List: out}.Build(), nil
}

// ListEntitlements returns all entitlements on the active sync,
// optionally filtered by Resource (resource_type_id, resource_id).
func (a *Adapter) ListEntitlements(ctx context.Context, req *v2.EntitlementsServiceListEntitlementsRequest) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	syncID := a.resolveActiveSync(req.GetActiveSyncId())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	out := make([]*v2.Entitlement, 0)
	var iter func(yield func(*v3.EntitlementRecord) bool) error
	if r := req.GetResource(); r != nil && r.GetId() != nil {
		iter = func(yield func(*v3.EntitlementRecord) bool) error {
			return a.engine.IterateEntitlementsByResource(ctx, syncID,
				r.GetId().GetResourceType(), r.GetId().GetResource(), yield)
		}
	} else {
		iter = func(yield func(*v3.EntitlementRecord) bool) error {
			return a.engine.IterateEntitlementsBySync(ctx, syncID, yield)
		}
	}
	if err := iter(func(rec *v3.EntitlementRecord) bool {
		out = append(out, V3EntitlementToV2(rec))
		return true
	}); err != nil {
		return nil, err
	}
	return v2.EntitlementsServiceListEntitlementsResponse_builder{List: out}.Build(), nil
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
		return nil, err
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
