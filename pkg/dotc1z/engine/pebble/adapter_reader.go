package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Reader gRPC service methods. The syncer (`pkg/sync/syncer.go`) and
// the grant expander (`pkg/sync/expand/expander.go`) hard-depend on
// these — without them the syncer can't run against the Pebble
// adapter.
//
// Each method translates the v2 wire shape to v3 record fetches via
// the engine, then returns a v2-shaped response (via the translation
// layer in translate_v2.go).

// GetEntitlement fetches a single entitlement by ID. Implements
// reader_v2.EntitlementsReaderServiceServer.
func (a *Adapter) GetEntitlement(ctx context.Context, req *reader_v2.EntitlementsReaderServiceGetEntitlementRequest) (*reader_v2.EntitlementsReaderServiceGetEntitlementResponse, error) {
	syncID := a.resolveActiveSyncForReader(req.GetAnnotations())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	rec, err := a.engine.GetEntitlementRecord(ctx, syncID, req.GetEntitlementId())
	err = adaptNotFound(err)
	if err != nil {
		return nil, err
	}
	return reader_v2.EntitlementsReaderServiceGetEntitlementResponse_builder{
		Entitlement: V3EntitlementToV2(rec),
	}.Build(), nil
}

// GetResource fetches a single resource by (resource_type_id,
// resource_id). Implements reader_v2.ResourcesReaderServiceServer.
func (a *Adapter) GetResource(ctx context.Context, req *reader_v2.ResourcesReaderServiceGetResourceRequest) (*reader_v2.ResourcesReaderServiceGetResourceResponse, error) {
	syncID := a.resolveActiveSyncForReader(req.GetAnnotations())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	rid := req.GetResourceId()
	if rid == nil {
		return nil, errors.New("GetResource: nil resource_id")
	}
	rec, err := a.engine.GetResourceRecord(ctx, syncID, rid.GetResourceType(), rid.GetResource())
	err = adaptNotFound(err)
	if err != nil {
		return nil, err
	}
	return reader_v2.ResourcesReaderServiceGetResourceResponse_builder{
		Resource: V3ResourceToV2(rec),
	}.Build(), nil
}

// GetResourceType fetches a single resource_type by ID. Implements
// reader_v2.ResourceTypesReaderServiceServer.
func (a *Adapter) GetResourceType(ctx context.Context, req *reader_v2.ResourceTypesReaderServiceGetResourceTypeRequest) (*reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse, error) {
	syncID := a.resolveActiveSyncForReader(req.GetAnnotations())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	rec, err := a.engine.GetResourceTypeRecord(ctx, syncID, req.GetResourceTypeId())
	err = adaptNotFound(err)
	if err != nil {
		return nil, err
	}
	return reader_v2.ResourceTypesReaderServiceGetResourceTypeResponse_builder{
		ResourceType: V3ResourceTypeToV2(rec),
	}.Build(), nil
}

// ListResourcesByIds returns all resources matching the supplied
// (resource_type_id, resource_id) pairs. Missing rows are silently
// omitted. Callers detect partial misses by length comparison.
func (a *Adapter) ListResourcesByIds(
	ctx context.Context,
	req *reader_v2.ResourcesReaderServiceListResourcesByIdsRequest,
) (*reader_v2.ResourcesReaderServiceListResourcesByIdsResponse, error) {
	syncID := a.resolveActiveSyncForReader(req.GetAnnotations())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	ids := req.GetResourceIds()
	out := make([]*v2.Resource, 0, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if id == nil {
			continue
		}
		rec, err := a.engine.GetResourceRecord(ctx, syncID, id.GetResourceType(), id.GetResource())
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, V3ResourceToV2(rec))
	}
	return reader_v2.ResourcesReaderServiceListResourcesByIdsResponse_builder{
		List: out,
	}.Build(), nil
}

// ListEntitlementsByIds returns entitlements for the requested
// external_ids. Missing rows are silently omitted.
func (a *Adapter) ListEntitlementsByIds(
	ctx context.Context,
	req *reader_v2.EntitlementsReaderServiceListEntitlementsByIdsRequest,
) (*reader_v2.EntitlementsReaderServiceListEntitlementsByIdsResponse, error) {
	syncID := a.resolveActiveSyncForReader(req.GetAnnotations())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	ids := req.GetEntitlementIds()
	out := make([]*v2.Entitlement, 0, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if id == "" {
			continue
		}
		rec, err := a.engine.GetEntitlementRecord(ctx, syncID, id)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, V3EntitlementToV2(rec))
	}
	return reader_v2.EntitlementsReaderServiceListEntitlementsByIdsResponse_builder{
		List: out,
	}.Build(), nil
}

// GetResourceTypes returns resource_types for the requested ids.
// Missing rows are silently omitted.
func (a *Adapter) GetResourceTypes(
	ctx context.Context,
	req *reader_v2.ResourceTypesReaderServiceGetResourceTypesRequest,
) (*reader_v2.ResourceTypesReaderServiceGetResourceTypesResponse, error) {
	syncID := a.resolveActiveSyncForReader(req.GetAnnotations())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	ids := req.GetResourceTypeIds()
	out := make([]*v2.ResourceType, 0, len(ids))
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if id == "" {
			continue
		}
		rec, err := a.engine.GetResourceTypeRecord(ctx, syncID, id)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			return nil, err
		}
		out = append(out, V3ResourceTypeToV2(rec))
	}
	return reader_v2.ResourceTypesReaderServiceGetResourceTypesResponse_builder{
		List: out,
	}.Build(), nil
}

// ListGrantsForEntitlement paginates grants on a specific
// entitlement, optionally narrowed by principal_id or
// principal_resource_type_ids. Implements
// reader_v2.GrantsReaderServiceServer.
func (a *Adapter) ListGrantsForEntitlement(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	syncID := a.resolveActiveSyncForReader(req.GetAnnotations())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	ent := req.GetEntitlement()
	if ent == nil || ent.GetId() == "" {
		return nil, errors.New("ListGrantsForEntitlement: missing entitlement id")
	}
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return nil, err
	}
	limit := clampPageSize(req.GetPageSize())
	cursor := req.GetPageToken()

	// Filters: principal_id (single principal) or
	// principal_resource_type_ids (filter by RT membership).
	principalID := req.GetPrincipalId()
	rtFilter := req.GetPrincipalResourceTypeIds()
	rtSet := make(map[string]struct{}, len(rtFilter))
	for _, rt := range rtFilter {
		rtSet[rt] = struct{}{}
	}

	// cursorFor returns the by_entitlement index key for rec —
	// needed because a post-filter break at len(out) == limit may
	// leave matching records unconsumed in the engine page, and
	// the engine's end-of-page cursor would skip them.
	cursorFor := func(rec *v3.GrantRecord) string {
		p := rec.GetPrincipal()
		return encodeCursor(encodeGrantByEntitlementIndexKey(
			idBytes,
			ent.GetId(),
			p.GetResourceTypeId(), p.GetResourceId(),
			rec.GetExternalId(),
		))
	}

	out := make([]*v2.Grant, 0, limit)
	var nextCursor string
	for len(out) < limit {
		pageLimit := limit - len(out)
		fetchLimit := pageLimit
		if principalID != nil || len(rtFilter) > 0 {
			fetchLimit = pageLimit * 4
			if fetchLimit > MaxPageSize {
				fetchLimit = MaxPageSize
			}
		}
		records, next, err := a.engine.PaginateGrantsByEntitlement(ctx, syncID,
			ent.GetId(), cursor, fetchLimit)
		if err != nil {
			return nil, err
		}
		nextCursor = next
		brokeEarly := false
		for _, rec := range records {
			if principalID != nil {
				p := rec.GetPrincipal()
				if p.GetResourceTypeId() != principalID.GetResourceType() ||
					p.GetResourceId() != principalID.GetResource() {
					continue
				}
			}
			if len(rtSet) > 0 {
				if _, ok := rtSet[rec.GetPrincipal().GetResourceTypeId()]; !ok {
					continue
				}
			}
			out = append(out, V3GrantToV2(rec))
			if len(out) == limit {
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
	return reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse_builder{
		List:          out,
		NextPageToken: nextCursor,
	}.Build(), nil
}

// ListGrantsForResourceType paginates grants whose principal is of
// the given resource_type_id, via idxGrantByPrincipalResourceType.
// The cursor is the index key.
//
// Implements reader_v2.GrantsReaderServiceServer.
func (a *Adapter) ListGrantsForResourceType(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForResourceTypeResponse, error) {
	syncID := a.resolveActiveSyncForReader(req.GetAnnotations())
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	rtFilter := req.GetResourceTypeId()
	if rtFilter == "" {
		return nil, errors.New("ListGrantsForResourceType: missing resource_type_id")
	}
	limit := clampPageSize(req.GetPageSize())
	cursor := req.GetPageToken()
	records, next, err := a.engine.PaginateGrantsByPrincipalResourceType(ctx, syncID, rtFilter, cursor, limit)
	if err != nil {
		return nil, err
	}
	out := make([]*v2.Grant, 0, len(records))
	for _, rec := range records {
		out = append(out, V3GrantToV2(rec))
	}
	return reader_v2.GrantsReaderServiceListGrantsForResourceTypeResponse_builder{
		List:          out,
		NextPageToken: next,
	}.Build(), nil
}

// GetSync fetches a single sync_run record by ID. Implements
// reader_v2.SyncsReaderServiceServer.
func (a *Adapter) GetSync(ctx context.Context, req *reader_v2.SyncsReaderServiceGetSyncRequest) (*reader_v2.SyncsReaderServiceGetSyncResponse, error) {
	if req.GetSyncId() == "" {
		return nil, errors.New("GetSync: empty sync_id")
	}
	rec, err := a.engine.GetSyncRunRecord(ctx, req.GetSyncId())
	if err != nil {
		return nil, adaptNotFound(err)
	}
	return reader_v2.SyncsReaderServiceGetSyncResponse_builder{
		Sync: v3SyncRunToV2(rec),
	}.Build(), nil
}

// ListSyncs paginates sync_run records across the engine. Order is
// the natural sync_id (KSUID) order — KSUIDs sort by timestamp, so
// this is also chronological. Implements
// reader_v2.SyncsReaderServiceServer.
func (a *Adapter) ListSyncs(ctx context.Context, req *reader_v2.SyncsReaderServiceListSyncsRequest) (*reader_v2.SyncsReaderServiceListSyncsResponse, error) {
	limit := clampPageSize(req.GetPageSize())
	cursorBytes, err := decodeCursor(req.GetPageToken())
	if err != nil {
		return nil, err
	}
	prefix := encodeSyncRunFullPrefix()
	lower, upper := rangeAfter(prefix, cursorBytes)
	iter, err := a.engine.DB().NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("ListSyncs: iter: %w", err)
	}
	defer iter.Close()
	out := make([]*reader_v2.SyncRun, 0, limit)
	var lastKey []byte
	hasMore := false
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(out) == limit {
			hasMore = true
			break
		}
		r := &v3.SyncRunRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
			return nil, err
		}
		lastKey = append(lastKey[:0], iter.Key()...)
		out = append(out, v3SyncRunToV2(r))
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	var nextCursor string
	if hasMore {
		nextCursor = encodeCursor(lastKey)
	}
	return reader_v2.SyncsReaderServiceListSyncsResponse_builder{
		Syncs:         out,
		NextPageToken: nextCursor,
	}.Build(), nil
}

// GetLatestFinishedSync returns the most-recently-ended sync_run,
// optionally filtered by sync_type. Implements
// reader_v2.SyncsReaderServiceServer.
//
// Linear scan across sync_runs (one record per sync; counts in the
// tens-thousands range for the lifetime of a tenant, not the
// millions). If this becomes hot, a "latest by type" sidecar key
// keyed on (typeFinishedSyncByType | type) → sync_id would make it
// O(1) — straightforward future work.
func (a *Adapter) GetLatestFinishedSync(ctx context.Context, req *reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest) (*reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse, error) {
	syncTypeFilter := req.GetSyncType()
	var latest *v3.SyncRunRecord
	if err := a.engine.IterateAllSyncRuns(ctx, func(rec *v3.SyncRunRecord) bool {
		if rec.GetEndedAt() == nil {
			return true
		}
		if syncTypeFilter != "" && v3SyncTypeToString(rec.GetType()) != syncTypeFilter {
			return true
		}
		if latest == nil || rec.GetEndedAt().AsTime().After(latest.GetEndedAt().AsTime()) {
			latest = rec
		}
		return true
	}); err != nil {
		return nil, err
	}
	if latest == nil {
		return reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse_builder{}.Build(), nil
	}
	return reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse_builder{
		Sync: v3SyncRunToV2(latest),
	}.Build(), nil
}

// v3SyncRunToV2 maps a v3.SyncRunRecord to the reader_v2.SyncRun
// gRPC shape (Id, StartedAt, EndedAt, SyncToken, SyncType,
// ParentSyncId).
func v3SyncRunToV2(rec *v3.SyncRunRecord) *reader_v2.SyncRun {
	if rec == nil {
		return nil
	}
	return reader_v2.SyncRun_builder{
		Id:           rec.GetSyncId(),
		StartedAt:    rec.GetStartedAt(),
		EndedAt:      rec.GetEndedAt(),
		SyncToken:    rec.GetSyncToken(),
		SyncType:     v3SyncTypeToString(rec.GetType()),
		ParentSyncId: rec.GetParentSyncId(),
	}.Build()
}

// v3SyncTypeToString maps the v3.SyncType enum to the
// connectorstore.SyncType string form used in the gRPC reader API.
func v3SyncTypeToString(t v3.SyncType) string {
	switch t {
	case v3.SyncType_SYNC_TYPE_FULL:
		return string(connectorstore.SyncTypeFull)
	case v3.SyncType_SYNC_TYPE_PARTIAL:
		return string(connectorstore.SyncTypePartial)
	case v3.SyncType_SYNC_TYPE_RESOURCES_ONLY:
		return string(connectorstore.SyncTypeResourcesOnly)
	case v3.SyncType_SYNC_TYPE_PARTIAL_UPSERTS:
		return string(connectorstore.SyncTypePartialUpserts)
	case v3.SyncType_SYNC_TYPE_PARTIAL_DELETIONS:
		return string(connectorstore.SyncTypePartialDeletions)
	case v3.SyncType_SYNC_TYPE_UNSPECIFIED:
		return ""
	}
	return ""
}

// resolveActiveSyncForReader resolves the sync_id for a read request.
// Today the v2 reader requests don't carry an active_sync_id field, so
// this is just the adapter's current sync; if/when annotations grow a
// way to override the active sync, this is the place to plumb it.
func (a *Adapter) resolveActiveSyncForReader(_ []*anypb.Any) string {
	return a.currentSyncID()
}
