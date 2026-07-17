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
	sdkannotations "github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// Compile-time assertion: *Adapter must satisfy the full
// GrantsReaderServiceServer contract, which now includes
// ListGrantsForPrincipal as a first-class required method.
var _ reader_v2.GrantsReaderServiceServer = (*Adapter)(nil)

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
	syncID, err := a.resolveActiveSyncForReader(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	rec, err := a.engine.GetEntitlementRecord(ctx, req.GetEntitlementId())
	err = c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
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
	syncID, err := a.resolveActiveSyncForReader(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	rid := req.GetResourceId()
	if rid == nil {
		return nil, errors.New("GetResource: nil resource_id")
	}
	rec, err := a.engine.GetResourceRecord(ctx, rid.GetResourceType(), rid.GetResource())
	err = c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
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
	syncID, err := a.resolveActiveSyncForReader(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	rec, err := a.engine.GetResourceTypeRecord(ctx, req.GetResourceTypeId())
	err = c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
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
//
//nolint:revive // method name mirrors the protobuf-generated gRPC server interface
func (a *Adapter) ListResourcesByIds(
	ctx context.Context,
	req *reader_v2.ResourcesReaderServiceListResourcesByIdsRequest,
) (*reader_v2.ResourcesReaderServiceListResourcesByIdsResponse, error) {
	syncID, err := a.resolveActiveSyncForReader(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
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
		rec, err := a.engine.GetResourceRecord(ctx, id.GetResourceType(), id.GetResource())
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
//
//nolint:revive // method name mirrors the protobuf-generated gRPC server interface
func (a *Adapter) ListEntitlementsByIds(
	ctx context.Context,
	req *reader_v2.EntitlementsReaderServiceListEntitlementsByIdsRequest,
) (*reader_v2.EntitlementsReaderServiceListEntitlementsByIdsResponse, error) {
	syncID, err := a.resolveActiveSyncForReader(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
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
		rec, err := a.engine.GetEntitlementRecord(ctx, id)
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

// GrantsForEntitlementPrincipalSorted reports whether ListGrantsForEntitlement
// yields grants in non-decreasing principal (resource_type, resource) order.
// True under the structured key layout: the primary grant key ends in the
// tuple-encoded (principal_rt, principal_id) components and the tuple codec
// is order-preserving, so a per-entitlement primary prefix scan IS a
// principal-ordered scan. This gates the topological-merge expansion path;
// streamingPrincipalGroupStream additionally fails loudly if the order
// invariant is ever violated, and the differential/parity suites compare
// the sorted and unsorted evaluators against SQLite.
func (a *Adapter) GrantsForEntitlementPrincipalSorted() bool { return true }

// ListGrantsForEntitlement paginates grants on a specific
// entitlement, optionally narrowed by principal_id or
// principal_resource_type_ids. Implements
// reader_v2.GrantsReaderServiceServer.
func (a *Adapter) ListGrantsForEntitlement(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	syncID, err := a.resolveActiveSyncForReader(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	ent := req.GetEntitlement()
	if ent == nil || ent.GetId() == "" {
		return nil, errors.New("ListGrantsForEntitlement: missing entitlement id")
	}
	entIdentity, err := a.entitlementIdentityForRequest(ctx, ent)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// Unknown entitlement → no grants, matching the legacy
			// empty-prefix-scan semantics.
			return reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse_builder{}.Build(), nil
		}
		return nil, err
	}
	limit := clampPageSize(req.GetPageSize())
	cursor := req.GetPageToken()

	// Filters: principal_id (single principal) or
	// principal_resource_type_ids (filter by RT membership).
	principalID := req.GetPrincipalId()           //nolint:staticcheck // ignore deprecated field
	rtFilter := req.GetPrincipalResourceTypeIds() //nolint:staticcheck // ignore deprecated field
	rtSet := make(map[string]struct{}, len(rtFilter))
	for _, rt := range rtFilter {
		rtSet[rt] = struct{}{}
	}

	// cursorFor returns the primary grant key for rec —
	// needed because a post-filter break at len(out) == limit may
	// leave matching records unconsumed in the engine page, and
	// the engine's end-of-page cursor would skip them.
	cursorFor := func(rec *v3.GrantRecord) string {
		id, err := grantIdentityFromRecord(rec)
		if err != nil {
			return ""
		}
		return encodeCursor(encodeGrantIdentityKey(id))
	}

	out := make([]*v2.Grant, 0, limit)
	var nextCursor string
	for len(out) < limit {
		pageLimit := limit - len(out)
		fetchLimit := pageLimit
		if len(rtFilter) > 0 {
			fetchLimit = pageLimit * 4
			if fetchLimit > MaxPageSize {
				fetchLimit = MaxPageSize
			}
		}
		var records []*v3.GrantRecord
		var next string
		if principalID != nil {
			records, next, err = a.engine.PaginateGrantsByEntitlementPrincipal(ctx,
				entIdentity, principalID.GetResourceType(), principalID.GetResource(), cursor, fetchLimit)
		} else {
			records, next, err = a.engine.PaginateGrantsByEntitlement(ctx,
				entIdentity, cursor, fetchLimit)
		}
		if err != nil {
			return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
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

// ListGrantPrincipalKeysForEntitlement returns the compact principal keys used
// by grant expansion prefetch. It avoids materializing full grant records when
// the caller only needs to know which principals already have a descendant
// entitlement grant.
func (a *Adapter) ListGrantPrincipalKeysForEntitlement(
	ctx context.Context,
	entitlement *v2.Entitlement,
	pageToken string,
	pageSize uint32,
) ([]string, string, error) {
	syncID, err := a.resolveActiveSyncForReader(ctx, nil)
	if err != nil {
		return nil, "", err
	}
	if syncID == "" {
		return nil, "", ErrNoCurrentSync
	}
	if entitlement == nil || entitlement.GetId() == "" {
		return nil, "", errors.New("ListGrantPrincipalKeysForEntitlement: missing entitlement id")
	}
	entIdentity, err := a.entitlementIdentityForRequest(ctx, entitlement)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, "", nil
		}
		return nil, "", err
	}
	keys, next, err := a.engine.PaginateGrantPrincipalKeysByEntitlement(ctx, entIdentity, pageToken, clampPageSize(pageSize))
	if err != nil {
		return nil, "", c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	return keys, next, nil
}

// entitlementIdentityForRequest resolves the structural identity for a
// request-supplied entitlement. When the request carries the resource ref
// (the normal case — the expander and c1 send full stubs), identity derives
// exactly from structured parts; otherwise the raw id string resolves
// through the bare-id lookup (exactly-one rule; pebble.ErrNotFound when the
// id matches nothing).
func (a *Adapter) entitlementIdentityForRequest(ctx context.Context, ent *v2.Entitlement) (entitlementIdentity, error) {
	if ent == nil || ent.GetId() == "" {
		return entitlementIdentity{}, errors.New("missing entitlement id")
	}
	if res := ent.GetResource(); res.GetId().GetResourceType() != "" && res.GetId().GetResource() != "" {
		return entitlementIdentityFromParts(res.GetId().GetResourceType(), res.GetId().GetResource(), ent.GetId()), nil
	}
	return a.engine.resolveGrantScanEntitlementIdentity(ctx, ent.GetId())
}

// ListGrantsForPrincipal returns all grants where the given principal_id is
// the principal, via the O(K) PaginateGrantsByPrincipal index walk. The
// optional Entitlement field narrows results to a single entitlement — since
// entitlement + principal is the full primary grant key, that case is an O(1)
// point lookup. Implements reader_v2.GrantsReaderServiceServer.
func (a *Adapter) ListGrantsForPrincipal(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForPrincipalRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForPrincipalResponse, error) {
	syncID, err := a.resolveActiveSyncForReader(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	principal := req.GetPrincipalId()
	if principal == nil || principal.GetResource() == "" {
		return nil, errors.New("ListGrantsForPrincipal: missing principal_id")
	}
	limit := clampPageSize(req.GetPageSize())
	cursor := req.GetPageToken()
	var records []*v3.GrantRecord
	var next string
	if ent := req.GetEntitlement(); ent != nil && ent.GetId() != "" {
		// Entitlement + principal is the full primary grant key, so this
		// is a point lookup rather than a filtered by_principal scan.
		entIdentity, err := a.entitlementIdentityForRequest(ctx, ent)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				// Unknown entitlement → no grants, matching the legacy
				// post-filter semantics.
				return reader_v2.GrantsReaderServiceListGrantsForPrincipalResponse_builder{}.Build(), nil
			}
			return nil, err
		}
		records, next, err = a.engine.PaginateGrantsByEntitlementPrincipal(ctx,
			entIdentity, principal.GetResourceType(), principal.GetResource(), cursor, limit)
		if err != nil {
			return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
		}
	} else {
		records, next, err = a.engine.PaginateGrantsByPrincipal(ctx,
			principal.GetResourceType(), principal.GetResource(), cursor, limit)
		if err != nil {
			return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
		}
	}
	out := make([]*v2.Grant, 0, len(records))
	for _, rec := range records {
		out = append(out, V3GrantToV2(rec))
	}
	return reader_v2.GrantsReaderServiceListGrantsForPrincipalResponse_builder{
		List:          out,
		NextPageToken: next,
	}.Build(), nil
}

// ListGrantsForResourceType paginates grants whose principal is of
// the given resource_type_id, via the by_principal index prefix.
// The cursor is the by_principal index key.
//
// Implements reader_v2.GrantsReaderServiceServer.
func (a *Adapter) ListGrantsForResourceType(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForResourceTypeResponse, error) {
	syncID, err := a.resolveActiveSyncForReader(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	rtFilter := req.GetResourceTypeId()
	if rtFilter == "" {
		return nil, errors.New("ListGrantsForResourceType: missing resource_type_id")
	}
	limit := clampPageSize(req.GetPageSize())
	cursor := req.GetPageToken()
	records, next, err := a.engine.PaginateGrantsByPrincipalResourceType(ctx, rtFilter, cursor, limit)
	if err != nil {
		return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
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
		return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}

	stats, err := a.syncStatsForRun(ctx, rec)
	if err != nil {
		return nil, err
	}

	return reader_v2.SyncsReaderServiceGetSyncResponse_builder{
		Sync: v3SyncRunToV2(rec, stats),
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
	lower, upper, err := rangeAfter(prefix, cursorBytes)
	if err != nil {
		return nil, err
	}
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

		stats, err := a.syncStatsForRun(ctx, r)
		if err != nil {
			return nil, err
		}

		out = append(out, v3SyncRunToV2(r, stats))
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
// reader_v2.SyncsReaderServiceServer. Delegates to
// Engine.LatestFinishedSyncRecord.
//
// Future work: if this becomes hot, a "latest by type" sidecar key
// keyed on (typeFinishedSyncByType | type) → sync_id would let
// Engine.LatestFinishedSyncRecord short-circuit to O(1).
func (a *Adapter) GetLatestFinishedSync(ctx context.Context, req *reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest) (*reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse, error) {
	latest, err := a.engine.LatestFinishedSyncRecord(ctx, syncTypeFilterFromString(req.GetSyncType()))
	if err != nil {
		return nil, err
	}
	if latest == nil {
		return reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse_builder{}.Build(), nil
	}

	stats, err := a.syncStatsForRun(ctx, latest)
	if err != nil {
		return nil, err
	}

	return reader_v2.SyncsReaderServiceGetLatestFinishedSyncResponse_builder{
		Sync: v3SyncRunToV2(latest, stats),
	}.Build(), nil
}

// syncStatsForRun returns SyncStats for a sync_run, preferring the
// sidecar and falling back to computeSyncStats. Token timings are
// present only when EndSync persisted them into the sidecar; older
// count-only caches and iteration fallbacks return counts alone.
func (a *Adapter) syncStatsForRun(ctx context.Context, rec *v3.SyncRunRecord) (*reader_v2.SyncStats, error) {
	if rec == nil {
		return nil, nil
	}
	stats, _, err := CachedSyncStats(ctx, a.engine, rec.GetSyncId())
	if err != nil {
		return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	if stats != nil {
		return stats, nil
	}
	computedStats, err := a.engine.computeSyncStats(ctx, rec.GetSyncId())
	if err != nil {
		return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	return SyncStatsFromRecord(computedStats), nil
}

// syncTypeFilterFromString returns a predicate that matches sync_runs
// whose v3 type round-trips to the given string form. Empty string
// returns nil (no filter), matching the
// Engine.LatestFinishedSyncRecord contract.
func syncTypeFilterFromString(s string) func(v3.SyncType) bool {
	if s == "" {
		return nil
	}
	return func(t v3.SyncType) bool { return v3SyncTypeToString(t) == s }
}

// v3SyncRunToV2 maps a v3.SyncRunRecord to the reader_v2.SyncRun
// gRPC shape (Id, StartedAt, EndedAt, SyncToken, SyncType,
// ParentSyncId).
func v3SyncRunToV2(rec *v3.SyncRunRecord, stats *reader_v2.SyncStats) *reader_v2.SyncRun {
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
		Stats:        stats,
	}.Build()
}

// v3SyncTypeToString maps the v3.SyncType enum to the
// connectorstore.SyncType string form used in the gRPC reader API. It is the
// string view of the same enum dispatch syncTypeV3ToConnectorstore performs for
// the SyncMeta projection; both share one switch so a new SyncType is handled
// in one place.
func v3SyncTypeToString(t v3.SyncType) string {
	return string(syncTypeV3ToConnectorstore(t))
}

// resolveActiveSyncForReader resolves the sync_id a read should scope
// to, in priority order, mirroring SQLite's resolveSyncIDForRead /
// getConnectorObject cascade (pkg/dotc1z):
//
//  1. c1zpb.SyncDetails annotation on the request.
//  2. The adapter's current sync (StartNewSync / SetCurrentSync).
//     Pebble has no separate "view sync" — SetCurrentSync serves the
//     read-selection role SQLite's viewSyncID does.
//  3. The most-recent finished sync of any type (so reads against a
//     closed c1z resolve to its stored data).
//  4. The most-recent in-progress sync started within the last week
//     (so an interrupted-only c1z still resolves).
//
// Returns ("", nil) only when no sync resolves cleanly. A malformed
// SyncDetails annotation, or a store error from the finished/unfinished
// lookups, surfaces as a non-nil error so callers don't silently fall
// through to the wrong sync (matching SQLite's resolveSyncIDForRead,
// which propagates those errors rather than swallowing them).
func (a *Adapter) resolveActiveSyncForReader(ctx context.Context, annos []*anypb.Any) (string, error) {
	annoSyncID, err := sdkannotations.GetSyncIdFromAnnotations(annos)
	if err != nil {
		return "", fmt.Errorf("pebble: read sync_id from annotations: %w", err)
	}
	if annoSyncID != "" {
		return annoSyncID, nil
	}
	if id := a.currentSyncID(); id != "" {
		return id, nil
	}
	id, err := a.LatestFinishedSyncID(ctx, connectorstore.SyncTypeAny)
	if err != nil {
		return "", fmt.Errorf("pebble: latest finished sync: %w", err)
	}
	if id != "" {
		return id, nil
	}
	// Final fallback, matching SQLite's resolveSyncIDForRead: the
	// latest in-progress sync (started within the last week). Lets
	// reads against a c1z whose only sync was interrupted before
	// EndSync resolve to that sync instead of ErrNoCurrentSync.
	rec, err := a.engine.LatestUnfinishedSyncRecord(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("pebble: latest unfinished sync: %w", err)
	}
	if rec != nil {
		return rec.GetSyncId(), nil
	}
	return "", nil
}

func (a *Adapter) InitCurrentSync(ctx context.Context) error {
	id, err := a.resolveActiveSyncForReader(ctx, nil)
	if err != nil {
		return fmt.Errorf("pebble: error resolving active sync: %w", err)
	}
	if id != "" {
		return a.SetCurrentSync(ctx, id)
	}
	return nil
}

// digestEntitlementIdentity maps a request-supplied entitlement stub to
// the structural identity the digest keyspace is addressed by, via the
// same entitlementIdentityForRequest the grants-for-entitlement readers
// use: exact derivation from the resource ref when present, bare-id
// resolution (exactly-one rule) otherwise. ok is false when a bare id
// matches nothing; an AMBIGUOUS id stays an error — a lossy string must
// never guess which digest to answer with. A nil stub or empty Id is an
// error, matching ListGrantsForEntitlement.
func (a *Adapter) digestEntitlementIdentity(ctx context.Context, ent *v2.Entitlement) (entitlementIdentity, bool, error) {
	id, err := a.entitlementIdentityForRequest(ctx, ent)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return entitlementIdentity{}, false, nil
		}
		return entitlementIdentity{}, false, err
	}
	return id, true, nil
}

// GetEntitlementGrantDigest implements connectorstore.EntitlementGrantDigestReader.
// It returns the stored grant-digest root (content hash + grant count)
// for the entitlement under the reader's active sync. found is false
// when no digest exists — either no active sync, an unknown bare id, or
// no digest was built for it (e.g. WithGrantDigestIndex(false), a file
// that predates the digest, or a post-seal mutation invalidated it). A
// nil error with found=false means "no digest".
func (a *Adapter) GetEntitlementGrantDigest(ctx context.Context, ent *v2.Entitlement) (connectorstore.GrantDigest, bool, error) {
	syncID, err := a.resolveActiveSyncForReader(ctx, nil)
	if err != nil {
		return connectorstore.GrantDigest{}, false, err
	}
	if syncID == "" {
		return connectorstore.GrantDigest{}, false, nil
	}
	id, ok, err := a.digestEntitlementIdentity(ctx, ent)
	if err != nil || !ok {
		return connectorstore.GrantDigest{}, false, err
	}
	root, ok, err := a.engine.GetEntitlementDigestRoot(ctx, id)
	if err != nil || !ok {
		return connectorstore.GrantDigest{}, false, err
	}
	return connectorstore.GrantDigest{Hash: root.Hash, Count: root.Count, Level: root.Bits}, true, nil
}

// GetEntitlementGrantDigestNodes implements
// connectorstore.EntitlementGrantDigestReader. It lists the entitlement's
// grant-digest rollup nodes at the requested level (2^level buckets;
// level 0 = the root). For 0 <= level <= the digest's native level it
// folds the stored leaves — one scan of the digest keyspace. For a finer
// level it scans the grant index directly (O(grants)) instead of
// erroring; the level is clamped to the bucket-hash resolution
// (digestMaxWidthBits).
func (a *Adapter) GetEntitlementGrantDigestNodes(ctx context.Context, ent *v2.Entitlement, level int) ([]connectorstore.GrantDigestNode, bool, error) {
	if level < 0 {
		return nil, false, fmt.Errorf("pebble: negative grant-digest level %d", level)
	}
	syncID, err := a.resolveActiveSyncForReader(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	if syncID == "" {
		return nil, false, nil
	}
	id, ok, err := a.digestEntitlementIdentity(ctx, ent)
	if err != nil || !ok {
		return nil, false, err
	}
	root, ok, err := a.engine.GetEntitlementDigestRoot(ctx, id)
	if err != nil || !ok {
		return nil, false, err
	}
	// Level 0 is the whole-entitlement root — return it directly (covers
	// the root-only digest, which has no stored leaves to fold).
	if level == 0 {
		return []connectorstore.GrantDigestNode{{Index: 0, Hash: root.Hash, Count: root.Count}}, true, nil
	}
	// The bucket hash carries at most digestMaxWidthBits of resolution;
	// a finer level can't address more buckets, so clamp.
	bits := min(level, digestMaxWidthBits)
	// At or below the stored width, fold the digest leaves (cheap). Finer
	// than what we stored, scan the grant index to compute the rollup.
	partition := digestPartitionForEntitlement(id)
	var folded []foldedBucket
	if bits <= root.Bits {
		folded, err = a.engine.foldedLeafBuckets(ctx, grantDigestSpec, partition, bits)
	} else {
		folded, err = a.engine.computeBucketsAtWidth(ctx, grantDigestSpec, partition, bits)
	}
	if err != nil {
		return nil, false, err
	}
	nodes := make([]connectorstore.GrantDigestNode, len(folded))
	for i := range folded {
		nodes[i] = connectorstore.GrantDigestNode{
			Index: folded[i].idx,
			Hash:  append([]byte(nil), folded[i].digest[:]...),
			Count: folded[i].count,
		}
	}
	return nodes, true, nil
}

// ScanEntitlementGrantBucket implements
// connectorstore.EntitlementGrantDigestReader. It yields every grant in
// the given digest bucket of the entitlement, translated to v2.Grant.
// Bucket Level 0 scans the whole entitlement; a finer Level is clamped
// to the bucket-hash resolution. Yields nothing when there is no active
// sync or a bare entitlement id resolves to nothing.
func (a *Adapter) ScanEntitlementGrantBucket(ctx context.Context, ent *v2.Entitlement, bucket connectorstore.GrantDigestBucket, yield func(*v2.Grant) bool) error {
	if bucket.Level < 0 {
		return fmt.Errorf("pebble: negative grant-digest level %d", bucket.Level)
	}
	syncID, err := a.resolveActiveSyncForReader(ctx, nil)
	if err != nil {
		return err
	}
	if syncID == "" {
		return nil
	}
	id, ok, err := a.digestEntitlementIdentity(ctx, ent)
	if err != nil || !ok {
		return err
	}
	bits := min(bucket.Level, digestMaxWidthBits)
	return a.engine.IterateGrantsByEntitlementBucket(ctx, id, DigestBucket{Index: bucket.Index, Bits: bits}, func(r *v3.GrantRecord) bool {
		return yield(V3GrantToV2(r))
	})
}
