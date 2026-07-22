package pebble

import (
	"context"
	"errors"

	"github.com/cockroachdb/pebble/v2"

	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// engineV3Grants is the v3 grants reader. Each method mirrors its reader_v2
// twin exactly (same sync resolution, fetch helpers, pagination and error
// behavior) but returns the rich *v3.GrantRecord instead of running it
// through V3GrantToV2, so fields like discovered_at survive. It lives on a
// wrapper, not on *Engine directly, because the v3 RPC names collide with the
// reader_v2 methods *Engine already carries and Go has no overloading;
// *Engine exposes it via V3GrantReader().
type engineV3Grants struct{ e *Engine }

var _ connectorstore.V3GrantReader = engineV3Grants{}

// V3GrantReader implements connectorstore.V3GrantReaderProvider.
func (e *Engine) V3GrantReader() connectorstore.V3GrantReader {
	return engineV3Grants{e: e}
}

// GetGrant mirrors the reader_v2 GetGrant, returning the rich v3.GrantRecord.
func (r engineV3Grants) GetGrant(
	ctx context.Context,
	req *reader_v3.GrantsReaderServiceGetGrantRequest,
) (*reader_v3.GrantsReaderServiceGetGrantResponse, error) {
	syncID := r.e.CurrentSyncID()
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	rec, err := r.e.GetGrantRecord(ctx, req.GetGrantId())
	if err != nil {
		return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	return reader_v3.GrantsReaderServiceGetGrantResponse_builder{
		Grant: rec,
	}.Build(), nil
}

// ListGrantsForEntitlement mirrors the reader_v2 method, returning rich v3.GrantRecords.
func (r engineV3Grants) ListGrantsForEntitlement(
	ctx context.Context,
	req *reader_v3.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v3.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	e := r.e
	syncID, err := e.resolveActiveSyncForReader(ctx, req.GetAnnotations())
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
	entIdentity, err := e.entitlementIdentityForRequest(ctx, ent)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// Unknown entitlement → no grants, matching the legacy
			// empty-prefix-scan semantics.
			return reader_v3.GrantsReaderServiceListGrantsForEntitlementResponse_builder{}.Build(), nil
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

	// cursorFor returns the primary grant key for rec — needed because a
	// post-filter break at len(out) == limit may leave matching records
	// unconsumed in the engine page, and the engine's end-of-page cursor
	// would skip them.
	cursorFor := func(rec *v3.GrantRecord) string {
		id, err := grantIdentityFromRecord(rec)
		if err != nil {
			return ""
		}
		return encodeCursor(encodeGrantIdentityKey(id))
	}

	out := make([]*v3.GrantRecord, 0, limit)
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
			records, next, err = e.PaginateGrantsByEntitlementPrincipal(ctx,
				entIdentity, principalID.GetResourceType(), principalID.GetResource(), cursor, fetchLimit)
		} else {
			records, next, err = e.PaginateGrantsByEntitlement(ctx,
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
			out = append(out, rec)
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
	return reader_v3.GrantsReaderServiceListGrantsForEntitlementResponse_builder{
		List:          out,
		NextPageToken: nextCursor,
	}.Build(), nil
}

// ListGrantsForResourceType mirrors the reader_v2 method, returning rich v3.GrantRecords.
func (r engineV3Grants) ListGrantsForResourceType(
	ctx context.Context,
	req *reader_v3.GrantsReaderServiceListGrantsForResourceTypeRequest,
) (*reader_v3.GrantsReaderServiceListGrantsForResourceTypeResponse, error) {
	e := r.e
	syncID, err := e.resolveActiveSyncForReader(ctx, req.GetAnnotations())
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
	records, next, err := e.PaginateGrantsByPrincipalResourceType(ctx, rtFilter, cursor, limit)
	if err != nil {
		return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	return reader_v3.GrantsReaderServiceListGrantsForResourceTypeResponse_builder{
		List:          records,
		NextPageToken: next,
	}.Build(), nil
}

// ListGrantsForEntitlements mirrors the reader_v2 batched method, returning rich v3.GrantRecords.
func (r engineV3Grants) ListGrantsForEntitlements(
	ctx context.Context,
	req *reader_v3.GrantsReaderServiceListGrantsForEntitlementsRequest,
) (*reader_v3.GrantsReaderServiceListGrantsForEntitlementsResponse, error) {
	e := r.e
	syncID, err := e.resolveActiveSyncForReader(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	ents := req.GetEntitlements()
	if len(ents) == 0 {
		return reader_v3.GrantsReaderServiceListGrantsForEntitlementsResponse_builder{}.Build(), nil
	}
	limit := int(req.GetPageSize())
	if limit <= 0 {
		limit = DefaultPageSize
	}
	if limit > MaxPageSize {
		limit = MaxPageSize
	}

	listChecksum := entitlementListChecksum(ents)

	startIdx, startIntra, err := decodeBatchCursor(req.GetPageToken(), listChecksum)
	if err != nil {
		return nil, err
	}

	out := make([]*v3.GrantRecord, 0, limit)
	var nextToken string

EntitlementLoop:
	for i := startIdx; i < len(ents); i++ {
		if ents[i].GetId() == "" {
			continue
		}
		entID, err := e.entitlementIdentityForRequest(ctx, ents[i])
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue // unknown entitlement → no grants
			}
			return nil, err
		}
		intraCursor := ""
		if i == startIdx {
			intraCursor = startIntra
		}
		for len(out) < limit {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			remaining := limit - len(out)
			records, next, err := e.PaginateGrantsByEntitlement(ctx, entID, intraCursor, remaining)
			if err != nil {
				return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
			}
			brokeEarly := false
			var lastIntra string
			for _, rec := range records {
				out = append(out, rec)
				if len(out) == limit {
					id, err := grantIdentityFromRecord(rec)
					if err != nil {
						return nil, err
					}
					lastIntra = encodeCursor(encodeGrantIdentityKey(id))
					brokeEarly = true
					break
				}
			}
			if brokeEarly {
				nextToken = encodeBatchCursor(i, lastIntra, listChecksum)
				break EntitlementLoop
			}
			if next == "" || len(records) == 0 {
				break
			}
			intraCursor = next
		}
		if len(out) >= limit && nextToken == "" {
			// Filled exactly on the entitlement boundary; resume at the
			// next entitlement with no intra-cursor.
			if i+1 < len(ents) {
				nextToken = encodeBatchCursor(i+1, "", listChecksum)
			}
			break
		}
	}

	return reader_v3.GrantsReaderServiceListGrantsForEntitlementsResponse_builder{
		List:          out,
		NextPageToken: nextToken,
	}.Build(), nil
}

// ListGrantsForPrincipal mirrors the reader_v2 method, returning rich v3.GrantRecords.
func (r engineV3Grants) ListGrantsForPrincipal(
	ctx context.Context,
	req *reader_v3.GrantsReaderServiceListGrantsForPrincipalRequest,
) (*reader_v3.GrantsReaderServiceListGrantsForPrincipalResponse, error) {
	e := r.e
	syncID, err := e.resolveActiveSyncForReader(ctx, req.GetAnnotations())
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
		entIdentity, err := e.entitlementIdentityForRequest(ctx, ent)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				// Unknown entitlement → no grants, matching the legacy
				// post-filter semantics.
				return reader_v3.GrantsReaderServiceListGrantsForPrincipalResponse_builder{}.Build(), nil
			}
			return nil, err
		}
		records, next, err = e.PaginateGrantsByEntitlementPrincipal(ctx,
			entIdentity, principal.GetResourceType(), principal.GetResource(), cursor, limit)
		if err != nil {
			return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
		}
	} else {
		records, next, err = e.PaginateGrantsByPrincipal(ctx,
			principal.GetResourceType(), principal.GetResource(), cursor, limit)
		if err != nil {
			return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
		}
	}
	return reader_v3.GrantsReaderServiceListGrantsForPrincipalResponse_builder{
		List:          records,
		NextPageToken: next,
	}.Build(), nil
}
