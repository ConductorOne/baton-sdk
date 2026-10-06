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
	syncID, err := r.e.resolveActiveSyncForReader(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
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
	records, next, err := r.e.listGrantsForEntitlement(ctx, req)
	if err != nil {
		return nil, err
	}
	return reader_v3.GrantsReaderServiceListGrantsForEntitlementResponse_builder{
		List:          records,
		NextPageToken: next,
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
	records, next, err := r.e.listGrantsForEntitlements(ctx, req)
	if err != nil {
		return nil, err
	}
	return reader_v3.GrantsReaderServiceListGrantsForEntitlementsResponse_builder{
		List:          records,
		NextPageToken: next,
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
