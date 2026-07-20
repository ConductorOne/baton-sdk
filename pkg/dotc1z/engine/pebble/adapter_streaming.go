package pebble

import (
	"context"
	"iter"

	"github.com/cockroachdb/pebble/v2"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// StreamGrants yields grants for syncID, optionally narrowed by
// opts. Implements connectorstore.StreamingReader.
func (e *Engine) StreamGrants(
	ctx context.Context,
	syncID string,
	opts connectorstore.StreamGrantsOptions,
) iter.Seq2[*v2.Grant, error] {
	return func(yield func(*v2.Grant, error) bool) {
		if syncID == "" {
			resolved, err := e.resolveActiveSyncForReader(ctx, nil)
			if err != nil {
				yield(nil, err)
				return
			}
			syncID = resolved
		}
		if syncID == "" {
			yield(nil, ErrNoCurrentSync)
			return
		}
		var iterErr error
		cb := func(rec *v3.GrantRecord) bool {
			if err := ctx.Err(); err != nil {
				iterErr = err
				return false
			}
			if opts.PrincipalResourceType != "" {
				if rec.GetPrincipal().GetResourceTypeId() != opts.PrincipalResourceType {
					return true
				}
			}
			if opts.PrincipalResourceID != "" {
				if rec.GetPrincipal().GetResourceId() != opts.PrincipalResourceID {
					return true
				}
			}
			return yield(V3GrantToV2(rec), nil)
		}
		var err error
		switch {
		case opts.EntitlementID != "":
			err = e.IterateGrantsByEntitlement(ctx, opts.EntitlementID, cb)
		case opts.PrincipalResourceType != "" && opts.PrincipalResourceID == "":
			err = e.IterateGrantsByPrincipalResourceType(ctx, opts.PrincipalResourceType, cb)
		default:
			err = e.IterateGrants(ctx, cb)
		}
		if iterErr != nil {
			yield(nil, iterErr)
			return
		}
		if err != nil {
			yield(nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound))
		}
	}
}

// StreamResources yields resources for syncID, optionally narrowed
// by resource_type. Implements connectorstore.StreamingReader.
func (e *Engine) StreamResources(
	ctx context.Context,
	syncID string,
	opts connectorstore.StreamResourcesOptions,
) iter.Seq2[*v2.Resource, error] {
	return func(yield func(*v2.Resource, error) bool) {
		if syncID == "" {
			resolved, err := e.resolveActiveSyncForReader(ctx, nil)
			if err != nil {
				yield(nil, err)
				return
			}
			syncID = resolved
		}
		if syncID == "" {
			yield(nil, ErrNoCurrentSync)
			return
		}
		var iterErr error
		err := e.IterateResources(ctx, func(rec *v3.ResourceRecord) bool {
			if err := ctx.Err(); err != nil {
				iterErr = err
				return false
			}
			if opts.ResourceTypeID != "" && rec.GetResourceTypeId() != opts.ResourceTypeID {
				return true
			}
			return yield(V3ResourceToV2(rec), nil)
		})
		if iterErr != nil {
			yield(nil, iterErr)
			return
		}
		if err != nil {
			yield(nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound))
		}
	}
}

// StreamEntitlements yields all entitlements for syncID.
// Implements connectorstore.StreamingReader.
func (e *Engine) StreamEntitlements(
	ctx context.Context,
	syncID string,
) iter.Seq2[*v2.Entitlement, error] {
	return func(yield func(*v2.Entitlement, error) bool) {
		if syncID == "" {
			resolved, err := e.resolveActiveSyncForReader(ctx, nil)
			if err != nil {
				yield(nil, err)
				return
			}
			syncID = resolved
		}
		if syncID == "" {
			yield(nil, ErrNoCurrentSync)
			return
		}
		var iterErr error
		err := e.IterateEntitlements(ctx, func(rec *v3.EntitlementRecord) bool {
			if err := ctx.Err(); err != nil {
				iterErr = err
				return false
			}
			return yield(V3EntitlementToV2(rec), nil)
		})
		if iterErr != nil {
			yield(nil, iterErr)
			return
		}
		if err != nil {
			yield(nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound))
		}
	}
}
