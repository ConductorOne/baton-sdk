package dotc1z

import (
	"context"
	"database/sql"
	"iter"

	"github.com/doug-martin/goqu/v9"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// StreamGrants yields grants for the active (or requested) sync.
// Implements connectorstore.StreamingReader. SQLite path uses
// Rows.Next() as the streaming pump — no client-side pagination
// buffer.
func (c *C1File) StreamGrants(
	ctx context.Context,
	syncID string,
	opts connectorstore.StreamGrantsOptions,
) iter.Seq2[*v2.Grant, error] {
	return func(yield func(*v2.Grant, error) bool) {
		if err := c.validateDb(ctx); err != nil {
			yield(nil, err)
			return
		}
		resolved, err := c.resolveStreamingSyncID(ctx, syncID)
		if err != nil {
			yield(nil, err)
			return
		}
		q := c.db.From(grants.Name()).Prepared(true).Select("data")
		if resolved != "" {
			q = q.Where(goqu.C("sync_id").Eq(resolved))
		}
		if opts.EntitlementID != "" {
			q = q.Where(goqu.C("entitlement_id").Eq(opts.EntitlementID))
		}
		if opts.PrincipalResourceType != "" {
			q = q.Where(goqu.C("principal_resource_type_id").Eq(opts.PrincipalResourceType))
		}
		if opts.PrincipalResourceID != "" {
			q = q.Where(goqu.C("principal_resource_id").Eq(opts.PrincipalResourceID))
		}
		q = q.Order(goqu.C("id").Asc())
		streamRows(ctx, c, q, func(data []byte) bool {
			m := &v2.Grant{}
			if err := proto.Unmarshal(data, m); err != nil {
				return yield(nil, err)
			}
			return yield(m, nil)
		}, yield)
	}
}

// StreamResources yields resources optionally filtered by RT.
func (c *C1File) StreamResources(
	ctx context.Context,
	syncID string,
	opts connectorstore.StreamResourcesOptions,
) iter.Seq2[*v2.Resource, error] {
	return func(yield func(*v2.Resource, error) bool) {
		if err := c.validateDb(ctx); err != nil {
			yield(nil, err)
			return
		}
		resolved, err := c.resolveStreamingSyncID(ctx, syncID)
		if err != nil {
			yield(nil, err)
			return
		}
		q := c.db.From(resources.Name()).Prepared(true).Select("data")
		if resolved != "" {
			q = q.Where(goqu.C("sync_id").Eq(resolved))
		}
		if opts.ResourceTypeID != "" {
			q = q.Where(goqu.C("resource_type_id").Eq(opts.ResourceTypeID))
		}
		q = q.Order(goqu.C("id").Asc())
		streamRows(ctx, c, q, func(data []byte) bool {
			m := &v2.Resource{}
			if err := proto.Unmarshal(data, m); err != nil {
				return yield(nil, err)
			}
			return yield(m, nil)
		}, yield)
	}
}

// StreamEntitlements yields entitlements for the active (or
// requested) sync.
func (c *C1File) StreamEntitlements(
	ctx context.Context,
	syncID string,
) iter.Seq2[*v2.Entitlement, error] {
	return func(yield func(*v2.Entitlement, error) bool) {
		if err := c.validateDb(ctx); err != nil {
			yield(nil, err)
			return
		}
		resolved, err := c.resolveStreamingSyncID(ctx, syncID)
		if err != nil {
			yield(nil, err)
			return
		}
		q := c.db.From(entitlements.Name()).Prepared(true).Select("data")
		if resolved != "" {
			q = q.Where(goqu.C("sync_id").Eq(resolved))
		}
		q = q.Order(goqu.C("id").Asc())
		streamRows(ctx, c, q, func(data []byte) bool {
			m := &v2.Entitlement{}
			if err := proto.Unmarshal(data, m); err != nil {
				return yield(nil, err)
			}
			return yield(m, nil)
		}, yield)
	}
}

// streamRows executes a SELECT data ... query and invokes
// recordYield for each row. recordYield returns false to abort
// iteration; errorYield is invoked at most once with the
// terminating error.
func streamRows[T any](
	ctx context.Context,
	c *C1File,
	q *goqu.SelectDataset,
	recordYield func(data []byte) bool,
	errorYield func(T, error) bool,
) {
	var zero T
	query, args, err := q.ToSQL()
	if err != nil {
		errorYield(zero, err)
		return
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		errorYield(zero, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			errorYield(zero, err)
			return
		}
		var data sql.RawBytes
		if err := rows.Scan(&data); err != nil {
			errorYield(zero, err)
			return
		}
		if !recordYield(data) {
			return
		}
	}
	if err := rows.Err(); err != nil {
		errorYield(zero, err)
	}
}

// resolveStreamingSyncID resolves the sync_id to use for a stream
// call. Same priority order as resolveSyncIDForRead but without
// annotation parsing — the streaming API takes the syncID
// directly.
func (c *C1File) resolveStreamingSyncID(ctx context.Context, syncID string) (string, error) {
	if syncID != "" {
		return syncID, nil
	}
	if c.currentSyncID != "" {
		return c.currentSyncID, nil
	}
	if c.viewSyncID != "" {
		return c.viewSyncID, nil
	}
	latest, err := c.getFinishedSync(ctx, 0, connectorstore.SyncTypeAny)
	if err != nil {
		return "", err
	}
	if latest != nil {
		return latest.ID, nil
	}
	unfinished, err := c.getLatestUnfinishedSync(ctx, connectorstore.SyncTypeAny)
	if err != nil {
		return "", err
	}
	if unfinished != nil {
		return unfinished.ID, nil
	}
	return "", nil
}
