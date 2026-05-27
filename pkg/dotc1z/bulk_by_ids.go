package dotc1z

import (
	"context"
	"fmt"

	"github.com/doug-martin/goqu/v9"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

// inClauseChunkSize bounds the IN (...) list size per query.
// SQLite's default SQLITE_MAX_VARIABLE_NUMBER is 32766 on modern
// builds; 500 stays well under any plausible build's limit and
// keeps planner cost predictable.
const inClauseChunkSize = 500

// ListEntitlementsByIds returns the entitlements for the requested
// ids in any order. Missing rows are silently omitted; callers
// detect partial misses by length / id comparison.
//
//nolint:revive // method name mirrors the protobuf-generated gRPC server interface
func (c *C1File) ListEntitlementsByIds(
	ctx context.Context,
	req *reader_v2.EntitlementsReaderServiceListEntitlementsByIdsRequest,
) (*reader_v2.EntitlementsReaderServiceListEntitlementsByIdsResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListEntitlementsByIds")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	syncID, err := c.resolveSyncIDForRead(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	ids := req.GetEntitlementIds()
	out := make([]*v2.Entitlement, 0, len(ids))
	err = c.scanByExternalIDs(ctx, entitlements.Name(), syncID, ids, func(data []byte) error {
		m := &v2.Entitlement{}
		if err := proto.Unmarshal(data, m); err != nil {
			return err
		}
		out = append(out, m)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return reader_v2.EntitlementsReaderServiceListEntitlementsByIdsResponse_builder{
		List: out,
	}.Build(), nil
}

// ListResourcesByIds returns the resources for the supplied
// ResourceId pairs (resource_type:resource composite externals).
// Missing rows are silently omitted.
//
//nolint:revive // method name mirrors the protobuf-generated gRPC server interface
func (c *C1File) ListResourcesByIds(
	ctx context.Context,
	req *reader_v2.ResourcesReaderServiceListResourcesByIdsRequest,
) (*reader_v2.ResourcesReaderServiceListResourcesByIdsResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListResourcesByIds")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	syncID, err := c.resolveSyncIDForRead(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	rids := req.GetResourceIds()
	externals := make([]string, 0, len(rids))
	for _, rid := range rids {
		if rid == nil || rid.GetResourceType() == "" || rid.GetResource() == "" {
			continue
		}
		externals = append(externals, fmt.Sprintf("%s:%s", rid.GetResourceType(), rid.GetResource()))
	}
	out := make([]*v2.Resource, 0, len(externals))
	err = c.scanByExternalIDs(ctx, resources.Name(), syncID, externals, func(data []byte) error {
		m := &v2.Resource{}
		if err := proto.Unmarshal(data, m); err != nil {
			return err
		}
		out = append(out, m)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return reader_v2.ResourcesReaderServiceListResourcesByIdsResponse_builder{
		List: out,
	}.Build(), nil
}

// scanByExternalIDs runs SELECT data FROM <table> WHERE external_id
// IN (...) AND sync_id = ? in chunks of inClauseChunkSize and
// invokes yield on each blob. Skips empty id strings.
func (c *C1File) scanByExternalIDs(
	ctx context.Context,
	tableName string,
	syncID string,
	ids []string,
	yield func(data []byte) error,
) error {
	if err := c.validateDb(ctx); err != nil {
		return err
	}
	cleaned := ids[:0:len(ids)]
	for _, id := range ids {
		if id == "" {
			continue
		}
		cleaned = append(cleaned, id)
	}
	if len(cleaned) == 0 {
		return nil
	}
	for start := 0; start < len(cleaned); start += inClauseChunkSize {
		end := start + inClauseChunkSize
		if end > len(cleaned) {
			end = len(cleaned)
		}
		batch := cleaned[start:end]
		q := c.db.From(tableName).Prepared(true).
			Select("data").
			Where(goqu.C("external_id").In(batch))
		if syncID != "" {
			q = q.Where(goqu.C("sync_id").Eq(syncID))
		}
		query, args, err := q.ToSQL()
		if err != nil {
			return err
		}
		rows, err := c.db.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		for rows.Next() {
			var data []byte
			if err := rows.Scan(&data); err != nil {
				rows.Close()
				return err
			}
			if err := yield(data); err != nil {
				rows.Close()
				return err
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
	}
	return nil
}

// resolveSyncIDForRead resolves the sync_id to use for a read
// request by following the same priority order as
// getConnectorObject: annotation override → currentSyncID →
// viewSyncID → latest-finished → latest-unfinished. Returns ""
// when there is no sync to read against (the c1z is fresh).
func (c *C1File) resolveSyncIDForRead(ctx context.Context, anns []*anypb.Any) (string, error) {
	syncID, err := annotations.GetSyncIdFromAnnotations(anns)
	if err != nil {
		return "", fmt.Errorf("read sync_id from annotations: %w", err)
	}
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
		return "", fmt.Errorf("get finished sync: %w", err)
	}
	if latest != nil {
		return latest.ID, nil
	}
	unfinished, err := c.getLatestUnfinishedSync(ctx, connectorstore.SyncTypeAny)
	if err != nil {
		return "", fmt.Errorf("get unfinished sync: %w", err)
	}
	if unfinished != nil {
		return unfinished.ID, nil
	}
	return "", nil
}
