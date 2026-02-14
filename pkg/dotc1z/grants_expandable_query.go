package dotc1z

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/doug-martin/goqu/v9"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// ListExpandableGrants lists expandable grants using the grants table's queryable columns.
// It reads the expansion column directly and returns lightweight ExpandableGrantDef structs,
// avoiding the cost of unmarshalling full grant protos.
func (c *C1File) ListExpandableGrants(ctx context.Context, opts ...connectorstore.ListExpandableGrantsOption) ([]*connectorstore.ExpandableGrantDef, string, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListExpandableGrants")
	defer span.End()

	if err := c.validateDb(ctx); err != nil {
		return nil, "", err
	}

	o := &connectorstore.ListExpandableGrantsOptions{}
	for _, opt := range opts {
		opt(o)
	}

	syncID, err := c.resolveSyncIDForInternalQuery(ctx, o.SyncID)
	if err != nil {
		return nil, "", err
	}

	q := c.db.From(grants.Name()).Prepared(true)
	q = q.Select(
		"id",
		"external_id",
		"entitlement_id",
		"principal_resource_type_id",
		"principal_resource_id",
		"expansion",
		"needs_expansion",
	)
	q = q.Where(goqu.C("sync_id").Eq(syncID))
	q = q.Where(goqu.C("expansion").IsNotNull())
	q = q.Where(goqu.L("length(expansion) > 0"))
	if o.NeedsExpansionOnly {
		q = q.Where(goqu.C("needs_expansion").Eq(1))
	}

	if o.PageToken != "" {
		id, err := strconv.ParseInt(o.PageToken, 10, 64)
		if err != nil {
			return nil, "", fmt.Errorf("invalid expandable grants page token %q: %w", o.PageToken, err)
		}
		q = q.Where(goqu.C("id").Gte(id))
	}

	pageSize := o.PageSize
	if pageSize > maxPageSize || pageSize == 0 {
		pageSize = maxPageSize
	}
	q = q.Order(goqu.C("id").Asc()).Limit(uint(pageSize + 1))

	query, args, err := q.ToSQL()
	if err != nil {
		return nil, "", err
	}

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	defs := make([]*connectorstore.ExpandableGrantDef, 0, pageSize)
	var (
		count   uint32
		lastRow int64
	)
	for rows.Next() {
		count++
		if count > pageSize {
			break
		}

		var (
			rowID             int64
			externalID        string
			targetEntID       string
			principalRTID     string
			principalRID      string
			expansionBlob     []byte
			needsExpansionInt int
		)

		if err := rows.Scan(
			&rowID,
			&externalID,
			&targetEntID,
			&principalRTID,
			&principalRID,
			&expansionBlob,
			&needsExpansionInt,
		); err != nil {
			return nil, "", err
		}
		lastRow = rowID

		ge := &v2.GrantExpandable{}
		if err := proto.Unmarshal(expansionBlob, ge); err != nil {
			return nil, "", fmt.Errorf("invalid expansion data for %q: %w", externalID, err)
		}

		defs = append(defs, &connectorstore.ExpandableGrantDef{
			RowID:                   rowID,
			GrantExternalID:         externalID,
			TargetEntitlementID:     targetEntID,
			PrincipalResourceTypeID: principalRTID,
			PrincipalResourceID:     principalRID,
			SourceEntitlementIDs:    ge.GetEntitlementIds(),
			Shallow:                 ge.GetShallow(),
			ResourceTypeIDs:         ge.GetResourceTypeIds(),
			NeedsExpansion:          needsExpansionInt != 0,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextPageToken := ""
	if count > pageSize {
		nextPageToken = strconv.FormatInt(lastRow+1, 10)
	}
	return defs, nextPageToken, nil
}

func (c *C1File) resolveSyncIDForInternalQuery(ctx context.Context, forced string) (string, error) {
	switch {
	case forced != "":
		return forced, nil
	case c.currentSyncID != "":
		return c.currentSyncID, nil
	case c.viewSyncID != "":
		return c.viewSyncID, nil
	default:
		latest, err := c.getCachedViewSyncRun(ctx)
		if err != nil {
			return "", err
		}
		if latest == nil {
			return "", sql.ErrNoRows
		}
		return latest.ID, nil
	}
}
