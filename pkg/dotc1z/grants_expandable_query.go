package dotc1z

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/doug-martin/goqu/v9"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// ExpandableGrantDef is a lightweight representation of an expandable grant row,
// using queryable columns instead of unmarshalling the full grant proto.
type ExpandableGrantDef struct {
	RowID                    int64
	GrantExternalID          string
	DstEntitlementID         string
	PrincipalResourceTypeID  string
	PrincipalResourceID      string
	SrcEntitlementIDs        []string
	Shallow                  bool
	PrincipalResourceTypeIDs []string
	NeedsExpansion           bool
}

type listExpandableGrantsOptions struct {
	pageToken          string
	pageSize           uint32
	needsExpansionOnly bool
	syncID             string
}

type ListExpandableGrantsOption func(*listExpandableGrantsOptions)

func WithExpandableGrantsPageToken(t string) ListExpandableGrantsOption {
	return func(o *listExpandableGrantsOptions) { o.pageToken = t }
}

func WithExpandableGrantsPageSize(n uint32) ListExpandableGrantsOption {
	return func(o *listExpandableGrantsOptions) { o.pageSize = n }
}

func WithExpandableGrantsNeedsExpansionOnly(b bool) ListExpandableGrantsOption {
	return func(o *listExpandableGrantsOptions) { o.needsExpansionOnly = b }
}

// WithExpandableGrantsSyncID forces listing expandable grants for a specific sync id.
// If omitted, we default to the current sync id, then view sync id, then latest finished sync.
func WithExpandableGrantsSyncID(syncID string) ListExpandableGrantsOption {
	return func(o *listExpandableGrantsOptions) { o.syncID = syncID }
}

// ListExpandableGrants lists expandable grants using the grants table's queryable columns.
// It avoids scanning/unmarshalling all grants.
func (c *C1File) ListExpandableGrants(ctx context.Context, opts ...ListExpandableGrantsOption) ([]*ExpandableGrantDef, string, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListExpandableGrants")
	defer span.End()

	if err := c.validateDb(ctx); err != nil {
		return nil, "", err
	}

	o := &listExpandableGrantsOptions{}
	for _, opt := range opts {
		opt(o)
	}

	syncID, err := c.resolveSyncIDForInternalQuery(ctx, o.syncID)
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
		"data",
		"needs_expansion",
	)
	q = q.Where(goqu.C("sync_id").Eq(syncID))
	q = q.Where(goqu.C("is_expandable").Eq(1))
	if o.needsExpansionOnly {
		q = q.Where(goqu.C("needs_expansion").Eq(1))
	}

	if o.pageToken != "" {
		// Page token is the grants table row ID.
		id, err := strconv.ParseInt(o.pageToken, 10, 64)
		if err != nil {
			return nil, "", fmt.Errorf("invalid expandable grants page token %q: %w", o.pageToken, err)
		}
		q = q.Where(goqu.C("id").Gte(id))
	}

	pageSize := o.pageSize
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

	defs := make([]*ExpandableGrantDef, 0, pageSize)
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
			dstEntitlementID  string
			principalRTID     string
			principalRID      string
			dataBlob          []byte
			needsExpansionInt int
		)

		if err := rows.Scan(
			&rowID,
			&externalID,
			&dstEntitlementID,
			&principalRTID,
			&principalRID,
			&dataBlob,
			&needsExpansionInt,
		); err != nil {
			return nil, "", err
		}
		lastRow = rowID

		var g v2.Grant
		if err := proto.Unmarshal(dataBlob, &g); err != nil {
			return nil, "", fmt.Errorf("invalid grant data for %q: %w", externalID, err)
		}

		annos := annotations.Annotations(g.GetAnnotations())
		ge := &v2.GrantExpandable{}
		if _, err := annos.Pick(ge); err != nil {
			return nil, "", fmt.Errorf("failed to extract GrantExpandable from grant %q: %w", externalID, err)
		}

		defs = append(defs, &ExpandableGrantDef{
			RowID:                    rowID,
			GrantExternalID:          externalID,
			DstEntitlementID:         dstEntitlementID,
			PrincipalResourceTypeID:  principalRTID,
			PrincipalResourceID:      principalRID,
			SrcEntitlementIDs:        ge.GetEntitlementIds(),
			Shallow:                  ge.GetShallow(),
			PrincipalResourceTypeIDs: ge.GetResourceTypeIds(),
			NeedsExpansion:           needsExpansionInt != 0,
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
