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

// ListGrantsInternal provides a single internal listing entrypoint with row shaping options.
func (c *C1File) ListGrantsInternal(ctx context.Context, opts connectorstore.GrantListOptions) (*connectorstore.InternalGrantListResponse, error) {
	if !opts.IncludeGrantPayload && !opts.IncludeExpansion {
		return nil, fmt.Errorf("invalid grant list options: at least one of IncludeGrantPayload or IncludeExpansion must be true")
	}
	if opts.NeedsExpansionOnly && !opts.IncludeExpansion {
		return nil, fmt.Errorf("invalid grant list options: NeedsExpansionOnly requires IncludeExpansion")
	}
	if opts.Request != nil && !opts.IncludeGrantPayload {
		return nil, fmt.Errorf("invalid grant list options: Request requires IncludeGrantPayload")
	}
	if opts.Request != nil && (opts.PageToken != "" || opts.PageSize != 0 || opts.SyncID != "") {
		return nil, fmt.Errorf("invalid grant list options: Request must not be combined with PageToken/PageSize/SyncID")
	}
	if opts.IncludeGrantPayload && opts.Request == nil && (opts.PageToken != "" || opts.PageSize != 0 || opts.SyncID != "") {
		return nil, fmt.Errorf("invalid grant list options: PageToken/PageSize/SyncID are only for expansion-only listing; use Request for grant payload listing")
	}

	// Expansion-only path uses direct SQL column reads and does not unmarshal full grant protos.
	if !opts.IncludeGrantPayload && opts.IncludeExpansion {
		defs, nextPageToken, err := c.listExpandableGrantsInternal(ctx, opts)
		if err != nil {
			return nil, err
		}
		rows := make([]*connectorstore.InternalGrantRow, 0, len(defs))
		for _, def := range defs {
			rows = append(rows, &connectorstore.InternalGrantRow{
				Expansion: def,
			})
		}
		return &connectorstore.InternalGrantListResponse{
			Rows:          rows,
			NextPageToken: nextPageToken,
		}, nil
	}

	req := opts.Request
	if req == nil {
		req = v2.GrantsServiceListGrantsRequest_builder{}.Build()
	}

	if opts.IncludeExpansion {
		if opts.SyncID != "" {
			return nil, fmt.Errorf("invalid grant list options: SyncID is not supported with IncludeGrantPayload")
		}
		resp, err := c.listGrantsWithExpansionInternal(ctx, req, opts.NeedsExpansionOnly)
		if err != nil {
			return nil, err
		}
		return resp, nil
	}

	grantProtos, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), req, func() *v2.Grant { return &v2.Grant{} })
	if err != nil {
		return nil, err
	}
	rows := make([]*connectorstore.InternalGrantRow, 0, len(grantProtos))
	for _, g := range grantProtos {
		rows = append(rows, &connectorstore.InternalGrantRow{
			Grant: g,
		})
	}
	return &connectorstore.InternalGrantListResponse{
		Rows:          rows,
		NextPageToken: nextPageToken,
	}, nil
}

func (c *C1File) listExpandableGrantsInternal(
	ctx context.Context,
	opts connectorstore.GrantListOptions,
) ([]*connectorstore.ExpandableGrantDef, string, error) {
	if err := c.validateDb(ctx); err != nil {
		return nil, "", err
	}

	syncID, err := c.resolveSyncIDForInternalQuery(ctx, opts.SyncID)
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
	if opts.NeedsExpansionOnly {
		q = q.Where(goqu.C("needs_expansion").Eq(1))
	}

	if opts.PageToken != "" {
		id, err := strconv.ParseInt(opts.PageToken, 10, 64)
		if err != nil {
			return nil, "", fmt.Errorf("invalid expandable grants page token %q: %w", opts.PageToken, err)
		}
		q = q.Where(goqu.C("id").Gte(id))
	}

	pageSize := opts.PageSize
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

func (c *C1File) listGrantsWithExpansionInternal(
	ctx context.Context,
	request *v2.GrantsServiceListGrantsRequest,
	needsExpansionOnly bool,
) (*connectorstore.InternalGrantListResponse, error) {
	grantProtos, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), request, func() *v2.Grant { return &v2.Grant{} })
	if err != nil {
		return nil, fmt.Errorf("error listing grants with expansion: %w", err)
	}

	if len(grantProtos) == 0 {
		return &connectorstore.InternalGrantListResponse{
			Rows:          make([]*connectorstore.InternalGrantRow, 0),
			NextPageToken: nextPageToken,
		}, nil
	}

	ids := make([]interface{}, 0, len(grantProtos))
	for _, g := range grantProtos {
		ids = append(ids, g.GetId())
	}

	q := c.db.From(grants.Name()).Prepared(true).
		Select("external_id", "entitlement_id", "principal_resource_type_id", "principal_resource_id", "expansion", "needs_expansion").
		Where(goqu.C("external_id").In(ids...)).
		Where(goqu.C("expansion").IsNotNull()).
		Where(goqu.L("length(expansion) > 0"))

	if needsExpansionOnly {
		q = q.Where(goqu.C("needs_expansion").Eq(1))
	}

	syncID, err := resolveSyncID(ctx, c, request)
	if err != nil {
		return nil, fmt.Errorf("error getting sync id for list grants with expansion: %w", err)
	}

	if syncID != "" {
		q = q.Where(goqu.C("sync_id").Eq(syncID))
	}

	query, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	expansionMap := make(map[string]*connectorstore.ExpandableGrantDef, len(grantProtos))
	for rows.Next() {
		var (
			externalID    string
			entID         string
			principalRTID string
			principalRID  string
			expansionBlob []byte
			needsExp      int
		)
		if err := rows.Scan(&externalID, &entID, &principalRTID, &principalRID, &expansionBlob, &needsExp); err != nil {
			return nil, err
		}
		if len(expansionBlob) == 0 {
			continue
		}
		ge := &v2.GrantExpandable{}
		if err := proto.Unmarshal(expansionBlob, ge); err != nil {
			return nil, fmt.Errorf("failed to unmarshal grant expansion for %s: %w", externalID, err)
		}
		expansionMap[externalID] = &connectorstore.ExpandableGrantDef{
			GrantExternalID:         externalID,
			TargetEntitlementID:     entID,
			PrincipalResourceTypeID: principalRTID,
			PrincipalResourceID:     principalRID,
			SourceEntitlementIDs:    ge.GetEntitlementIds(),
			Shallow:                 ge.GetShallow(),
			ResourceTypeIDs:         ge.GetResourceTypeIds(),
			NeedsExpansion:          needsExp != 0,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make([]*connectorstore.InternalGrantRow, 0, len(grantProtos))
	for _, g := range grantProtos {
		row := &connectorstore.InternalGrantRow{
			Grant:     g,
			Expansion: expansionMap[g.GetId()],
		}
		if needsExpansionOnly && row.Expansion == nil {
			continue
		}
		result = append(result, row)
	}

	return &connectorstore.InternalGrantListResponse{
		Rows:          result,
		NextPageToken: nextPageToken,
	}, nil
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
