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
	if opts.Request != nil && (opts.PageToken != "" || opts.PageSize != 0 || opts.SyncID != "") {
		return nil, fmt.Errorf("invalid grant list options: Request must not be combined with PageToken/PageSize/SyncID")
	}
	switch opts.Mode {
	case connectorstore.GrantListModeExpansion, connectorstore.GrantListModeExpansionNeedsOnly:
		if opts.Request != nil {
			return nil, fmt.Errorf("invalid grant list options: Request is not supported for expansion-only modes")
		}
		defs, nextPageToken, err := c.listExpandableGrantsInternal(ctx, opts)
		if err != nil {
			return nil, err
		}
		rows := make([]*connectorstore.InternalGrantRow, 0, len(defs))
		for _, def := range defs {
			rows = append(rows, &connectorstore.InternalGrantRow{Expansion: def})
		}
		return &connectorstore.InternalGrantListResponse{
			Rows:          rows,
			NextPageToken: nextPageToken,
		}, nil

	case connectorstore.GrantListModePayload, connectorstore.GrantListModePayloadWithExpansion:
		if opts.PageToken != "" || opts.PageSize != 0 || opts.SyncID != "" {
			return nil, fmt.Errorf("invalid grant list options: PageToken/PageSize/SyncID are only for expansion-only modes; use Request for payload modes")
		}
		req := opts.Request
		if req == nil {
			req = v2.GrantsServiceListGrantsRequest_builder{}.Build()
		}
		if opts.NeedsExpansionOnly {
			return nil, fmt.Errorf("invalid grant list options: NeedsExpansionOnly does not support payload modes")
		}

		if opts.Mode == connectorstore.GrantListModePayloadWithExpansion {
			return c.listGrantsWithExpansionInternal(ctx, req, opts.ExpandableOnly)
		}

		grantProtos, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), req, func() *v2.Grant { return &v2.Grant{} })
		if err != nil {
			return nil, err
		}
		rows := make([]*connectorstore.InternalGrantRow, 0, len(grantProtos))
		for _, g := range grantProtos {
			rows = append(rows, &connectorstore.InternalGrantRow{Grant: g})
		}
		return &connectorstore.InternalGrantListResponse{
			Rows:          rows,
			NextPageToken: nextPageToken,
		}, nil

	default:
		return nil, fmt.Errorf("invalid grant list options: unknown mode %d", opts.Mode)
	}
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
	if opts.Mode == connectorstore.GrantListModeExpansionNeedsOnly || opts.NeedsExpansionOnly {
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
	expandableOnly bool,
) (*connectorstore.InternalGrantListResponse, error) {
	if err := c.validateDb(ctx); err != nil {
		return nil, err
	}

	syncID, err := resolveSyncID(ctx, c, request)
	if err != nil {
		return nil, fmt.Errorf("error getting sync id for list grants with expansion: %w", err)
	}

	q := c.db.From(grants.Name()).Prepared(true).
		Select(
			"id",
			"data",
			"external_id",
			"entitlement_id",
			"principal_resource_type_id",
			"principal_resource_id",
			"expansion",
			"needs_expansion",
		)

	if resource := request.GetResource(); resource != nil {
		q = q.Where(goqu.C("resource_id").Eq(resource.GetId().GetResource()))
		q = q.Where(goqu.C("resource_type_id").Eq(resource.GetId().GetResourceType()))
	}

	if syncID != "" {
		q = q.Where(goqu.C("sync_id").Eq(syncID))
	}

	if expandableOnly {
		q = q.Where(goqu.C("expansion").IsNotNull())
	}
	if request.GetPageToken() != "" {
		q = q.Where(goqu.C("id").Gte(request.GetPageToken()))
	}

	pageSize := request.GetPageSize()
	if pageSize > maxPageSize || pageSize == 0 {
		pageSize = maxPageSize
	}
	q = q.Order(goqu.C("id").Asc()).Limit(uint(pageSize + 1))

	query, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	unmarshalerOptions := proto.UnmarshalOptions{
		Merge:          true,
		DiscardUnknown: true,
	}

	var (
		count   uint32
		lastRow int
	)
	result := make([]*connectorstore.InternalGrantRow, 0, pageSize)

	for rows.Next() {
		count++
		if count > pageSize {
			break
		}

		var (
			rowID         int
			grantData     []byte
			externalID    string
			entID         string
			principalRTID string
			principalRID  string
			expansionBlob []byte
			needsExp      int
		)
		if err := rows.Scan(
			&rowID,
			&grantData,
			&externalID,
			&entID,
			&principalRTID,
			&principalRID,
			&expansionBlob,
			&needsExp,
		); err != nil {
			return nil, err
		}
		lastRow = rowID

		grant := &v2.Grant{}
		if err := unmarshalerOptions.Unmarshal(grantData, grant); err != nil {
			return nil, err
		}

		var expansion *connectorstore.ExpandableGrantDef
		if len(expansionBlob) > 0 {
			ge := &v2.GrantExpandable{}
			if err := proto.Unmarshal(expansionBlob, ge); err != nil {
				return nil, fmt.Errorf("failed to unmarshal grant expansion for %s: %w", externalID, err)
			}
			expansion = &connectorstore.ExpandableGrantDef{
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

		result = append(result, &connectorstore.InternalGrantRow{
			Grant:     grant,
			Expansion: expansion,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	nextPageToken := ""
	if count > pageSize {
		nextPageToken = strconv.Itoa(lastRow + 1)
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
