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

func parseGrantPageToken(pageToken string, tokenType string) (int64, error) {
	id, err := strconv.ParseInt(pageToken, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s grants page token %q: %w", tokenType, pageToken, err)
	}
	return id, nil
}

// ListGrantsInternal provides a single internal listing entrypoint with row shaping options.
func (c *C1File) ListGrantsInternal(ctx context.Context, opts connectorstore.GrantListOptions) (*connectorstore.InternalGrantListResponse, error) {
	switch opts.Mode {
	case connectorstore.GrantListModeExpansion, connectorstore.GrantListModeExpansionNeedsOnly:
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
		if opts.SyncID != "" {
			return nil, fmt.Errorf("invalid grant list options: SyncID is not supported for payload modes")
		}
		if opts.NeedsExpansionOnly {
			return nil, fmt.Errorf("invalid grant list options: NeedsExpansionOnly does not support payload modes")
		}

		if opts.Mode == connectorstore.GrantListModePayloadWithExpansion {
			return c.listGrantsWithExpansionInternal(ctx, opts)
		}

		return c.listGrantsPayloadInternal(ctx, opts)

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
		id, err := parseGrantPageToken(opts.PageToken, "expandable")
		if err != nil {
			return nil, "", err
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

func (c *C1File) listGrantsWithExpansionInternal(ctx context.Context, opts connectorstore.GrantListOptions) (*connectorstore.InternalGrantListResponse, error) {
	if err := c.validateDb(ctx); err != nil {
		return nil, err
	}

	syncID, err := c.resolveSyncIDForPayloadQuery(ctx)
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

	if resource := opts.Resource; resource != nil {
		q = q.Where(goqu.C("resource_id").Eq(resource.GetId().GetResource()))
		q = q.Where(goqu.C("resource_type_id").Eq(resource.GetId().GetResourceType()))
	}

	if syncID != "" {
		q = q.Where(goqu.C("sync_id").Eq(syncID))
	}

	if opts.ExpandableOnly {
		q = q.Where(goqu.C("expansion").IsNotNull())
	}
	if opts.PageToken != "" {
		id, err := parseGrantPageToken(opts.PageToken, "payload")
		if err != nil {
			return nil, err
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
		return nil, err
	}

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	unmarshalerOptions := proto.UnmarshalOptions{
		Merge:          true,
		DiscardUnknown: true,
	}

	var (
		count   uint32
		lastRow int64
	)
	result := make([]*connectorstore.InternalGrantRow, 0, pageSize)
	// Only populated when a slim row is observed — on pre-slim c1zs
	// (every existing tenant today) these slices stay nil and the
	// hydration pass is skipped entirely.
	var pageGrants []*v2.Grant
	var pageKeys []grantJoinKeys
	var sawSlim bool

	for rows.Next() {
		count++
		if count > pageSize {
			break
		}

		var (
			rowID         int64
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
			rows.Close()
			return nil, err
		}
		lastRow = rowID

		grant := &v2.Grant{}
		if err := unmarshalerOptions.Unmarshal(grantData, grant); err != nil {
			rows.Close()
			return nil, err
		}

		var expansion *connectorstore.ExpandableGrantDef
		if len(expansionBlob) > 0 {
			ge := &v2.GrantExpandable{}
			if err := proto.Unmarshal(expansionBlob, ge); err != nil {
				rows.Close()
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
		if grant.GetEntitlement() == nil || grant.GetPrincipal() == nil {
			if !sawSlim {
				pageGrants = make([]*v2.Grant, 0, pageSize)
				pageKeys = make([]grantJoinKeys, 0, pageSize)
				sawSlim = true
			}
			pageGrants = append(pageGrants, grant)
			pageKeys = append(pageKeys, grantJoinKeys{
				EntitlementID:           entID,
				PrincipalResourceTypeID: principalRTID,
				PrincipalResourceID:     principalRID,
			})
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	// Close rows before hydration to release the pooled SQLite connection —
	// SetMaxOpenConns(1) would otherwise deadlock any follow-up query.
	if err := rows.Close(); err != nil {
		return nil, err
	}

	if sawSlim {
		if err := hydrateGrants(ctx, c, syncID, pageGrants, pageKeys); err != nil {
			return nil, err
		}
	}

	nextPageToken := ""
	if count > pageSize {
		nextPageToken = strconv.FormatInt(lastRow+1, 10)
	}

	return &connectorstore.InternalGrantListResponse{
		Rows:          result,
		NextPageToken: nextPageToken,
	}, nil
}

func (c *C1File) listGrantsPayloadInternal(ctx context.Context, opts connectorstore.GrantListOptions) (*connectorstore.InternalGrantListResponse, error) {
	if err := c.validateDb(ctx); err != nil {
		return nil, err
	}

	syncID, err := c.resolveSyncIDForPayloadQuery(ctx)
	if err != nil {
		return nil, err
	}

	q := c.db.From(grants.Name()).Prepared(true).Select(
		"id",
		"data",
		"entitlement_id",
		"principal_resource_type_id",
		"principal_resource_id",
	)
	if opts.Resource != nil {
		q = q.Where(goqu.C("resource_id").Eq(opts.Resource.GetId().GetResource()))
		q = q.Where(goqu.C("resource_type_id").Eq(opts.Resource.GetId().GetResourceType()))
	}
	if syncID != "" {
		q = q.Where(goqu.C("sync_id").Eq(syncID))
	}
	if opts.PageToken != "" {
		id, err := parseGrantPageToken(opts.PageToken, "payload")
		if err != nil {
			return nil, err
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
		return nil, err
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	unmarshalerOptions := proto.UnmarshalOptions{
		Merge:          true,
		DiscardUnknown: true,
	}
	result := make([]*connectorstore.InternalGrantRow, 0, pageSize)
	var pageGrants []*v2.Grant
	var pageKeys []grantJoinKeys
	var (
		count   uint32
		lastRow int64
		sawSlim bool
	)
	for rows.Next() {
		count++
		if count > pageSize {
			break
		}

		var (
			rowID         int64
			grantData     []byte
			entID         string
			principalRTID string
			principalRID  string
		)
		if err := rows.Scan(&rowID, &grantData, &entID, &principalRTID, &principalRID); err != nil {
			rows.Close()
			return nil, err
		}
		lastRow = rowID

		grant := &v2.Grant{}
		if err := unmarshalerOptions.Unmarshal(grantData, grant); err != nil {
			rows.Close()
			return nil, err
		}
		result = append(result, &connectorstore.InternalGrantRow{Grant: grant})
		if grant.GetEntitlement() == nil || grant.GetPrincipal() == nil {
			if !sawSlim {
				pageGrants = make([]*v2.Grant, 0, pageSize)
				pageKeys = make([]grantJoinKeys, 0, pageSize)
				sawSlim = true
			}
			pageGrants = append(pageGrants, grant)
			pageKeys = append(pageKeys, grantJoinKeys{
				EntitlementID:           entID,
				PrincipalResourceTypeID: principalRTID,
				PrincipalResourceID:     principalRID,
			})
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	// Release the pooled connection before hydration — see comment in
	// listGrantsWithExpansionInternal above.
	if err := rows.Close(); err != nil {
		return nil, err
	}

	if sawSlim {
		if err := hydrateGrants(ctx, c, syncID, pageGrants, pageKeys); err != nil {
			return nil, err
		}
	}

	nextPageToken := ""
	if count > pageSize {
		nextPageToken = strconv.FormatInt(lastRow+1, 10)
	}
	return &connectorstore.InternalGrantListResponse{
		Rows:          result,
		NextPageToken: nextPageToken,
	}, nil
}

func (c *C1File) resolveSyncIDForPayloadQuery(ctx context.Context) (string, error) {
	if c.currentSyncID != "" {
		return c.currentSyncID, nil
	}
	if c.viewSyncID != "" {
		return c.viewSyncID, nil
	}
	latestSyncRun, err := c.getCachedViewSyncRun(ctx)
	if err != nil {
		return "", err
	}
	if latestSyncRun != nil {
		return latestSyncRun.ID, nil
	}
	return "", nil
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
