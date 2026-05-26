package dotc1z

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/doug-martin/goqu/v9"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// listResourcesByTrait implements the trait-filter path on
// ResourcesService.ListResources (RFC §B2). It (1) loads
// the resource_types for the active sync that carry the requested
// trait, then (2) lists resources whose resource_type_id is in
// that set, applying any additional filters from the request.
func (c *C1File) listResourcesByTrait(
	ctx context.Context,
	req *v2.ResourcesServiceListResourcesRequest,
) ([]*v2.Resource, string, error) {
	if err := c.validateDb(ctx); err != nil {
		return nil, "", err
	}
	reqSyncID, err := resolveSyncID(ctx, c, req)
	if err != nil {
		return nil, "", err
	}

	rtIDs, err := c.resourceTypeIDsWithTrait(ctx, reqSyncID, req.GetTrait())
	if err != nil {
		return nil, "", err
	}
	if len(rtIDs) == 0 {
		return nil, "", nil
	}
	// Honor an explicit resource_type_id filter: it intersects the
	// trait-resolved set.
	if explicit := req.GetResourceTypeId(); explicit != "" {
		found := false
		for _, id := range rtIDs {
			if id == explicit {
				found = true
				break
			}
		}
		if !found {
			return nil, "", nil
		}
		rtIDs = []string{explicit}
	}

	q := c.db.From(resources.Name()).Prepared(true).
		Select("id", "data").
		Where(goqu.C("resource_type_id").In(rtIDs))

	if parent := req.GetParentResourceId(); parent != nil && parent.GetResource() != "" {
		q = q.Where(goqu.C("parent_resource_id").Eq(parent.GetResource()))
		q = q.Where(goqu.C("parent_resource_type_id").Eq(parent.GetResourceType()))
	}
	if reqSyncID != "" {
		q = q.Where(goqu.C("sync_id").Eq(reqSyncID))
	}
	if req.GetPageToken() != "" {
		q = q.Where(goqu.C("id").Gte(req.GetPageToken()))
	}
	pageSize := req.GetPageSize()
	if pageSize > maxPageSize || pageSize == 0 {
		pageSize = maxPageSize
	}
	q = q.Order(goqu.C("id").Asc()).Limit(uint(pageSize + 1))

	query, args, err := q.ToSQL()
	if err != nil {
		return nil, "", err
	}
	start := time.Now()
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	if dur := time.Since(start); dur > c.slowQueryThreshold {
		c.throttledWarnSlowQuery(ctx, query, dur)
	}

	unmarshal := proto.UnmarshalOptions{Merge: true, DiscardUnknown: true}
	out := make([]*v2.Resource, 0, pageSize)
	var rowID int64
	var data sql.RawBytes
	var nextPageToken string
	count := uint32(0)
	for rows.Next() {
		if err := rows.Scan(&rowID, &data); err != nil {
			return nil, "", err
		}
		if count == pageSize {
			nextPageToken = fmt.Sprintf("%d", rowID)
			break
		}
		r := &v2.Resource{}
		if err := unmarshal.Unmarshal(data, r); err != nil {
			return nil, "", err
		}
		out = append(out, r)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	return out, nextPageToken, nil
}

// resourceTypeIDsWithTrait scans v1_resource_types for syncID and
// returns the IDs of resource_types whose proto-encoded traits list
// includes the requested trait. resource_types is small (typically
// O(10–100)) so a full table scan is fine.
func (c *C1File) resourceTypeIDsWithTrait(
	ctx context.Context,
	syncID string,
	trait v2.ResourceType_Trait,
) ([]string, error) {
	q := c.db.From(resourceTypes.Name()).Prepared(true).Select("external_id", "data")
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

	unmarshal := proto.UnmarshalOptions{Merge: true, DiscardUnknown: true}
	out := []string{}
	var externalID string
	var data sql.RawBytes
	for rows.Next() {
		if err := rows.Scan(&externalID, &data); err != nil {
			return nil, err
		}
		rt := &v2.ResourceType{}
		if err := unmarshal.Unmarshal(data, rt); err != nil {
			return nil, err
		}
		for _, t := range rt.GetTraits() {
			if t == trait {
				out = append(out, externalID)
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
