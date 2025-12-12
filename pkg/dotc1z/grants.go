package dotc1z

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/doug-martin/goqu/v9"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

const grantsTableVersion = "1"
const grantsTableName = "grants"
const grantsTableSchema = `
create table if not exists %s (
    id integer primary key,
	resource_type_id text not null,
    resource_id text not null,
    entitlement_id text not null,
    principal_resource_type_id text not null,
    principal_resource_id text not null,
    external_id text not null,
    data blob not null,
    sync_id text not null,
    discovered_at datetime not null,
    sources text not null default '{}'
);
create index if not exists %s on %s (resource_type_id, resource_id);
create index if not exists %s on %s (principal_resource_type_id, principal_resource_id);
create index if not exists %s on %s (entitlement_id, principal_resource_type_id, principal_resource_id);
create unique index if not exists %s on %s (external_id, sync_id);`

var grants = (*grantsTable)(nil)

var _ tableDescriptor = (*grantsTable)(nil)

type grantsTable struct{}

func (r *grantsTable) Version() string {
	return grantsTableVersion
}

func (r *grantsTable) Name() string {
	return fmt.Sprintf("v%s_%s", r.Version(), grantsTableName)
}

func (r *grantsTable) Schema() (string, []any) {
	return grantsTableSchema, []any{
		r.Name(),
		fmt.Sprintf("idx_grants_resource_type_id_resource_id_v%s", r.Version()),
		r.Name(),
		fmt.Sprintf("idx_grants_principal_id_v%s", r.Version()),
		r.Name(),
		fmt.Sprintf("idx_grants_entitlement_id_principal_id_v%s", r.Version()),
		r.Name(),
		fmt.Sprintf("idx_grants_external_sync_v%s", r.Version()),
		r.Name(),
	}
}

func (r *grantsTable) Migrations(ctx context.Context, db *goqu.Database) error {
	// Check if sources column exists
	var sourcesExists int
	err := db.QueryRowContext(ctx, fmt.Sprintf("select count(*) from pragma_table_info('%s') where name='sources'", r.Name())).Scan(&sourcesExists)
	if err != nil {
		return err
	}
	if sourcesExists == 0 {
		_, err = db.ExecContext(ctx, fmt.Sprintf("alter table %s add column sources text not null default '{}'", r.Name()))
		if err != nil {
			return err
		}

		//TODO: Grab grant sources from each row and update sources column.
	}

	return nil
}

// DropGrantIndexes drops the indexes on the grants table.
// This should only be called when compacting the grants table.
// These indexes are re-created when we open the database again.
func (c *C1File) DropGrantIndexes(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "C1File.DropGrantsIndexes")
	defer span.End()

	indexes := []string{
		fmt.Sprintf("idx_grants_resource_type_id_resource_id_v%s", grants.Version()),
		fmt.Sprintf("idx_grants_principal_id_v%s", grants.Version()),
		fmt.Sprintf("idx_grants_entitlement_id_principal_id_v%s", grants.Version()),
		fmt.Sprintf("idx_grants_external_sync_v%s", grants.Version()),
	}

	for _, index := range indexes {
		_, err := c.db.ExecContext(ctx, fmt.Sprintf("DROP INDEX IF EXISTS %s", index))
		if err != nil {
			return err
		}
	}
	return nil
}

// DropGrantExpandIndexes drops the indexes on the grants table.
// This should only be called when compacting the grants table.
// These indexes are re-created when we open the database again.
func (c *C1File) DropGrantExpandIndexes(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "C1File.DropGrantsIndexes")
	defer span.End()

	indexes := []string{
		fmt.Sprintf("idx_grants_resource_type_id_resource_id_v%s", grants.Version()),
		fmt.Sprintf("idx_grants_principal_id_v%s", grants.Version()),
		// fmt.Sprintf("idx_grants_entitlement_id_principal_id_v%s", grants.Version()),
		fmt.Sprintf("idx_grants_external_sync_v%s", grants.Version()),
	}

	for _, index := range indexes {
		_, err := c.db.ExecContext(ctx, fmt.Sprintf("DROP INDEX IF EXISTS %s", index))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *C1File) ListGrants(ctx context.Context, request *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListGrants")
	defer span.End()

	grantRows := make([]*v2.Grant, 0, 10000)
	ret, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), request, func() *v2.Grant { return &v2.Grant{} }, grantRows)
	if err != nil {
		return nil, fmt.Errorf("error listing grants: %w", err)
	}

	return v2.GrantsServiceListGrantsResponse_builder{
		List:          ret,
		NextPageToken: nextPageToken,
	}.Build(), nil
}

func (c *C1File) GetGrant(ctx context.Context, request *reader_v2.GrantsReaderServiceGetGrantRequest) (*reader_v2.GrantsReaderServiceGetGrantResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.GetGrant")
	defer span.End()

	ret := &v2.Grant{}
	syncId, err := annotations.GetSyncIdFromAnnotations(request.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("error getting sync id from annotations for grant '%s': %w", request.GetGrantId(), err)
	}
	err = c.getConnectorObject(ctx, grants.Name(), request.GetGrantId(), syncId, ret)
	if err != nil {
		return nil, fmt.Errorf("error fetching grant '%s': %w", request.GetGrantId(), err)
	}

	return reader_v2.GrantsReaderServiceGetGrantResponse_builder{
		Grant: ret,
	}.Build(), nil
}

// ListGrantsForEntitlementPooled lists grants using a caller-provided factory function.
// This allows the caller to manage a pool of Grant objects for reuse.
// The caller is responsible for resetting/releasing grants after use.
func (c *C1File) ListGrantsForEntitlementPooled(
	ctx context.Context,
	request *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
	acquireGrant func() *v2.Grant,
	grantRows []*v2.Grant,
) ([]*v2.Grant, string, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListGrantsForEntitlementPooled")
	defer span.End()

	ret, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), request, acquireGrant, grantRows)
	if err != nil {
		return nil, "", fmt.Errorf("error listing grants for entitlement '%s': %w", request.GetEntitlement().GetId(), err)
	}

	return ret, nextPageToken, nil
}

func (c *C1File) ListGrantsForEntitlement(
	ctx context.Context,
	request *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListGrantsForEntitlement")
	defer span.End()
	grantRows := make([]*v2.Grant, 0, 10000)
	ret, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), request, func() *v2.Grant { return &v2.Grant{} }, grantRows)
	if err != nil {
		return nil, fmt.Errorf("error listing grants for entitlement '%s': %w", request.GetEntitlement().GetId(), err)
	}

	return reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse_builder{
		List:          ret,
		NextPageToken: nextPageToken,
	}.Build(), nil
}

func (c *C1File) ListGrantsForPrincipal(
	ctx context.Context,
	request *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListGrantsForPrincipal")
	defer span.End()

	grantRows := make([]*v2.Grant, 0, 10000)
	ret, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), request, func() *v2.Grant { return &v2.Grant{} }, grantRows)
	if err != nil {
		return nil, fmt.Errorf("error listing grants for principal '%s': %w", request.GetPrincipalId(), err)
	}

	return reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse_builder{
		List:          ret,
		NextPageToken: nextPageToken,
	}.Build(), nil
}

func (c *C1File) ListGrantsForResourceType(
	ctx context.Context,
	request *reader_v2.GrantsReaderServiceListGrantsForResourceTypeRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForResourceTypeResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListGrantsForResourceType")
	defer span.End()

	grantRows := make([]*v2.Grant, 0, 10000)
	ret, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), request, func() *v2.Grant { return &v2.Grant{} }, grantRows)
	if err != nil {
		return nil, fmt.Errorf("error listing grants for resource type '%s': %w", request.GetResourceTypeId(), err)
	}

	return reader_v2.GrantsReaderServiceListGrantsForResourceTypeResponse_builder{
		List:          ret,
		NextPageToken: nextPageToken,
	}.Build(), nil
}

func (c *C1File) PutGrants(ctx context.Context, bulkGrants ...*v2.Grant) error {
	ctx, span := tracer.Start(ctx, "C1File.PutGrants")
	defer span.End()

	return c.putGrantsInternal(ctx, bulkPutConnectorObject, bulkGrants...)
}

func (c *C1File) PutGrantsIfNewer(ctx context.Context, bulkGrants ...*v2.Grant) error {
	ctx, span := tracer.Start(ctx, "C1File.PutGrantsIfNewer")
	defer span.End()

	return c.putGrantsInternal(ctx, bulkPutConnectorObjectIfNewer, bulkGrants...)
}

type grantPutFunc func(context.Context, *C1File, string, func(m *v2.Grant) (goqu.Record, error), ...*v2.Grant) error

func (c *C1File) putGrantsInternal(ctx context.Context, f grantPutFunc, bulkGrants ...*v2.Grant) error {
	err := f(ctx, c, grants.Name(),
		func(grant *v2.Grant) (goqu.Record, error) {
			sources := grant.GetSources().GetSources()
			if sources == nil {
				sources = make(map[string]*v2.GrantSources_GrantSource)
			}
			sourcesJSON, err := json.Marshal(sources)
			if err != nil {
				return goqu.Record{}, err
			}
			return goqu.Record{
				"resource_type_id":           grant.GetEntitlement().GetResource().GetId().GetResourceType(),
				"resource_id":                grant.GetEntitlement().GetResource().GetId().GetResource(),
				"entitlement_id":             grant.GetEntitlement().GetId(),
				"principal_resource_type_id": grant.GetPrincipal().GetId().GetResourceType(),
				"principal_resource_id":      grant.GetPrincipal().GetId().GetResource(),
				"sources":                    sourcesJSON,
			}, nil
		},
		bulkGrants...,
	)
	if err != nil {
		return err
	}
	c.dbUpdated = true
	return nil
}

// RectifyGrantSources updates grant protobuf data to match the normalized grant_sources table.
// This handles two cases:
// 1. Grants with empty data blobs (newly inserted via SQL) - fully reconstruct the protobuf.
// 2. Grants with valid data but outdated sources - just update the sources field.
//
// Deprecated: This function is no longer needed when using ExpandGrantsSingleEdgeWithProto,
// which creates grants with complete protobuf data directly in SQL. This function is kept
// for backward compatibility but should not be used for new code.
func (c *C1File) RectifyGrantSources(ctx context.Context) (int64, error) {
	ctx, span := tracer.Start(ctx, "C1File.RectifyGrantSources")
	defer span.End()

	gTable := grants.Name()
	eTable := entitlements.Name()
	rTable := resources.Name()
	gsTable := grantSources.Name()

	const pageSize = 1000
	var updated int64
	var lastID int64 = 0

	// Cache maps for unmarshaled entitlements and principals
	// These persist across pages since entitlements/principals are often reused
	entitlementCache := make(map[string]*v2.Entitlement)
	principalCache := make(map[string]*v2.Resource)

	// Process grants in batches of pageSize
	for {
		// Query grants that either have empty data OR have entries in grant_sources
		// Include entitlement and principal data for reconstruction
		// ORDER BY entitlement_id, principal to enable better cache hits within a page
		rows, err := c.db.QueryContext(ctx, `
			SELECT 
				g.id,
				g.external_id,
				g.data,
				g.entitlement_id,
				g.principal_resource_type_id,
				g.principal_resource_id,
				e.data AS entitlement_data,
				r.data AS principal_data,
				COALESCE(
					(SELECT group_concat(gs.source_entitlement_id, '||') 
					 FROM `+gsTable+` gs 
					 WHERE gs.grant_id = g.id AND gs.sync_id = g.sync_id),
					''
				) AS sources_list
			FROM `+gTable+` g
			JOIN `+eTable+` e ON e.external_id = g.entitlement_id AND e.sync_id = g.sync_id
			JOIN `+rTable+` r ON r.resource_type_id = g.principal_resource_type_id 
			                  AND r.resource_id = g.principal_resource_id
			                  AND r.sync_id = g.sync_id
			WHERE g.sync_id = ? 
			  AND g.id > ?
			  AND (
			      length(g.data) = 0
			      OR EXISTS (SELECT 1 FROM `+gsTable+` gs WHERE gs.grant_id = g.id AND gs.sync_id = g.sync_id)
			  )
			ORDER BY g.entitlement_id, g.principal_resource_type_id, g.principal_resource_id, g.id
			LIMIT ?
		`, c.currentSyncID, lastID, pageSize)
		if err != nil {
			return 0, fmt.Errorf("error querying grants for rectification: %w", err)
		}

		// Collect grants to update during iteration
		grantsToUpdate := make([]*v2.Grant, 0, pageSize)
		rowsProcessed := 0

		// Iterate through all rows in this batch
		for rows.Next() {
			var rowID int64
			var externalID string
			var data []byte
			var entitlementID string
			var principalTypeID, principalID string
			var entitlementData, principalData []byte
			var sourcesList string

			if err := rows.Scan(&rowID, &externalID, &data, &entitlementID,
				&principalTypeID, &principalID, &entitlementData, &principalData, &sourcesList); err != nil {
				rows.Close()
				return 0, fmt.Errorf("error scanning grant row: %w", err)
			}

			// Parse the sources from the normalized table (|| delimited)
			var sourcesMap map[string]*v2.GrantSources_GrantSource
			if sourcesList != "" {
				parts := splitSourcesList(sourcesList)
				sourcesMap = make(map[string]*v2.GrantSources_GrantSource, len(parts))
				for _, src := range parts {
					if src != "" {
						sourcesMap[src] = &v2.GrantSources_GrantSource{}
					}
				}
			}

			lastID = rowID
			rowsProcessed++

			var grant *v2.Grant
			needsUpdate := false

			// Check if data is empty (needs full reconstruction)
			if len(data) == 0 {
				// Get entitlement from cache or unmarshal
				entitlement, ok := entitlementCache[entitlementID]
				if !ok {
					entitlement = &v2.Entitlement{}
					if err := proto.Unmarshal(entitlementData, entitlement); err != nil {
						rows.Close()
						return 0, fmt.Errorf("error unmarshalling entitlement data: %w", err)
					}
					entitlementCache[entitlementID] = entitlement
				}

				// Get principal from cache or unmarshal
				principalKey := principalTypeID + ":" + principalID
				principal, ok := principalCache[principalKey]
				if !ok {
					principal = &v2.Resource{}
					if err := proto.Unmarshal(principalData, principal); err != nil {
						rows.Close()
						return 0, fmt.Errorf("error unmarshalling principal data: %w", err)
					}
					principalCache[principalKey] = principal
				}

				// Build full grant with immutable annotation (expanded grants are immutable)
				var annos annotations.Annotations
				annos.Update(&v2.GrantImmutable{})

				var sources *v2.GrantSources
				if sourcesMap != nil {
					sources = &v2.GrantSources{Sources: sourcesMap}
				}

				grant = v2.Grant_builder{
					Id:          externalID,
					Entitlement: entitlement,
					Principal:   principal,
					Sources:     sources,
					Annotations: annos,
				}.Build()
				needsUpdate = true
			} else {
				// Parse existing grant and check if sources need update
				grant = &v2.Grant{}
				if err := proto.Unmarshal(data, grant); err != nil {
					rows.Close()
					return 0, fmt.Errorf("error unmarshalling grant data: %w", err)
				}

				existingSources := grant.GetSources().GetSources()
				if existingSources == nil {
					existingSources = make(map[string]*v2.GrantSources_GrantSource)
				}

				// Check if sources need update
				if len(sourcesMap) != len(existingSources) {
					needsUpdate = true
				} else {
					for k := range sourcesMap {
						if _, ok := existingSources[k]; !ok {
							needsUpdate = true
							break
						}
					}
				}

				if needsUpdate {
					if sourcesMap == nil {
						grant.SetSources(nil)
					} else {
						grant.SetSources(&v2.GrantSources{Sources: sourcesMap})
					}
				}
			}

			if needsUpdate {
				grantsToUpdate = append(grantsToUpdate, grant)
			}
		}

		// Check for errors from iteration
		if err := rows.Err(); err != nil {
			rows.Close()
			return 0, fmt.Errorf("error iterating grant rows: %w", err)
		}
		rows.Close()

		// Put grants after we're done with rows.Next() to avoid conflicts due to exclusive locking
		if len(grantsToUpdate) > 0 {
			if err := c.PutGrants(ctx, grantsToUpdate...); err != nil {
				return 0, fmt.Errorf("error updating grants: %w", err)
			}
			updated += int64(len(grantsToUpdate))
		}

		// If we got fewer rows than the page size, we've reached the end
		if rowsProcessed < pageSize {
			break
		}
	}

	return updated, nil
}

// splitSourcesList splits a || delimited string into parts.
func splitSourcesList(s string) []string {
	if s == "" {
		return nil
	}
	var parts []string
	start := 0
	for i := 0; i < len(s)-1; i++ {
		if s[i] == '|' && s[i+1] == '|' {
			parts = append(parts, s[start:i])
			start = i + 2
			i++ // Skip the second |
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func (c *C1File) DeleteGrant(ctx context.Context, grantId string) error {
	ctx, span := tracer.Start(ctx, "C1File.DeleteGrant")
	defer span.End()

	err := c.validateSyncDb(ctx)
	if err != nil {
		return err
	}

	q := c.db.Delete(grants.Name())
	q = q.Where(goqu.C("external_id").Eq(grantId))
	if c.currentSyncID != "" {
		q = q.Where(goqu.C("sync_id").Eq(c.currentSyncID))
	}
	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	return nil
}

// UpdateGrantSourcesProto updates the sources field in both the JSON column and protobuf blob
// for a specific grant without unmarshalling the full protobuf record.
// This is more efficient than RectifyGrantSources for individual updates.
func (c *C1File) UpdateGrantSourcesProto(ctx context.Context, grantID int64, sourcesJSON []byte) error {
	ctx, span := tracer.Start(ctx, "C1File.UpdateGrantSourcesProto")
	defer span.End()

	gTable := grants.Name()

	// Update both sources JSON column and data blob using the SQLite function
	_, err := c.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET sources = ?,
		    data = update_grant_sources_proto(data, ?)
		WHERE id = ? AND sync_id = ?
	`, gTable), sourcesJSON, string(sourcesJSON), grantID, c.currentSyncID)
	if err != nil {
		return fmt.Errorf("error updating grant sources: %w", err)
	}

	c.dbUpdated = true
	return nil
}

// UpdateGrantSourcesProtoBatch updates sources for multiple grants in a single transaction.
// This is more efficient than calling UpdateGrantSourcesProto multiple times.
func (c *C1File) UpdateGrantSourcesProtoBatch(ctx context.Context, updates map[int64][]byte) error {
	ctx, span := tracer.Start(ctx, "C1File.UpdateGrantSourcesProtoBatch")
	defer span.End()

	if len(updates) == 0 {
		return nil
	}

	gTable := grants.Name()

	// Use a prepared statement for better performance
	stmt, err := c.rawDb.PrepareContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET sources = ?,
		    data = update_grant_sources_proto(data, ?)
		WHERE id = ? AND sync_id = ?
	`, gTable))
	if err != nil {
		return fmt.Errorf("error preparing update statement: %w", err)
	}
	defer stmt.Close()

	for grantID, sourcesJSON := range updates {
		_, err = stmt.ExecContext(ctx, sourcesJSON, string(sourcesJSON), grantID, c.currentSyncID)
		if err != nil {
			return fmt.Errorf("error updating grant %d: %w", grantID, err)
		}
	}

	c.dbUpdated = true
	return nil
}
