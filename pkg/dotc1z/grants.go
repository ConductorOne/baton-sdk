package dotc1z

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/doug-martin/goqu/v9"

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
