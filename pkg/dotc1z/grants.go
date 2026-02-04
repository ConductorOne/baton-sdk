package dotc1z

import (
	"context"
	"fmt"
	"strings"

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
    is_expandable integer not null default 0,   -- 1 if data contains a GrantExpandable annotation; used to build the expansion graph.
    needs_expansion integer not null default 0, -- 1 if grant should be processed during expansion.
    data blob not null,
    sync_id text not null,
    discovered_at datetime not null
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

// isAlreadyExistsError returns true if err is a SQLite "duplicate column name" error.
func isAlreadyExistsError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "duplicate column name")
}

func (r *grantsTable) Migrations(ctx context.Context, db *goqu.Database) error {
	// Add is_expandable column if missing (for older files).
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"alter table %s add column is_expandable integer not null default 0", r.Name(),
	)); err != nil && !isAlreadyExistsError(err) {
		return err
	}

	// Add needs_expansion column if missing.
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"alter table %s add column needs_expansion integer not null default 0", r.Name(),
	)); err != nil && !isAlreadyExistsError(err) {
		return err
	}

	// Create the index only after the columns exist.
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"create index if not exists %s on %s (sync_id, needs_expansion)",
		fmt.Sprintf("idx_grants_sync_needs_expansion_v%s", r.Version()),
		r.Name(),
	)); err != nil {
		return err
	}

	// Backfill from stored grant bytes for rows that haven't been classified yet.
	return backfillGrantExpandableColumns(ctx, db, r.Name())
}

func (c *C1File) ListGrants(ctx context.Context, request *v2.GrantsServiceListGrantsRequest) (*v2.GrantsServiceListGrantsResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListGrants")
	defer span.End()

	ret, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), request, func() *v2.Grant { return &v2.Grant{} })
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

func (c *C1File) ListGrantsForEntitlement(
	ctx context.Context,
	request *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListGrantsForEntitlement")
	defer span.End()
	ret, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), request, func() *v2.Grant { return &v2.Grant{} })
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

	ret, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), request, func() *v2.Grant { return &v2.Grant{} })
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

	ret, nextPageToken, err := listConnectorObjects(ctx, c, grants.Name(), request, func() *v2.Grant { return &v2.Grant{} })
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

	return c.putGrantsInternal(ctx, bulkPutGrants, bulkGrants...)
}

func (c *C1File) PutGrantsIfNewer(ctx context.Context, bulkGrants ...*v2.Grant) error {
	ctx, span := tracer.Start(ctx, "C1File.PutGrantsIfNewer")
	defer span.End()

	return c.putGrantsInternal(ctx, bulkPutGrantsIfNewer, bulkGrants...)
}

type grantPutFunc func(context.Context, *C1File, string, func(m *v2.Grant) (goqu.Record, error), ...*v2.Grant) error

func (c *C1File) putGrantsInternal(ctx context.Context, f grantPutFunc, bulkGrants ...*v2.Grant) error {
	if c.readOnly {
		return ErrReadOnly
	}

	err := f(ctx, c, grants.Name(),
		func(grant *v2.Grant) (goqu.Record, error) {
			isExpandable, needsExpansion := grantExpandableColumns(grant)

			return goqu.Record{
				"resource_type_id":           grant.GetEntitlement().GetResource().GetId().GetResourceType(),
				"resource_id":                grant.GetEntitlement().GetResource().GetId().GetResource(),
				"entitlement_id":             grant.GetEntitlement().GetId(),
				"principal_resource_type_id": grant.GetPrincipal().GetId().GetResourceType(),
				"principal_resource_id":      grant.GetPrincipal().GetId().GetResource(),
				"is_expandable":              isExpandable,
				"needs_expansion":            needsExpansion,
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

// grantExpandableColumns returns (is_expandable, needs_expansion).
// is_expandable is 1 if the grant has a valid GrantExpandable annotation, 0 otherwise.
func grantExpandableColumns(grant *v2.Grant) (int, int) {
	annos := annotations.Annotations(grant.GetAnnotations())
	expandable := &v2.GrantExpandable{}
	ok, err := annos.Pick(expandable)
	if err != nil || !ok || len(expandable.GetEntitlementIds()) == 0 {
		return 0, 0
	}

	// Check that there's at least one non-whitespace entitlement ID.
	for _, id := range expandable.GetEntitlementIds() {
		if strings.TrimSpace(id) != "" {
			// On initial insert, we want expandable grants to be picked up by expansion.
			// On updates, bulkPutGrants* preserves needs_expansion unless is_expandable changes.
			return 1, 1
		}
	}
	return 0, 0
}

func backfillGrantExpandableColumns(ctx context.Context, db *goqu.Database, tableName string) error {
	// Scan for rows that contain "GrantExpandable" in the proto blob but haven't been
	// backfilled yet (is_expandable=0). The LIKE filter skips the 99%+ of rows that
	// don't have expandable annotations, making this fast even on large tables.
	for {
		rows, err := db.QueryContext(ctx, fmt.Sprintf(
			`SELECT id, data FROM %s 
			 WHERE is_expandable=0 AND data LIKE '%%GrantExpandable%%' 
			 LIMIT 1000`,
			tableName,
		))
		if err != nil {
			return err
		}

		type row struct {
			id   int64
			data []byte
		}
		batch := make([]row, 0, 1000)
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.data); err != nil {
				_ = rows.Close()
				return err
			}
			batch = append(batch, r)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		_ = rows.Close()

		if len(batch) == 0 {
			return nil
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(
			`UPDATE %s SET is_expandable=?, needs_expansion=? WHERE id=?`,
			tableName,
		))
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		for _, r := range batch {
			g := &v2.Grant{}
			if err := proto.Unmarshal(r.data, g); err != nil {
				_ = stmt.Close()
				_ = tx.Rollback()
				return err
			}
			isExpandable, needsExpansion := grantExpandableColumns(g)
			// Only update if we found a valid expandable annotation.
			// Rows with "GrantExpandable" in the blob but no valid annotation are skipped.
			if isExpandable == 0 {
				continue
			}
			if _, err := stmt.ExecContext(ctx, isExpandable, needsExpansion, r.id); err != nil {
				_ = stmt.Close()
				_ = tx.Rollback()
				return err
			}
		}

		_ = stmt.Close()
		if err := tx.Commit(); err != nil {
			return err
		}
	}
}

func bulkPutGrants(
	ctx context.Context, c *C1File,
	tableName string,
	extractFields func(m *v2.Grant) (goqu.Record, error),
	msgs ...*v2.Grant,
) error {
	return bulkPutGrantsInternal(ctx, c, tableName, extractFields, false, msgs...)
}

func bulkPutGrantsIfNewer(
	ctx context.Context, c *C1File,
	tableName string,
	extractFields func(m *v2.Grant) (goqu.Record, error),
	msgs ...*v2.Grant,
) error {
	return bulkPutGrantsInternal(ctx, c, tableName, extractFields, true, msgs...)
}

func bulkPutGrantsInternal(
	ctx context.Context, c *C1File,
	tableName string,
	extractFields func(m *v2.Grant) (goqu.Record, error),
	ifNewer bool,
	msgs ...*v2.Grant,
) error {
	if len(msgs) == 0 {
		return nil
	}
	ctx, span := tracer.Start(ctx, "C1File.bulkPutGrants")
	defer span.End()

	if err := c.validateSyncDb(ctx); err != nil {
		return err
	}

	// Prepare rows.
	rows, err := prepareConnectorObjectRows(c, msgs, extractFields)
	if err != nil {
		return err
	}

	// needs_expansion should only flip to 1 when is_expandable changes.
	// If a grant is no longer expandable (is_expandable=0), needs_expansion should be forced to 0.
	needsExpansionExpr := goqu.L(
		`CASE
			WHEN EXCLUDED.is_expandable = 0 THEN 0
			WHEN EXCLUDED.is_expandable != ?.is_expandable THEN 1
			ELSE ?.needs_expansion
		END`,
		goqu.I(tableName), goqu.I(tableName),
	)

	buildQueryFn := func(insertDs *goqu.InsertDataset, chunkedRows []*goqu.Record) (*goqu.InsertDataset, error) {
		update := goqu.Record{
			"data":            goqu.I("EXCLUDED.data"),
			"is_expandable":   goqu.I("EXCLUDED.is_expandable"),
			"needs_expansion": needsExpansionExpr,
		}
		if ifNewer {
			update["discovered_at"] = goqu.I("EXCLUDED.discovered_at")
			return insertDs.
				OnConflict(goqu.DoUpdate("external_id, sync_id", update).Where(
					goqu.L("EXCLUDED.discovered_at > ?.discovered_at", goqu.I(tableName)),
				)).
				Rows(chunkedRows).
				Prepared(true), nil
		}

		return insertDs.
			OnConflict(goqu.DoUpdate("external_id, sync_id", update)).
			Rows(chunkedRows).
			Prepared(true), nil
	}

	return executeChunkedInsert(ctx, c, tableName, rows, buildQueryFn)
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
