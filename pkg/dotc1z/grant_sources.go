package dotc1z

import (
	"context"
	"fmt"

	"github.com/doug-martin/goqu/v9"
)

const grantSourcesTableVersion = "1"
const grantSourcesTableName = "grant_sources"
const grantSourcesTableSchema = `
create table if not exists %s (
    grant_id integer not null,
    source_entitlement_id text not null,
    sync_id text not null,
    PRIMARY KEY (grant_id, source_entitlement_id)
);
create index if not exists %s on %s (source_entitlement_id, sync_id);
create index if not exists %s on %s (sync_id);`

var grantSources = (*grantSourcesTable)(nil)

var _ tableDescriptor = (*grantSourcesTable)(nil)

type grantSourcesTable struct{}

func (r *grantSourcesTable) Version() string {
	return grantSourcesTableVersion
}

func (r *grantSourcesTable) Name() string {
	return fmt.Sprintf("v%s_%s", r.Version(), grantSourcesTableName)
}

func (r *grantSourcesTable) Schema() (string, []any) {
	return grantSourcesTableSchema, []any{
		r.Name(),
		fmt.Sprintf("idx_grant_sources_source_ent_v%s", r.Version()),
		r.Name(),
		fmt.Sprintf("idx_grant_sources_sync_id_v%s", r.Version()),
		r.Name(),
	}
}

func (r *grantSourcesTable) Migrations(ctx context.Context, db *goqu.Database) error {
	return nil
}
