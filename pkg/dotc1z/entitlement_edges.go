package dotc1z

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/doug-martin/goqu/v9"
)

const entitlementEdgesTableVersion = "1"
const entitlementEdgesTableName = "entitlement_edges"
const entitlementEdgesTableSchema = `
create table if not exists %s (
    id integer primary key,
    src_entitlement_id text not null,
    dst_entitlement_id text not null,
    shallow integer not null,
    resource_type_ids_json text not null,
    edge_key blob not null,
    source_grant_external_id text not null,
    sync_id text not null,
    discovered_at datetime not null
);
create index if not exists %s on %s (sync_id, src_entitlement_id);
create index if not exists %s on %s (sync_id, dst_entitlement_id);
create index if not exists %s on %s (sync_id, source_grant_external_id);
create unique index if not exists %s on %s (sync_id, edge_key);`

var entitlementEdges = (*entitlementEdgesTable)(nil)

type entitlementEdgesTable struct{}

func (r *entitlementEdgesTable) Version() string {
	return entitlementEdgesTableVersion
}

func (r *entitlementEdgesTable) Name() string {
	return fmt.Sprintf("v%s_%s", r.Version(), entitlementEdgesTableName)
}

func (r *entitlementEdgesTable) Schema() (string, []any) {
	return entitlementEdgesTableSchema, []any{
		r.Name(),
		fmt.Sprintf("idx_entitlement_edges_src_v%s", r.Version()),
		r.Name(),
		fmt.Sprintf("idx_entitlement_edges_dst_v%s", r.Version()),
		r.Name(),
		fmt.Sprintf("idx_entitlement_edges_source_grant_v%s", r.Version()),
		r.Name(),
		fmt.Sprintf("idx_entitlement_edges_unique_v%s", r.Version()),
		r.Name(),
	}
}

func (r *entitlementEdgesTable) Migrations(ctx context.Context, db *goqu.Database) error {
	// New table; no migrations yet.
	return nil
}

func makeEdgeKey(srcEntitlementID, dstEntitlementID string, shallow bool, resourceTypeIDs []string) ([]byte, string, error) {
	// Use a stable JSON encoding for the resource type IDs (sorted at call sites).
	rtJSON, err := json.Marshal(resourceTypeIDs)
	if err != nil {
		return nil, "", err
	}
	payload := append([]byte(srcEntitlementID+"\x00"+dstEntitlementID+"\x00"), rtJSON...)
	if shallow {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}
	h := sha256.Sum256(payload)
	return h[:], string(rtJSON), nil
}
