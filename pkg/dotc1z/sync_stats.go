package dotc1z

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/doug-martin/goqu/v9"
)

// v1_sync_stats — per-sync row populated at endSyncRun with the
// counts every Stats() / GrantStats() call needs. Without this
// table, those callers paid O(N) COUNT(*) + GROUP BY queries on
// every invocation; the row makes them O(1) reads.
//
// Schema: sync_id (PK) + integer counts + two JSON-encoded maps
// for the per-resource-type breakdowns. JSON instead of proto
// keeps SQLite path's dependency surface tight (no v3 proto
// import in the dotc1z package).

const syncStatsTableVersion = "1"
const syncStatsTableName = "sync_stats"
const syncStatsTableSchema = `
create table if not exists %s (
    sync_id text primary key,
    resource_types integer not null default 0,
    resources integer not null default 0,
    entitlements integer not null default 0,
    grants integer not null default 0,
    assets integer not null default 0,
    resources_by_resource_type text,                       -- JSON: {rt_id: count}
    grants_by_entitlement_resource_type text,              -- JSON: {rt_id: count}
    written_at datetime not null
);`

var syncStatsTable = (*syncStatsTableImpl)(nil)

var _ tableDescriptor = (*syncStatsTableImpl)(nil)

type syncStatsTableImpl struct{}

func (r *syncStatsTableImpl) Version() string { return syncStatsTableVersion }
func (r *syncStatsTableImpl) Name() string {
	return fmt.Sprintf("v%s_%s", r.Version(), syncStatsTableName)
}
func (r *syncStatsTableImpl) Schema() (string, []any) {
	return syncStatsTableSchema, []any{r.Name()}
}
func (r *syncStatsTableImpl) Migrations(ctx context.Context, db *goqu.Database) (bool, error) {
	return false, nil
}

// upsertSyncStats writes a stats row for syncID, computing the
// counts in a single pass over the per-table aggregations.
// Idempotent: a syncID that already has a row gets overwritten.
//
// Failures here are non-fatal at the caller (endSyncRun); Stats()
// falls back to the legacy COUNT(*)/GROUP BY path when the row is
// missing.
func (c *C1File) upsertSyncStats(ctx context.Context, syncID string) error {
	if syncID == "" {
		return fmt.Errorf("upsertSyncStats: empty sync_id")
	}

	// Use the same aggregate query family that the legacy Stats path
	// runs. One scan per table; relies on the existing sync_id
	// columns + (external_id, sync_id) unique indexes.
	resourceCounts, err := c.countBySyncAndResourceType(ctx, resources.Name(), syncID)
	if err != nil {
		return fmt.Errorf("count resources: %w", err)
	}
	grantCounts, err := c.countBySyncAndResourceType(ctx, grants.Name(), syncID)
	if err != nil {
		return fmt.Errorf("count grants: %w", err)
	}

	var rtTotal int64
	rtRows, err := c.db.From(resourceTypes.Name()).Where(goqu.C("sync_id").Eq(syncID)).CountContext(ctx)
	if err != nil {
		return fmt.Errorf("count resource_types: %w", err)
	}
	rtTotal = rtRows

	var resourceTotal int64
	for _, n := range resourceCounts {
		resourceTotal += n
	}

	entitlementTotal, err := c.db.From(entitlements.Name()).Where(goqu.C("sync_id").Eq(syncID)).CountContext(ctx)
	if err != nil {
		return fmt.Errorf("count entitlements: %w", err)
	}

	var grantTotal int64
	for _, n := range grantCounts {
		grantTotal += n
	}

	assetTotal, err := c.db.From(assets.Name()).Where(goqu.C("sync_id").Eq(syncID)).CountContext(ctx)
	if err != nil {
		return fmt.Errorf("count assets: %w", err)
	}

	resourcesByRTJSON, err := json.Marshal(resourceCounts)
	if err != nil {
		return fmt.Errorf("marshal resourcesByRT: %w", err)
	}
	grantsByRTJSON, err := json.Marshal(grantCounts)
	if err != nil {
		return fmt.Errorf("marshal grantsByRT: %w", err)
	}

	insert := c.db.Insert(syncStatsTable.Name()).Prepared(true).Rows(goqu.Record{
		"sync_id":                             syncID,
		"resource_types":                      rtTotal,
		"resources":                           resourceTotal,
		"entitlements":                        entitlementTotal,
		"grants":                              grantTotal,
		"assets":                              assetTotal,
		"resources_by_resource_type":          string(resourcesByRTJSON),
		"grants_by_entitlement_resource_type": string(grantsByRTJSON),
		"written_at":                          time.Now().UTC(),
	}).OnConflict(goqu.DoUpdate("sync_id", goqu.Record{
		"resource_types":                      rtTotal,
		"resources":                           resourceTotal,
		"entitlements":                        entitlementTotal,
		"grants":                              grantTotal,
		"assets":                              assetTotal,
		"resources_by_resource_type":          string(resourcesByRTJSON),
		"grants_by_entitlement_resource_type": string(grantsByRTJSON),
		"written_at":                          time.Now().UTC(),
	}))

	q, args, err := insert.ToSQL()
	if err != nil {
		return fmt.Errorf("upsertSyncStats: build SQL: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("upsertSyncStats: exec: %w", err)
	}
	return nil
}

// cachedSyncStats reads the v1_sync_stats row for syncID. Returns
// (nil, nil) when no row exists (legacy c1z, or sync didn't go
// through endSyncRun). Stats() falls back to the legacy COUNT(*)
// path on a miss.
type cachedSyncStats struct {
	syncID                          string
	resourceTypes                   int64
	resources                       int64
	entitlements                    int64
	grants                          int64
	assets                          int64
	resourcesByResourceType         map[string]int64
	grantsByEntitlementResourceType map[string]int64
}

func (c *C1File) readCachedSyncStats(ctx context.Context, syncID string) (*cachedSyncStats, error) {
	q, args, err := c.db.From(syncStatsTable.Name()).Prepared(true).Where(goqu.C("sync_id").Eq(syncID)).ToSQL()
	if err != nil {
		return nil, err
	}
	row := c.db.QueryRowContext(ctx, q, args...)
	var (
		s                    cachedSyncStats
		resourcesByRTJSON    string
		grantsByRTJSON       string
		writtenAtIgnoredHere time.Time
	)
	if scanErr := row.Scan(
		&s.syncID,
		&s.resourceTypes,
		&s.resources,
		&s.entitlements,
		&s.grants,
		&s.assets,
		&resourcesByRTJSON,
		&grantsByRTJSON,
		&writtenAtIgnoredHere,
	); scanErr != nil {
		// sql.ErrNoRows OR a real error — let the caller fall back
		// to the legacy path either way. We avoid distinguishing
		// here to keep the contract simple: nil means "no fast
		// path available."
		return nil, nil //nolint:nilerr // intentional fall-through to legacy aggregate path
	}
	if resourcesByRTJSON != "" {
		if err := json.Unmarshal([]byte(resourcesByRTJSON), &s.resourcesByResourceType); err != nil {
			return nil, fmt.Errorf("decode resourcesByRT: %w", err)
		}
	}
	if grantsByRTJSON != "" {
		if err := json.Unmarshal([]byte(grantsByRTJSON), &s.grantsByEntitlementResourceType); err != nil {
			return nil, fmt.Errorf("decode grantsByRT: %w", err)
		}
	}
	return &s, nil
}
