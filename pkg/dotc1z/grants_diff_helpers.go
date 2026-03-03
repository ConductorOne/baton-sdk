package dotc1z

import (
	"context"

	"github.com/doug-martin/goqu/v9"
)

// ListDistinctEntitlementIDsForSync returns the set of entitlement IDs that appear in v1_entitlements for the given sync.
func (c *C1File) ListDistinctEntitlementIDsForSync(ctx context.Context, syncID string) ([]string, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListDistinctEntitlementIDsForSync")
	defer span.End()

	if err := c.validateDb(ctx); err != nil {
		return nil, err
	}

	q := c.db.From(entitlements.Name()).Prepared(true)
	q = q.Select("external_id").Distinct()
	q = q.Where(goqu.C("sync_id").Eq(syncID))

	query, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id != "" {
			out = append(out, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
