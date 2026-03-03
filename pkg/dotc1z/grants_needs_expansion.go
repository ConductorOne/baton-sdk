package dotc1z

import (
	"context"
	"fmt"

	"github.com/doug-martin/goqu/v9"
)

// SetNeedsExpansionForGrants sets needs_expansion for the provided grant external IDs in the given sync.
func (c *C1File) SetNeedsExpansionForGrants(ctx context.Context, syncID string, grantExternalIDs []string, needsExpansion bool) error {
	ctx, span := tracer.Start(ctx, "C1File.SetNeedsExpansionForGrants")
	defer span.End()

	if c.readOnly {
		return ErrReadOnly
	}
	if err := c.validateDb(ctx); err != nil {
		return err
	}
	if len(grantExternalIDs) == 0 {
		return nil
	}

	q := c.db.Update(grants.Name()).Prepared(true)
	q = q.Set(goqu.Record{"needs_expansion": needsExpansion})
	q = q.Where(goqu.C("sync_id").Eq(syncID))
	q = q.Where(goqu.C("external_id").In(grantExternalIDs))

	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}
	if _, err := c.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("set needs_expansion: %w", err)
	}
	c.dbUpdated = true
	return nil
}

// ClearNeedsExpansionForSync clears needs_expansion for all grants in the given sync.
func (c *C1File) ClearNeedsExpansionForSync(ctx context.Context, syncID string) error {
	ctx, span := tracer.Start(ctx, "C1File.ClearNeedsExpansionForSync")
	defer span.End()

	if c.readOnly {
		return ErrReadOnly
	}
	if err := c.validateDb(ctx); err != nil {
		return err
	}

	q := c.db.Update(grants.Name()).Prepared(true)
	q = q.Set(goqu.Record{"needs_expansion": 0})
	q = q.Where(goqu.C("sync_id").Eq(syncID))
	q = q.Where(goqu.C("needs_expansion").Eq(1))

	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}
	if _, err := c.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("clear needs_expansion: %w", err)
	}
	c.dbUpdated = true
	return nil
}
