package dotc1z

import (
	"context"
	"time"

	"github.com/doug-martin/goqu/v9"
)

// ClearAssets removes all assets from the database.
func (c *C1File) ClearAssets(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "C1File.ClearAssets")
	defer span.End()

	err := c.validateDb(ctx)
	if err != nil {
		return err
	}

	q := c.db.Delete(assets.Name())
	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	c.dbUpdated = true
	return nil
}

// ClearAllSessions removes all session data from the database.
func (c *C1File) ClearAllSessions(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "C1File.ClearAllSessions")
	defer span.End()

	err := c.validateDb(ctx)
	if err != nil {
		return err
	}

	q := c.db.Delete(sessionStore.Name())
	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	c.dbUpdated = true
	return nil
}

// ClearSyncRunTimestamps clears all timestamps from sync runs.
// This removes timing information that could potentially be used to identify when syncs occurred.
func (c *C1File) ClearSyncRunTimestamps(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "C1File.ClearSyncRunTimestamps")
	defer span.End()

	err := c.validateDb(ctx)
	if err != nil {
		return err
	}

	// Clear timestamps by setting them to a fixed epoch value
	epoch := time.Unix(0, 0).Format("2006-01-02 15:04:05.999999999")
	q := c.db.Update(syncRuns.Name())
	q = q.Set(goqu.Record{
		"started_at": epoch,
		"ended_at":   epoch,
	})

	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}

	_, err = c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}

	c.dbUpdated = true
	return nil
}
