package dotc1z

import (
	"context"
	"errors"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

// ErrSyncNotFinished is returned when a rollback targets a sync that has
// not finished. Expansion completeness for an in-progress sync lives in
// the entitlement graph persisted in the sync token, not in the grant
// rows. Deleting expansion output from such a sync would strand that
// graph: a later resume would treat the affected edges as already
// expanded and never regenerate the deleted grants. Rollback only
// operates on finished syncs.
var ErrSyncNotFinished = errors.New("c1z: refusing to roll back expansion on an unfinished sync")

// ErrSyncNotExpanded is returned when a rollback targets a finished sync
// that never ran grant expansion (no supports_diff marker). There is
// nothing to roll back, and a missing marker also means a resumable
// graph was never produced.
var ErrSyncNotExpanded = errors.New("c1z: sync did not run grant expansion (no supports_diff marker); nothing to roll back")

// RollbackResult reports what a rollback changed, or — for a dry run —
// what it would change.
type RollbackResult struct {
	SyncID         string
	DerivedDeleted int
	DryRun         bool
}

// RollbackExpansion deletes the grants the expander created for one
// finished, expanded sync so the expansion can be replayed.
//
// The expander is the only writer that creates rows through the
// preserve-expansion upsert path, so those rows — and only those — carry
// derived_by_expansion=1. Synced grants (including external-resource and
// external-principal grants, which can carry GrantImmutable or
// GrantSources of their own) are written through the replace path and
// stay 0, so this never deletes them. Grants the expander merely mutated
// in place keep their prior 0 and are left untouched; a replay re-runs
// the full expansion and re-applies those source injections idempotently,
// so they need no reverting here.
//
// The delete is a single set-based statement in one transaction, so it
// is all-or-nothing and does not buffer rows in memory.
//
// A c1z whose expansion predates the derived_by_expansion column has all
// rows at 0; rollback then deletes nothing rather than guess which grants
// were derived.
func (c *C1File) RollbackExpansion(ctx context.Context, syncID string, dryRun bool) (*RollbackResult, error) {
	l := ctxzap.Extract(ctx)

	sr, err := c.getSync(ctx, syncID)
	if err != nil {
		return nil, fmt.Errorf("c1z: rollback could not load sync %q: %w", syncID, err)
	}
	if sr.EndedAt == nil {
		return nil, fmt.Errorf("%w: %q", ErrSyncNotFinished, syncID)
	}
	if !sr.SupportsDiff {
		return nil, fmt.Errorf("%w: %q", ErrSyncNotExpanded, syncID)
	}

	if err := c.SetCurrentSync(ctx, syncID); err != nil {
		return nil, err
	}

	tableName := grants.Name()

	var count int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE derived_by_expansion = 1 AND sync_id = ?", tableName)
	if err := c.db.QueryRowContext(ctx, countQuery, syncID).Scan(&count); err != nil {
		return nil, fmt.Errorf("c1z: rollback could not count derived grants: %w", err)
	}

	res := &RollbackResult{SyncID: syncID, DerivedDeleted: count, DryRun: dryRun}
	if dryRun {
		return res, nil
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE derived_by_expansion = 1 AND sync_id = ?", tableName)
	if _, err := tx.ExecContext(ctx, deleteQuery, syncID); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return nil, errors.Join(rbErr, err)
		}
		return nil, fmt.Errorf("c1z: rollback delete failed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	c.dbUpdated = true

	l.Info("c1z: rolled back grant expansion",
		zap.String("sync_id", syncID),
		zap.Int("derived_deleted", res.DerivedDeleted),
	)
	return res, nil
}
