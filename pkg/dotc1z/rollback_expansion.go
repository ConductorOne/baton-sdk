package dotc1z

import (
	"context"
	"errors"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// ErrSyncNotFinished is returned when a rollback targets a sync that has
// not finished. Expansion completeness for an in-progress sync lives in
// the entitlement graph persisted in the sync token, not in the grant
// rows. Deleting expansion output from such a sync would strand that
// graph: a later resume would treat the affected edges as already
// expanded and never regenerate the deleted grants. Rollback therefore
// only operates on finished syncs.
var ErrSyncNotFinished = errors.New("c1z: refusing to roll back expansion on an unfinished sync")

// RollbackResult reports what a rollback changed, or — for a dry run —
// what it would change.
type RollbackResult struct {
	SyncID         string
	DerivedDeleted int
	SourcesCleared int
	DryRun         bool
}

// RollbackExpansion reverts grant expansion for one finished sync so it
// can be replayed.
//
// Expansion writes the c1z two ways: it ADDS derived grants — each
// carrying a GrantImmutable annotation plus a GrantSources map — and it
// INJECTS source entries into the GrantSources of pre-existing grants it
// reaches. Rollback deletes the former (identified by GrantImmutable)
// and clears the GrantSources of the latter, so a replay reclassifies
// every base grant from a clean state. The expandable originals
// (needs_expansion=1, GrantExpandable in the expansion column) are left
// untouched, so expansion can run again over the same source set.
//
// All mutations run in one transaction so a crash leaves the c1z either
// fully rolled back or unchanged, never half-deleted.
//
// Source-attribution caveat: without per-entry provenance the GrantSources
// of a surviving grant cannot be split into connector-original vs
// expansion-injected, so the whole map is cleared. A connector that emits
// direct grants carrying GrantSources would lose them here; that is
// acceptable because replay regenerates expansion-derived sources and
// because the command only ever writes to a separate output file.
func (c *C1File) RollbackExpansion(ctx context.Context, syncID string, dryRun bool) (*RollbackResult, error) {
	l := ctxzap.Extract(ctx)

	sr, err := c.getSync(ctx, syncID)
	if err != nil {
		return nil, fmt.Errorf("c1z: rollback could not load sync %q: %w", syncID, err)
	}
	if sr.EndedAt == nil {
		return nil, fmt.Errorf("%w: %q", ErrSyncNotFinished, syncID)
	}

	if err := c.SetCurrentSync(ctx, syncID); err != nil {
		return nil, err
	}

	tableName := grants.Name()

	// Read + classify. No mutation here, so this path is safe on a
	// read-only handle (the dry-run case).
	var toDelete []string
	type unmutated struct {
		externalID string
		data       []byte
	}
	var toClear []unmutated

	selectQuery := fmt.Sprintf("SELECT external_id, data FROM %s WHERE sync_id = ?", tableName)
	rows, err := c.db.QueryContext(ctx, selectQuery, syncID)
	if err != nil {
		return nil, fmt.Errorf("c1z: rollback could not scan grants: %w", err)
	}
	scanErr := func() error {
		defer rows.Close()
		for rows.Next() {
			var externalID string
			var data []byte
			if err := rows.Scan(&externalID, &data); err != nil {
				return err
			}
			g := &v2.Grant{}
			if err := proto.Unmarshal(data, g); err != nil {
				l.Warn("c1z: rollback skipping unreadable grant", zap.String("external_id", externalID), zap.Error(err))
				continue
			}
			switch {
			case hasGrantImmutable(g):
				toDelete = append(toDelete, externalID)
			case len(g.GetSources().GetSources()) > 0:
				clone := proto.Clone(g).(*v2.Grant)
				clone.SetSources(nil)
				cleared, err := protoMarshaler.Marshal(clone)
				if err != nil {
					return err
				}
				toClear = append(toClear, unmutated{externalID: externalID, data: cleared})
			}
		}
		return rows.Err()
	}()
	if scanErr != nil {
		return nil, fmt.Errorf("c1z: rollback classification failed: %w", scanErr)
	}

	res := &RollbackResult{
		SyncID:         syncID,
		DerivedDeleted: len(toDelete),
		SourcesCleared: len(toClear),
		DryRun:         dryRun,
	}
	if dryRun {
		return res, nil
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE external_id = ? AND sync_id = ?", tableName)
	updateQuery := fmt.Sprintf("UPDATE %s SET data = ? WHERE external_id = ? AND sync_id = ?", tableName)
	txErr := func() error {
		for _, id := range toDelete {
			if _, err := tx.ExecContext(ctx, deleteQuery, id, syncID); err != nil {
				return err
			}
		}
		for _, u := range toClear {
			if _, err := tx.ExecContext(ctx, updateQuery, u.data, u.externalID, syncID); err != nil {
				return err
			}
		}
		return nil
	}()
	if txErr != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return nil, errors.Join(rbErr, txErr)
		}
		return nil, fmt.Errorf("c1z: rollback transaction failed: %w", txErr)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	c.dbUpdated = true

	l.Info("c1z: rolled back grant expansion",
		zap.String("sync_id", syncID),
		zap.Int("derived_deleted", res.DerivedDeleted),
		zap.Int("sources_cleared", res.SourcesCleared),
	)
	return res, nil
}

// hasGrantImmutable reports whether the grant carries a GrantImmutable
// annotation, which the expander stamps on every grant it creates (and
// on no grant it merely mutates), making it the discriminator for an
// expansion-derived row.
func hasGrantImmutable(grant *v2.Grant) bool {
	immutable := &v2.GrantImmutable{}
	for _, a := range grant.GetAnnotations() {
		if a.MessageIs(immutable) {
			return true
		}
	}
	return false
}
