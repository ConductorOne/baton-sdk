package dotc1z

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
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

// rollbackPageSize bounds how many grant rows are read per round so a
// large sync does not buffer the whole table in memory.
const rollbackPageSize = 1000

// RollbackResult reports what a rollback changed, or — for a dry run —
// what it would change.
type RollbackResult struct {
	SyncID string
	// GrantsDeleted is the number of purely expander-derived grants
	// removed: their Sources carried only foreign entitlement keys, never
	// a self-source for their own entitlement.
	GrantsDeleted int
	// SourcesCleared is the number of surviving direct grants whose
	// Sources were reset to empty, restoring the pre-expansion shape.
	SourcesCleared int
	// SuspectConnectorSourced counts grants that had Sources but no
	// self-source AND lacked the GrantImmutable annotation that every
	// expander-created grant carries — a shape that looks connector-set
	// rather than expander-set. By default these are deleted (and counted
	// in GrantsDeleted); with WithPreserveSuspectGrants they are kept
	// instead (counted in SuspectPreserved, not GrantsDeleted). Either way
	// the count is surfaced so a real connector-sourced grant is visible.
	// Zero in the common case.
	SuspectConnectorSourced int
	// SuspectPreserved counts suspect grants kept rather than deleted
	// because WithPreserveSuspectGrants was set. Always 0 in the default
	// (delete) mode; a subset of SuspectConnectorSourced when set.
	SuspectPreserved int
	DryRun           bool
}

// rollbackConfig holds the tunable behavior of a rollback.
type rollbackConfig struct {
	preserveSuspect bool
}

// RollbackOption configures RollbackExpansion.
type RollbackOption func(*rollbackConfig)

// WithPreserveSuspectGrants makes rollback KEEP suspect connector-sourced
// grants (Sources present, no self-source, no GrantImmutable) instead of
// deleting them. Default OFF: a suspect grant is an expander artifact in
// the common case and is deleted, but because Grant.Sources is public
// connector data and replay runs an empty connector, a genuinely
// connector-set grant of that shape would be permanently dropped — this
// option trades a possibly-stale expander grant left in the output for
// never silently dropping real connector data.
func WithPreserveSuspectGrants() RollbackOption {
	return func(c *rollbackConfig) { c.preserveSuspect = true }
}

// queryContexter is the read surface shared by *goqu.Database and
// *goqu.TxDatabase, so the row scan can page over either the live db
// (dry run) or the open transaction (write).
type queryContexter interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// RollbackExpansion restores a finished, expanded sync to its
// pre-grant-expansion shape so the expansion can be replayed and timed.
//
// It is driven entirely by each grant's Sources map (the GrantSources
// provenance that already exists on synced c1z files), so it needs no
// schema change and no sync-write change — it is a subcommand-only
// operation over an existing c1z.
//
// For every grant in the target sync:
//   - No Sources at all → a plain direct grant the expander never
//     touched. Left untouched.
//   - Sources present, but none keyed to the grant's own entitlement →
//     a purely expander-derived grant. Deleted.
//   - Sources present and including a self-source for the grant's own
//     entitlement → a direct grant the expander touched on first use.
//     Kept, but its Sources are cleared to empty: pre-expansion these
//     grants had no Sources, so clearing restores the faithful baseline.
//
// The grant's own entitlement id is read from the entitlement_id column
// rather than from the data blob, because the blob is slimmed on write
// (slimGrantForWrite nils Entitlement/Principal) while the column is
// populated pre-slim. Sources are not slimmed, so they are read from the
// blob.
//
// The deletes, the source clears, and the sync-token reset run in one
// transaction, so the rollback is all-or-nothing.
func (c *C1File) RollbackExpansion(ctx context.Context, syncID string, dryRun bool, opts ...RollbackOption) (*RollbackResult, error) {
	l := ctxzap.Extract(ctx)

	cfg := &rollbackConfig{}
	for _, o := range opts {
		o(cfg)
	}

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

	res := &RollbackResult{SyncID: syncID, DryRun: dryRun}

	if dryRun {
		if err := c.classifyRollback(ctx, c.db, syncID, res, cfg.preserveSuspect, nil, nil); err != nil {
			return nil, err
		}
		return res, nil
	}

	tableName := grants.Name()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	txErr := func() error {
		deleteRow := func(id int64) error {
			_, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = ?", tableName), id)
			return err
		}
		clearRow := func(id int64, data []byte) error {
			_, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET data = ? WHERE id = ?", tableName), data, id)
			return err
		}
		if err := c.classifyRollback(ctx, tx, syncID, res, cfg.preserveSuspect, deleteRow, clearRow); err != nil {
			return err
		}
		// Clearing the sync token leaves the sync ready to be re-expanded.
		// A completed sync's token persists a finished state with an empty
		// action queue; on replay the syncer would resume that token, find
		// nothing to do, and exit without expanding. An empty token instead
		// makes the syncer seed a fresh InitOp, which pushes the grant-
		// expansion step.
		//
		// stats is cleared in the same statement: rollback mutates grant
		// rows, but Stats() returns the cached sync_runs.stats verbatim when
		// present, so without clearing it `baton stats` on a
		// rollback-without-replay output reports the pre-rollback grant
		// count. NULL stats forces Stats()'s slow path to recompute from the
		// rolled-back rows (replay recomputes stats at sync end regardless).
		resetQuery := fmt.Sprintf("UPDATE %s SET sync_token = '', stats = NULL WHERE sync_id = ?", syncRuns.Name())
		if _, err := tx.ExecContext(ctx, resetQuery, syncID); err != nil {
			return err
		}
		return nil
	}()
	if txErr != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return nil, errors.Join(rbErr, txErr)
		}
		return nil, fmt.Errorf("c1z: rollback failed: %w", txErr)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	c.dbUpdated = true
	// The cached view sync run carries the now-stale stats; drop it so the
	// next Stats()/GetSync recomputes against the rolled-back rows.
	c.invalidateCachedViewSyncRun()

	l.Info("c1z: rolled back grant expansion",
		zap.String("sync_id", syncID),
		zap.Int("grants_deleted", res.GrantsDeleted),
		zap.Int("sources_cleared", res.SourcesCleared),
		zap.Int("suspect_connector_sourced", res.SuspectConnectorSourced),
		zap.Int("suspect_preserved", res.SuspectPreserved),
	)
	return res, nil
}

// classifyRollback pages over every grant in the sync, tallies the
// outcome into res, and — when applyDelete/applyClear are non-nil —
// applies the mutation for each row before advancing. Passing nil
// callbacks makes it a counting-only pass for dry runs. Each page is read
// fully and its cursor closed before any mutation runs, so reads and
// writes never share an open cursor on the same connection.
func (c *C1File) classifyRollback(
	ctx context.Context,
	q queryContexter,
	syncID string,
	res *RollbackResult,
	preserveSuspect bool,
	applyDelete func(id int64) error,
	applyClear func(id int64, data []byte) error,
) error {
	tableName := grants.Name()
	unmarshal := proto.UnmarshalOptions{Merge: true, DiscardUnknown: true}

	// Page by external_id, not the rowid id. The grants table has a unique
	// index on (external_id, sync_id) but none on (sync_id, id), so the old
	// `WHERE sync_id = ? AND id > ? ORDER BY id` had no index to seek: on a
	// multi-sync file (a direct dry run over the original c1z) it scanned
	// every grant row — reading each row's data blob — to filter sync_id.
	// `WHERE sync_id = ? AND external_id > ? ORDER BY external_id` drives the
	// scan off the (external_id, sync_id) index: external_id is the leading
	// range/order column and sync_id is resident in the index, so unrelated
	// syncs' rows are filtered at the index without fetching their data
	// blobs. external_id is unique within a sync, so it is a safe page
	// cursor.
	var lastExternalID string
	for {
		type grantRow struct {
			id         int64
			ownEntID   string
			externalID string
			data       []byte
		}
		var batch []grantRow
		if err := func() error {
			query := fmt.Sprintf(
				"SELECT id, entitlement_id, external_id, data FROM %s WHERE sync_id = ? AND external_id > ? ORDER BY external_id LIMIT %d",
				tableName, rollbackPageSize,
			)
			rs, err := q.QueryContext(ctx, query, syncID, lastExternalID)
			if err != nil {
				return err
			}
			defer func() { _ = rs.Close() }()
			batch = make([]grantRow, 0, rollbackPageSize)
			for rs.Next() {
				var r grantRow
				if err := rs.Scan(&r.id, &r.ownEntID, &r.externalID, &r.data); err != nil {
					return err
				}
				batch = append(batch, r)
			}
			return rs.Err()
		}(); err != nil {
			return err
		}

		if len(batch) == 0 {
			break
		}
		lastExternalID = batch[len(batch)-1].externalID

		for _, r := range batch {
			g := &v2.Grant{}
			if err := unmarshal.Unmarshal(r.data, g); err != nil {
				return fmt.Errorf("c1z: rollback could not deserialize grant %d: %w", r.id, err)
			}
			srcMap := g.GetSources().GetSources()
			if len(srcMap) == 0 {
				continue
			}
			if _, hasSelf := srcMap[r.ownEntID]; !hasSelf {
				// Every expander-created grant carries GrantImmutable
				// (newExpandedGrant sets it). A grant that has Sources but
				// no self-source AND lacks GrantImmutable does not look
				// expander-created — it is suspect: possibly connector-set.
				annos := annotations.Annotations(g.GetAnnotations())
				suspect := !annos.Contains(&v2.GrantImmutable{})
				if suspect {
					res.SuspectConnectorSourced++
				}
				// Preserve mode keeps a suspect grant intact (Sources and
				// all) rather than deleting it — never silently drop real
				// connector data. Non-suspect derived grants are always
				// deleted; suspect grants are deleted only in the default
				// mode.
				if suspect && preserveSuspect {
					res.SuspectPreserved++
					continue
				}
				res.GrantsDeleted++
				if applyDelete != nil {
					if err := applyDelete(r.id); err != nil {
						return err
					}
				}
				continue
			}
			res.SourcesCleared++
			if applyClear != nil {
				g.SetSources(nil)
				data, err := protoMarshaler.Marshal(g)
				if err != nil {
					return fmt.Errorf("c1z: rollback could not re-marshal grant %d: %w", r.id, err)
				}
				if err := applyClear(r.id, data); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// GrantSourcesForSync returns, for every grant in the sync, a canonical
// string form of its GrantSources keyed by the grant's external id. It is
// used to validate a rollback+replay round trip: every grant present
// before must be present after with identical Sources, since replay
// re-derives exactly what rollback removed. Pages off the same
// (external_id, sync_id) index the classify scan uses.
func (c *C1File) GrantSourcesForSync(ctx context.Context, syncID string) (map[string]string, error) {
	tableName := grants.Name()
	unmarshal := proto.UnmarshalOptions{Merge: true, DiscardUnknown: true}
	out := map[string]string{}
	var lastExternalID string
	for {
		type row struct {
			externalID string
			data       []byte
		}
		var batch []row
		if err := func() error {
			query := fmt.Sprintf(
				"SELECT external_id, data FROM %s WHERE sync_id = ? AND external_id > ? ORDER BY external_id LIMIT %d",
				tableName, rollbackPageSize,
			)
			rs, err := c.db.QueryContext(ctx, query, syncID, lastExternalID)
			if err != nil {
				return err
			}
			defer func() { _ = rs.Close() }()
			for rs.Next() {
				var r row
				if err := rs.Scan(&r.externalID, &r.data); err != nil {
					return err
				}
				batch = append(batch, r)
			}
			return rs.Err()
		}(); err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		lastExternalID = batch[len(batch)-1].externalID
		for _, r := range batch {
			g := &v2.Grant{}
			if err := unmarshal.Unmarshal(r.data, g); err != nil {
				return nil, fmt.Errorf("c1z: source validation could not deserialize grant %q: %w", r.externalID, err)
			}
			out[r.externalID] = canonicalGrantSources(g)
		}
	}
	return out, nil
}

// canonicalGrantSources renders a grant's Sources as a sorted, stable
// string ("<sourceEntitlementID>=<isDirect>" joined) so map iteration
// order and proto framing never produce a false pre/post divergence.
func canonicalGrantSources(g *v2.Grant) string {
	srcMap := g.GetSources().GetSources()
	if len(srcMap) == 0 {
		return ""
	}
	parts := make([]string, 0, len(srcMap))
	for k, v := range srcMap {
		parts = append(parts, fmt.Sprintf("%s=%t", k, v.GetIsDirect()))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
