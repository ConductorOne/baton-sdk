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
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
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

// rollbackPageSize bounds how many grant rows are read per round, and — since
// each page's mutations commit in their own transaction — also bounds the
// per-transaction write-ahead-log footprint. It is a var, not a const, only so
// the page-size benchmark can measure alternative values; production never
// reassigns it. See BenchmarkRollbackExpansionPageSize for the measurements
// behind this value.
var rollbackPageSize = 1000

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
	preserveSuspect  bool
	rewriteSyncToken func(token string) (string, error)
}

// RollbackOption configures RollbackExpansion.
type RollbackOption func(*rollbackConfig)

// WithSyncTokenRewrite supplies how the sync's persisted token is rewritten
// once the grants are rolled back. The function receives the current token
// and returns the replacement; rollback writes the result verbatim. Without
// it, rollback clears the token to empty, which makes a resumed syncer seed a
// fresh run. A caller that wants to preserve the token's other state passes a
// rewrite that re-marks the sync for expansion instead. Ignored on a dry run.
func WithSyncTokenRewrite(fn func(token string) (string, error)) RollbackOption {
	return func(c *rollbackConfig) { c.rewriteSyncToken = fn }
}

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

// clearOp is a pending source-clear: the grant row id and the re-marshaled
// blob with Sources reset to empty.
type clearOp struct {
	id   int64
	data []byte
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
// The grant mutations run one transaction per page rather than one
// transaction for the whole sync, so the SQLite rollback journal never has
// to hold every changed grant at once — on a large sync the expander-derived
// rows can be most of the table. Writes always go to a fresh clone of the
// sync (the caller never mutates the input), so a failure partway through
// leaves a discardable output rather than a half-rolled-back source file.
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

	// A sync that holds no grants — a resource-only sync, say — has nothing
	// to roll back. Skip the scan and the token rewrite entirely.
	hasGrants, err := c.syncHasGrants(ctx, syncID)
	if err != nil {
		return nil, fmt.Errorf("c1z: rollback could not check sync %q for grants: %w", syncID, err)
	}
	if !hasGrants {
		l.Info("c1z: rollback found no grants in sync; nothing to roll back", zap.String("sync_id", syncID))
		return res, nil
	}

	if dryRun {
		if err := c.classifyRollback(ctx, syncID, res, cfg.preserveSuspect, nil); err != nil {
			return nil, err
		}
		return res, nil
	}

	tableName := grants.Name()
	flush := func(deletes []int64, clears []clearOp) error {
		if len(deletes) == 0 && len(clears) == 0 {
			return nil
		}
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		// Rolls back on every early return; a no-op once Commit succeeds, so it
		// also covers a Commit that fails and leaves the tx open.
		defer func() { _ = tx.Rollback() }()
		for _, id := range deletes {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = ?", tableName), id); err != nil {
				return err
			}
		}
		for _, op := range clears {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET data = ? WHERE id = ?", tableName), op.data, op.id); err != nil {
				return err
			}
		}
		return tx.Commit()
	}
	if err := c.classifyRollback(ctx, syncID, res, cfg.preserveSuspect, flush); err != nil {
		return nil, fmt.Errorf("c1z: rollback failed: %w", err)
	}

	// Leave the sync ready to be re-expanded. A completed sync's token
	// persists a finished state with an empty action queue; on replay the
	// syncer would resume that token, find nothing to do, and exit without
	// expanding. The rewrite (when supplied) re-marks the sync for expansion
	// while preserving the token's other state; without one the token is
	// cleared, which makes the syncer seed a fresh run.
	//
	// stats is cleared at the same time: rollback mutates grant rows, but
	// Stats() returns the cached sync_runs.stats verbatim when present, so
	// without clearing it `baton stats` on a rollback-without-replay output
	// reports the pre-rollback grant count. NULL stats forces Stats()'s slow
	// path to recompute from the rolled-back rows (replay recomputes stats at
	// sync end regardless).
	newToken := ""
	if cfg.rewriteSyncToken != nil {
		newToken, err = cfg.rewriteSyncToken(sr.SyncToken)
		if err != nil {
			return nil, fmt.Errorf("c1z: rollback could not rewrite sync token: %w", err)
		}
	}
	resetQuery := fmt.Sprintf("UPDATE %s SET sync_token = ?, stats = NULL WHERE sync_id = ?", syncRuns.Name())
	if _, err := c.db.ExecContext(ctx, resetQuery, newToken, syncID); err != nil {
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

// syncHasGrants reports whether the sync has at least one grant row, used to
// short-circuit a rollback over a sync that cannot have expansion to undo.
func (c *C1File) syncHasGrants(ctx context.Context, syncID string) (bool, error) {
	var one int
	row := c.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT 1 FROM %s WHERE sync_id = ? LIMIT 1", grants.Name()), syncID)
	switch err := row.Scan(&one); {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

// classifyRollback pages over every grant in the sync, tallies the outcome
// into res, and — when flush is non-nil — hands each page's deletes and
// source-clears to flush before reading the next page. A nil flush makes
// this a counting-only pass for dry runs. Reads always run on the live db,
// never inside a write transaction, and each page's cursor is fully drained
// and closed before flush runs, so reads and writes never share an open
// cursor on the same connection. Because the page cursor only advances
// (external_id strictly increasing), deleting or rewriting already-read rows
// between pages does not disturb the remaining scan.
func (c *C1File) classifyRollback(
	ctx context.Context,
	syncID string,
	res *RollbackResult,
	preserveSuspect bool,
	flush func(deletes []int64, clears []clearOp) error,
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
			rs, err := c.db.QueryContext(ctx, query, syncID, lastExternalID)
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

		var deletes []int64
		var clears []clearOp
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
				if flush != nil {
					deletes = append(deletes, r.id)
				}
				continue
			}
			res.SourcesCleared++
			if flush != nil {
				g.SetSources(nil)
				data, err := protoMarshaler.Marshal(g)
				if err != nil {
					return fmt.Errorf("c1z: rollback could not re-marshal grant %d: %w", r.id, err)
				}
				clears = append(clears, clearOp{id: r.id, data: data})
			}
		}

		if flush != nil {
			if err := flush(deletes, clears); err != nil {
				return err
			}
		}
	}
	return nil
}

// GrantSourcesForSync returns, for every grant in the sync, a canonical
// string form of its GrantSources keyed by the grant's id.
//
// Deprecated: the rollback subcommand now computes this in the command layer.
// This method is retained for external callers that depended on it and will
// be removed in a future minor release. It streams the grants one at a time
// rather than buffering the table.
func (c *C1File) GrantSourcesForSync(ctx context.Context, syncID string) (map[string]string, error) {
	out := map[string]string{}
	for g, err := range c.StreamGrants(ctx, syncID, connectorstore.StreamGrantsOptions{}) {
		if err != nil {
			return nil, err
		}
		out[g.GetId()] = canonicalGrantSources(g)
	}
	return out, nil
}

// canonicalGrantSources renders a grant's Sources as a sorted, stable string
// ("<sourceEntitlementID>=<isDirect>" joined) so map iteration order and
// proto framing never produce a false pre/post divergence.
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
