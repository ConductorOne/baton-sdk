package pebble

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Secondary-index migration framework.
//
// Each registered indexMigration describes one secondary index by
// name + target version + backfill function. On engine Open the
// applyIndexMigrations pass:
//
//  1. Reads the per-index "applied version" entries from the
//     engine-meta keyspace.
//  2. For any registered migration whose stored version is older
//     than the target, runs the backfill (iterate primaries →
//     emit index entries via writeXxxIndexes).
//  3. Writes the new applied-version on success.
//
// Why per-index versions and not a single combined version:
// adding the Nth secondary index later doesn't re-run earlier
// backfills. The metadata key shape is
//
//	v3 | typeEngineMeta | tup_string("idx") | tup_string(<index_name>) -> uint32 BE
//
// so reopening an older c1z just sees missing keys → all known
// migrations run once and get persisted.

// indexMigration describes one secondary-index version. Add a new
// entry whenever a new secondary index is introduced or an
// existing one's wire shape changes.
type indexMigration struct {
	// Name is the migration's stable identifier. Choose a short,
	// descriptive token (e.g. "grant_needs_expansion"). Stored
	// verbatim in the engine-meta keyspace; renaming an existing
	// migration breaks the upgrade path, so don't.
	Name string

	// Version is the migration's target version. Bump when the
	// index's wire shape changes; the framework will re-run the
	// backfill against the new code. Versions start at 1.
	Version uint32

	// Apply backfills the index. It MUST be idempotent — the
	// framework may invoke it on an engine that already has some
	// (but not all) of the expected index entries on disk.
	// Idiomatic implementations re-emit the entire index by
	// iterating primaries and calling the standard
	// writeXxxIndexes helper, which is naturally idempotent on
	// Pebble (Set on an existing key just overwrites).
	Apply func(ctx context.Context, e *Engine) error
}

// indexMigrations is the canonical migration registry. Append-only;
// adding an entry here means the next engine Open will backfill
// the corresponding index for any existing c1z that doesn't have
// it yet.
var indexMigrations = []indexMigration{
	{
		Name:    "grant_needs_expansion",
		Version: 1,
		Apply:   backfillGrantNeedsExpansion,
	},
}

// applyIndexMigrations runs on engine Open (writable opens only —
// read-only files are immutable on disk). For each registered
// migration whose stored applied-version is older than the
// target, it invokes Apply and persists the new version on
// success. Errors surface to Open; callers can decide whether
// to abort or proceed with the partially-migrated engine.
func (e *Engine) applyIndexMigrations(ctx context.Context) error {
	if e.opts.readOnly {
		return nil
	}
	for _, m := range indexMigrations {
		applied, err := e.readAppliedIndexVersion(m.Name)
		if err != nil {
			return fmt.Errorf("read applied index version %q: %w", m.Name, err)
		}
		if applied >= m.Version {
			continue
		}
		if err := m.Apply(ctx, e); err != nil {
			return fmt.Errorf("apply index migration %q v%d: %w", m.Name, m.Version, err)
		}
		if err := e.writeAppliedIndexVersion(m.Name, m.Version); err != nil {
			return fmt.Errorf("write applied index version %q v%d: %w", m.Name, m.Version, err)
		}
	}
	return nil
}

// encodeIndexAppliedKey returns the engine-meta key for the
// applied-version of an index migration.
//
//	v3 | typeEngineMeta | tup_string("idx") | tup_string(name)
func encodeIndexAppliedKey(name string) []byte {
	buf := make([]byte, 0, 4+len("idx")+len(name)+4)
	buf = append(buf, versionV3, typeEngineMeta)
	buf = codec.AppendTupleString(buf, "idx")
	buf = codec.AppendTupleSeparator(buf)
	buf = codec.AppendTupleString(buf, name)
	return buf
}

// readAppliedIndexVersion returns the stored applied-version for
// the named migration. Zero (with nil error) means "no record
// found" — the migration is unapplied.
func (e *Engine) readAppliedIndexVersion(name string) (uint32, error) {
	val, closer, err := e.db.Get(encodeIndexAppliedKey(name))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()
	if len(val) != 4 {
		return 0, fmt.Errorf("malformed applied-version blob: %d bytes, want 4", len(val))
	}
	return binary.BigEndian.Uint32(val), nil
}

// writeAppliedIndexVersion stores the applied-version for the
// named migration. Uses pebble.Sync — these writes are rare and
// must survive a crash so we don't re-run migrations on every
// open.
func (e *Engine) writeAppliedIndexVersion(name string, version uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], version)
	return e.db.Set(encodeIndexAppliedKey(name), buf[:], pebble.Sync)
}

// backfillGrantNeedsExpansion is the migration for the
// idxGrantByNeedsExpansion keyspace. Walks every sync_run record,
// iterates its primary grants, and re-emits the full set of index
// entries via writeGrantIndexes — which is idempotent (Set is a
// no-op-or-overwrite). For grants whose NeedsExpansion=true the
// needs_expansion index key lands; for the rest writeGrantIndexes
// is a cheap pass that re-Sets the entitlement and principal
// index keys (already present, so a wasted Set but no incorrect
// state).
func backfillGrantNeedsExpansion(ctx context.Context, e *Engine) error {
	syncIDs, err := collectSyncIDs(ctx, e)
	if err != nil {
		return err
	}
	for _, syncID := range syncIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		idBytes, err := codec.EncodeSyncID(syncID)
		if err != nil {
			return fmt.Errorf("encode sync_id %q: %w", syncID, err)
		}
		batch := e.db.NewBatch()
		count := 0
		var loopErr error
		if iterErr := e.IterateGrantsBySync(ctx, syncID, func(r *v3.GrantRecord) bool {
			if loopErr = ctx.Err(); loopErr != nil {
				return false
			}
			if loopErr = e.writeGrantIndexes(batch, idBytes, r); loopErr != nil {
				return false
			}
			count++
			if count >= migrationBatchKeys {
				if loopErr = batch.Commit(pebble.Sync); loopErr != nil {
					return false
				}
				_ = batch.Close()
				batch = e.db.NewBatch()
				count = 0
			}
			return true
		}); iterErr != nil {
			_ = batch.Close()
			return fmt.Errorf("iterate grants for sync %q: %w", syncID, iterErr)
		}
		if loopErr != nil {
			_ = batch.Close()
			return loopErr
		}
		if !batch.Empty() {
			if err := batch.Commit(pebble.Sync); err != nil {
				_ = batch.Close()
				return err
			}
		}
		_ = batch.Close()
	}
	return nil
}

// migrationBatchKeys caps per-batch memory during backfill. The
// same chunking constant as CloneSync's copyRange.
const migrationBatchKeys = 10_000

// collectSyncIDs returns every sync_id present in the engine.
// Used by migrations that need to walk every primary-record range.
func collectSyncIDs(ctx context.Context, e *Engine) ([]string, error) {
	out := []string{}
	if err := e.IterateAllSyncRuns(ctx, func(r *v3.SyncRunRecord) bool {
		out = append(out, r.GetSyncId())
		return true
	}); err != nil {
		return nil, err
	}
	return out, nil
}
