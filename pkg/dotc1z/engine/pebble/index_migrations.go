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
		// Backfill the by_entitlement_principal_hash index and the
		// per-entitlement merkle trees for files written before either
		// existed. Idempotent: re-emitting an index entry is a Set over
		// the same key/value, and each tree rebuild range-clears the
		// entitlement's typeMerkle keyspace before writing — which is
		// also what makes the version bump effective: v1 (sha256-fold,
		// root+leaves) nodes are byte-length-identical to v2 (XOR,
		// all-levels) nodes, so only the clear removes them.
		// New files persist this version at their initial (empty) Open,
		// so the inline write path maintains both and the backfill never
		// re-runs for them.
		//
		// v2: XOR combiner, all levels stored sparsely, count on every
		// node (RFC 0003). Pre-GA in-place change; no production v3
		// data existed at v1.
		Name:    "grant_by_entitlement_principal_hash",
		Version: 2,
		Apply: func(ctx context.Context, e *Engine) error {
			return e.backfillGrantHashIndexAndMerkle(ctx)
		},
	},
}

// backfillGrantHashIndexAndMerkle reconstructs the
// by_entitlement_principal_hash index for every grant in every sync,
// then rebuilds the per-entitlement merkle trees. The index must be
// committed before the trees are built because BuildAllMerkleTrees folds
// over the committed index.
func (e *Engine) backfillGrantHashIndexAndMerkle(ctx context.Context) error {
	var syncIDs []string
	if err := e.IterateAllSyncRuns(ctx, func(r *v3.SyncRunRecord) bool {
		syncIDs = append(syncIDs, r.GetSyncId())
		return true
	}); err != nil {
		return fmt.Errorf("backfill hash index: list syncs: %w", err)
	}

	for _, syncID := range syncIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		idBytes, err := codec.EncodeSyncID(syncID)
		if err != nil {
			return err
		}
		// Re-emit the hash index entry for each grant in this sync.
		batch := e.db.NewBatch()
		err = e.IterateGrantsBySync(ctx, syncID, func(r *v3.GrantRecord) bool {
			hk := grantHashIndexKey(idBytes, r)
			if hk == nil {
				return true
			}
			if setErr := batch.Set(hk, grantContentHash(r), nil); setErr != nil {
				err = setErr
				return false
			}
			return true
		})
		if err != nil {
			batch.Close()
			return fmt.Errorf("backfill hash index: sync %q: %w", syncID, err)
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			batch.Close()
			return fmt.Errorf("backfill hash index: commit %q: %w", syncID, err)
		}
		batch.Close()

		if err := e.BuildAllMerkleTrees(ctx, syncID); err != nil {
			return fmt.Errorf("backfill merkle trees: sync %q: %w", syncID, err)
		}
	}
	return nil
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
//	v3 | typeEngineMeta | "idx" | 0x00 | name
//
// Note: unlike the standard keys.go layout this shape has no
// sync_id region — index migrations are engine-global, not
// per-sync.
func encodeIndexAppliedKey(name string) []byte {
	buf := make([]byte, 0, 4+len("idx")+len(name)+4)
	buf = append(buf, versionV3, typeEngineMeta)
	return codec.AppendTupleStrings(buf, "idx", name)
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
