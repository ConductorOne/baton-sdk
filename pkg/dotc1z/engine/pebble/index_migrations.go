package pebble

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

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
//
// Deliberately empty today. The by_entitlement_principal_hash index +
// grant digests are NOT backfilled at Open: they are rebuilt from the
// primaries at every seal (the fused deferred pass / BuildGrantDigests),
// so a file that predates them simply has no digests — readers see
// "missing, recalculate" (never a wrong answer) and the file's next
// sync seals them in. Open-time grant backfills have historically been incident
// bait (unbounded latency and memory at Open on large files); prefer
// seal-time derivation or explicit rebuild commands over registering
// one here.
var indexMigrations []indexMigration

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
	return e.db.MetaSet(encodeIndexAppliedKey(name), buf[:], pebble.Sync)
}
