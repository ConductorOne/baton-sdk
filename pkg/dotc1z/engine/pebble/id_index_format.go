package pebble

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// id-index format stamps. 0 (no stamp) is the legacy external-id key
// layout; 1 was an interim escaped-identity layout that never shipped
// (dev artifacts only) and is rejected loudly; 2 is the current
// raw-component identity layout (flag/tail byte-prefix compression, see
// identity.go).
const (
	idIndexFormatStructuredV1 uint32 = 1
	idIndexFormatRawTupleV2   uint32 = 2

	idIndexFormatCurrent = idIndexFormatRawTupleV2
)

func encodeIDIndexFormatKey() []byte {
	buf := make([]byte, 0, 2+len("grant_entitlement_id_format"))
	buf = append(buf, versionV3, typeEngineMeta)
	return codec.AppendTupleStrings(buf, "grant_entitlement_id_format")
}

// The deferred-index rebuild marker (rawdb.DeferredIdxPendingKey) and
// its arm/clear/restore contract moved to rawdb, which owns the
// deferred-index crash state (see rawdb.ArmDeferredGrantIndex).

// encodeGrantDigestBuildPendingKey is the durable marker that a grant
// digest build is mid-flight: armed (fsync'd) before the build's first
// digest-node commit, cleared only after the whole build — merge, hash
// index ingest, zero-grant root backfill — has completed
// (buildGrantDigestsFromSpill), and consumed by the drop that restores
// the safe "digests absent" state (dropAllGrantDigestStateLocked).
//
// It exists because the build commits digest nodes in bounded batches
// DURING the merge and again at fold.finish() — including the global
// root — before the hash-index IngestAndExcise. Committed WAL writes
// survive a process kill, so a crash inside that window leaves
// correct-looking digest roots durable over a hash index that was never
// ingested; a resumed EndSync's RepairMissingGrantDigests would trust
// every surviving root (or fast-path on the global root) and seal the
// file with digests over an empty index — the silent
// skip-entitlements-at-uplift failure mode. The fused seal pass is
// additionally protected by deferredIdxPending (a crash reruns the full
// build), but the standalone BuildGrantDigests path has no such marker,
// and this one covers both. Marker present at Open (or at repair) →
// drop all digest state first: every crash converts to "digests
// absent", which present-means-exact (digest.go) already treats as safe.
func encodeGrantDigestBuildPendingKey() []byte {
	buf := make([]byte, 0, 2+len("grant_digest_build_pending"))
	buf = append(buf, versionV3, typeEngineMeta)
	return codec.AppendTupleStrings(buf, "grant_digest_build_pending")
}

func (e *Engine) readIDIndexFormat() (uint32, error) {
	val, closer, err := e.db.Get(encodeIDIndexFormatKey())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()
	if len(val) != 4 {
		return 0, fmt.Errorf("malformed id-index-format blob: %d bytes, want 4", len(val))
	}
	return binary.BigEndian.Uint32(val), nil
}

func (e *Engine) writeIDIndexFormat(version uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], version)
	return e.db.MetaSet(encodeIDIndexFormatKey(), buf[:], pebble.Sync)
}

func (e *Engine) verifyOrStampIDIndexFormat(ctx context.Context) error {
	version, err := e.readIDIndexFormat()
	if err != nil {
		return err
	}
	if version == idIndexFormatCurrent {
		return nil
	}
	if version == idIndexFormatStructuredV1 {
		return fmt.Errorf("pebble: unsupported grant/entitlement id-index format v%d (interim pre-release layout); regenerate this c1z", version)
	}
	if version != 0 {
		return fmt.Errorf("pebble: unsupported grant/entitlement id-index format v%d", version)
	}
	empty, err := e.isDataKeyspaceEmpty()
	if err != nil {
		return err
	}
	if e.opts.readOnly {
		if empty {
			return nil
		}
		// A read-only engine cannot migrate in place, and current readers
		// would silently miss every point lookup against legacy keys. The
		// dotc1z store layer avoids this by running a writable migration
		// pre-open on its unpacked temp copy; a direct read-only Open of a
		// legacy dir must fail loudly instead.
		return errors.New("pebble: this file uses the legacy grant/entitlement id layout; a writable open is required to migrate it")
	}
	if !empty {
		return e.migrateIDIndexFormatToStructuredV1(ctx)
	}
	return e.writeIDIndexFormat(idIndexFormatCurrent)
}

func (e *Engine) isDataKeyspaceEmpty() (bool, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{versionV3, typeResourceType},
		UpperBound: []byte{versionV3, typeEngineMeta},
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()
	return !iter.First(), iter.Error()
}

func (e *Engine) manifestIDIndexFormat() c1zv3.PebbleIdIndexFormat {
	version, err := e.readIDIndexFormat()
	if err != nil {
		return c1zv3.PebbleIdIndexFormat_PEBBLE_ID_INDEX_FORMAT_UNSPECIFIED
	}
	switch version {
	// The advisory manifest enum value predates the raw-tuple layout; since
	// no file with the interim v1 layout ever shipped, STRUCTURED_V1 simply
	// means "structured identity keys" (the current raw-tuple layout).
	case idIndexFormatCurrent:
		return c1zv3.PebbleIdIndexFormat_PEBBLE_ID_INDEX_FORMAT_STRUCTURED_V1
	case 0:
		return c1zv3.PebbleIdIndexFormat_PEBBLE_ID_INDEX_FORMAT_LEGACY_EXTERNAL_ID
	default:
		return c1zv3.PebbleIdIndexFormat_PEBBLE_ID_INDEX_FORMAT_UNSPECIFIED
	}
}
