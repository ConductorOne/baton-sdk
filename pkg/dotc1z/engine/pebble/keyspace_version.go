package pebble

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// keyspaceVersion is the on-disk key-layout version this SDK writes and
// reads.
//
//   - v2 (current): single-sync layout. Keys carry no sync_id; the file
//     holds exactly one sync.
//   - v1 (implicit, never stamped): the original multi-sync layout that
//     embedded a raw 20-byte sync_id between the fixed header and the
//     tuple tail.
//
// The two layouts differ by a 21-byte region (sync_id + separator), so
// decoding a v1 file with the v2 encoders mis-aligns every tail and
// surfaces as empty/garbage reads rather than an error. The stamp turns
// that silent failure into a loud one at Open. Bump this constant on any
// future incompatible key-encoding change.
const keyspaceVersion uint32 = 2

// encodeKeyspaceVersionKey is the fixed engine-meta key holding the
// keyspace layout version:
//
//	v3 | typeEngineMeta | "keyspace_version"
//
// Engine-global (no sync_id), same shape as encodeIndexAppliedKey.
func encodeKeyspaceVersionKey() []byte {
	buf := make([]byte, 0, 2+len("keyspace_version"))
	buf = append(buf, versionV3, typeEngineMeta)
	return codec.AppendTupleStrings(buf, "keyspace_version")
}

// verifyOrStampKeyspaceVersion enforces the key-layout contract on Open:
//
//   - stamp present and == keyspaceVersion: ok.
//   - stamp absent on an empty DB (fresh create, writable): stamp it.
//   - stamp absent on a non-empty DB (an old v1 multi-sync file), or any
//     other value: refuse, because the current encoders would silently
//     mis-decode the keys.
//
// Read-only opens never stamp; an empty read-only DB is allowed through
// (there is nothing to mis-decode, and reads simply resolve no sync).
func (e *Engine) verifyOrStampKeyspaceVersion(ctx context.Context) error {
	val, closer, err := e.db.Get(encodeKeyspaceVersionKey())
	switch {
	case err == nil:
		defer closer.Close()
		if len(val) != 4 {
			return fmt.Errorf("pebble: malformed keyspace-version stamp: %d bytes, want 4", len(val))
		}
		if got := binary.BigEndian.Uint32(val); got != keyspaceVersion {
			return fmt.Errorf("pebble: unsupported keyspace layout v%d (want v%d single-sync); regenerate this c1z with a current SDK", got, keyspaceVersion)
		}
		return nil
	case errors.Is(err, pebble.ErrNotFound):
		empty, derr := e.isKeyspaceEmpty()
		if derr != nil {
			return derr
		}
		if !empty {
			return fmt.Errorf("pebble: unsupported keyspace layout (no version stamp on a non-empty file; want v%d single-sync); regenerate this c1z with a current SDK", keyspaceVersion)
		}
		if e.opts.readOnly {
			return nil
		}
		return e.stampKeyspaceVersion()
	default:
		return fmt.Errorf("pebble: read keyspace-version stamp: %w", err)
	}
}

// stampKeyspaceVersion writes the current keyspace-version stamp. Uses
// pebble.Sync — it is written once per file and must survive a crash so
// the file is never left unstamped-but-populated (which the next Open
// would reject).
func (e *Engine) stampKeyspaceVersion() error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], keyspaceVersion)
	return e.db.MetaSet(encodeKeyspaceVersionKey(), buf[:], pebble.Sync)
}

// isKeyspaceEmpty reports whether the DB holds any v3 key at all (data,
// sync-run, or engine-meta). Distinguishes a fresh create from an
// old-layout file written before the version stamp existed.
func (e *Engine) isKeyspaceEmpty() (bool, error) {
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{versionV3},
		UpperBound: []byte{versionV3 + 1},
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()
	return !iter.First(), iter.Error()
}
