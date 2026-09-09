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

// keyspaceVersionLedgerInFlight is the stamp a file carries while a
// ledgered sync (atomic pages, docs/tasks/sound-syncs-solutions-brief.md
// §3) is in flight. The key layout is v2 plus the additive ledger family;
// the stamp exists so an SDK that resumes from the checkpoint token
// alone REFUSES the file at Open instead of resuming a sync whose truth
// is in a family it cannot read. Written (pebble.Sync) before the first
// ledger row can commit — no image holds a row under v2 — and restored
// to keyspaceVersion at seal (endSyncFinalize), so a finished file is
// readable by every v2 reader; they are family-bounded and never see the
// rows. An older SDK opening an in-flight ledgered file sees
// "unsupported keyspace layout v3; regenerate this c1z with a current
// SDK": the sync is stuck until a ledger-aware SDK resumes it or the
// file is discarded, which is the intended failure (§3.8).
const keyspaceVersionLedgerInFlight uint32 = 3

// supportedKeyspaceVersions is what this SDK will open. A package var so
// a test can shrink it to an older SDK's set and prove the in-flight
// stamp refuses.
var supportedKeyspaceVersions = map[uint32]bool{
	keyspaceVersion:               true,
	keyspaceVersionLedgerInFlight: true,
}

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
		got := binary.BigEndian.Uint32(val)
		if !supportedKeyspaceVersions[got] {
			return fmt.Errorf("pebble: unsupported keyspace layout v%d (want v%d single-sync); regenerate this c1z with a current SDK", got, keyspaceVersion)
		}
		e.ledgerInFlight.Store(got == keyspaceVersionLedgerInFlight)
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
	return e.stampKeyspaceVersionValue(keyspaceVersion)
}

func (e *Engine) stampKeyspaceVersionValue(v uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	return e.db.MetaSet(encodeKeyspaceVersionKey(), buf[:], pebble.Sync)
}

// keyspaceVersionStamp reads the stamp (tests and diagnostics).
func (e *Engine) keyspaceVersionStamp() (uint32, error) {
	val, closer, err := e.db.Get(encodeKeyspaceVersionKey())
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(val) != 4 {
		return 0, fmt.Errorf("pebble: malformed keyspace-version stamp: %d bytes, want 4", len(val))
	}
	return binary.BigEndian.Uint32(val), nil
}

// markLedgerInFlight stamps keyspaceVersionLedgerInFlight, once per
// open. Called by PageUnit.Commit BEFORE the unit's batch, synced, so
// the stamp is durable in every image the row is durable in. Must be
// called with the write gate held (caller is inside withWrite).
func (e *Engine) markLedgerInFlight() error {
	if e.ledgerInFlight.Load() {
		return nil
	}
	if err := e.stampKeyspaceVersionValue(keyspaceVersionLedgerInFlight); err != nil {
		return fmt.Errorf("pebble: stamp ledger in-flight: %w", err)
	}
	e.ledgerInFlight.Store(true)
	return nil
}

// clearLedgerInFlight restores keyspaceVersion at seal. Idempotent; a
// crash between it and the ended_at stamp leaves an unfinished v2 file
// with rows, which the syncer's attempt guard tolerates.
func (e *Engine) clearLedgerInFlight() error {
	if !e.ledgerInFlight.Load() {
		return nil
	}
	if err := e.stampKeyspaceVersionValue(keyspaceVersion); err != nil {
		return fmt.Errorf("pebble: clear ledger in-flight stamp: %w", err)
	}
	e.ledgerInFlight.Store(false)
	return nil
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
