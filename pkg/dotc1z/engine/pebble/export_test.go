package pebble

import (
	"encoding/binary"

	"google.golang.org/protobuf/proto"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Test-only exports for external (pebble_test / dotc1z) tests that need to
// fabricate LEGACY-layout keyspaces the production code can no longer write.

// LegacyEntitlementKeyForTest returns the pre-identity entitlement primary
// key (keyed by external id).
func LegacyEntitlementKeyForTest(externalID string) []byte {
	buf := []byte{versionV3, typeEntitlement}
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

// LegacyGrantKeyForTest returns the pre-identity grant primary key (keyed
// by external id).
func LegacyGrantKeyForTest(externalID string) []byte {
	buf := []byte{versionV3, typeGrant}
	buf = codec.AppendTupleSeparator(buf)
	return codec.AppendTupleStrings(buf, externalID)
}

// LegacyKeyspaceMetaForTest returns the engine-meta (key, value) pair that
// stamps the single-sync keyspace version, making a fabricated dir look
// like a real pre-identity file (which carries this stamp but no id-index
// format stamp).
func LegacyKeyspaceMetaForTest() ([]byte, []byte) {
	var version [4]byte
	binary.BigEndian.PutUint32(version[:], 2)
	key := append([]byte{versionV3, typeEngineMeta}, nil...)
	key = codec.AppendTupleStrings(key, "keyspace_version")
	return key, version[:]
}

// SyncRunKeyForTest returns the fixed sync-run record key.
func SyncRunKeyForTest() []byte { return []byte{versionV3, typeSyncRun} }

// MarshalRecordForTest marshals a record with the engine's deterministic
// marshal options.
func MarshalRecordForTest(msg proto.Message) ([]byte, error) {
	return marshalRecord(msg)
}

// SetWriteBarrierOwnerChecksForTest turns the barrier ownership
// bookkeeping on or off and returns the prior setting. It exists for
// benchmarks: the checks are gated on testing.Testing(), which a
// benchmark binary satisfies, and each barrier acquisition then formats a
// runtime stack — around 2µs and an allocation, against roughly 7µs for
// the grant write being measured. Left on, the write benchmarks report
// the check, and only on the Pebble side, which skews every
// Pebble-vs-SQLite comparison in production_bench_test.go.
//
// Only benchmarks should call this. A test that turns the checks off
// gives up the panic that reports a deadlock and gets the hang back.
func SetWriteBarrierOwnerChecksForTest(enabled bool) bool {
	prev := writeBarrierOwnerChecks
	writeBarrierOwnerChecks = enabled
	return prev
}
