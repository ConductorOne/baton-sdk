package pebble

import (
	"context"
	"encoding/binary"
	"errors"

	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
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

func (e *Engine) SetSyncToken(ctx context.Context, token string) error {
	syncID := e.CurrentSyncID()
	if syncID == "" {
		return errors.New("SetSyncToken: no open sync")
	}
	existing, err := e.SyncRuns().GetSyncRunRecord(ctx, syncID)
	if err != nil {
		return err
	}
	updated := proto.Clone(existing).(*v3.SyncRunRecord)
	updated.SetSyncToken(token)
	return e.SyncRuns().PutSyncRunRecord(ctx, updated)
}
