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

const idIndexFormatStructuredV1 uint32 = 1

func encodeIDIndexFormatKey() []byte {
	buf := make([]byte, 0, 2+len("grant_entitlement_id_format"))
	buf = append(buf, versionV3, typeEngineMeta)
	return codec.AppendTupleStrings(buf, "grant_entitlement_id_format")
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
	return e.db.Set(encodeIDIndexFormatKey(), buf[:], pebble.Sync)
}

func (e *Engine) verifyOrStampIDIndexFormat(ctx context.Context) error {
	version, err := e.readIDIndexFormat()
	if err != nil {
		return err
	}
	if version == idIndexFormatStructuredV1 {
		return nil
	}
	if version != 0 {
		return fmt.Errorf("pebble: unsupported grant/entitlement id-index format v%d", version)
	}
	if e.opts.readOnly {
		return nil
	}
	empty, err := e.isDataKeyspaceEmpty()
	if err != nil {
		return err
	}
	if !empty {
		return e.migrateIDIndexFormatToStructuredV1(ctx)
	}
	return e.writeIDIndexFormat(idIndexFormatStructuredV1)
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
	case idIndexFormatStructuredV1:
		return c1zv3.PebbleIdIndexFormat_PEBBLE_ID_INDEX_FORMAT_STRUCTURED_V1
	case 0:
		return c1zv3.PebbleIdIndexFormat_PEBBLE_ID_INDEX_FORMAT_LEGACY_EXTERNAL_ID
	default:
		return c1zv3.PebbleIdIndexFormat_PEBBLE_ID_INDEX_FORMAT_UNSPECIFIED
	}
}
