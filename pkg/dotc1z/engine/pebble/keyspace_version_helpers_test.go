package pebble

import (
	"encoding/binary"
	"fmt"
)

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
