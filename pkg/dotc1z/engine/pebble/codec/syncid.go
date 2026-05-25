package codec

import (
	"fmt"

	"github.com/segmentio/ksuid"
)

// EncodeSyncID converts a KSUID string into its 20-byte canonical
// binary form. baton's sync_id values are KSUIDs (27-char base62);
// storing them in keys as base62 strings would burn ~7 bytes per
// occurrence × N indexes × 100M+ rows = real space. The binary form
// is uniformly 20 bytes and lex-compares identically to the base62
// form because KSUIDs are sortable by their timestamp prefix.
//
// Returns ErrInvalidSyncID if s is not a valid KSUID string.
func EncodeSyncID(s string) ([]byte, error) {
	k, err := ksuid.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSyncID, err)
	}
	b := k.Bytes()
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

// DecodeSyncID converts a 20-byte canonical KSUID back to its base62
// string form for human-readable display. Returns an empty string if
// b is not exactly 20 bytes.
func DecodeSyncID(b []byte) string {
	if len(b) != 20 /* ksuid byteLength (private const upstream) */ {
		return ""
	}
	var k ksuid.KSUID
	copy(k[:], b)
	return k.String()
}
