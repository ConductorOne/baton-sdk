package codec

import (
	"bytes"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
)

func TestEncodeSyncID_Roundtrip(t *testing.T) {
	for i := 0; i < 16; i++ {
		original := ksuid.New().String()
		encoded, err := EncodeSyncID(original)
		require.NoError(t, err, "encode %q: %v", original, err)
		require.Len(t, encoded, 20, "encode %q: got %d bytes, want %d", original, len(encoded), 20)
		decoded := DecodeSyncID(encoded)
		require.Equal(t, original, decoded, "roundtrip: got %q, want %q", decoded, original)
	}
}

func TestEncodeSyncID_InvalidInput(t *testing.T) {
	cases := []string{
		"",
		"too-short",
		"not-a-ksuid-at-all-just-text-padding",
	}
	for _, c := range cases {
		_, err := EncodeSyncID(c)
		require.ErrorIs(t, err, ErrInvalidSyncID, "%q: expected ErrInvalidSyncID, got %v", c, err)
	}
}

func TestEncodeSyncID_OrderPreserving(t *testing.T) {
	// KSUIDs are timestamp-prefixed; sequential generation produces
	// strings AND bytes that sort identically. Verify that property
	// is preserved by EncodeSyncID.
	const n = 32
	strings := make([]string, n)
	bytesEnc := make([][]byte, n)
	for i := 0; i < n; i++ {
		k := ksuid.New()
		strings[i] = k.String()
		var err error
		bytesEnc[i], err = EncodeSyncID(strings[i])
		require.NoError(t, err)
	}
	for i := 1; i < n; i++ {
		if strings[i-1] >= strings[i] {
			continue // KSUID gen is monotonic but not strict on the millisecond tick
		}
		require.Less(t, bytes.Compare(bytesEnc[i-1], bytesEnc[i]), 0,
			"string[%d]=%q < string[%d]=%q but encoded bytes don't compare the same",
			i-1, strings[i-1], i, strings[i])
	}
}

func TestDecodeSyncID_InvalidLength(t *testing.T) {
	for _, n := range []int{0, 1, 19, 21, 100} {
		got := DecodeSyncID(make([]byte, n))
		require.Empty(t, got, "decode %d bytes: got %q, want empty", n, got)
	}
}
