//go:build batonsdkv2

package codec

import (
	"bytes"
	"errors"
	"testing"

	"github.com/segmentio/ksuid"
)

func TestEncodeSyncID_Roundtrip(t *testing.T) {
	for i := 0; i < 16; i++ {
		original := ksuid.New().String()
		encoded, err := EncodeSyncID(original)
		if err != nil {
			t.Fatalf("encode %q: %v", original, err)
		}
		if len(encoded) != 20 {
			t.Fatalf("encode %q: got %d bytes, want %d", original, len(encoded), 20)
		}
		decoded := DecodeSyncID(encoded)
		if decoded != original {
			t.Errorf("roundtrip: got %q, want %q", decoded, original)
		}
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
		if !errors.Is(err, ErrInvalidSyncID) {
			t.Errorf("%q: expected ErrInvalidSyncID, got %v", c, err)
		}
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
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < n; i++ {
		if strings[i-1] >= strings[i] {
			continue // KSUID gen is monotonic but not strict on the millisecond tick
		}
		if bytes.Compare(bytesEnc[i-1], bytesEnc[i]) >= 0 {
			t.Errorf("string[%d]=%q < string[%d]=%q but encoded bytes don't compare the same",
				i-1, strings[i-1], i, strings[i])
		}
	}
}

func TestDecodeSyncID_InvalidLength(t *testing.T) {
	for _, n := range []int{0, 1, 19, 21, 100} {
		got := DecodeSyncID(make([]byte, n))
		if got != "" {
			t.Errorf("decode %d bytes: got %q, want empty", n, got)
		}
	}
}
