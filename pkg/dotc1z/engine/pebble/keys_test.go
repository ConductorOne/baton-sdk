package pebble

import (
	"bytes"
	"testing"
)

func TestUpperBoundOf(t *testing.T) {
	tests := []struct {
		name   string
		prefix []byte
		want   []byte
	}{
		{
			name:   "increments last byte",
			prefix: []byte{0x03, 0x10, 0x20},
			want:   []byte{0x03, 0x10, 0x21},
		},
		{
			name:   "carries through trailing ff bytes",
			prefix: []byte{0x03, 0x10, 0xff, 0xff},
			want:   []byte{0x03, 0x11},
		},
		{
			name:   "all ff has no finite upper bound",
			prefix: []byte{0xff, 0xff},
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := upperBoundOf(tt.prefix)
			if !bytes.Equal(got, tt.want) {
				t.Fatalf("upperBoundOf(%x) = %x, want %x", tt.prefix, got, tt.want)
			}
		})
	}
}
