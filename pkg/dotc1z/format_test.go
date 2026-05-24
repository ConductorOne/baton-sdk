package dotc1z

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReadHeaderFormat(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    C1ZFormat
		wantErr error
	}{
		{
			name:  "v1 magic",
			input: append([]byte("C1ZF\x00"), 0xde, 0xad, 0xbe, 0xef),
			want:  C1ZFormatV1,
		},
		{
			name:  "v3 magic",
			input: append([]byte("C1Z3\x00"), 0xca, 0xfe, 0xba, 0xbe),
			want:  C1ZFormatV3,
		},
		{
			name:    "unknown magic",
			input:   []byte("XXXX\x00garbage"),
			want:    C1ZFormatUnknown,
			wantErr: ErrInvalidFile,
		},
		{
			name:    "short read",
			input:   []byte("C1Z"),
			want:    C1ZFormatUnknown,
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "empty input",
			input:   []byte{},
			want:    C1ZFormatUnknown,
			wantErr: io.EOF,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ReadHeaderFormat(bytes.NewReader(tc.input))
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("err: got %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tc.want {
				t.Errorf("format: got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReadHeaderFormat_SeekerRewinds(t *testing.T) {
	// A seekable reader positioned past the header should be rewound
	// to 0 before the format is read.
	r := bytes.NewReader(append([]byte("C1Z3\x00"), 'x', 'y', 'z'))
	if _, err := r.Seek(7, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, err := ReadHeaderFormat(r)
	if err != nil {
		t.Fatal(err)
	}
	if got != C1ZFormatV3 {
		t.Fatalf("got %v want v3", got)
	}
}

func TestFormatString(t *testing.T) {
	for _, tc := range []struct {
		f    C1ZFormat
		want string
	}{
		{C1ZFormatV1, "v1"},
		{C1ZFormatV3, "v3"},
		{C1ZFormatUnknown, "unknown"},
	} {
		if got := tc.f.String(); got != tc.want {
			t.Errorf("String(%d): got %q want %q", tc.f, got, tc.want)
		}
	}
}

func TestEngineConstants(t *testing.T) {
	// Smoke-check the engine constants are non-empty and distinct.
	if EngineSQLite == "" || EnginePebble == "" {
		t.Fatal("engine constants must be non-empty")
	}
	if EngineSQLite == EnginePebble {
		t.Fatal("engine constants must be distinct")
	}
}

func TestErrEngineNotAvailable(t *testing.T) {
	// Ensure the sentinel is well-formed and discoverable via errors.Is.
	if ErrEngineNotAvailable == nil {
		t.Fatal("ErrEngineNotAvailable must be set")
	}
	wrapped := errors.New("upstream: " + ErrEngineNotAvailable.Error())
	if !strings.Contains(wrapped.Error(), "engine not available") {
		t.Fatalf("error message changed unexpectedly: %v", wrapped)
	}
}
