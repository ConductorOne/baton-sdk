package dotc1z

import (
	"bytes"
	"fmt"
	"io"
)

const (
	HeaderSize    = 5
	DefaultFormat = C1ZFormatV1
)

// File format headers.
var (
	C1ZV1Header = []byte("C1ZF\x00") // Legacy V1 SQLite format
	C1ZV2Header = []byte("C1Z2\x00") // V2 format
)

// C1ZFormat represents the detected file format.
type C1ZFormat int

const (
	C1ZFormatUnknown C1ZFormat = iota
	C1ZFormatV1                // Legacy V1 SQLite format
	C1ZFormatV2                // V2 manifest-based format
)

// GetFormat returns the supported format OR DefaultFormat if it's unsupported or unknown.
func GetFormat(format C1ZFormat) C1ZFormat {
	switch format {
	case C1ZFormatV1:
		return C1ZFormatV1
	case C1ZFormatV2:
		return C1ZFormatV2
	default:
		return DefaultFormat
	}
}

func (f C1ZFormat) String() string {
	switch f {
	case C1ZFormatV1:
		return "v1"
	case C1ZFormatV2:
		return "v2"
	default:
		return "unknown"
	}
}

var (
	ErrInvalidFile = fmt.Errorf("c1z: invalid file")
)

// ReadHeader reads len(C1ZFileHeader) bytes from the given io.Reader and compares them to C1ZFileHeader, returning an error if they don't match.
// If possible, ReadHeader will Seek() to the start of the stream before checking the header bytes.
// On return, the reader will be pointing to the first byte after the header.
func ReadHeader(reader io.Reader) (C1ZFormat, error) {
	rs, ok := reader.(io.Seeker)
	if ok {
		_, err := rs.Seek(0, 0)
		if err != nil {
			return C1ZFormatUnknown, err
		}
	}

	headerBytes := make([]byte, HeaderSize)
	_, err := reader.Read(headerBytes)
	if err != nil {
		return C1ZFormatUnknown, err
	}

	switch {
	case bytes.Equal(headerBytes, C1ZV1Header):
		return C1ZFormatV1, nil
	case bytes.Equal(headerBytes, C1ZV2Header):
		return C1ZFormatV2, nil
	default:
		return C1ZFormatUnknown, ErrInvalidFile
	}
}
