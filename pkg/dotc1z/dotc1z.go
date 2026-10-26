package dotc1z

import (
	"io"
)

// NewC1ZFileDecoder wraps a given .c1z io.Reader that validates the .c1z and decompresses/decodes the underlying file.
// Defaults: 128MiB max memory and 3GiB max decoded size
// You must close the resulting io.ReadCloser when you are done, do not forget to close the given io.Reader if necessary.
func NewC1ZFileDecoder(f io.Reader, opts ...DecoderOption) (io.ReadCloser, error) {
	return NewDecoder(f, opts...)
}
