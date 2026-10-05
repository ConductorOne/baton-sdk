package v3

import (
	"archive/tar"
	"bytes"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// A GNU sparse entry declares a logical size that archive/tar fills with holes
// it synthesizes, so extraction would write far more than the payload carries.
func TestSecurity_ExtractionRejectsSparseEntries(t *testing.T) {
	const size = 64 << 20
	stream := sparseTar(t, size)
	hdr, err := tar.NewReader(bytes.NewReader(stream)).Next()
	require.NoError(t, err)
	require.EqualValues(t, size, hdr.Size, "premise: %d bytes of tar declare %d", len(stream), size)

	dest := t.TempDir()
	require.ErrorIs(t, ExtractZstdTar(bytes.NewReader(stream), dest), ErrMaxSizeExceeded)
	entries, err := os.ReadDir(dest)
	require.NoError(t, err)
	require.Empty(t, entries)
}

// sparseTar returns a PAX 0.1 sparse tar holding one byte at the end of a
// size-byte file. tar.Writer drops GNU.sparse.* records, so they are written
// under another prefix of the same length and renamed in the output.
func sparseTar(t *testing.T, size int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "000001.sst",
		Mode:     0o644,
		Size:     1,
		Format:   tar.FormatPAX,
		PAXRecords: map[string]string{
			"XXX.sparse.major":     "0",
			"XXX.sparse.minor":     "1",
			"XXX.sparse.numblocks": "1",
			"XXX.sparse.size":      strconv.FormatInt(size, 10),
			"XXX.sparse.map":       strconv.FormatInt(size-1, 10) + ",1",
		},
	}))
	_, err := tw.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	return bytes.ReplaceAll(buf.Bytes(), []byte("XXX.sparse."), []byte("GNU.sparse."))
}
