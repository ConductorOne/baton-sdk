package v3

import (
	"archive/tar"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildSparsePAXTar produces a real GNU sparse PAX tar on disk: a source
// file of realsize bytes with one small data extent and the rest holes,
// archived by the system GNU tar with --format=pax -S. The result is a few
// KB of physical tar that declares the full logical size — exactly the
// hostile shape a c1z author would embed.
func buildSparsePAXTar(t *testing.T, realsize int64) []byte {
	t.Helper()
	if _, err := exec.LookPath("tar"); err != nil {
		t.Skip("system tar not available")
	}
	dir := t.TempDir()
	src := filepath.Join(dir, "sparse_src.bin")
	f, err := os.OpenFile(src, os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	_, err = f.Write([]byte("X"))
	require.NoError(t, err)
	require.NoError(t, f.Truncate(realsize))
	require.NoError(t, f.Close())

	out := filepath.Join(dir, "sparse.tar")
	cmd := exec.Command("tar", "--format=pax", "-S", "-cf", out, "-C", dir, "sparse_src.bin")
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run())
	data, err := os.ReadFile(out)
	require.NoError(t, err)
	return data
}

// TestSecurity_SparseTarEntriesRejected: a GNU sparse entry decouples its
// logical size (what extraction writes) from its physical bytes (what the
// decoded-byte budget meters). archive/tar expands the holes from its
// internal zero reader, so io.Copy writes hdr.Size bytes while only a few
// KB of physical tar cross the budget's reader — a c1z of a few hundred
// bytes can declare a 16 GiB file. The honest producer (tar.Writer over a
// zstd-compressed payload) never emits sparse records, so sparse entries
// are rejected, never extracted.
func TestSecurity_SparseTarEntriesRejected(t *testing.T) {
	const realsize = int64(16 << 30)
	stream := buildSparsePAXTar(t, realsize)

	// Sanity: the archive/tar reader does expand the entry to realsize
	// (the mechanism this finding exploits), proving the fixture is the
	// hostile shape.
	tr := tar.NewReader(bytes.NewReader(stream))
	hdr, err := tr.Next()
	require.NoError(t, err)
	require.Equal(t, realsize, hdr.Size)
	require.True(t, tarEntryIsSparse(hdr), "fixture must carry GNU.sparse records")
	n, err := hdr.Size, error(nil)
	_ = n
	// Do NOT read 16 GiB here; the header check is sufficient.

	dest := t.TempDir()
	err = ExtractZstdTar(bytes.NewReader(stream), dest)
	require.ErrorIs(t, err, ErrMaxSizeExceeded)
	entries, err := os.ReadDir(dest)
	require.NoError(t, err)
	require.Empty(t, entries, "nothing may be extracted from a rejected payload")
}

// TestSecurity_NonSparseTarStillExtracts is the control: an equivalent
// non-sparse archive (the only shape tar.Writer emits) extracts normally.
func TestSecurity_NonSparseTarStillExtracts(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	require.NoError(t, tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: "CURRENT", Mode: 0o644, Size: 5}))
	_, err := tw.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	dest := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.MkdirAll(dest, 0o755))
	require.NoError(t, ExtractZstdTar(bytes.NewReader(buf.Bytes()), dest))
	got, err := os.ReadFile(filepath.Join(dest, "CURRENT"))
	require.NoError(t, err)
	require.Equal(t, "hello", string(got))
}

// TestTarEntryIsSparse pins the predicate against both sparse encodings'
// record names and honest headers.
func TestTarEntryIsSparse(t *testing.T) {
	require.True(t, tarEntryIsSparse(&tar.Header{PAXRecords: map[string]string{"GNU.sparse.realsize": "16"}}))
	require.True(t, tarEntryIsSparse(&tar.Header{PAXRecords: map[string]string{"GNU.sparse.size": "16", "path": "x"}}))
	require.False(t, tarEntryIsSparse(&tar.Header{Name: "CURRENT", PAXRecords: map[string]string{"path": "CURRENT"}}))
	require.False(t, tarEntryIsSparse(&tar.Header{Name: strings.Repeat("a", 32)}))
}
