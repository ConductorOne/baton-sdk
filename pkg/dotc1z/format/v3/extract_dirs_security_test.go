package v3

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Entry names choose how many directories an extraction creates. Both
// extractors refuse a payload that needs more than maxExtractedDirs, without
// creating any past the cap, while directories shared by many entries count
// once.
func TestSecurity_ExtractionCapsDirectories(t *testing.T) {
	t.Run("tar, one deep name", func(t *testing.T) {
		dest := t.TempDir()
		err := ExtractZstdTar(tarOf(t, tarEntry{name: strings.Repeat("a/", maxExtractedDirs+1) + "f"}), dest)
		require.ErrorIs(t, err, ErrMaxSizeExceeded)
		require.Zero(t, countDirs(t, dest))
	})

	t.Run("tar, many shallow directories", func(t *testing.T) {
		entries := make([]tarEntry, maxExtractedDirs+1)
		for i := range entries {
			entries[i] = tarEntry{name: fmt.Sprintf("d%04d/", i), dir: true}
		}
		dest := t.TempDir()
		err := ExtractZstdTar(tarOf(t, entries...), dest)
		require.ErrorIs(t, err, ErrMaxSizeExceeded)
		require.Equal(t, maxExtractedDirs, countDirs(t, dest))
	})

	t.Run("tar, shared parents", func(t *testing.T) {
		entries := []tarEntry{{name: "x/", dir: true}, {name: "x/y/", dir: true}}
		for i := range 2 * maxExtractedDirs {
			entries = append(entries, tarEntry{name: fmt.Sprintf("x/y/f%04d", i)})
		}
		dest := t.TempDir()
		require.NoError(t, ExtractZstdTar(tarOf(t, entries...), dest))
		require.Equal(t, 2, countDirs(t, dest))
		require.FileExists(t, filepath.Join(dest, "x", "y", "f0000"))
	})

	t.Run("indexed, many shallow directories", func(t *testing.T) {
		files := make(map[string][]byte, maxExtractedDirs+1)
		for i := range maxExtractedDirs + 1 {
			files[fmt.Sprintf("d%04d/f", i)] = nil
		}
		f, err := os.Open(writeIndexedEnvelope(t, writeTestPayloadDir(t, files)))
		require.NoError(t, err)
		defer f.Close()
		dest := t.TempDir()
		_, _, err = ExtractEnvelopePayload(f, dest)
		require.ErrorIs(t, err, ErrMaxSizeExceeded)
		require.Equal(t, maxExtractedDirs, countDirs(t, dest))
	})
}

type tarEntry struct {
	name string
	dir  bool
}

func tarOf(t *testing.T, entries ...tarEntry) io.Reader {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		hdr := &tar.Header{Name: e.name, Typeflag: tar.TypeReg, Mode: 0o644}
		if e.dir {
			hdr.Typeflag, hdr.Mode = tar.TypeDir, 0o755
		}
		require.NoError(t, tw.WriteHeader(hdr))
	}
	require.NoError(t, tw.Close())
	return &buf
}

func countDirs(t *testing.T, root string) int {
	t.Helper()
	n := 0
	require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() && path != root {
			n++
		}
		return err
	}))
	return n
}
