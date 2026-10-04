package v3

import (
	"archive/tar"
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// TestSecurity_TarExtractionBudgetBoundsDirectoryFanout: a tar header
// costs 512 decoded bytes, but its name can materialize up to ~127
// directories (255-byte ustar name of one-byte components) — an
// inode/directory-block cost the byte budget never saw. Fanout is now
// charged to the same budget: an envelope whose directory fanout
// exceeds the budget fails with ErrMaxSizeExceeded after a few
// entries instead of materializing the tree.
//
// The budget is set via BATON_DECODER_MAX_DECODED_SIZE_MB=1 (1 MiB),
// resolved by both readEnvelope (PayloadReader wrap) and the
// ExtractZstdTar default, so this test is red pre-fix with only the
// current public API.
func TestSecurity_TarExtractionBudgetBoundsDirectoryFanout(t *testing.T) {
	t.Setenv("BATON_DECODER_MAX_DECODED_SIZE_MB", "1")

	// 128 TypeDir entries, each a 127-deep chain of one-byte components
	// ("eNN/aa/.../d", 254 chars — just inside the 255-byte ustar cap):
	// ~64 KiB of headers, no PAX, zero content bytes — comfortably
	// inside the 1 MiB byte budget, while the implied directory tree is
	// ~16k directories.
	const (
		numEntries = 128
		depth      = 127
	)
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)
	for i := range numEntries {
		name := fmt.Sprintf("e%02d/", i) + strings.Repeat("a/", depth-2) + "d"
		hdr := &tar.Header{
			Typeflag: tar.TypeDir,
			Name:     name,
			Mode:     0o755,
		}
		require.NoError(t, tw.WriteHeader(hdr))
	}
	require.NoError(t, tw.Close())

	envBytes := buildTarEnvelope(t, tarBuf.Bytes())

	envPath := filepath.Join(t.TempDir(), "hostile-fanout.c1z3")
	require.NoError(t, os.WriteFile(envPath, envBytes, 0o600))

	f, err := os.Open(envPath)
	require.NoError(t, err)
	defer f.Close()
	env, err := ReadEnvelope(f)
	require.NoError(t, err)
	defer env.Close()

	dest := filepath.Join(t.TempDir(), "out")
	err = ExtractZstdTar(env.PayloadReader, dest)

	// Red pre-fix: extraction returns nil and materializes ~16,256
	// directories. Green post-fix: the fanout charge (127 directories x
	// 512 bytes = 65,024 bytes per entry against the 1 MiB budget)
	// trips at the ~16th entry.
	require.ErrorIs(t, err, ErrMaxSizeExceeded)
	require.Less(t, countDirs(t, dest), 2048, "fanout must trip the budget within a few entries")
}

// buildTarEnvelope wraps raw tar bytes as a minimal PAYLOAD_ENCODING_TAR
// envelope in memory: C1Z3 magic + u32 BE manifest length + manifest +
// payload. Mirrors the in-memory fixture pattern of
// extract_cardinality_security_test.go.
func buildTarEnvelope(t *testing.T, tarBytes []byte) []byte {
	t.Helper()

	// Minimal manifest: engine "pebble3" (field 1), payload_encoding =
	// TAR (field 4, varint 2).
	var manifest []byte
	manifest = protowire.AppendTag(manifest, 1, protowire.BytesType)
	manifest = protowire.AppendString(manifest, "pebble3")
	manifest = protowire.AppendTag(manifest, 4, protowire.VarintType)
	manifest = protowire.AppendVarint(manifest, uint64(c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR))

	var env bytes.Buffer
	env.Write([]byte("C1Z3\x00"))
	manifestLen := len(manifest)
	if manifestLen > math.MaxUint32 {
		t.Fatalf("manifest too large")
	}
	mlen := make([]byte, 4)
	binary.BigEndian.PutUint32(mlen, uint32(manifestLen)) // #nosec G115 -- bounded to MaxUint32 by the check above.
	env.Write(mlen)
	env.Write(manifest)
	env.Write(tarBytes)
	return env.Bytes()
}

func countDirs(t *testing.T, root string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk dest: %v", err)
	}
	return n
}
