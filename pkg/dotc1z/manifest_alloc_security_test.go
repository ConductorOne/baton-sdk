package dotc1z

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/record"
	"github.com/stretchr/testify/require"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// Version-edit tags from pebble internal/manifest/version_edit.go.
// That package is internal to pebble, so the values are copied.
const (
	veTagComparator     = 1
	veTagNewFile4       = 103
	veCustomTagBlobRefs = 69
)

// hostileSliceLen is a version-edit length with no matching payload.
// It is past the runtime max allocation (1<<heapAddrBits), so makeslice
// panics "len out of range" before malloc. A length the runtime would
// accept is reserved in full before the short read returns an error.
const hostileSliceLen = uint64(1) << 62

// Opening a c1z whose pebble MANIFEST claims hostileSliceLen bytes, or
// that many blob references, must return a corruption error.
// versionEditDecoder.readBytes and the tagNewFile4 blob-reference count
// both allocate that length before checking the record contains it.
func TestSecurity_ManifestUnvalidatedLength(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		edit []byte
	}{
		{name: "read_bytes", edit: versionEditReadBytesLength(hostileSliceLen)},
		{name: "blob_references", edit: versionEditBlobReferenceCount(hostileSliceLen)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := buildHostileManifestC1z(t, tc.edit)
			info, err := os.Stat(src)
			require.NoError(t, err)
			require.Less(t, info.Size(), int64(1<<20), "premise: c1z is %d bytes", info.Size())

			for _, readOnly := range []bool{false, true} {
				t.Run(fmt.Sprintf("read_only=%t", readOnly), func(t *testing.T) {
					path := copyFile(t, src, filepath.Join(t.TempDir(), "hostile.c1z"))
					require.NotPanics(t, func() {
						err := openAndListResources(ctx, path, readOnly)
						require.Error(t, err)
						require.True(t, cpebble.IsCorruptionError(err), "got %v", err)
					})
				})
			}
		})
	}
}

func openAndListResources(ctx context.Context, path string, readOnly bool) error {
	store, err := NewStore(ctx, path, WithReadOnly(readOnly), WithTmpDir(filepath.Dir(path)))
	if err != nil {
		return err
	}
	defer func() { _ = store.Close(ctx) }()
	_, err = store.ListResources(ctx, (&v2.ResourcesServiceListResourcesRequest_builder{}).Build())
	return err
}

// buildHostileManifestC1z returns a v3 c1z whose pebble MANIFEST is one
// log record holding edit. The format-version marker, OPTIONS, and
// manifest marker come from a real store, so Open reaches version-edit
// decode.
func buildHostileManifestC1z(t *testing.T, edit []byte) string {
	t.Helper()
	ctx := context.Background()

	dir := t.TempDir()
	honestPath := filepath.Join(dir, "honest.c1z")
	store, err := NewStore(ctx, honestPath)
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	unpackDir := filepath.Join(dir, "db")
	require.NoError(t, os.MkdirAll(unpackDir, 0o755))
	_, _, _, err = unpackExistingPebbleC1Z(honestPath, unpackDir, 0, 0, nil)
	require.NoError(t, err)

	encoded := encodeManifestRecord(t, edit)
	require.Less(t, len(encoded), 256, "premise: manifest record is %d bytes", len(encoded))
	replaceManifest(t, unpackDir, encoded)

	outPath := filepath.Join(dir, "hostile.c1z")
	f, err := os.Create(outPath) // #nosec G304 -- fixture path under the test temp dir.
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	manifest := c1zv3.C1ZManifestV3_builder{
		Engine: string(c1zstore.PebbleManifestEngine),
	}.Build()
	require.NoError(t, formatv3.WriteEnvelope(f, manifest, unpackDir))
	return outPath
}

// encodeManifestRecord wraps edit in a legacy MANIFEST log record and
// reads it back. versionSet.load stops on an invalid record without
// decoding, which would miss the allocation.
func encodeManifestRecord(t *testing.T, edit []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := record.NewWriter(&buf)
	_, err := w.WriteRecord(edit)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	rr := record.NewReader(bytes.NewReader(buf.Bytes()), 0)
	r, err := rr.Next()
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, edit, got)
	return buf.Bytes()
}

func replaceManifest(t *testing.T, dir string, contents []byte) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	name := ""
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "MANIFEST-") {
			continue
		}
		require.Empty(t, name, "multiple MANIFEST files, also %s", e.Name())
		name = e.Name()
	}
	require.NotEmpty(t, name, "no MANIFEST file in unpacked checkpoint")
	// #nosec G304 G703 -- fixture MANIFEST under the test temp dir.
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), contents, 0o600))
}

// versionEditReadBytesLength is a tagComparator field whose length
// prefix is n and whose payload is absent. Decode calls readBytes,
// which allocates n.
func versionEditReadBytesLength(n uint64) []byte {
	var b []byte
	b = appendUvarint(b, veTagComparator)
	b = appendUvarint(b, n)
	return b
}

// versionEditBlobReferenceCount is a tagNewFile4 entry whose blob
// reference count is n and whose reference list is absent. Decode
// allocates []BlobReference of length n.
func versionEditBlobReferenceCount(n uint64) []byte {
	var b []byte
	b = appendUvarint(b, veTagNewFile4)
	b = appendUvarint(b, 0) // level
	b = appendUvarint(b, 1) // file number
	b = appendUvarint(b, 1) // size
	b = appendPrefixedBytes(b, []byte{0x01})
	b = appendPrefixedBytes(b, []byte{0x02})
	b = appendUvarint(b, 1) // smallest seqnum
	b = appendUvarint(b, 1) // largest seqnum
	b = appendUvarint(b, veCustomTagBlobRefs)
	b = appendUvarint(b, 0) // blob reference depth
	b = appendUvarint(b, n)
	return b
}

func appendUvarint(dst []byte, v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

func appendPrefixedBytes(dst, p []byte) []byte {
	dst = appendUvarint(dst, uint64(len(p)))
	return append(dst, p...)
}

func copyFile(t *testing.T, src, dst string) string {
	t.Helper()
	in, err := os.Open(src) // #nosec G304 -- fixture path under the test temp dir.
	require.NoError(t, err)
	defer func() { _ = in.Close() }()
	out, err := os.Create(dst) // #nosec G304 -- fixture path under the test temp dir.
	require.NoError(t, err)
	_, err = io.Copy(out, in)
	require.NoError(t, err)
	require.NoError(t, out.Close())
	return dst
}
