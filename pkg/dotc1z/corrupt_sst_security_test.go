package dotc1z

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cpebble "github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// A corrupt SST data block in an untrusted .c1z must fail the open or read
// with a corruption error. Without the engine's DataCorruption listener,
// pebble calls Logger.Fatalf on that read, which panics this test.
func TestSecurity_CorruptSSTFailsWithError(t *testing.T) {
	ctx := context.Background()
	c1zPath := buildCorruptSSTC1z(t, ctx)

	for _, readOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("read_only=%t", readOnly), func(t *testing.T) {
			err := listResourcesFrom(ctx, c1zPath, readOnly)
			require.Error(t, err)
			require.True(t, cpebble.IsCorruptionError(err), "want a pebble corruption error, got: %v", err)
		})
	}
}

func listResourcesFrom(ctx context.Context, path string, readOnly bool) error {
	store, err := NewStore(ctx, path, WithReadOnly(readOnly))
	if err != nil {
		return err
	}
	defer func() { _ = store.Close(ctx) }()
	_, err = store.ListResources(ctx, (&v2.ResourcesServiceListResourcesRequest_builder{}).Build())
	return err
}

// buildCorruptSSTC1z returns a v3 .c1z whose largest SST has one flipped
// interior byte, so a block checksum fails on read while every file keeps
// the size its MANIFEST records.
func buildCorruptSSTC1z(t *testing.T, ctx context.Context) string {
	t.Helper()

	dir := t.TempDir()
	honestPath := filepath.Join(dir, "honest.c1z")
	store, err := NewStore(ctx, honestPath)
	require.NoError(t, err)
	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()))
	const rows = 2000
	for i := range rows {
		res := v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     fmt.Sprintf("u%06d", i),
			}.Build(),
		}.Build()
		require.NoError(t, store.PutResources(ctx, res))
	}
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))

	workDir := t.TempDir()
	unpackDir := filepath.Join(workDir, "db")
	require.NoError(t, os.MkdirAll(unpackDir, 0o755))
	_, _, _, err = unpackExistingPebbleC1Z(honestPath, unpackDir, 0, 0, nil)
	require.NoError(t, err)

	sst := largestSST(t, unpackDir)
	flipMiddleByte(t, sst)

	outPath := filepath.Join(dir, "corrupt.c1z")
	f, err := os.Create(outPath)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	manifest := c1zv3.C1ZManifestV3_builder{
		Engine: string(c1zstore.PebbleManifestEngine),
	}.Build()
	require.NoError(t, formatv3.WriteEnvelope(f, manifest, unpackDir))
	return outPath
}

// flipMiddleByte corrupts a data block of the SST at path without changing
// its size, so pebble's open-time size check still passes.
func flipMiddleByte(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(raw), 4096, "sst too small for an interior flip")
	raw[len(raw)/2] ^= 0xFF
	require.NoError(t, os.WriteFile(path, raw, 0o600)) // #nosec G703 -- fixture sst in the test's temp dir.
}

func largestSST(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	best := ""
	var bestSize int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		info, err := e.Info()
		require.NoError(t, err)
		if info.Size() > bestSize {
			bestSize = info.Size()
			best = filepath.Join(dir, e.Name())
		}
	}
	require.NotEmpty(t, best, "no sst files in unpacked checkpoint")
	return best
}
