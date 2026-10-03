package dotc1z

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// TestSecurity_CorruptSSTFailsOpenNotProcess guards C1Z-SEC-005: a v3 .c1z
// whose Pebble checkpoint carries a corrupted SST (flipped interior byte,
// file sizes kept equal so pebble's open-time MANIFEST-vs-size check passes)
// must fail the open with an ordinary error on the first read that touches
// the corrupt block — never route through pebble's default DataCorruption →
// Logger.Fatalf, which discardPebbleLogger turns into os.Exit(1), killing
// the whole process (backend compaction workers are shared and multi-tenant).
//
// The child-run shape (re-exec of this test binary via os.Args[0], the
// package's established subprocess convention — see to_pebble_localtime_test.go)
// exists because the vulnerable behavior IS process death: an in-process
// assertion would kill the suite before it could report. Pre-fix, the child
// exits 1 with "pebble FATAL" on stderr; post-fix, the child prints OPEN_ERR
// and exits 0, proving the open failed with an error and the process survived.
func TestSecurity_CorruptSSTFailsOpenNotProcess(t *testing.T) {
	if os.Getenv("BATON_CORRUPT_SST_CHILD") != "" {
		corruptSSTChild()
		return
	}

	ctx := context.Background()
	c1zPath := buildCorruptSSTC1z(t, ctx)

	cmd := exec.CommandContext(ctx, os.Args[0], // #nosec G204,G702 -- os.Args[0] is this test binary itself, the package's documented self-exec convention (to_pebble_localtime_test.go).
		"-test.run=TestSecurity_CorruptSSTFailsOpenNotProcess",
		"-test.count=1", "-test.timeout=120s")
	cmd.Env = append(os.Environ(), "BATON_CORRUPT_SST_CHILD="+c1zPath)
	out, err := cmd.CombinedOutput()

	require.NotContains(t, string(out), "pebble FATAL",
		"pebble routed artifact corruption to Fatalf: the process-exit vulnerability is live")
	require.NoError(t, err,
		"the child process died opening the corrupt artifact; combined output:\n%s", out)
	require.Contains(t, string(out), "OPEN_ERR:",
		"the corrupt artifact must fail the open with an error, not open successfully or kill the process; output:\n%s", out)
}

func corruptSSTChild() {
	path := os.Getenv("BATON_CORRUPT_SST_CHILD")
	ctx := context.Background()
	store, err := NewStore(ctx, path)
	if err != nil {
		fmt.Fprintf(os.Stdout, "OPEN_ERR: %v\n", err)
		os.Exit(0)
	}
	// The open alone may not touch the corrupt data block: read the
	// actual resource rows so the corrupt block is exercised.
	resp, err := store.ListResources(ctx, (&v2.ResourcesServiceListResourcesRequest_builder{}).Build())
	if err != nil {
		_ = store.Close(ctx)
		fmt.Fprintf(os.Stdout, "OPEN_ERR: %v\n", err)
		os.Exit(0)
	}
	_ = resp
	_ = store.Close(ctx)
	fmt.Fprint(os.Stdout, "OPEN_OK\n")
	os.Exit(0)
}

// buildCorruptSSTC1z produces a v3 .c1z whose unpacked Pebble checkpoint
// contains one SST with a flipped interior byte (block checksum mismatch on
// read) while every file keeps its original on-disk size.
func buildCorruptSSTC1z(t *testing.T, ctx context.Context) string {
	t.Helper()

	// 1. Honest store with real data; EndSync + Close flush SSTs.
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

	// 2. Unpack the checkpoint (the same unpack OpenStore performs).
	workDir := t.TempDir()
	unpackDir := filepath.Join(workDir, "db")
	require.NoError(t, os.MkdirAll(unpackDir, 0o755))
	_, _, _, err = unpackExistingPebbleC1Z(honestPath, unpackDir, 0, 0, nil)
	require.NoError(t, err)

	// 3. Flip one interior byte of the largest SST (data-block region;
	// footer occupies the trailing ~52 bytes), preserving size.
	sst := largestSST(t, unpackDir)
	flipByteInDataBlocks(t, sst)

	// 4. Re-frame the corrupted dir as a tar+zstd v3 envelope.
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

// flipByteInDataBlocks flips one interior byte of the SST, keeping the file
// size identical. Mid-file is always block data for a table this size; the
// flip fails the block checksum on read — the corruption class pebble routes
// to DataCorruption — while open-time checkConsistency (file sizes vs
// MANIFEST) still passes.
func flipByteInDataBlocks(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(raw), 4096, "sst too small for a deterministic interior flip")
	raw[len(raw)/2] ^= 0xFF
	require.NoError(t, os.WriteFile(path, raw, 0o600)) // #nosec G703 -- path is a fixture sst inside the test's private temp dir.
}

// largestSST returns the path of the largest *.sst under dir.
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
