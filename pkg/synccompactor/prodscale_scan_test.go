package synccompactor

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestProdScaleScanVsFoldDebt measures what artifact shape costs the
// READER: it chains real fold compactions and measures consumer-side
// operations at several points, against a kway-rebuilt reference:
//
//   - a full sequential grant scan (the ingestion/uplift pattern), and
//   - 10k point lookups (diff-style consumers).
//
// Historical findings (12M grants, 1.1GB): the sync-produced base
// already carries ~240 L0 files (bulk-load flushes; the L0 tuning
// lets them persist), fold sessions add a handful more, and scans are
// noise-identical across all of them. The kway rebuild's flat LSM is
// ~21% smaller and ~3.7x faster on point lookups — a property of
// rebuilding generally, not of avoiding fold. Folds now run with
// normal compactions, so L0 self-manages and converges downward over
// successive folds.
//
// Requires the prod-scale base fixture (run
// TestProdScaleSkewedCompaction first). Gated, like its siblings,
// behind BATON_PROD_SCALE_TEST=1.
func TestProdScaleScanVsFoldDebt(t *testing.T) {
	if os.Getenv("BATON_PROD_SCALE_TEST") == "" {
		t.Skip("set BATON_PROD_SCALE_TEST=1 to run")
	}
	ctx := context.Background()
	require.NoError(t, ensurePebbleRegistered())

	fixDir := os.Getenv("BATON_PROD_SCALE_DIR")
	if fixDir == "" {
		fixDir = filepath.Join("..", "..", ".profiles", "prodscale")
	}
	basePath := filepath.Join(fixDir, "base.c1z")
	// #nosec G703 - developer-provided fixture path.
	if _, err := os.Stat(basePath); err != nil {
		t.Skipf("base fixture missing (%v); run TestProdScaleSkewedCompaction first", err)
	}
	grants := envInt("BATON_PROD_SCALE_GRANTS", 5_000_000)

	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "fold")

	keepDir := t.TempDir()
	curPath := filepath.Join(keepDir, "chain.c1z")
	require.NoError(t, copyFileForFold(basePath, curPath))

	measure := func(label string, path string) {
		t.Helper()
		w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
		require.NoError(t, err)
		defer w.Close(ctx)
		eng, ok := enginepkg.AsEngine(w)
		require.True(t, ok)
		// NOTE: read-only pebble opens report Sublevels=0 even with L0
		// files present; TablesCount is accurate in both modes. (The
		// fold's production debt gate reads metrics from a writable
		// open, where Sublevels is correct.)
		l0Files := eng.DB().Metrics().Levels[0].TablesCount

		start := time.Now()
		count := 0
		require.NoError(t, eng.IterateGrants(ctx, func(*v3.GrantRecord) bool {
			count++
			return true
		}))
		scanDur := time.Since(start)

		rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic benchmark sampling, not crypto.
		const lookups = 10_000
		start = time.Now()
		for i := 0; i < lookups; i++ {
			id := scaleID("g", rng.Intn(grants))
			require.NoError(t, err, "lookup %s", id)
		}
		lookupDur := time.Since(start)

		info, err := os.Stat(path) // #nosec G703 - test fixture path.
		require.NoError(t, err)
		t.Logf("RESULT %-10s l0files=%-3d scan=%-12s (%d grants)  lookups10k=%-12s  size=%.1fMB",
			label, l0Files, scanDur.Round(time.Millisecond), count, lookupDur.Round(time.Millisecond),
			float64(info.Size())/(1<<20))
	}

	measure("flat", curPath)

	round := 0
	foldOnce := func() {
		t.Helper()
		round++
		partialPath := filepath.Join(t.TempDir(), "round-partial.c1z")
		// Distinct override window + fresh timestamps per round so the
		// keep-newer merge actually writes (equal timestamps would be
		// skipped and nothing would flush).
		buildProdScalePartial(t, ctx, partialPath, 1000+round)
		c, cleanup, err := NewCompactor(ctx, t.TempDir(),
			[]*CompactableSync{
				{FilePath: curPath, SyncID: manifestSyncID(t, curPath)},
				{FilePath: partialPath, SyncID: manifestSyncID(t, partialPath)},
			},
			WithTmpDir(t.TempDir()), WithEngine(dotc1z.EnginePebble), WithSkipGrantExpansion())
		require.NoError(t, err)
		out, err := c.Compact(ctx)
		require.NoError(t, err)
		require.NotNil(t, out)
		require.NoError(t, copyFileForFold(out.FilePath, curPath))
		require.NoError(t, cleanup())
	}

	for round < 4 {
		foldOnce()
	}
	measure("debt-4", curPath)

	for round < 10 {
		foldOnce()
	}
	measure("debt-10", curPath)

	// Reference point: a kway REBUILD of the same data. Ingested SSTs
	// land in deep levels, so this is what a genuinely flat artifact
	// looks like — the sync-produced base itself carries hundreds of
	// L0 files from bulk-load flushes (L0CompactionFileThreshold is
	// 500, so they persist).
	t.Setenv("BATON_EXPERIMENTAL_PEBBLE_COMPACTOR", "kway")
	partialPath := filepath.Join(t.TempDir(), "rebuild-partial.c1z")
	buildProdScalePartial(t, ctx, partialPath, 2000)
	c, cleanup, err := NewCompactor(ctx, t.TempDir(),
		[]*CompactableSync{
			{FilePath: basePath, SyncID: manifestSyncID(t, basePath)},
			{FilePath: partialPath, SyncID: manifestSyncID(t, partialPath)},
		},
		WithTmpDir(t.TempDir()), WithEngine(dotc1z.EnginePebble), WithSkipGrantExpansion())
	require.NoError(t, err)
	defer func() { require.NoError(t, cleanup()) }()
	out, err := c.Compact(ctx)
	require.NoError(t, err)
	require.NotNil(t, out)
	measure("rebuilt", out.FilePath)
}
