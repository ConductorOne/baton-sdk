package synccompactor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

// TestProdScaleFoldPhases isolates the fold's phase costs against the
// cached prod-scale base fixture: file copy, store open (payload
// unpack), and dirty save (checkpoint + envelope write, where frame
// splicing should fire). Run after TestProdScaleSkewedCompaction has
// built the fixture. The development logger surfaces the engine's
// spliced/encoded frame counts.
func TestProdScaleFoldPhases(t *testing.T) {
	if os.Getenv("BATON_PROD_SCALE_TEST") == "" {
		t.Skip("set BATON_PROD_SCALE_TEST=1 to run")
	}
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	ctx := ctxzap.ToContext(context.Background(), logger)
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

	workPath := filepath.Join(t.TempDir(), "fold-base.c1z")
	start := time.Now()
	require.NoError(t, copyFileForFold(basePath, workPath))
	t.Logf("phase copy: %s", time.Since(start).Round(time.Millisecond))

	start = time.Now()
	w, err := dotc1z.NewStore(ctx, workPath, dotc1z.WithTmpDir(t.TempDir()))
	require.NoError(t, err)
	t.Logf("phase open(unpack): %s", time.Since(start).Round(time.Millisecond))

	require.True(t, enginepkg.MarkStoreDirty(w))
	start = time.Now()
	require.NoError(t, w.Close(ctx))
	t.Logf("phase save(close): %s", time.Since(start).Round(time.Millisecond))
}
