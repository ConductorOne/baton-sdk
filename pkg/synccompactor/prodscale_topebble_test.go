package synccompactor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// TestProdScaleToPebbleCurve measures sqlite→pebble conversion
// (C1File.ToPebble) at production scale to establish the conversion-time
// curve. For each grant-count point it builds (and caches) a sqlite v1
// base c1z with the prod-scale generator, then times the two real-world
// phases separately:
//
//   - open: NewC1ZFile on the .c1z (zstd unpack of the sqlite db), which
//     the bench in pkg/dotc1z excludes but a real conversion pays.
//   - convert: ToPebble end-to-end, including the destination envelope
//     save at close. Per-stage rows/durations come from ConvertStats.
//
// Defaults sweep 1M / 2.5M / 5M grants with the users/ents ratios of the
// prodscale fixture (grants/25 users, grants/125 ents); 5M grants is the
// shape that produced the 1.1GB pebble base.
//
// Gated and cached like the other prod-scale experiments:
//
//	BATON_PROD_SCALE_TOPEBBLE=1 go test ./pkg/synccompactor -run TestProdScaleToPebbleCurve -v -timeout 180m
//
// BATON_TOPEBBLE_GRANTS overrides the sweep points (comma-separated grant
// counts, e.g. "1000000,5000000").
func TestProdScaleToPebbleCurve(t *testing.T) {
	if os.Getenv("BATON_PROD_SCALE_TOPEBBLE") == "" {
		t.Skip("set BATON_PROD_SCALE_TOPEBBLE=1 to run the to-pebble conversion experiment")
	}
	ctx := context.Background()
	// Surface the conversion's Debug-level phase timings (EndSync /
	// checkpoint / envelope encode) in the test output.
	if logger, err := zap.NewDevelopment(); err == nil {
		ctx = ctxzap.ToContext(ctx, logger)
	}
	require.NoError(t, ensurePebbleRegistered())

	fixDir := os.Getenv("BATON_PROD_SCALE_DIR")
	if fixDir == "" {
		fixDir = filepath.Join("..", "..", ".profiles", "prodscale-topebble")
	} else {
		fixDir = filepath.Join(fixDir, "topebble")
	}
	require.NoError(t, os.MkdirAll(fixDir, 0o755)) // #nosec G703 - developer-provided fixture dir env var.

	points := envIntList("BATON_TOPEBBLE_GRANTS", []int{1_000_000, 2_500_000, 5_000_000})

	for _, grants := range points {
		grants := grants
		t.Run(fmt.Sprintf("grants=%d", grants), func(t *testing.T) {
			users := max(grants/25, 1000)
			ents := max(grants/125, 100)

			basePath := filepath.Join(fixDir, fmt.Sprintf("base-sqlite-%d.c1z", grants))
			// #nosec G703 - fixture paths derive from a developer-provided env var.
			if _, err := os.Stat(basePath); errors.Is(err, os.ErrNotExist) {
				t.Logf("building sqlite base: grants=%d users=%d ents=%d", grants, users, ents)
				start := time.Now()
				buildProdScaleBaseStore(t, ctx, basePath, grants, users, ents,
					// Same write-speed pragmas the compactor uses; the
					// fixture build is not the thing under measurement.
					dotc1z.WithPragma("journal_mode", "OFF"),
					dotc1z.WithPragma("synchronous", "OFF"),
				)
				t.Logf("sqlite base built in %s", time.Since(start).Round(time.Second))
			}
			logFileSize(t, "sqlite base", basePath)
			baseBytes := fileSizeOrZero(basePath)

			tmp := t.TempDir()
			openStart := time.Now()
			src, err := dotc1z.NewC1ZFile(ctx, basePath,
				dotc1z.WithTmpDir(tmp),
				dotc1z.WithReadOnly(true),
				dotc1z.WithDecoderOptions(dotc1z.WithDecoderConcurrency(-1)),
			)
			require.NoError(t, err)
			openElapsed := time.Since(openStart)
			defer func() { require.NoError(t, src.Close(ctx)) }()

			outPath := filepath.Join(tmp, "converted.c1z")
			convStart := time.Now()
			stats, err := src.ToPebble(ctx, outPath, "", dotc1z.WithConvertTmpDir(tmp))
			convElapsed := time.Since(convStart)
			require.NoError(t, err)
			require.Equal(t, int64(grants), stats.Grants.Rows, "converted grant count must match the fixture")

			outBytes := fileSizeOrZero(outPath)
			grantsPerSec := float64(stats.Grants.Rows) / stats.Grants.Duration.Seconds()
			t.Logf("grants=%d: open(unpack)=%s convert=%s total=%s", grants,
				openElapsed.Round(time.Millisecond), convElapsed.Round(time.Millisecond),
				(openElapsed + convElapsed).Round(time.Millisecond))
			t.Logf("  stages: resource_types=%s resources=%s (%d rows) entitlements=%s (%d rows) grants=%s (%d rows, %.0f/s)",
				stats.ResourceTypes.Duration.Round(time.Millisecond),
				stats.Resources.Duration.Round(time.Millisecond), stats.Resources.Rows,
				stats.Entitlements.Duration.Round(time.Millisecond), stats.Entitlements.Rows,
				stats.Grants.Duration.Round(time.Millisecond), stats.Grants.Rows, grantsPerSec)
			t.Logf("  bytes: sqlite_c1z=%.1fMB pebble_c1z=%.1fMB convert_throughput=%.1fMB/s",
				float64(baseBytes)/(1<<20), float64(outBytes)/(1<<20),
				float64(baseBytes)/(1<<20)/convElapsed.Seconds())
		})
	}
}

// envIntList parses a comma-separated list of positive ints from an env
// var, falling back to def when unset or unparseable.
func envIntList(name string, def []int) []int {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	var out []int
	for _, part := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n <= 0 {
			return def
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return def
	}
	return out
}
