package logging

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSetLogLevelUpdatesActiveLogger(t *testing.T) {
	t.Parallel()

	ctx, err := Init(context.Background(), WithLogLevel("info"))
	require.NoError(t, err, "Init")
	logger := ctxzap.Extract(ctx)
	require.Nil(t, logger.Check(zap.DebugLevel, "debug"), "debug should be disabled at info level")

	require.NoError(t, SetLogLevel("debug"), "SetLogLevel")
	require.NotNil(t, logger.Check(zap.DebugLevel, "debug"), "debug should be enabled after SetLogLevel")
}

func TestSetLogLevelRejectsInvalidLevel(t *testing.T) {
	t.Parallel()

	err := SetLogLevel("verbose")
	require.Error(t, err, "expected invalid log level to fail")
}

// closeActiveRotator releases the rotator installed by the most recent Init so
// the test's temp dir can be cleaned up. Safe to call when none is active.
func closeActiveRotator(t *testing.T) {
	t.Helper()
	activeMu.Lock()
	defer activeMu.Unlock()
	if activeRotator != nil {
		require.NoError(t, activeRotator.Close(), "close rotator")
		activeRotator = nil
	}
}

func TestFileRotationWritesToFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "baton.log")

	ctx, err := Init(context.Background(),
		WithFileRotation(logPath, DefaultMaxSizeMB, 0, 7),
		WithFileOnly(true),
	)
	require.NoError(t, err, "Init")
	t.Cleanup(func() { closeActiveRotator(t) })

	ctxzap.Extract(ctx).Info("hello rotation")

	data, err := os.ReadFile(logPath)
	require.NoError(t, err, "ReadFile")
	require.Contains(t, string(data), "hello rotation")
}

func TestFileRotationConfiguresLumberjack(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "baton.log")

	_, err := Init(context.Background(),
		WithFileRotation(logPath, 50, 5, 7),
		WithFileOnly(true),
	)
	require.NoError(t, err, "Init")
	t.Cleanup(func() { closeActiveRotator(t) })

	activeMu.RLock()
	r := activeRotator
	activeMu.RUnlock()
	require.NotNil(t, r, "rotator should be installed")
	require.Equal(t, logPath, r.Filename)
	require.Equal(t, 50, r.MaxSize, "maxSizeMB plumbed to MaxSize")
	require.Equal(t, 5, r.MaxBackups, "maxBackups plumbed to MaxBackups")
	require.Equal(t, 7, r.MaxAge, "retentionDays plumbed to MaxAge")
	require.True(t, r.Compress, "rotated files should be gzip-compressed")
}

func TestFileRotationPreservesInitialFields(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "baton.log")

	ctx, err := Init(context.Background(),
		WithInitialFields(map[string]interface{}{"connector": "acme"}),
		WithFileRotation(logPath, DefaultMaxSizeMB, 0, 7),
		WithFileOnly(true),
	)
	require.NoError(t, err, "Init")
	t.Cleanup(func() { closeActiveRotator(t) })

	ctxzap.Extract(ctx).Info("with fields")

	data, err := os.ReadFile(logPath)
	require.NoError(t, err, "ReadFile")
	require.Contains(t, string(data), `"connector":"acme"`, "InitialFields must survive in rotation mode")
}

func TestFileRotationIgnoresOutputPaths(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "baton.log")
	strayPath := filepath.Join(dir, "stray.log")

	ctx, err := Init(context.Background(),
		WithOutputPaths([]string{strayPath}), // should be ignored in rotation mode
		WithFileRotation(logPath, DefaultMaxSizeMB, 0, 7),
		WithFileOnly(true),
	)
	require.NoError(t, err, "Init")
	t.Cleanup(func() { closeActiveRotator(t) })

	ctxzap.Extract(ctx).Info("routed to rotator")

	data, err := os.ReadFile(logPath)
	require.NoError(t, err, "ReadFile rotating log")
	require.Contains(t, string(data), "routed to rotator")
	require.NoFileExists(t, strayPath, "WithOutputPaths must not open a separate sink in rotation mode")
}

func TestReinitWithoutRotationClosesRotator(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "baton.log")

	_, err := Init(context.Background(), WithFileRotation(logPath, DefaultMaxSizeMB, 0, 7), WithFileOnly(true))
	require.NoError(t, err, "Init with rotation")

	activeMu.RLock()
	require.NotNil(t, activeRotator, "rotator should be installed")
	activeMu.RUnlock()

	// Re-init without rotation must release the previous rotator's file handle.
	_, err = Init(context.Background(), WithLogLevel("info"))
	require.NoError(t, err, "Init without rotation")

	activeMu.RLock()
	require.Nil(t, activeRotator, "previous rotator should be closed and cleared on reinit")
	activeMu.RUnlock()
}

func TestFileRotationRotatesOnSize(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "baton.log")

	// 1 MB is lumberjack's minimum rotation granularity; write well past it.
	ctx, err := Init(context.Background(),
		WithFileRotation(logPath, 1, 0, 7),
		WithFileOnly(true),
	)
	require.NoError(t, err, "Init")
	t.Cleanup(func() { closeActiveRotator(t) })

	logger := ctxzap.Extract(ctx)
	payload := strings.Repeat("x", 512)
	for i := 0; i < 5000; i++ { // ~3MB of payload -> forces at least one rotation
		logger.Info("filler", zap.String("p", payload))
	}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err, "ReadDir")
	require.Greater(t, len(entries), 1, "expected the active log plus at least one rotated file")
}
