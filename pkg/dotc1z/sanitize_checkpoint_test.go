package dotc1z

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestSanitizeResumableCheckpointWriterInterface verifies that *C1File
// implements SanitizeResumableCheckpointWriter and that calling
// WriteSanitizeResumableCheckpoint on a live writable store produces a
// valid, openable c1z snapshot.
func TestSanitizeResumableCheckpointWriterInterface(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Create and populate a c1z.
	dstPath := filepath.Join(dir, "dst.c1z")
	dst, err := newC1ZFile(ctx, dstPath)
	require.NoError(t, err)
	_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, dst.PutResourceTypes(ctx, checkpointResourceType("rt-1")))

	// Type-assert to the capability interface.
	writer, ok := interface{}(dst).(SanitizeResumableCheckpointWriter)
	require.True(t, ok, "*C1File must implement SanitizeResumableCheckpointWriter")

	// Write a checkpoint while the live handle is still open.
	snapPath := filepath.Join(dir, "snap.c1z")
	err = writer.WriteSanitizeResumableCheckpoint(ctx, snapPath)
	require.NoError(t, err)

	// The snapshot must exist and be non-empty.
	fi, err := os.Stat(snapPath)
	require.NoError(t, err)
	require.Greater(t, fi.Size(), int64(0), "snapshot file must be non-empty")

	// The live dst handle must still be usable after the snapshot.
	require.NoError(t, dst.EndSync(ctx))
	require.NoError(t, dst.Close(ctx))

	// The snapshot must be openable.
	snap, err := OpenStore(ctx, snapPath, WithReadOnly(true))
	require.NoError(t, err)
	require.NoError(t, snap.Close(ctx))
}

// TestSanitizeResumableCheckpointWriterOptions verifies that
// WithCheckpointBulkLoad and WithCheckpointSkipVacuum are forwarded to
// SnapshotTo correctly.
func TestSanitizeResumableCheckpointWriterOptions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	dstPath := filepath.Join(dir, "dst.c1z")
	dst, err := newC1ZFile(ctx, dstPath)
	require.NoError(t, err)
	_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, dst.PutResourceTypes(ctx, checkpointResourceType("rt-1")))

	snapPath := filepath.Join(dir, "snap.c1z")
	err = dst.WriteSanitizeResumableCheckpoint(ctx, snapPath,
		WithCheckpointBulkLoad(true),
		WithCheckpointSkipVacuum(true),
	)
	require.NoError(t, err)

	fi, err := os.Stat(snapPath)
	require.NoError(t, err)
	require.Greater(t, fi.Size(), int64(0))

	require.NoError(t, dst.Close(ctx))
}

// TestSanitizeResumableCheckpointWriterUnsupportedError verifies that a
// non-SQLite store does not implement the capability (no panic, just no
// interface).
func TestSanitizeResumableCheckpointWriterUnsupportedError(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	pebblePath := filepath.Join(dir, "pebble.c1z")
	store, err := CreateStore(ctx, pebblePath, WithEngine(EnginePebble))
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	_, ok := store.(SanitizeResumableCheckpointWriter)
	require.False(t, ok, "pebbleStore must not implement SanitizeResumableCheckpointWriter (not yet supported)")
}

// TestSanitizeResumableCheckpointWriterRejectsReadOnly verifies that calling
// WriteSanitizeResumableCheckpoint on a read-only handle returns an error
// (SnapshotTo refuses read-only handles).
func TestSanitizeResumableCheckpointWriterRejectsReadOnly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	dstPath := filepath.Join(dir, "dst.c1z")
	seed, err := newC1ZFile(ctx, dstPath)
	require.NoError(t, err)
	_, err = seed.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, seed.EndSync(ctx))
	require.NoError(t, seed.Close(ctx))

	ro, err := newC1ZFile(ctx, dstPath, WithReadOnly(true))
	require.NoError(t, err)
	defer func() { _ = ro.Close(ctx) }()

	snapPath := filepath.Join(dir, "snap.c1z")
	err = ro.WriteSanitizeResumableCheckpoint(ctx, snapPath)
	require.Error(t, err, "WriteSanitizeResumableCheckpoint on read-only handle must return an error")
}

// TestSanitizeResumableCheckpointWriterNoOverwrite verifies that
// WriteSanitizeResumableCheckpoint refuses to overwrite an existing file at
// outPath (mirrors SnapshotTo's outPath-must-not-exist contract).
func TestSanitizeResumableCheckpointWriterNoOverwrite(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	dstPath := filepath.Join(dir, "dst.c1z")
	dst, err := newC1ZFile(ctx, dstPath)
	require.NoError(t, err)
	_, err = dst.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// Pre-create the snap path so SnapshotTo must refuse it.
	snapPath := filepath.Join(dir, "snap.c1z")
	f, err := os.Create(snapPath)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	err = dst.WriteSanitizeResumableCheckpoint(ctx, snapPath)
	require.Error(t, err, "WriteSanitizeResumableCheckpoint must not overwrite existing outPath")

	require.NoError(t, dst.Close(ctx))
}

// checkpointResourceType is a test helper that builds a minimal ResourceType proto.
func checkpointResourceType(id string) *v2.ResourceType {
	return v2.ResourceType_builder{Id: id}.Build()
}
