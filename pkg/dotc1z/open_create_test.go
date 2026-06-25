package dotc1z

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestOpenStoreNotFound verifies that OpenStore returns ErrStoreNotFound
// when the target path does not exist.
func TestOpenStoreNotFound(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "missing.c1z")
	_, err := OpenStore(ctx, path)
	require.ErrorIs(t, err, ErrStoreNotFound, "OpenStore on missing path must return ErrStoreNotFound")
}

// TestOpenStoreEmpty verifies that OpenStore rejects an existing empty file:
// open requires a valid c1z, while NewStore keeps the historical open-or-create
// placeholder behavior.
func TestOpenStoreEmpty(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "empty.c1z")
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = OpenStore(ctx, path)
	require.ErrorIs(t, err, ErrStoreEmpty, "OpenStore on empty file must return ErrStoreEmpty")
}

// TestOpenStoreCorrupt verifies that OpenStore returns an error satisfying
// ErrStoreCorrupt when the file has content but is not a valid c1z.
func TestOpenStoreCorrupt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "corrupt.c1z")
	require.NoError(t, os.WriteFile(path, []byte("this is not a valid c1z file"), 0o600))

	_, err := OpenStore(ctx, path)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrStoreCorrupt),
		"OpenStore on corrupt file must return an error satisfying ErrStoreCorrupt; got %T: %v", err, err)

	// Cause must be preserved via errors.As / Unwrap.
	var corrupt *StoreCorruptError
	require.True(t, errors.As(err, &corrupt), "error must be a *StoreCorruptError")
	require.NotNil(t, corrupt.Cause, "StoreCorruptError.Cause must be set")
}

// TestOpenStoreCorruptPreservesCause ensures the low-level cause embedded in
// StoreCorruptError is distinguishable from ErrStoreCorrupt itself, so callers
// can both match the sentinel and inspect the root reason.
func TestOpenStoreCorruptPreservesCause(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bad-magic.c1z")
	require.NoError(t, os.WriteFile(path, []byte("BADMAGIC\x00extra bytes here"), 0o600))

	_, err := OpenStore(ctx, path)
	require.True(t, errors.Is(err, ErrStoreCorrupt))

	var corrupt *StoreCorruptError
	require.True(t, errors.As(err, &corrupt))
	// The underlying cause should be ErrInvalidFile (bad magic bytes).
	require.True(t, errors.Is(corrupt.Cause, ErrInvalidFile),
		"StoreCorruptError.Cause must be ErrInvalidFile for an invalid magic; got %v", corrupt.Cause)
}

func TestWrapOpenExistingStoreErrorLeavesTransientErrorsUnwrapped(t *testing.T) {
	err := wrapOpenExistingStoreError(&os.PathError{
		Op:   "write",
		Path: "db",
		Err:  syscall.ENOSPC,
	})
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.False(t, errors.Is(err, ErrStoreCorrupt), "ENOSPC must not be classified as store corruption")

	err = wrapOpenExistingStoreError(context.DeadlineExceeded)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, errors.Is(err, ErrStoreCorrupt), "context deadline must not be classified as store corruption")

	err = wrapOpenExistingStoreError(&os.PathError{
		Op:   "open",
		Path: "db",
		Err:  syscall.EMFILE,
	})
	require.ErrorIs(t, err, syscall.EMFILE)
	require.False(t, errors.Is(err, ErrStoreCorrupt), "EMFILE must not be classified as store corruption")
}

func TestWrapOpenExistingStoreErrorClassifiesKnownDecodeErrorsAsCorrupt(t *testing.T) {
	err := wrapOpenExistingStoreError(ErrInvalidFile)
	require.ErrorIs(t, err, ErrStoreCorrupt)
	require.ErrorIs(t, err, ErrInvalidFile)
}

// TestOpenStoreValidV1 verifies that OpenStore opens an existing v1/SQLite c1z
// and dispatches to a *C1File implementation.
func TestOpenStoreValidV1(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v1.c1z")

	// Seed a valid v1 c1z.
	seed, err := newC1ZFile(ctx, path)
	require.NoError(t, err)
	_, err = seed.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, seed.EndSync(ctx))
	require.NoError(t, seed.Close(ctx))

	store, err := OpenStore(ctx, path)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	_, ok := store.(*C1File)
	require.True(t, ok, "OpenStore on v1 file must return *C1File; got %T", store)
}

// TestOpenStoreValidV3 verifies that OpenStore opens an existing v3/Pebble c1z.
func TestOpenStoreValidV3(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v3.c1z")
	writeV3EnvelopeForEngine(t, path, EnginePebble)

	store, err := OpenStore(ctx, path)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	_, ok := store.(*pebbleStore)
	require.True(t, ok, "OpenStore on v3 file must return *pebbleStore; got %T", store)
}

// TestOpenStoreDoesNotAutoConvert verifies that OpenStore does NOT silently
// convert a v1/SQLite file to Pebble even when WithEngine(EnginePebble) is
// passed. The on-disk format is authoritative for OpenStore.
func TestOpenStoreDoesNotAutoConvert(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v1-no-convert.c1z")

	seed, err := newC1ZFile(ctx, path)
	require.NoError(t, err)
	_, err = seed.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, seed.EndSync(ctx))
	require.NoError(t, seed.Close(ctx))

	// Should still open as SQLite even with WithEngine(EnginePebble).
	store, err := OpenStore(ctx, path, WithEngine(EnginePebble))
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	_, ok := store.(*C1File)
	require.True(t, ok, "OpenStore must not auto-convert; expected *C1File, got %T", store)
}

// TestCreateStoreNewPath verifies that CreateStore creates a new SQLite c1z
// at a path that does not exist.
func TestCreateStoreNewPath(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "new.c1z")

	store, err := CreateStore(ctx, path)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	_, ok := store.(*C1File)
	require.True(t, ok, "CreateStore default must return *C1File (SQLite); got %T", store)
}

// TestCreateStoreAcceptsEmptyExistingPath verifies that CreateStore accepts an
// existing empty (zero-byte) placeholder file as a create target.
func TestCreateStoreAcceptsEmptyExistingPath(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "empty.c1z")
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	store, err := CreateStore(ctx, path)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()
}

// TestCreateStorePebbleEngine verifies that CreateStore respects
// WithEngine(EnginePebble) and returns a Pebble-backed store.
func TestCreateStorePebbleEngine(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "new-pebble.c1z")

	store, err := CreateStore(ctx, path, WithEngine(EnginePebble))
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()

	_, ok := store.(*pebbleStore)
	require.True(t, ok, "CreateStore with EnginePebble must return *pebbleStore; got %T", store)
}

// TestCreateStoreRejectsNonEmptyExistingPath verifies that CreateStore returns
// an error if the path already contains a non-empty file.
func TestCreateStoreRejectsNonEmptyExistingPath(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "existing.c1z")

	// Seed a valid c1z so the file is non-empty.
	seed, err := newC1ZFile(ctx, path)
	require.NoError(t, err)
	_, err = seed.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, seed.EndSync(ctx))
	require.NoError(t, seed.Close(ctx))

	_, err = CreateStore(ctx, path)
	require.Error(t, err, "CreateStore on a non-empty path must return an error")
}

// TestNewStoreOpenOrCreatePreserved verifies that the deprecated NewStore
// preserves its historical open-or-create semantics: a missing path succeeds
// (create), an empty placeholder succeeds (create), and an existing valid path
// succeeds (open).
func TestNewStoreOpenOrCreatePreserved(t *testing.T) {
	ctx := context.Background()

	// Create case: path does not exist.
	newPath := filepath.Join(t.TempDir(), "new.c1z")
	store, err := NewStore(ctx, newPath)
	require.NoError(t, err, "NewStore on missing path must succeed (create)")
	require.NoError(t, store.Close(ctx))

	// Empty placeholder case: path exists but has no c1z content yet.
	emptyPath := filepath.Join(t.TempDir(), "empty.c1z")
	emptyFile, err := os.Create(emptyPath)
	require.NoError(t, err)
	require.NoError(t, emptyFile.Close())
	store, err = NewStore(ctx, emptyPath)
	require.NoError(t, err, "NewStore on empty path must succeed (create)")
	require.NoError(t, store.Close(ctx))

	// Open case: path already has a valid c1z.
	existingPath := filepath.Join(t.TempDir(), "existing.c1z")
	seed, err := newC1ZFile(ctx, existingPath)
	require.NoError(t, err)
	_, err = seed.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, seed.EndSync(ctx))
	require.NoError(t, seed.Close(ctx))

	store2, err := NewStore(ctx, existingPath)
	require.NoError(t, err, "NewStore on existing path must succeed (open)")
	require.NoError(t, store2.Close(ctx))
}

// TestStoreOptionsSkipVacuumBulkLoad verifies that WithSkipVacuum and
// WithBulkLoad are correctly propagated through CreateStore to the underlying
// SQLite driver. Previously these options were silently dropped by the
// StoreOptions translation path.
func TestStoreOptionsSkipVacuumBulkLoad(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "opts.c1z")

	store, err := CreateStore(ctx, path, WithSkipVacuum(true), WithBulkLoad(true), WithSkipVacuum(true))
	require.NoError(t, err)
	cf, ok := store.(*C1File)
	require.True(t, ok)

	require.True(t, cf.skipVacuum, "skipVacuum must be set via CreateStore+WithSkipVacuum")
	require.True(t, cf.bulkLoad, "bulkLoad must be set via CreateStore+WithBulkLoad")
	_ = store.Close(ctx)
}
