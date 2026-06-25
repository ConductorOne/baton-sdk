package dotc1z

import (
	"context"
	"errors"
	"fmt"
	"os"
)

// DiffResult carries the sync IDs of the two diff syncs written into the
// applied c1z by GenerateSyncDiffFromFiles.
type DiffResult struct {
	// UpsertsSyncID is the sync ID of the SyncTypePartialUpserts sync
	// (records present in the applied/NEW state that were absent or
	// different in the base/OLD state).
	UpsertsSyncID string
	// DeletionsSyncID is the sync ID of the SyncTypePartialDeletions sync
	// (records present in the base/OLD state that are absent from the
	// applied/NEW state).
	DeletionsSyncID string
}

// DiffOptions carries the engine-neutral knobs for GenerateSyncDiffFromFiles.
type DiffOptions struct {
	// TmpDir overrides the temporary directory used while opening the c1z
	// files. Empty means the default (os.TempDir()).
	TmpDir string
	// SyncLimit limits the number of syncs retained on the applied c1z after
	// the diff syncs are written. Zero means the store's default.
	SyncLimit int
	// SkipVacuum skips the VACUUM step on the applied c1z. Useful when the
	// caller will consume and discard the output immediately.
	SkipVacuum bool
}

// DiffOption configures a GenerateSyncDiffFromFiles call.
type DiffOption func(*DiffOptions)

// WithDiffTmpDir sets the temporary directory used while opening both c1z
// files. Replaces dotc1z.WithC1FTmpDir at call sites that previously
// constructed the stores manually.
func WithDiffTmpDir(dir string) DiffOption {
	return func(o *DiffOptions) {
		o.TmpDir = dir
	}
}

// WithDiffSyncLimit limits the syncs retained on the applied c1z.
func WithDiffSyncLimit(n int) DiffOption {
	return func(o *DiffOptions) {
		o.SyncLimit = n
	}
}

// WithDiffSkipVacuum skips the VACUUM step on the applied c1z.
func WithDiffSkipVacuum(skip bool) DiffOption {
	return func(o *DiffOptions) {
		o.SkipVacuum = skip
	}
}

// ErrDiffEngineUnsupported is returned by GenerateSyncDiffFromFiles when one
// or both input files are not in the v1/SQLite format. Cross-file diff for the
// Pebble engine is a planned follow-on; for now only v1/v1 pairs are
// supported.
var ErrDiffEngineUnsupported = errors.New("dotc1z: GenerateSyncDiffFromFiles is only supported for v1/SQLite c1z files")

// GenerateSyncDiffFromFiles computes the diff between baseSyncID (in
// basePath) and appliedSyncID (in appliedPath), writing two new partial syncs
// (SyncTypePartialUpserts and SyncTypePartialDeletions) directly into
// appliedPath. It returns the IDs of those two new syncs.
//
// Both files must be v1/SQLite format. Pebble (v3) inputs return
// ErrDiffEngineUnsupported.
//
// appliedPath is opened read-write; the diff syncs are flushed to disk when
// the function returns. basePath is opened read-only.
//
// This is the engine-neutral replacement for the c1-side pattern:
//
//	compactedFile.AttachFile(baseFile, "attached")
//	attached.GenerateSyncDiffFromFile(ctx, diffBaseID, result.SyncID)
func GenerateSyncDiffFromFiles(
	ctx context.Context,
	basePath, appliedPath string,
	baseSyncID, appliedSyncID string,
	opts ...DiffOption,
) (*DiffResult, error) {
	options := &DiffOptions{}
	for _, o := range opts {
		o(options)
	}

	// Both paths must be non-empty existing files before we try to open them.
	for _, p := range []string{basePath, appliedPath} {
		stat, err := os.Stat(p)
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("diff-files: %s: %w", p, ErrStoreNotFound)
		}
		if err != nil {
			return nil, fmt.Errorf("diff-files: stat %s: %w", p, err)
		}
		if stat.Size() == 0 {
			return nil, fmt.Errorf("diff-files: %s: %w", p, ErrStoreEmpty)
		}
	}

	// Verify both are v1/SQLite. Open each file just to read the header.
	for _, p := range []string{basePath, appliedPath} {
		format, err := fileFormat(p)
		if err != nil {
			return nil, fmt.Errorf("diff-files: read header %s: %w", p, err)
		}
		if format != C1ZFormatV1 {
			return nil, fmt.Errorf("diff-files: %s is %s format; %w", p, format, ErrDiffEngineUnsupported)
		}
	}

	// Build C1ZOption slices for each open.
	appliedC1ZOpts := []C1ZOption{}
	if options.TmpDir != "" {
		appliedC1ZOpts = append(appliedC1ZOpts, WithTmpDir(options.TmpDir))
	}
	if options.SyncLimit > 0 {
		appliedC1ZOpts = append(appliedC1ZOpts, WithSyncLimit(options.SyncLimit))
	}
	if options.SkipVacuum {
		appliedC1ZOpts = append(appliedC1ZOpts, WithSkipVacuum(true))
	}

	baseC1ZOpts := []C1ZOption{WithReadOnly(true)}
	if options.TmpDir != "" {
		baseC1ZOpts = append(baseC1ZOpts, WithTmpDir(options.TmpDir))
	}

	// Open applied (NEW/compacted) writable — diff syncs are written here.
	appliedStore, err := OpenStore(ctx, appliedPath, appliedC1ZOpts...)
	if err != nil {
		return nil, fmt.Errorf("diff-files: open applied %s: %w", appliedPath, err)
	}
	defer func() {
		if appliedStore != nil {
			_ = appliedStore.Close(ctx)
		}
	}()
	appliedFile, ok := AsSQLiteStore(appliedStore)
	if !ok {
		return nil, fmt.Errorf("diff-files: applied store is not SQLite-backed; %w", ErrDiffEngineUnsupported)
	}

	// Open base (OLD) read-only — only read for attach.
	baseStore, err := OpenStore(ctx, basePath, baseC1ZOpts...)
	if err != nil {
		return nil, fmt.Errorf("diff-files: open base %s: %w", basePath, err)
	}
	defer func() {
		if baseStore != nil {
			_ = baseStore.Close(ctx)
		}
	}()
	baseFile, ok := AsSQLiteStore(baseStore)
	if !ok {
		return nil, fmt.Errorf("diff-files: base store is not SQLite-backed; %w", ErrDiffEngineUnsupported)
	}

	// Attach base to applied for cross-file SQL diff.
	attached, err := appliedFile.AttachFile(baseFile, "attached")
	if err != nil {
		return nil, fmt.Errorf("diff-files: attach base to applied: %w", err)
	}
	defer func() {
		if attached != nil {
			_, _ = attached.DetachFile("attached")
		}
	}()

	// Generate the diff. This writes two new syncs into appliedFile.
	upsertsSyncID, deletionsSyncID, err := attached.GenerateSyncDiffFromFile(ctx, baseSyncID, appliedSyncID)
	if err != nil {
		return nil, fmt.Errorf("diff-files: generate diff: %w", err)
	}

	// Detach explicitly before closing so the attached handle is not left
	// referencing a file that is about to be closed.
	if _, detachErr := attached.DetachFile("attached"); detachErr != nil {
		return nil, fmt.Errorf("diff-files: detach base: %w", detachErr)
	}
	attached = nil

	if err := baseStore.Close(ctx); err != nil {
		return nil, fmt.Errorf("diff-files: close base: %w", err)
	}
	baseStore = nil

	// Close applied — this flushes the new diff syncs to the .c1z envelope.
	if err := appliedStore.Close(ctx); err != nil {
		return nil, fmt.Errorf("diff-files: close applied: %w", err)
	}
	appliedStore = nil

	return &DiffResult{
		UpsertsSyncID:   upsertsSyncID,
		DeletionsSyncID: deletionsSyncID,
	}, nil
}

// fileFormat opens path, reads its format header, and returns the format.
// It is a thin helper so the caller does not have to manage a file handle.
func fileFormat(path string) (C1ZFormat, error) {
	f, err := os.Open(path)
	if err != nil {
		return C1ZFormatUnknown, err
	}
	defer f.Close()
	return ReadHeaderFormat(f)
}
