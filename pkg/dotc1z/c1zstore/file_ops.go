package c1zstore

import "context"

// FileOps is the file-level operations sub-store of Store. It covers
// operations that span sync runs or produce new sync runs within the same
// c1z file. Cross-file operations (SQL ATTACH) live in pkg/synccompactor,
// not here.
type FileOps interface {
	// CloneSync materializes the given sync run's data into a freshly
	// created standalone c1z file at outPath. Used by c1's storeCompletedSyncC1Z
	// activity to archive a completed sync.
	CloneSync(ctx context.Context, outPath string, syncID string, opts ...CloneSyncOption) error

	// CopyIsolateSync materializes the given sync run into a freshly created
	// standalone c1z at outPath like CloneSync, but copies the source working
	// database and deletes the other syncs from the copy instead of rebuilding
	// the target sync row-by-row. It is the optimized isolation step for large
	// files; the output contains only the target sync and is schema-normalized.
	CopyIsolateSync(ctx context.Context, outPath string, syncID string, opts ...CloneSyncOption) error

	// GenerateSyncDiff computes the diff between two existing sync runs
	// in this same file and writes the delta as a new SyncTypePartial
	// sync. Returns the new sync's id. Used by the local differ CLI.
	GenerateSyncDiff(ctx context.Context, baseSyncID, appliedSyncID string) (diffSyncID string, err error)
}

// CloneSyncOptions carries the engine-neutral knobs for FileOps.CloneSync.
// Engines apply what they support and ignore the rest (the Pebble engine
// currently ignores all of them).
type CloneSyncOptions struct {
	// TmpDir overrides the temporary directory used while assembling the
	// destination c1z. Empty means the engine's default.
	TmpDir string
}

// CloneSyncOption configures a FileOps.CloneSync call.
type CloneSyncOption func(*CloneSyncOptions)

// WithCloneTmpDir sets the temporary directory used while assembling the
// cloned c1z. Replaces the old dotc1z.WithC1FTmpDir at CloneSync call sites.
func WithCloneTmpDir(dir string) CloneSyncOption {
	return func(o *CloneSyncOptions) {
		o.TmpDir = dir
	}
}

// NewCloneSyncOptions folds opts into a CloneSyncOptions struct. Helper for
// FileOps implementations.
func NewCloneSyncOptions(opts ...CloneSyncOption) CloneSyncOptions {
	var out CloneSyncOptions
	for _, opt := range opts {
		opt(&out)
	}
	return out
}
