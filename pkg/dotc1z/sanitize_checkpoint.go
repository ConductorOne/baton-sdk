package dotc1z

import "context"

// SanitizeResumableCheckpointWriter is the capability interface for c1z stores
// that can materialize their current committed state as a standalone .c1z file
// while the live handle remains open and writable.
//
// This snapshot is used as a resumable checkpoint by the sanitizer's snapshot
// loop: if the sanitize activity is interrupted, the next attempt can restore
// the partial destination from the last uploaded checkpoint and resume from
// the embedded sync token rather than starting over.
//
// Not a part of C1ZStore. This capability is scoped exclusively to the
// sanitize/resume lifecycle and should not be required of all store
// implementations. Callers obtain it via a type assertion:
//
//	if w, ok := store.(dotc1z.SanitizeResumableCheckpointWriter); ok {
//	    err = w.WriteSanitizeResumableCheckpoint(ctx, snapPath, opts...)
//	}
type SanitizeResumableCheckpointWriter interface {
	WriteSanitizeResumableCheckpoint(ctx context.Context, outPath string, opts ...SanitizeResumableCheckpointOption) error
}

// SanitizeResumableCheckpointOptions carries the engine-neutral knobs for
// WriteSanitizeResumableCheckpoint. SQLite translates these to C1FOptions;
// a future Pebble implementation will use its own checkpoint/envelope
// machinery.
type SanitizeResumableCheckpointOptions struct {
	TmpDir     string
	BulkLoad   bool
	SkipVacuum bool
}

// SanitizeResumableCheckpointOption configures a
// WriteSanitizeResumableCheckpoint call.
type SanitizeResumableCheckpointOption func(*SanitizeResumableCheckpointOptions)

// WithCheckpointTmpDir sets the temporary directory used while building the
// checkpoint c1z. Empty means the store's default.
func WithCheckpointTmpDir(dir string) SanitizeResumableCheckpointOption {
	return func(o *SanitizeResumableCheckpointOptions) {
		o.TmpDir = dir
	}
}

// WithCheckpointBulkLoad enables deferred secondary-index creation for the
// checkpoint destination. Recommended for whale-scale sanitize runs; mirrors
// WithC1FBulkLoad semantics. Pair with WithCheckpointSkipVacuum(true).
func WithCheckpointBulkLoad(enabled bool) SanitizeResumableCheckpointOption {
	return func(o *SanitizeResumableCheckpointOptions) {
		o.BulkLoad = enabled
	}
}

// WithCheckpointSkipVacuum skips the VACUUM step on the checkpoint destination.
// The checkpoint output is consumed once on resume and discarded, so the VACUUM
// cost is wasted work. Mirrors WithC1FSkipVacuum semantics.
func WithCheckpointSkipVacuum(skip bool) SanitizeResumableCheckpointOption {
	return func(o *SanitizeResumableCheckpointOptions) {
		o.SkipVacuum = skip
	}
}

// WriteSanitizeResumableCheckpoint implements SanitizeResumableCheckpointWriter
// for the SQLite-backed *C1File by delegating to SnapshotTo with the
// engine-neutral options translated to C1FOptions.
func (c *C1File) WriteSanitizeResumableCheckpoint(ctx context.Context, outPath string, opts ...SanitizeResumableCheckpointOption) error {
	options := &SanitizeResumableCheckpointOptions{}
	for _, o := range opts {
		o(options)
	}
	var c1fOpts []C1FOption
	if options.TmpDir != "" {
		c1fOpts = append(c1fOpts, WithC1FTmpDir(options.TmpDir))
	}
	if options.BulkLoad {
		c1fOpts = append(c1fOpts, WithC1FBulkLoad(true))
	}
	if options.SkipVacuum {
		c1fOpts = append(c1fOpts, WithC1FSkipVacuum(true))
	}
	return c.SnapshotTo(ctx, outPath, c1fOpts...)
}

// Compile-time assertion: *C1File implements SanitizeResumableCheckpointWriter.
var _ SanitizeResumableCheckpointWriter = (*C1File)(nil)
