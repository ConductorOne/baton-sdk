package dotc1z

import "github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"

// The file-operations contract lives in pkg/dotc1z/c1zstore so storage
// engines can implement it without importing this package. These aliases
// preserve the historical dotc1z names.

// FileOps is the file-level operations sub-store of C1ZStore. See
// c1zstore.FileOps for the full contract.
type FileOps = c1zstore.FileOps

// CloneSyncOption configures a FileOps.CloneSync call. See
// c1zstore.CloneSyncOption.
type CloneSyncOption = c1zstore.CloneSyncOption

// CloneSyncOptions carries the engine-neutral knobs for FileOps.CloneSync.
// See c1zstore.CloneSyncOptions.
type CloneSyncOptions = c1zstore.CloneSyncOptions

// WithCloneTmpDir sets the temporary directory used while assembling the
// cloned c1z. Replaces WithC1FTmpDir at FileOps.CloneSync call sites.
func WithCloneTmpDir(dir string) CloneSyncOption {
	return c1zstore.WithCloneTmpDir(dir)
}
