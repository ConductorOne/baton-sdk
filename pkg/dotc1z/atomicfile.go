package dotc1z

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// AtomicFile stages a file next to its final output path and publishes it
// with a rename, so a crash mid-write never leaves a torn output.
//
// Every c1z write path stages before renaming, and the staged file holds the
// complete sync artifact — PII plus the access graph. Two properties of the
// staging are security requirements, not conveniences:
//
//   - The staging name must be unpredictable and created exclusively. A
//     deterministic "<out>.tmp" opened with O_CREATE|O_TRUNC lets any local
//     account that can create entries in the output directory pre-plant the
//     name: a planted directory deterministically fails every save (a
//     poison-pill against the connector's publish path), and a planted file
//     or symlink receives the artifact's bytes. CreateTemp gives both: an
//     unguessable name and O_EXCL creation.
//   - The staged file must be private (0600). The published artifact replaces
//     the caller's placeholder, and c1api handlers create that placeholder
//     0600 via os.CreateTemp; a 0644 staging file silently republishes the
//     whole artifact world-readable in a shared temp directory.
//
// Usage:
//
//	f, err := dotc1z.NewAtomicFile(outPath)
//	if err != nil { ... }
//	write(f)                    // or f.File directly
//	if err := f.Commit(); err != nil { ... } // sync, close, chmod 0600, rename
//	// On any error before Commit: f.Abort() (idempotent, removes only the
//	// file this process created).
type AtomicFile struct {
	// File is the staged file, open for writing. Valid until Commit or
	// Abort; nil afterwards.
	File *os.File

	path   string // staged path; removed by Abort
	target string // caller's final output path
}

// NewAtomicFile creates the staging file for target: an exclusively created,
// unpredictable sibling in target's directory, mode 0600. The caller writes
// to f.File and finishes with Commit (or Abort on any error).
func NewAtomicFile(target string) (*AtomicFile, error) {
	dir, base := filepath.Split(target)
	if dir == "" {
		dir = "."
	}
	// "*" is replaced by a random suffix; CreateTemp uses O_RDWR|O_CREATE|O_EXCL.
	f, err := os.CreateTemp(dir, base+".tmp-*") // #nosec G304 -- staging sibling of the caller-provided output path.
	if err != nil {
		return nil, fmt.Errorf("atomic file: create staging file for %s: %w", target, err)
	}
	return &AtomicFile{File: f, path: f.Name(), target: target}, nil
}

// Commit syncs the staged file to disk, closes it, pins its mode to 0600
// (CreateTemp created it 0600; chmod makes that hold even under a future
// change of the creation default), and renames it over the target path.
// The rename is atomic: readers see either the old output or the complete
// new one, never a partial write.
func (f *AtomicFile) Commit() error {
	if f.File == nil {
		return errors.New("atomic file: Commit on a closed file")
	}
	if err := f.File.Sync(); err != nil {
		return fmt.Errorf("atomic file: sync %s: %w", f.path, err)
	}
	if err := f.File.Close(); err != nil {
		f.File = nil
		return fmt.Errorf("atomic file: close %s: %w", f.path, err)
	}
	f.File = nil
	if err := os.Chmod(f.path, 0o600); err != nil {
		_ = os.Remove(f.path)
		return fmt.Errorf("atomic file: chmod %s: %w", f.path, err)
	}
	if err := os.Rename(f.path, f.target); err != nil {
		// The staged file still exists; leave removal to Abort (the caller
		// holds no other reference to the failure and may want to retry).
		return fmt.Errorf("atomic file: rename %s to %s: %w", f.path, f.target, err)
	}
	return nil
}

// Abort closes the staged file if still open and removes it. It only ever
// touches the file this process created (the unpredictable CreateTemp name),
// so a pre-existing entry anywhere in the directory is never removed.
// Idempotent and nil-safe.
func (f *AtomicFile) Abort() {
	if f == nil {
		return
	}
	if f.File != nil {
		_ = f.File.Close()
		f.File = nil
	}
	if f.path != "" {
		_ = os.Remove(f.path)
		f.path = ""
	}
}