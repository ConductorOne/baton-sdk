// Package atomicfile replaces a file by writing a staged sibling and renaming
// it over the target, so readers see either the old file or the complete new
// one.
package atomicfile

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
)

// File is a staged replacement for a target path. Write to it, then call
// CloseAtomicallyReplace. Defer Cleanup to remove the staged file on any
// path that does not reach the replace. A process that exits before either
// leaves the staged file in the target's directory, and nothing removes it
// later.
type File struct {
	*os.File
	target   string
	replaced bool
}

// Create stages a replacement for target in target's directory. The staged
// name is random and created exclusively, so another account that can write
// to the directory can neither pre-create it nor learn it in advance. Its
// permissions are the existing target's when the target is a regular file, and
// 0644 otherwise, both narrowed by the umask: replacing a file never widens
// its mode.
func Create(target string) (*File, error) {
	perm := os.FileMode(0o644)
	if fi, err := os.Lstat(target); err == nil && fi.Mode().IsRegular() {
		perm = fi.Mode().Perm()
	}
	name := filepath.Join(filepath.Dir(target), filepath.Base(target)+".tmp-"+rand.Text())
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, perm) // #nosec G304 -- sibling of the caller's output path.
	if err != nil {
		return nil, fmt.Errorf("atomicfile: stage %s: %w", target, err)
	}
	return &File{File: f, target: target}, nil
}

// CloseAtomicallyReplace syncs and closes the staged file and renames it over
// the target.
func (f *File) CloseAtomicallyReplace() error {
	if err := f.Sync(); err != nil {
		return fmt.Errorf("atomicfile: sync %s: %w", f.Name(), err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("atomicfile: close %s: %w", f.Name(), err)
	}
	if err := os.Rename(f.Name(), f.target); err != nil {
		return fmt.Errorf("atomicfile: replace %s: %w", f.target, err)
	}
	f.replaced = true
	return nil
}

// Cleanup closes and removes the staged file unless CloseAtomicallyReplace
// succeeded. It removes nothing else.
func (f *File) Cleanup() {
	if f.replaced {
		return
	}
	_ = f.Close()
	_ = os.Remove(f.Name())
}
