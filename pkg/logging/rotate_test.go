package logging

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// backupsOf returns the rotated backup files for path, oldest first.
func backupsOf(t *testing.T, path string) []string {
	t.Helper()
	matches, err := filepath.Glob(path + ".*")
	require.NoError(t, err)
	sort.Strings(matches)
	return matches
}

func TestRotatingWriter_NoRotationWhenDisabled(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	// maxSizeMB=0 disables rotation - the writer is never even constructed
	// by the logging package in that case, but rotatingWriter itself should
	// also behave sanely (maxBytes==0 means "rotate isn't skipped by the
	// size>0 guard alone", so exercise it via a caller that never rotates:
	// a writer whose maxBytes is huge relative to the payload).
	w, err := newRotatingWriter(path, 100, 5)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	payload := []byte("hello, world\n")
	for i := 0; i < 10; i++ {
		n, err := w.Write(payload)
		require.NoError(t, err)
		require.Equal(t, len(payload), n)
	}

	require.NoError(t, w.Sync())

	backups := backupsOf(t, path)
	require.Empty(t, backups, "no rotation should have occurred")

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, len(payload)*10, len(data))
}

func TestRotatingWriter_RotatesPastMaxBytes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	// maxSizeMB is expressed in MB by the constructor; use a tiny custom
	// writer directly so the test can work in bytes instead of megabytes.
	// maxBackups is set high enough that pruning never kicks in - this
	// test is purely about not losing data across a rotation, not about
	// capacity trimming (see TestRotatingWriter_PruneKeepsExactlyMaxBackups
	// for that).
	w, err := newRotatingWriter(path, 0, 1000)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 50 // override for a byte-scale test

	line := []byte("0123456789\n") // 11 bytes
	var allWritten [][]byte
	for i := 0; i < 20; i++ {
		payload := append([]byte(fmt.Sprintf("%02d:", i)), line...)
		_, err := w.Write(payload)
		require.NoError(t, err)
		allWritten = append(allWritten, payload)
	}

	require.NoError(t, w.Sync())

	// (a) active file stays roughly bounded by maxBytes.
	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.LessOrEqual(t, fi.Size(), w.maxBytes+int64(len(allWritten[len(allWritten)-1])),
		"active file should not accumulate unboundedly")

	// (b) at least one backup file appeared.
	backups := backupsOf(t, path)
	require.NotEmpty(t, backups, "expected at least one rotated backup")

	// (d) no lines lost across rotation: concatenating backups (oldest
	// first) + the active file reproduces every write, in order.
	var combined []byte
	for _, b := range backups {
		data, err := os.ReadFile(b)
		require.NoError(t, err)
		combined = append(combined, data...)
	}
	activeData, err := os.ReadFile(path)
	require.NoError(t, err)
	combined = append(combined, activeData...)

	var expected []byte
	for _, p := range allWritten {
		expected = append(expected, p...)
	}
	require.True(t, bytes.Equal(expected, combined), "combined backup+active content must contain every write, in order")
}

func TestRotatingWriter_PruneKeepsExactlyMaxBackups(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	const maxBackups = 3
	w, err := newRotatingWriter(path, 0, maxBackups)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 20

	line := []byte("0123456789\n") // 11 bytes, one line rotates every ~2 writes
	for i := 0; i < 40; i++ {
		_, err := w.Write(line)
		require.NoError(t, err)
	}
	require.NoError(t, w.Sync())

	backups := backupsOf(t, path)
	require.Len(t, backups, maxBackups, "prune should keep exactly maxBackups rotated files")
}

func TestRotatingWriter_MaxBackupsZeroKeepsNone(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	w, err := newRotatingWriter(path, 0, 0)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 20

	line := []byte("0123456789\n")
	for i := 0; i < 20; i++ {
		_, err := w.Write(line)
		require.NoError(t, err)
	}
	require.NoError(t, w.Sync())

	backups := backupsOf(t, path)
	require.Empty(t, backups, "maxBackups<=0 should keep no rotated history")
}

func TestRotatingWriter_ReopensAfterRotate(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	w, err := newRotatingWriter(path, 0, 5)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10

	_, err = w.Write([]byte("0123456789")) // exactly fills the budget, no rotate yet (size was 0)
	require.NoError(t, err)
	require.Equal(t, int64(10), w.size)

	// This write should trigger a rotation first (size>0 && size+len>max).
	_, err = w.Write([]byte("x"))
	require.NoError(t, err)
	require.Equal(t, int64(1), w.size, "size should reset after rotation")

	backups := backupsOf(t, path)
	require.Len(t, backups, 1)

	data, err := os.ReadFile(backups[0])
	require.NoError(t, err)
	require.Equal(t, "0123456789", string(data))

	activeData, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "x", string(activeData))
}

func TestNewRotatingWriter_CreatesDirAndAppends(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	nested := filepath.Join(dir, "nested", "dir")
	path := filepath.Join(nested, "baton.log")

	w, err := newRotatingWriter(path, 100, 5)
	require.NoError(t, err)
	_, err = w.Write([]byte("first\n"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Re-opening should append rather than truncate.
	w2, err := newRotatingWriter(path, 100, 5)
	require.NoError(t, err)
	defer func() { _ = w2.Close() }()
	require.Equal(t, int64(len("first\n")), w2.size, "constructor should stat existing size")

	_, err = w2.Write([]byte("second\n"))
	require.NoError(t, err)
	require.NoError(t, w2.Sync())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "first\nsecond\n", string(data))
}
