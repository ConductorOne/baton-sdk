package logging

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// backupsOf returns every rotated backup of path, oldest first by name. It
// mirrors prune()'s name matcher but not its ownership check, so tests can
// assert on entries prune() must not touch.
func backupsOf(t *testing.T, path string) []string {
	t.Helper()
	dir := filepath.Dir(path)
	active := filepath.Base(path)
	ext := filepath.Ext(active)
	stem := strings.TrimSuffix(active, ext)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var matches []string
	for _, e := range entries {
		if _, ok := backupSuffix(stem, ext, e.Name()); ok && e.Name() != active {
			matches = append(matches, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(matches)
	return matches
}

// siblingsOf returns the names of every directory entry next to path.
func siblingsOf(t *testing.T, path string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(path))
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		if e.Name() != filepath.Base(path) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

// contentsOf reads each path and returns the contents in the same order.
func contentsOf(t *testing.T, paths []string) []string {
	t.Helper()
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		out = append(out, string(data))
	}
	return out
}

// rotateNow disables the post-failure rotation backoff so a test can drive
// consecutive rotation attempts without waiting for defaultRotateRetryAfter.
func rotateNow(w *rotatingWriter) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rotateRetryAfter = 0
}

// writePermsEnforced reports whether chmod actually denies access: Windows
// ignores the mode bits and root bypasses them.
func writePermsEnforced() bool {
	return runtime.GOOS != "windows" && os.Geteuid() != 0
}

// requireWritePermsEnforced skips tests that need chmod to actually deny access.
func requireWritePermsEnforced(t *testing.T) {
	t.Helper()
	if !writePermsEnforced() {
		t.Skip("requires POSIX permission enforcement as a non-root user")
	}
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
	w, err := newRotatingWriter(path, 100, 5, nil)
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

func TestNewRotatingWriter_ClampsSizeToFloor(t *testing.T) {
	t.Parallel()

	// A sub-floor size (here 0 MB) must clamp up to minRotationBytes rather than
	// leaving a tiny/zero threshold that would rotate on nearly every write.
	w, err := newRotatingWriter(filepath.Join(t.TempDir(), "baton.log"), 0, 3, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	require.Equal(t, minRotationBytes, w.maxBytes)
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
	w, err := newRotatingWriter(path, 0, 1000, nil)
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
	w, err := newRotatingWriter(path, 0, maxBackups, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10

	// Each write is exactly maxBytes, so write N+1 rotates write N into its own
	// backup: generation content identifies which backups survived.
	const generations = 8
	for i := 0; i < generations; i++ {
		_, err := fmt.Fprintf(w, "gen%07d", i)
		require.NoError(t, err)
		// Distinct timestamps and mtimes, so the assertion below is about which
		// files prune kept and not about same-millisecond tie-breaking.
		time.Sleep(2 * time.Millisecond)
	}
	require.NoError(t, w.Sync())

	backups := backupsOf(t, path)
	require.Len(t, backups, maxBackups, "prune should keep exactly maxBackups rotated files")

	// Which ones were kept matters as much as how many: everything before the
	// last maxBackups rotations must be the part that was deleted.
	require.Equal(t, []string{"gen0000004", "gen0000005", "gen0000006"}, contentsOf(t, backups),
		"prune must keep the newest backups, not just the right number of them")

	active, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "gen0000007", string(active))
}

func TestRotatingWriter_PruneKeepsNewestWhenNameOrderInvertsAge(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	w, err := newRotatingWriter(path, 0, 1, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	// Backup names whose lexical order is the reverse of their age. Both a
	// backwards clock step (NTP, VM resume) and a name freed by an earlier prune
	// and reclaimed by the next rotation produce exactly this.
	now := time.Now()
	ages := map[string]time.Duration{
		"20260727T120503.123Z": 3 * time.Hour, // newest name, oldest file
		"20260727T120502.123Z": 2 * time.Hour,
		"20260727T120501.123Z": 1 * time.Hour, // oldest name, newest file
	}
	for ts, age := range ages {
		p := w.backupPath(ts)
		require.NoError(t, os.WriteFile(p, []byte(ts), 0600))
		mtime := now.Add(-age)
		require.NoError(t, os.Chtimes(p, mtime, mtime))
	}

	w.mu.Lock()
	pruneErr := w.prune()
	w.mu.Unlock()
	require.NoError(t, pruneErr)

	require.Equal(t, []string{"20260727T120501.123Z"}, contentsOf(t, backupsOf(t, path)),
		"prune must rank backups by mtime; file names are not age order")
}

func TestRotatingWriter_PruneMatchesLegacyBackupNames(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	w, err := newRotatingWriter(path, 0, 0, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	// An earlier build appended the timestamp after the extension. Those files
	// must still be pruned, not orphaned in the log directory forever.
	legacy := filepath.Join(dir, "baton.log.20260727T120501.123Z")
	require.NoError(t, os.WriteFile(legacy, []byte("old scheme"), 0600))

	w.mu.Lock()
	pruneErr := w.prune()
	w.mu.Unlock()
	require.NoError(t, pruneErr)

	require.NoFileExists(t, legacy, "backups from the previous naming scheme must still be pruned")
}

func TestRotatingWriter_PrunesWhenPathContainsBrackets(t *testing.T) {
	t.Parallel()

	// "[Legacy]" is a legal Windows directory name and a well-formed glob
	// character class: as a pattern it matches nothing, with no error, so a
	// glob-based prune would silently never delete anything.
	dir := filepath.Join(t.TempDir(), "Vendor [Legacy]")
	require.NoError(t, os.MkdirAll(dir, 0700))
	path := filepath.Join(dir, "baton.log")

	var sink bytes.Buffer
	w, err := newRotatingWriter(path, 0, 1, &sink)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10

	for i := 0; i < 6; i++ {
		_, err := fmt.Fprintf(w, "gen%07d", i)
		require.NoError(t, err)
		time.Sleep(2 * time.Millisecond)
	}
	require.NoError(t, w.Sync())

	require.Len(t, backupsOf(t, path), 1, "pruning must work for a log path containing brackets")
	require.Empty(t, sink.String(), "no diagnostics expected on the happy path")
}

func TestRotatingWriter_MaxBackupsZeroKeepsNone(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	w, err := newRotatingWriter(path, 0, 0, nil)
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

	w, err := newRotatingWriter(path, 0, 5, nil)
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

	w, err := newRotatingWriter(path, 100, 5, nil)
	require.NoError(t, err)
	_, err = w.Write([]byte("first\n"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Re-opening should append rather than truncate.
	w2, err := newRotatingWriter(path, 100, 5, nil)
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

func TestRotatingWriter_BackupKeepsLogExtension(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	w, err := newRotatingWriter(path, 0, 5, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10

	_, err = w.Write([]byte("0123456789"))
	require.NoError(t, err)
	_, err = w.Write([]byte("x"))
	require.NoError(t, err)

	backups := backupsOf(t, path)
	require.Len(t, backups, 1)
	require.Equal(t, ".log", filepath.Ext(backups[0]), "rotated files should keep the .log extension")
	require.Regexp(t, `baton\.\d{8}T\d{6}\.\d{3}Z\.log$`, backups[0])
}

func TestRotatingWriter_NextBackupPathIsBounded(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "baton.log")
	w, err := newRotatingWriter(path, 0, 5, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	const ts = "20260727T120501.123Z"
	require.NoError(t, os.WriteFile(w.backupPath(ts), nil, 0600))
	for i := 1; i <= maxBackupNameProbes; i++ {
		require.NoError(t, os.WriteFile(w.backupPath(fmt.Sprintf("%s_%02d", ts, i)), nil, 0600))
	}

	_, err = w.nextBackupPath(ts)
	require.ErrorContains(t, err, "unused backup name", "the probe loop must give up rather than search on")

	// A single free slot inside the bound is enough.
	require.NoError(t, os.Remove(w.backupPath(ts+"_07")))
	got, err := w.nextBackupPath(ts)
	require.NoError(t, err)
	require.Equal(t, w.backupPath(ts+"_07"), got)
}

func TestRotatingWriter_PruneLeavesForeignFilesAlone(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	// Files another tool could plausibly leave next to baton.log. None of these
	// names match the backup pattern, so prune never even considers them.
	foreignFiles := []string{
		filepath.Join(dir, "baton.log.lock"),
		filepath.Join(dir, "baton.other.log"),
		filepath.Join(dir, "baton.2026-07-27.log"),
		filepath.Join(dir, "other.20260727T120501.123Z.log"),
	}
	for _, f := range foreignFiles {
		require.NoError(t, os.WriteFile(f, []byte("not ours"), 0600))
	}
	// This one does match, but is not a regular file, which is the only
	// ownership check prune actually performs.
	foreignDir := filepath.Join(dir, "baton.20260727T120501.123Z.log")
	require.NoError(t, os.Mkdir(foreignDir, 0700))

	// maxBackups=0 means every rotation prunes all history it claims as its own.
	w, err := newRotatingWriter(path, 0, 0, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 20

	for i := 0; i < 20; i++ {
		_, err := w.Write([]byte("0123456789\n"))
		require.NoError(t, err)
	}
	require.NoError(t, w.Sync())

	for _, f := range foreignFiles {
		require.FileExists(t, f, "prune must not delete files it did not create")
	}
	require.DirExists(t, foreignDir, "prune must not delete a directory that matches the backup pattern")

	// Nothing survives except the foreign entries: every real backup was pruned.
	require.Equal(t,
		[]string{
			"baton.2026-07-27.log",
			"baton.20260727T120501.123Z.log",
			"baton.log.lock",
			"baton.other.log",
			"other.20260727T120501.123Z.log",
		},
		siblingsOf(t, path))
}

func TestRotatingWriter_PruneDeletesForeignFileMatchingBackupPattern(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	w, err := newRotatingWriter(path, 0, 0, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	// Documents the known limit of backupSuffixRe: it is a narrow name filter,
	// not proof of ownership. A regular file another tool happened to name like
	// a backup is deleted, and only the "not a regular file" check spares one.
	lookalike := w.backupPath("20260727T120501.123Z")
	require.NoError(t, os.WriteFile(lookalike, []byte("someone else's"), 0600))

	w.mu.Lock()
	pruneErr := w.prune()
	w.mu.Unlock()
	require.NoError(t, pruneErr)

	require.NoFileExists(t, lookalike, "a regular file matching the backup pattern is claimed by prune")
}

// TestRotatingWriter_PruneNeverDeletesAnotherLiveRotatorsActiveFile reproduces
// the R2 finding: a second, unrelated rotatingWriter's active file (a plausible
// per-run timestamped name like baton.<ts>.log, sitting next to a rotating
// baton.log) matches the first writer's backup pattern by pure coincidence.
// prune() must not delete it just because ownership is decided by a filename
// regex - it must first check whether that path is some other rotator's live
// file. Not parallel: registers a rotator in the package-level activeRotators.
func TestRotatingWriter_PruneNeverDeletesAnotherLiveRotatorsActiveFile(t *testing.T) {
	dir := rotatorTestDir(t)

	victimPath := filepath.Join(dir, "baton.20260727T120501.123Z.log")
	_, err := InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 2},
		WithOutputPaths([]string{victimPath}))
	require.NoError(t, err, "InitWithRotation for the victim rotator")

	activeRotatorsMu.Lock()
	victim := activeRotators[canonicalOutputPath(victimPath)]
	activeRotatorsMu.Unlock()
	require.NotNil(t, victim, "the victim rotator must be registered")

	// An unrelated second rotatingWriter for baton.log in the same directory:
	// its prune() claims any file in dir matching its backup pattern, which
	// victimPath's name does purely by coincidence.
	w, err := newRotatingWriter(filepath.Join(dir, "baton.log"), 0, 0, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	w.mu.Lock()
	pruneErr := w.prune()
	w.mu.Unlock()
	require.NoError(t, pruneErr)

	require.FileExists(t, victimPath, "prune must not delete another rotator's live active file")

	// Not just present on disk as a leftover: still genuinely the file the
	// victim rotator is writing to, not an unlinked inode.
	_, err = victim.Write([]byte("still alive\n"))
	require.NoError(t, err)
	data, err := os.ReadFile(victimPath)
	require.NoError(t, err)
	require.Contains(t, string(data), "still alive")
}

func TestRotatingWriter_PruneReportsRemoveFailure(t *testing.T) {
	requireWritePermsEnforced(t)
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	w, err := newRotatingWriter(path, 0, 0, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	var sink bytes.Buffer
	w.errSink = &sink

	require.NoError(t, os.WriteFile(w.backupPath("20260727T120501.123Z"), []byte("old"), 0600))
	require.NoError(t, os.Chmod(dir, 0500)) // read-only directory: unlink is denied
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	w.mu.Lock() // prune documents that callers hold the lock
	pruneErr := w.prune()
	w.mu.Unlock()
	require.ErrorContains(t, pruneErr, "failed to remove rotated log file")

	w.reportError(pruneErr)
	require.Contains(t, sink.String(), "failed to remove rotated log file", "a failed removal must not be silent")
}

func TestRotatingWriter_RotateRoutesPruneFailureToSink(t *testing.T) {
	requireWritePermsEnforced(t)
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	var sink bytes.Buffer
	w, err := newRotatingWriter(path, 0, 1, &sink)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10

	_, err = w.Write([]byte("0123456789"))
	require.NoError(t, err)

	// Write+execute but not read: rename and reopen still work, listing the
	// directory does not, so prune fails inside a rotation that otherwise succeeds.
	require.NoError(t, os.Chmod(dir, 0300))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	_, err = w.Write([]byte("x"))
	require.NoError(t, err, "a prune failure must not fail the write")

	require.NoError(t, os.Chmod(dir, 0700))
	require.Contains(t, sink.String(), "failed to list rotated log files",
		"rotate() must route prune errors to the sink, not swallow them")
	require.Len(t, backupsOf(t, path), 1, "the rotation itself should still have happened")
}

func TestRotatingWriter_WriteAfterCloseDoesNotReopen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	w, err := newRotatingWriter(path, 100, 3, nil)
	require.NoError(t, err)

	_, err = w.Write([]byte("first\n"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Close must be terminal. A closed writer that reopened itself would keep
	// renaming and pruning a file another rotator now owns (the Windows service
	// initializes its logger twice against the same path).
	_, err = w.Write([]byte("second\n"))
	require.ErrorIs(t, err, os.ErrClosed)

	w.mu.Lock()
	require.Nil(t, w.f, "a closed writer must not hold a handle")
	w.mu.Unlock()

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "first\n", string(data))
}

func TestRotatingWriter_AppendsWhenRotationKeepsFailing(t *testing.T) {
	requireWritePermsEnforced(t)
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	var sink bytes.Buffer
	w, err := newRotatingWriter(path, 0, 3, &sink)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10
	rotateNow(w)

	_, err = w.Write([]byte("0123456789"))
	require.NoError(t, err)

	// Read-only directory: the rename can never succeed. Losing the line would
	// be worse than an oversized file, and on a Windows service there is no
	// fallback sink for it to land in.
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	for i := 0; i < 3; i++ {
		_, err = w.Write([]byte("x"))
		require.NoError(t, err, "a failing rotation must not drop the log line")
	}

	require.NoError(t, os.Chmod(dir, 0700))
	require.Contains(t, sink.String(), "failed to rotate log file", "the failure must still be reported")
	require.Empty(t, backupsOf(t, path))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "0123456789xxx", string(data), "every line must be appended to the oversized file")
}

// TestRotatingWriter_ConcurrentReportErrorDoesNotRaceOnSink reproduces the R1
// finding: several goroutines share one rotatingWriter (as goroutines sharing
// one *zap.Logger do), rotation fails on every write, and each failure queues
// a diagnostic that Write flushes via reportError after releasing w.mu.
// Without serializing those sink writes, concurrent reportError calls race on
// the shared, non-concurrency-safe *bytes.Buffer sink - caught by -race, and
// reproducible as a plain panic (slice bounds out of range) without it.
func TestRotatingWriter_ConcurrentReportErrorDoesNotRaceOnSink(t *testing.T) {
	requireWritePermsEnforced(t)

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	var sink bytes.Buffer
	w, err := newRotatingWriter(path, 0, 3, &sink)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	// maxBytes and the initial write are sized so every one of the 1-byte
	// writes below both crosses maxBytes (triggering a rotation attempt) and
	// stays well under the 10x oversize ceiling (so none of them get dropped -
	// this test is about the sink race, not the oversize path).
	w.maxBytes = 10_000
	rotateNow(w)

	_, err = w.Write([]byte(strings.Repeat("0", 10_001)))
	require.NoError(t, err)

	// Read-only directory: rotate()'s rename fails forever, so every write from
	// here on queues a diagnostic without ever shrinking w.size back down -
	// each subsequent write attempts (and fails) rotation again.
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	const goroutines = 8
	const writesPerGoroutine = 50
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				_, writeErr := w.Write([]byte("x"))
				assert.NoError(t, writeErr, "a failing rotation must not drop the log line")
			}
		}()
	}
	wg.Wait()

	require.NoError(t, os.Chmod(dir, 0700))
	require.Contains(t, sink.String(), "failed to rotate log file", "the failure must still be reported")
}

// TestInitWithRotationConcurrentReportErrorAcrossSharedSink reproduces the S1
// finding: the R1 fix above (TestRotatingWriter_ConcurrentReportErrorDoesNotRaceOnSink)
// only serializes writes from one rotatingWriter's own reportError calls.
// adoptRotators hands the *same* RotationConfig.ErrSink to every rotator it
// adopts for one Init, so as soon as OutputPaths names more than one real
// file, two independent writers - each still only serializing on its own
// lock - end up writing to one shared, non-concurrency-safe sink concurrently.
// Not parallel: asserts on the package-level activeRotators set by Init.
func TestInitWithRotationConcurrentReportErrorAcrossSharedSink(t *testing.T) {
	requireWritePermsEnforced(t)
	dir := rotatorTestDir(t)
	paths := []string{filepath.Join(dir, "a.log"), filepath.Join(dir, "b.log")}
	var sink bytes.Buffer

	_, err := InitWithRotation(context.Background(), RotationConfig{MaxSizeMB: 1, MaxBackups: 1, ErrSink: &sink},
		WithOutputPaths(paths))
	require.NoError(t, err, "InitWithRotation")

	activeRotatorsMu.Lock()
	writers := make([]*rotatingWriter, 0, len(paths))
	for _, p := range paths {
		rw := activeRotators[canonicalOutputPath(p)]
		require.NotNil(t, rw)
		writers = append(writers, rw)
	}
	activeRotatorsMu.Unlock()

	// Prime both writers exactly as the R1 test does, so every 1-byte write
	// below crosses maxBytes on both without ever hitting the oversize ceiling.
	for _, w := range writers {
		w.maxBytes = 10_000
		rotateNow(w)
		_, err := w.Write([]byte(strings.Repeat("0", 10_001)))
		require.NoError(t, err)
	}

	// Read-only directory: rotation fails permanently for both writers, so
	// every write from here on queues a diagnostic for reportError to flush.
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	const goroutines = 8
	const writesPerGoroutine = 50
	var wg sync.WaitGroup
	for _, w := range writers {
		w := w
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < writesPerGoroutine; j++ {
					_, writeErr := w.Write([]byte("x"))
					assert.NoError(t, writeErr, "a failing rotation must not drop the log line")
				}
			}()
		}
	}
	wg.Wait()

	require.NoError(t, os.Chmod(dir, 0700))
	require.Contains(t, sink.String(), "failed to rotate log file", "the failure must still be reported")
}

func TestRotatingWriter_StopsAppendingAtOversizeCeiling(t *testing.T) {
	requireWritePermsEnforced(t)
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	var sink bytes.Buffer
	w, err := newRotatingWriter(path, 0, 3, &sink)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10
	rotateNow(w) // retry (and re-report) on every write, not on a 5s timer

	_, err = w.Write([]byte("0123456789"))
	require.NoError(t, err)

	// A permanently failing rotation. Appending forever would fill the volume,
	// which loses every line plus the service.
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	var dropped int
	for i := 0; i < 1000; i++ {
		if _, err := w.Write([]byte("x")); err != nil {
			require.ErrorIs(t, err, errLogFileOversized)
			dropped++
		}
	}
	require.NoError(t, os.Chmod(dir, 0700))

	require.Positive(t, dropped, "writes past the ceiling must be dropped, not appended forever")

	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.LessOrEqual(t, fi.Size(), w.maxBytes*maxOversizeFactor,
		"a permanently failing rotation must leave a bounded file")

	// Bounded loss has to be loud, and stay loud: one diagnostic at the start
	// would be indistinguishable from the silent-drop behavior this replaced.
	require.Greater(t, strings.Count(sink.String(), "past its oversize ceiling"), 1,
		"dropping must be reported repeatedly, not once")
}

func TestRotatingWriter_OversizeCeilingNeverDropsALineThatCannotFit(t *testing.T) {
	requireWritePermsEnforced(t)
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	var sink bytes.Buffer
	w, err := newRotatingWriter(path, 0, 3, &sink)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10 // ceiling 100
	rotateNow(w)

	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	// A single line larger than the whole ceiling would otherwise be dropped on
	// every attempt, forever. It goes to an empty file instead, so the real bound
	// is the ceiling plus at most one line.
	huge := bytes.Repeat([]byte("z"), 500)
	n, err := w.Write(huge)
	require.NoError(t, err, "a line too big for the ceiling must still be written to an empty file")
	require.Equal(t, len(huge), n)
	require.NoError(t, os.Chmod(dir, 0700))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, data, len(huge))
}

func TestRotatingWriter_OversizeCeilingReleasedOnceRotationWorks(t *testing.T) {
	requireWritePermsEnforced(t)
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	var sink bytes.Buffer // keeps the expected diagnostics off the test's stderr
	w, err := newRotatingWriter(path, 0, 3, &sink)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10
	rotateNow(w)

	_, err = w.Write([]byte("0123456789"))
	require.NoError(t, err)
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	for i := 0; i < 200; i++ {
		_, _ = w.Write([]byte("x"))
	}
	require.NoError(t, os.Chmod(dir, 0700))

	// The ceiling suppresses writes only while rotation is broken; once the fault
	// clears the next write must rotate and be accepted.
	_, err = w.Write([]byte("y"))
	require.NoError(t, err, "recovery must not be blocked by the ceiling")
	require.NoError(t, w.Sync())

	require.Len(t, backupsOf(t, path), 1)
	active, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "y", string(active))
}

func TestRotatingWriter_PruneBreaksMtimeTiesOnTimestamp(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	w, err := newRotatingWriter(path, 0, 1, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	// Coarse mtime granularity (FAT32 2s, ext3 1s) makes ties real, and the
	// upgrade window is exactly when both layouts coexist. Falling back to the
	// raw path here sorts every "baton.<ts>.log" before every "baton.log.<ts>"
	// because digits sort before 'l', which would delete the newest backup.
	older := filepath.Join(dir, "baton.log.20260727T120501.123Z") // legacy layout, older
	newer := w.backupPath("20260727T120502.123Z")                 // current layout, newer
	shared := time.Now().Add(-time.Hour)
	for _, p := range []string{older, newer} {
		require.NoError(t, os.WriteFile(p, []byte(filepath.Base(p)), 0600))
		require.NoError(t, os.Chtimes(p, shared, shared))
	}

	w.mu.Lock()
	pruneErr := w.prune()
	w.mu.Unlock()
	require.NoError(t, pruneErr)

	require.NoFileExists(t, older, "the older backup must be the one pruned")
	require.FileExists(t, newer, "an mtime tie must be broken on the timestamp, not the path")
}

func TestRotatingWriter_PruneMatchesLegacyDisambiguatedBackupNames(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	w, err := newRotatingWriter(path, 0, 0, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	// The previous build disambiguated same-millisecond rotations with a ".<n>"
	// suffix. Those files exist in the field and must not be orphaned.
	legacy := []string{
		filepath.Join(dir, "baton.log.20260727T120501.123Z.1"),
		filepath.Join(dir, "baton.log.20260727T120501.123Z.12"),
		filepath.Join(dir, "baton.20260727T120502.123Z.1.log"),
	}
	for _, p := range legacy {
		require.NoError(t, os.WriteFile(p, []byte("old scheme"), 0600))
	}

	w.mu.Lock()
	pruneErr := w.prune()
	w.mu.Unlock()
	require.NoError(t, pruneErr)

	for _, p := range legacy {
		require.NoFileExists(t, p, "disambiguated backups from the previous naming scheme must be pruned")
	}
}

func TestRotatingWriter_BacksOffAfterRotationFailure(t *testing.T) {
	requireWritePermsEnforced(t)
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	var sink bytes.Buffer
	w, err := newRotatingWriter(path, 0, 3, &sink)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10

	_, err = w.Write([]byte("0123456789"))
	require.NoError(t, err)

	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	for i := 0; i < 5; i++ {
		_, err = w.Write([]byte("x"))
		require.NoError(t, err)
	}
	require.NoError(t, os.Chmod(dir, 0700))

	require.Equal(t, 1, strings.Count(sink.String(), "failed to rotate log file"),
		"a persistently failing rotation must be retried on a timer, not once per line")
}

func TestRotatingWriter_RotateContinuesWhenSyncFails(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("relies on fsync failing for a pipe")
	}
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	var sink bytes.Buffer
	w, err := newRotatingWriter(path, 0, 3, &sink)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10

	_, err = w.Write([]byte("0123456789"))
	require.NoError(t, err)

	// fsync on a pipe fails with EINVAL while close succeeds: a flush failure
	// (a flaky SMB share, say) must not permanently disable rotation.
	pr, pw, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pr.Close(); _ = pw.Close() })
	w.mu.Lock()
	replaced := w.f
	w.f = pw
	w.mu.Unlock()
	require.NoError(t, replaced.Close())

	_, err = w.Write([]byte("x"))
	require.NoError(t, err)

	require.Contains(t, sink.String(), "failed to sync log file", "the flush failure must still be reported")
	backups := backupsOf(t, path)
	require.Len(t, backups, 1, "rotation must proceed despite the failed flush")
	require.Equal(t, []string{"0123456789"}, contentsOf(t, backups))

	active, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "x", string(active))
}

func TestRotatingWriter_RecoversWhenCloseBeforeRotateFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	var sink bytes.Buffer
	w, err := newRotatingWriter(path, 0, 3, &sink)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10

	_, err = w.Write([]byte("0123456789"))
	require.NoError(t, err)

	// Close the descriptor behind the writer's back, so the close inside rotate
	// fails and leaves no usable handle.
	w.mu.Lock()
	require.NoError(t, w.f.Close())
	w.mu.Unlock()

	_, err = w.Write([]byte("x"))
	require.NoError(t, err, "a failed close must leave a state the next write can recover from")
	require.Contains(t, sink.String(), "failed to close log file")

	w.mu.Lock()
	require.NotNil(t, w.f, "the write should have reopened the file")
	w.mu.Unlock()

	require.NoError(t, w.Sync())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "0123456789x", string(data))
}

func TestRotatingWriter_SyncReportsMissingHandle(t *testing.T) {
	t.Parallel()

	w, err := newRotatingWriter(filepath.Join(t.TempDir(), "baton.log"), 100, 1, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	// Emulate a rotation whose reopen failed. Reporting success from Sync would
	// claim a durability guarantee for a file that isn't even open.
	w.mu.Lock()
	require.NoError(t, w.f.Close())
	w.f = nil
	w.mu.Unlock()

	require.ErrorContains(t, w.Sync(), "no active file handle")
}

func TestRotatingWriter_WriteReopensAfterLostHandle(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	w, err := newRotatingWriter(path, 100, 3, nil)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()

	_, err = w.Write([]byte("first\n"))
	require.NoError(t, err)

	// Drop the handle the way a rotation whose reopen failed does. Not via
	// Close(): Close is terminal, and using it here would assert the opposite
	// of TestRotatingWriter_WriteAfterCloseDoesNotReopen.
	w.mu.Lock()
	require.NoError(t, w.f.Close())
	w.f = nil
	w.mu.Unlock()

	// Inline rather than a subtest: it mutates the same file the rest of the
	// test depends on, so it cannot run in parallel with anything.
	if writePermsEnforced() {
		require.NoError(t, os.Chmod(path, 0400))
		_, err = w.Write([]byte("dropped\n"))
		require.ErrorContains(t, err, "failed to open log file",
			"a write with no handle and no way to get one must report why")
		require.NoError(t, os.Chmod(path, 0600))
	}

	_, err = w.Write([]byte("second\n"))
	require.NoError(t, err, "a writer without a handle must reopen instead of failing forever")
	require.NoError(t, w.Sync())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "first\nsecond\n", string(data))
}

func TestRotatingWriter_RecoversWhenRenameAndReopenFail(t *testing.T) {
	requireWritePermsEnforced(t)
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "baton.log")

	var sink bytes.Buffer
	w, err := newRotatingWriter(path, 0, 3, &sink)
	require.NoError(t, err)
	defer func() { _ = w.Close() }()
	w.maxBytes = 10
	rotateNow(w) // the recovering write below must not be held off by the backoff

	_, err = w.Write([]byte("0123456789"))
	require.NoError(t, err)

	// A read-only directory fails the rename; a read-only file fails the
	// reopen that follows it.
	require.NoError(t, os.Chmod(path, 0400))
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) })

	_, err = w.Write([]byte("x"))
	require.ErrorContains(t, err, "failed to rotate log file")
	require.ErrorContains(t, err, "failed to open log file", "both failures should be reported")

	w.mu.Lock()
	require.Nil(t, w.f, "a failed reopen must not leave a closed handle behind")
	w.mu.Unlock()

	require.NoError(t, os.Chmod(dir, 0700))
	require.NoError(t, os.Chmod(path, 0600))

	// The very next write must recover rather than fail for the rest of the run.
	_, err = w.Write([]byte("y"))
	require.NoError(t, err)
	require.NoError(t, w.Sync())

	backups := backupsOf(t, path)
	require.Len(t, backups, 1, "the deferred rotation should complete on the recovering write")
	data, err := os.ReadFile(backups[0])
	require.NoError(t, err)
	require.Equal(t, "0123456789", string(data))

	active, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "y", string(active))
}

func TestRotatingWriter_CloseJoinsSyncAndCloseErrors(t *testing.T) {
	t.Parallel()

	w, err := newRotatingWriter(filepath.Join(t.TempDir(), "baton.log"), 100, 1, nil)
	require.NoError(t, err)

	// Close the descriptor behind the writer's back so both Sync and Close fail.
	require.NoError(t, w.f.Close())

	err = w.Close()
	require.ErrorContains(t, err, "failed to sync log file")
	require.ErrorContains(t, err, "failed to close log file")

	require.NoError(t, w.Close(), "closing an already-closed writer should be a no-op")
}

// TestRotationBytesSaturates: log-max-size-mb is only bounded below, so an
// operator can enter a value whose byte conversion overflows int64. Wrapping
// negative would trip the minRotationBytes clamp and rotate every 1 MiB -
// the opposite of what an enormous value asks for.
func TestRotationBytesSaturates(t *testing.T) {
	require.Equal(t, int64(math.MaxInt64), rotationBytes(math.MaxInt), "an overflowing size must saturate, not wrap into the floor")
	require.Equal(t, int64(math.MaxInt64), rotationBytes(int(maxRotationMB)+1), "the boundary saturates too")
	require.Equal(t, minRotationBytes, rotationBytes(0), "zero still clamps up to the floor")
	require.Equal(t, int64(100)*1024*1024, rotationBytes(100), "ordinary values are unaffected")
}

// TestRotatingWriter_DroppedLinesReportedWhenRotationRecovers: queueDropReport
// only clears droppedLines on a tick it actually emits, so drops that land
// inside the retry window are stranded if rotation succeeds before the window
// elapses. docs/log-rotation.md promises "never a silent drop", so the residual
// has to be flushed on recovery. The other ceiling tests set the retry window
// to zero, which is exactly why this gap wasn't exercised.
func TestRotatingWriter_DroppedLinesReportedWhenRotationRecovers(t *testing.T) {
	// No chmod here: the oversize and throttle state is set on the writer
	// directly, so this needs no permission enforcement and must not skip on
	// Windows or as root - Windows is the platform the ceiling exists for.
	t.Parallel()

	dir := t.TempDir()
	sink := &bytes.Buffer{}
	w, err := newRotatingWriter(filepath.Join(dir, "baton.log"), 1, 1, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// Past the ceiling with rotation blocked, and a retry window long enough
	// that no drop diagnostic can fire on its own.
	w.mu.Lock()
	w.size = w.oversizeCeiling() + 1
	w.rotateRetryAfter = time.Hour
	w.nextRotateAttempt = time.Now().Add(time.Hour)
	w.nextDropReport = time.Now().Add(time.Hour)
	w.mu.Unlock()

	_, err = w.Write([]byte("dropped\n"))
	require.ErrorIs(t, err, errLogFileOversized, "the line must be dropped at the ceiling")
	require.NotContains(t, sink.String(), "dropped", "throttled: nothing reported yet")

	// Let rotation succeed on the next write.
	w.mu.Lock()
	w.nextRotateAttempt = time.Time{}
	w.mu.Unlock()
	_, err = w.Write([]byte("after recovery\n"))
	require.NoError(t, err)

	require.Contains(t, sink.String(), "rotation recovered after dropping 1 log line(s)",
		"the stranded drop count must be reported once rotation works again")
}

// TestRotatingWriter_DroppedLinesReportedOnClose: the recovery flush above only
// fires on a rotation that succeeds, so a count still inside the retry window
// when the process shuts down was discarded - a silent drop, which
// docs/log-rotation.md promises never happens.
func TestRotatingWriter_DroppedLinesReportedOnClose(t *testing.T) {
	t.Parallel()

	sink := &bytes.Buffer{}
	w, err := newRotatingWriter(filepath.Join(t.TempDir(), "baton.log"), 1, 1, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	// Past the ceiling, rotation blocked, and a retry window long enough that no
	// drop diagnostic can fire on its own.
	w.mu.Lock()
	w.size = w.oversizeCeiling() + 1
	w.rotateRetryAfter = time.Hour
	w.nextRotateAttempt = time.Now().Add(time.Hour)
	w.nextDropReport = time.Now().Add(time.Hour)
	w.mu.Unlock()

	_, err = w.Write([]byte("dropped\n"))
	require.ErrorIs(t, err, errLogFileOversized, "the line must be dropped at the ceiling")
	require.Empty(t, sink.String(), "throttled: nothing reported yet")

	require.NoError(t, w.Close())
	require.Contains(t, sink.String(), "1 log line(s) dropped, unreported before close",
		"a drop count still throttled at shutdown must not vanish")
}

// TestRotatingWriter_DroppedLinesReportedOnCloseAfterReconfigure: raising
// maxBytes past the current size strands the count a second way - rotation is no
// longer attempted at all, so the recovery flush in the write path is never
// reached no matter how many writes follow.
func TestRotatingWriter_DroppedLinesReportedOnCloseAfterReconfigure(t *testing.T) {
	t.Parallel()

	sink := &bytes.Buffer{}
	w, err := newRotatingWriter(filepath.Join(t.TempDir(), "baton.log"), 1, 1, sink)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	w.mu.Lock()
	w.size = w.oversizeCeiling() + 1
	w.rotateRetryAfter = time.Hour
	w.nextRotateAttempt = time.Now().Add(time.Hour)
	w.nextDropReport = time.Now().Add(time.Hour)
	w.mu.Unlock()

	_, err = w.Write([]byte("dropped\n"))
	require.ErrorIs(t, err, errLogFileOversized)

	// A larger cap puts the file back under both the threshold and the ceiling,
	// so writes resume without ever rotating.
	w.reconfigure(64, 1, nil)
	_, err = w.Write([]byte("fits now\n"))
	require.NoError(t, err, "the write should land under the raised cap")
	require.Empty(t, sink.String(), "no rotation happened, so nothing has flushed the count yet")

	require.NoError(t, w.Close())
	require.Contains(t, sink.String(), "1 log line(s) dropped, unreported before close",
		"raising the cap must not silently discard earlier drops")
}
