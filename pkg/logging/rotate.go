package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
)

// backupSuffixRe matches the "<timestamp>" suffix (and optional ".<n>"
// same-millisecond disambiguator) that rotate() appends after the log path, so
// prune() only ever deletes files this writer created - never a sibling like
// "baton.log.lock" or another tool's "baton.log.<something>".
var backupSuffixRe = regexp.MustCompile(`^\d{8}T\d{6}\.\d{3}Z(\.\d+)?$`)

// rotatingWriter is a zapcore.WriteSyncer that bounds a log file's size by
// rotating it once it grows past maxBytes, keeping at most maxBackups
// rotated copies around.
//
// It is deliberately minimal compared to third-party rotators: no
// compression, no age-based retention, no background goroutines. Sync()
// calls f.Sync() on the active file, which gives callers a real durability
// guarantee (this is the property lumberjack lacks).
//
// Windows-safe by construction: rotate() closes the active handle, renames
// the now-closed file out of the way, and only then opens a fresh file at
// the original path. It never removes/recreates the active path while a
// handle to it might still be open.
type rotatingWriter struct {
	mu         sync.Mutex
	path       string
	maxBytes   int64
	maxBackups int
	f          *os.File
	size       int64
}

var (
	_ zapcore.WriteSyncer = (*rotatingWriter)(nil)
	_ io.Closer           = (*rotatingWriter)(nil)
)

// minRotationBytes is the smallest rotation threshold the writer will honor.
// The config surface is in whole megabytes (log-max-size-mb), so the normal
// path is always >= 1 MiB; this floor just guarantees that a rotatingWriter
// constructed with a nonsensically small size (a bug, or a future config
// change) can never rotate on nearly every write.
const minRotationBytes int64 = 1024 * 1024

// newRotatingWriter opens (creating if necessary) the log file at path and
// returns a rotatingWriter that rotates it once it grows past maxSizeMB
// (clamped up to minRotationBytes), retaining maxBackups rotated files
// (<=0 keeps none).
func newRotatingWriter(path string, maxSizeMB, maxBackups int) (*rotatingWriter, error) {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory %q: %w", dir, err)
		}
	}

	// 0600 keeps the log owner-only because these files can carry PII/tokens.
	// The mode is enforced on non-Windows (subject to umask). On Windows - the
	// primary target for file logging - os.OpenFile only honors the read-only
	// bit, so 0600 vs 0666 is functionally identical there; access is governed
	// by the directory ACLs the connector sets, not this mode. (Same mode is
	// reused at the two reopen sites in rotate().)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %q: %w", path, err)
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("failed to stat log file %q: %w", path, err)
	}

	maxBytes := int64(maxSizeMB) * 1024 * 1024
	if maxBytes < minRotationBytes {
		maxBytes = minRotationBytes
	}

	return &rotatingWriter{
		path:       path,
		maxBytes:   maxBytes,
		maxBackups: maxBackups,
		f:          f,
		size:       fi.Size(),
	}, nil
}

// Write implements zapcore.WriteSyncer / io.Writer. It rotates the file
// first if the incoming write would push it past maxBytes.
func (w *rotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.maxBytes > 0 && w.size > 0 && w.size+int64(len(p)) > w.maxBytes {
		if err := w.rotate(); err != nil {
			return 0, err
		}
	}

	n, err := w.f.Write(p)
	w.size += int64(n)
	if err != nil {
		return n, fmt.Errorf("failed to write to log file %q: %w", w.path, err)
	}
	return n, nil
}

// rotate closes the active file, renames it aside with a UTC timestamp
// suffix, and reopens a fresh file at the original path. Callers must hold
// w.mu.
func (w *rotatingWriter) rotate() error {
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file %q before rotation: %w", w.path, err)
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("failed to close log file %q before rotation: %w", w.path, err)
	}

	backupPath := w.path + "." + time.Now().UTC().Format("20060102T150405.000Z")
	// A second rotation within the same millisecond would collide on the
	// backup name. Probe for a free ".<n>" suffix, breaking on the first name
	// that doesn't already exist. Break on *any* Stat error (not only
	// IsNotExist) so a permission/IO error can't spin this loop forever.
	if _, statErr := os.Stat(backupPath); statErr == nil {
		for i := 1; ; i++ {
			candidate := fmt.Sprintf("%s.%d", backupPath, i)
			if _, statErr := os.Stat(candidate); statErr != nil {
				backupPath = candidate
				break
			}
		}
	}

	if err := os.Rename(w.path, backupPath); err != nil {
		// The old handle is already closed. Reopen the original path so logging
		// keeps working instead of wedging every future Write on a
		// permanently-closed handle; still report the rotation failure.
		if f, reopenErr := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600); reopenErr == nil {
			w.f = f
		}
		return fmt.Errorf("failed to rotate log file %q: %w", w.path, err)
	}

	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("failed to reopen log file %q after rotation: %w", w.path, err)
	}
	w.f = f
	w.size = 0

	w.prune()

	return nil
}

// prune deletes rotated backups beyond maxBackups, keeping the newest ones.
// maxBackups<=0 means keep none - every rotation clears prior history.
// Callers must hold w.mu.
func (w *rotatingWriter) prune() {
	candidates, err := filepath.Glob(w.path + ".*")
	if err != nil || len(candidates) == 0 {
		return
	}

	// Keep only files whose suffix is one this writer's rotate() produced, so a
	// prefix-sharing sibling (baton.log.lock, another tool's file) is never
	// deleted.
	prefix := w.path + "."
	var matches []string
	for _, c := range candidates {
		if backupSuffixRe.MatchString(strings.TrimPrefix(c, prefix)) {
			matches = append(matches, c)
		}
	}
	if len(matches) == 0 {
		return
	}

	// The timestamp suffix (20060102T150405.000Z) sorts lexically in the
	// same order as chronologically, so a plain string sort is sufficient.
	sort.Strings(matches)

	keep := w.maxBackups
	if keep < 0 {
		keep = 0
	}
	if len(matches) <= keep {
		return
	}

	for _, stale := range matches[:len(matches)-keep] {
		_ = os.Remove(stale)
	}
}

// Sync flushes the active file to stable storage.
func (w *rotatingWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

// Close syncs and closes the active file handle.
func (w *rotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	syncErr := w.f.Sync()
	closeErr := w.f.Close()
	if syncErr != nil {
		return fmt.Errorf("failed to sync log file %q on close: %w", w.path, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close log file %q: %w", w.path, closeErr)
	}
	return nil
}
