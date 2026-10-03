package logging

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
)

// backupTimestampFormat is the UTC timestamp rotate() inserts into backup
// names. It is fixed width, so it sorts lexically in chronological order, which
// is how prune() breaks ties between backups that share an mtime.
const backupTimestampFormat = "20060102T150405.000Z"

// backupSuffixRe matches the "<timestamp>" infix that rotate() inserts before
// the log file's extension, plus the optional same-millisecond disambiguator:
// "_<nn>" as written today, ".<n>" as an earlier build wrote it - those files
// exist in the field and would otherwise never be pruned. prune() deletes only
// files matching this pattern; that is a narrow filter, not proof of ownership -
// a file another tool happened to name the same way would still match. Tracking
// renames in memory instead would be exact, but would leak every backup across a
// crash or restart, which defeats the point of a bounded log directory for a
// service that restarts.
var backupSuffixRe = regexp.MustCompile(`^\d{8}T\d{6}\.\d{3}Z(_\d{2}|\.\d+)?$`)

// logFileMode matches zap's own file sink, so enabling rotation never changes an
// existing deployment's log permissions. On Windows — this feature's target — the
// mode is all but ignored and ACLs govern; restrict the log directory there.
const logFileMode os.FileMode = 0666

// maxBackupNameProbes bounds the search for an unused backup name when several
// rotations land within the same millisecond: that many candidates are tried in
// total, "<ts>" plus "<ts>_01".."<ts>_<n-1>".
const maxBackupNameProbes = 10

// nextBackupPath formats the disambiguator with %02d, so the probe count has to
// stay small enough to fit. Coupled here rather than left as a comment: this
// fails to compile if maxBackupNameProbes ever grows past 100.
const _ = uint(100 - maxBackupNameProbes)

// defaultRotateRetryAfter throttles rotation attempts after a failed rotation.
// Without it a persistently failing rotate (a rename blocked by another handle,
// a full or read-only log directory) runs the whole rotate cycle - stat probes,
// fsync, close, rename - once per log line.
const defaultRotateRetryAfter = 5 * time.Second

// maxOversizeFactor caps the "append anyway" behavior in Write. A rotation that
// keeps failing (read-only or full log directory, an ACL change) may grow the
// active file to this multiple of maxBytes and no further; past that, lines are
// dropped. Unbounded growth beats losing lines only up to the point where the
// log fills the volume and takes the service down with it, which loses every
// line plus the service. The factor is generous on purpose: the transient faults
// this covers clear well inside 10x, so only a permanent fault reaches the
// ceiling - where it degrades to a bounded file plus a diagnostic repeated on
// the rotation-retry cadence rather than a single one at the start.
const maxOversizeFactor = 10

// errLogFileOversized is returned by Write once the ceiling above is reached.
var errLogFileOversized = errors.New("rotation is failing and the log file is past its oversize ceiling")

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
	// closed distinguishes "Close() happened" from "the last open failed, retry
	// on the next Write". Both leave f nil, but only the first is terminal - a
	// closed writer that reopened itself would rename and prune underneath
	// whichever writer owns the file now.
	closed bool
	// nextRotateAttempt suppresses rotation until this instant after a failed
	// rotation; zero means rotate as soon as the size threshold is crossed.
	nextRotateAttempt time.Time
	rotateRetryAfter  time.Duration
	// droppedLines counts lines dropped at the oversize ceiling since the last
	// diagnostic; nextDropReport throttles that diagnostic to the same cadence
	// as rotation retries, so a permanent fault stays loud without emitting one
	// line of diagnostics per dropped line.
	droppedLines   int64
	nextDropReport time.Time
	// pending holds diagnostics produced while w.mu is held, for Write to emit
	// after releasing it. See queueError.
	pending []error
	// errSink receives diagnostics that cannot go through zap: rotation runs
	// inside a zap write, so logging from there would deadlock on this same
	// writer. Always a *lockedErrSink (see wrapErrSink): the caller-supplied
	// sink is shared across every rotator adopted from one RotationConfig, so
	// the lock has to live on the shared sink, not on this writer, or two
	// writers sharing one sink would each serialize on a lock of their own and
	// still race each other.
	errSink io.Writer
}

var (
	_ zapcore.WriteSyncer = (*rotatingWriter)(nil)
	_ io.Closer           = (*rotatingWriter)(nil)
)

// maxRotationMB is the largest log-max-size-mb that converts to bytes without
// overflowing int64. Anything above it saturates; see rotationBytes.
const maxRotationMB int64 = math.MaxInt64 / (1024 * 1024)

// minRotationBytes is the smallest rotation threshold the writer will honor.
// The config surface is in whole megabytes (log-max-size-mb), so the normal
// path is always >= 1 MiB; this floor just guarantees that a rotatingWriter
// constructed with a nonsensically small size (a bug, or a future config
// change) can never rotate on nearly every write.
const minRotationBytes int64 = 1024 * 1024

// rotationBytes converts a whole-MB config value to the byte threshold the
// writer rotates at, clamped up to minRotationBytes.
func rotationBytes(maxSizeMB int) int64 {
	// Saturate rather than wrap: at maxSizeMB >= 2^43 the product overflows
	// negative, trips the clamp below, and a fat-fingered "effectively
	// unlimited" value would rotate every 1 MiB - the opposite of the intent.
	if int64(maxSizeMB) > maxRotationMB {
		return math.MaxInt64
	}
	maxBytes := int64(maxSizeMB) * 1024 * 1024
	if maxBytes < minRotationBytes {
		maxBytes = minRotationBytes
	}
	return maxBytes
}

// newRotatingWriter opens (creating if necessary) the log file at path and
// returns a rotatingWriter that rotates it once it grows past maxSizeMB
// (clamped up to minRotationBytes), retaining maxBackups rotated files
// (<=0 keeps none). A nil errSink falls back to os.Stderr.
func newRotatingWriter(path string, maxSizeMB, maxBackups int, errSink io.Writer) (*rotatingWriter, error) {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory %q: %w", dir, err)
		}
	}

	w := &rotatingWriter{
		path:             path,
		maxBytes:         rotationBytes(maxSizeMB),
		maxBackups:       maxBackups,
		rotateRetryAfter: defaultRotateRetryAfter,
		errSink:          wrapErrSink(errSink),
	}
	if err := w.open(); err != nil {
		return nil, err
	}
	return w, nil
}

// reconfigure applies a later Init's rotation settings to a writer that is
// being reused for the same file, so the newest configuration wins without
// creating a second rotator for one path.
func (w *rotatingWriter) reconfigure(maxSizeMB, maxBackups int, errSink io.Writer) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.maxBytes = rotationBytes(maxSizeMB)
	w.maxBackups = maxBackups
	if errSink != nil {
		w.errSink = wrapErrSink(errSink)
	}
}

// lockedErrSink serializes writes to a caller-supplied ErrSink. adoptRotators
// hands the same RotationConfig.ErrSink to every rotator it adopts for one
// Init, so the lock has to be shared by all of them, not private to each
// writer - see wrapErrSink, which is what makes that sharing happen.
type lockedErrSink struct {
	mu   sync.Mutex
	sink io.Writer
}

func (s *lockedErrSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sink.Write(p)
}

// wrapErrSink wraps sink (or os.Stderr if sink is nil) in a *lockedErrSink,
// unless it already is one. This is the only place that wrapping happens:
// adoptRotators calls it once per Init and passes the single result to every
// rotator it creates or reconfigures for that call, so a sink shared across
// paths gets one shared lock instead of one per writer; the idempotent check
// here means a writer built directly with an already-wrapped sink (as
// adoptRotators does) doesn't get nested in a second, useless layer.
func wrapErrSink(sink io.Writer) io.Writer {
	if sink == nil {
		sink = os.Stderr
	}
	if _, ok := sink.(*lockedErrSink); ok {
		return sink
	}
	return &lockedErrSink{sink: sink}
}

// open adopts a fresh handle at w.path as the active file and resyncs w.size to
// its current length. On failure w.f is left nil rather than pointing at a dead
// handle, so a later Write can retry the open instead of failing forever.
// Callers must hold w.mu.
func (w *rotatingWriter) open() error {
	w.f = nil

	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, logFileMode)
	if err != nil {
		return fmt.Errorf("failed to open log file %q: %w", w.path, err)
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to stat log file %q: %w", w.path, err)
	}

	w.f = f
	w.size = fi.Size()
	return nil
}

// rotateDue reports whether rotation may be attempted, i.e. whether the backoff
// from a previous rotation failure has elapsed. Callers must hold w.mu.
func (w *rotatingWriter) rotateDue() bool {
	return w.nextRotateAttempt.IsZero() || !time.Now().Before(w.nextRotateAttempt)
}

// Write implements zapcore.WriteSyncer / io.Writer. It rotates the file
// first if the incoming write would push it past maxBytes.
func (w *rotatingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	n, err := w.writeLocked(p)
	pending := w.pending
	w.pending = nil
	w.mu.Unlock()

	// Emitted after the unlock: errSink is the Windows event log on the platform
	// this feature targets, and ReportEventW is an RPC - a hung or throttled
	// Event Log service would otherwise stall every log write in the process.
	for _, e := range pending {
		w.reportError(e)
	}
	return n, err
}

// writeLocked is Write's body. Diagnostics are queued rather than emitted; the
// caller flushes them after releasing the lock. Callers must hold w.mu.
func (w *rotatingWriter) writeLocked(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("log file %q: %w", w.path, os.ErrClosed)
	}

	// An earlier rotation may have failed to reopen. Retry here so one transient
	// failure doesn't silently swallow every remaining log line in the process.
	if w.f == nil {
		if err := w.open(); err != nil {
			return 0, err
		}
	}

	var rotateErr error
	if w.maxBytes > 0 && w.size > 0 && w.size+int64(len(p)) > w.maxBytes && w.rotateDue() {
		if rotateErr = w.rotate(); rotateErr != nil {
			// Deliberately not returning here: an oversized log file is a much
			// smaller problem than dropping the line, and on a Windows service
			// there is no fallback sink for it to land in. Bounded by the
			// ceiling below so a permanent fault can't fill the volume.
			w.queueError(rotateErr)
			w.nextRotateAttempt = time.Now().Add(w.rotateRetryAfter)
		} else {
			w.nextRotateAttempt = time.Time{}
			// Lines dropped inside the retry window are only cleared by a
			// queueDropReport that actually emits, so a rotation recovering
			// first would strand the count and silently break the "never a
			// silent drop" guarantee.
			if w.droppedLines > 0 {
				w.queueError(fmt.Errorf("%w: rotation recovered after dropping %d log line(s)",
					errLogFileOversized, w.droppedLines))
				w.droppedLines = 0
				w.nextDropReport = time.Time{}
			}
		}
	}

	// A failed rotation can leave no active handle behind (see rotate).
	if w.f == nil {
		if err := w.open(); err != nil {
			return 0, errors.Join(rotateErr, err)
		}
	}

	// Checked after the reopen above: a rotate that renamed but failed to
	// reopen leaves w.size holding the pre-rotation size for a file that is no
	// longer at w.path, and open() is what resyncs it.
	// w.size > 0 for the same reason rotation checks it: a line that cannot fit
	// under the ceiling on its own is written rather than dropped forever, so the
	// real bound is the ceiling plus at most one line.
	if ceiling := w.oversizeCeiling(); ceiling > 0 && w.size > 0 && w.size+int64(len(p)) > ceiling {
		w.droppedLines++
		w.queueDropReport(ceiling)
		return 0, fmt.Errorf("log file %q: %w", w.path, errLogFileOversized)
	}

	n, err := w.f.Write(p)
	w.size += int64(n)
	if err != nil {
		return n, fmt.Errorf("failed to write to log file %q: %w", w.path, err)
	}
	return n, nil
}

// oversizeCeiling is the size past which Write drops lines instead of appending
// to a file whose rotation keeps failing. 0 means no ceiling. Callers must hold
// w.mu.
func (w *rotatingWriter) oversizeCeiling() int64 {
	if w.maxBytes <= 0 {
		return 0
	}
	ceiling := w.maxBytes * maxOversizeFactor
	if ceiling < w.maxBytes {
		return 0 // overflow on an absurd maxBytes; no reachable ceiling anyway
	}
	return ceiling
}

// queueDropReport queues a diagnostic for the lines dropped at the ceiling, at
// most once per rotateRetryAfter. Callers must hold w.mu.
func (w *rotatingWriter) queueDropReport(ceiling int64) {
	now := time.Now()
	if !w.nextDropReport.IsZero() && now.Before(w.nextDropReport) {
		return
	}
	w.nextDropReport = now.Add(w.rotateRetryAfter)
	dropped := w.droppedLines
	w.droppedLines = 0
	w.queueError(fmt.Errorf("%w (%d bytes, ceiling %d = %dx max size): dropped %d log line(s)",
		errLogFileOversized, w.size, ceiling, maxOversizeFactor, dropped))
}

// backupPath inserts suffix before the log file's extension, so a rotated
// baton.log stays a .log file and keeps its file-type association.
func (w *rotatingWriter) backupPath(suffix string) string {
	ext := filepath.Ext(w.path)
	return strings.TrimSuffix(w.path, ext) + "." + suffix + ext
}

// nextBackupPath returns a currently unused backup path for the active log file
// at timestamp ts, or an error if none is free within maxBackupNameProbes
// attempts.
func (w *rotatingWriter) nextBackupPath(ts string) (string, error) {
	// Rotations inside the same millisecond collide on the timestamp, so probe a
	// bounded number of "_<nn>" variants instead of searching open-endedly. Only
	// a genuinely absent name is free: os.Rename silently overwrites an existing
	// destination, so treating (say) a permission error as "free" would destroy
	// an existing backup.
	for i := 0; i < maxBackupNameProbes; i++ {
		candidate := w.backupPath(ts)
		if i > 0 {
			candidate = w.backupPath(fmt.Sprintf("%s_%02d", ts, i))
		}
		_, err := os.Lstat(candidate)
		switch {
		case err == nil:
			continue // taken
		case errors.Is(err, os.ErrNotExist):
			return candidate, nil
		default:
			return "", fmt.Errorf("failed to check backup name %q: %w", candidate, err)
		}
	}
	return "", fmt.Errorf("failed to find an unused backup name for log file %q after %d attempts", w.path, maxBackupNameProbes)
}

// rotate closes the active file, renames it aside under a timestamped backup
// name, and reopens a fresh file at the original path. On failure it may leave
// w.f nil; Write reopens the path so the pending line still lands somewhere.
// Callers must hold w.mu.
func (w *rotatingWriter) rotate() error {
	// Choose the backup name before touching the handle, so a naming failure
	// leaves the writer exactly as it was.
	backupPath, err := w.nextBackupPath(time.Now().UTC().Format(backupTimestampFormat))
	if err != nil {
		return err
	}

	// A failed flush is not a reason to abandon the rotation: the bytes are
	// already in the OS cache and follow the file through the rename, and
	// aborting here would mean never rotating again on a flaky share.
	if err := w.f.Sync(); err != nil {
		w.queueError(fmt.Errorf("failed to sync log file %q before rotation: %w", w.path, err))
	}
	if err := w.f.Close(); err != nil {
		// The descriptor is unusable whether or not Close reported an error.
		w.f = nil
		return fmt.Errorf("failed to close log file %q before rotation: %w", w.path, err)
	}
	w.f = nil

	if renameErr := os.Rename(w.path, backupPath); renameErr != nil {
		return fmt.Errorf("failed to rotate log file %q: %w", w.path, renameErr)
	}

	if err := w.open(); err != nil {
		return fmt.Errorf("failed to reopen log file %q after rotation: %w", w.path, err)
	}

	if pruneErr := w.prune(); pruneErr != nil {
		w.queueError(pruneErr)
	}

	return nil
}

// backupSuffix reports whether name (a bare file name) is a rotated copy of the
// log file whose name is stem+ext, and returns the timestamp suffix it carries.
// Both the current "<stem>.<ts><ext>" layout and the "<stem><ext>.<ts>" layout
// an earlier build wrote are accepted, so upgrading a deployment doesn't orphan
// its existing backups forever.
func backupSuffix(stem, ext, name string) (string, bool) {
	if suffix, ok := strings.CutPrefix(name, stem+ext+"."); ok && backupSuffixRe.MatchString(suffix) {
		return suffix, true
	}
	infix, ok := strings.CutPrefix(name, stem+".")
	if !ok {
		return "", false
	}
	suffix, ok := strings.CutSuffix(infix, ext)
	if !ok || !backupSuffixRe.MatchString(suffix) {
		return "", false
	}
	return suffix, true
}

// prune deletes rotated backups beyond maxBackups, keeping the newest ones.
// maxBackups 0 (or any negative value, which the CLI rejects) keeps none -
// every rotation clears prior history. Callers must hold w.mu.
func (w *rotatingWriter) prune() error {
	dir := filepath.Dir(w.path)
	active := filepath.Base(w.path)
	ext := filepath.Ext(active)
	stem := strings.TrimSuffix(active, ext)

	// os.ReadDir rather than filepath.Glob: a log path containing a well-formed
	// bracket expression (legal on Windows, e.g. "Vendor [Legacy]") is a valid
	// pattern that matches nothing, which would disable pruning silently.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to list rotated log files for %q: %w", w.path, err)
	}

	type backup struct {
		path    string
		stamp   string
		modTime time.Time
	}
	var matches []backup
	for _, e := range entries {
		name := e.Name()
		// backupSuffix can't match the active name (it has no timestamp infix),
		// so this guard is unreachable today. Kept: the cost is one comparison,
		// and the failure mode a future matcher change would open up is prune
		// unlinking the live log file.
		if name == active {
			continue
		}
		stamp, ok := backupSuffix(stem, ext, name)
		if !ok {
			continue
		}
		candidate := filepath.Join(dir, name)
		// A directory or symlink named like a backup is not something rotation
		// created, so never unlink it.
		fi, lstatErr := os.Lstat(candidate)
		if lstatErr != nil || !fi.Mode().IsRegular() {
			continue
		}
		// A name matching the backup pattern is only ever a filename-regex
		// coincidence, not proof of ownership (see backupSuffixRe): if another
		// rotatingWriter in this process actively owns this path, it is that
		// rotator's live file, not one of ours, no matter how it's named.
		if isActiveRotatorPath(candidate) {
			continue
		}
		matches = append(matches, backup{path: candidate, stamp: stamp, modTime: fi.ModTime()})
	}

	keep := w.maxBackups
	if keep < 0 {
		keep = 0
	}
	if len(matches) <= keep {
		return nil
	}

	// Order by mtime, not by name: a backup name freed by an earlier prune gets
	// reclaimed by the next rotation in the same millisecond, and the clock can
	// step backwards (NTP, VM resume), so name order is not age order. Ties are
	// real (FAT32 has 2s mtime granularity, ext3 1s) and are broken on the
	// embedded timestamp, never the raw path: the two naming layouts interleave,
	// so path order sorts every "<stem>.<ts><ext>" backup ahead of every legacy
	// "<stem><ext>.<ts>" one regardless of age.
	sort.Slice(matches, func(i, j int) bool {
		if !matches[i].modTime.Equal(matches[j].modTime) {
			return matches[i].modTime.Before(matches[j].modTime)
		}
		if matches[i].stamp != matches[j].stamp {
			return matches[i].stamp < matches[j].stamp
		}
		return matches[i].path < matches[j].path
	})

	var errs []error
	for _, stale := range matches[:len(matches)-keep] {
		if rmErr := os.Remove(stale.path); rmErr != nil && !os.IsNotExist(rmErr) {
			errs = append(errs, fmt.Errorf("failed to remove rotated log file %q: %w", stale.path, rmErr))
		}
	}
	return errors.Join(errs...)
}

// queueError defers a diagnostic raised under w.mu until the lock is released.
// Callers must hold w.mu.
func (w *rotatingWriter) queueError(err error) {
	w.pending = append(w.pending, err)
}

// reportError surfaces a rotation problem that has nowhere else to go. It takes
// w.mu only to read the sink, and never writes to the sink while holding it -
// see Write. Concurrent callers instead serialize on the sink's own lock (see
// wrapErrSink), so a sink that isn't itself concurrency-safe (e.g. a
// *bytes.Buffer) can't be corrupted by two writers racing on it, even when
// they are two different rotatingWriters sharing one caller-supplied sink.
// Callers must not hold w.mu.
func (w *rotatingWriter) reportError(err error) {
	w.mu.Lock()
	sink := w.errSink
	w.mu.Unlock()
	if sink == nil {
		return
	}
	fmt.Fprintf(sink, "baton: log rotation for %q: %v\n", w.path, err)
}

// Sync flushes the active file to stable storage. A writer with no handle
// because its last open failed reports that rather than a false success -
// durability is the point of this Sync. A closed writer reports nil: Close
// already flushed, so there is genuinely nothing outstanding.
func (w *rotatingWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	if w.f == nil {
		return fmt.Errorf("failed to sync log file %q: no active file handle", w.path)
	}
	return w.f.Sync()
}

// Close syncs and closes the active file handle. It is terminal: later writes
// fail with os.ErrClosed rather than reopening the file.
func (w *rotatingWriter) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true

	// droppedLines is only cleared by a queueDropReport that emits or by a
	// rotation that recovers, so shutting down first discards the count - the
	// same silent drop the ceiling diagnostics exist to prevent. Two ways in:
	// Close landing inside the retry window, and reconfigure raising maxBytes
	// past the current size, after which rotation is no longer attempted and the
	// recovery flush is never reached. This is the last chance to say so.
	// Not zeroed afterwards, unlike the other two flush sites: w.closed is
	// already set, so writeLocked returns os.ErrClosed and a second Close
	// short-circuits above - nothing reads droppedLines again.
	if w.droppedLines > 0 {
		w.queueError(fmt.Errorf("%w: %d log line(s) dropped, unreported before close",
			errLogFileOversized, w.droppedLines))
	}

	var errs []error
	if w.f != nil {
		syncErr := w.f.Sync()
		closeErr := w.f.Close()
		w.f = nil

		if syncErr != nil {
			errs = append(errs, fmt.Errorf("failed to sync log file %q on close: %w", w.path, syncErr))
		}
		if closeErr != nil {
			errs = append(errs, fmt.Errorf("failed to close log file %q: %w", w.path, closeErr))
		}
	}

	pending := w.pending
	w.pending = nil
	w.mu.Unlock()

	// After the unlock, as in Write: reportError takes w.mu, and errSink is an
	// Event Log RPC on the platform this targets.
	for _, e := range pending {
		w.reportError(e)
	}

	return errors.Join(errs...)
}
