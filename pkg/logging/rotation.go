package logging

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	// DefaultLogRotationDays is the default number of days to keep compressed log files.
	DefaultLogRotationDays = 30
	logFileExt             = ".log"
	dateFormat             = "2006-01-02"
)

// DailyRotator is a zapcore.WriteSyncer that rotates log files daily,
// compresses rotated logs with gzip, and removes logs older than the
// configured retention period.
//
// File naming convention:
//
//	Active:     {dir}/{prefix}-{today}.log         e.g. baton-2026-03-29.log
//	Compressed: {dir}/{prefix}-{date}.log.gz       e.g. baton-2026-03-28.log.gz
type DailyRotator struct {
	mu              sync.Mutex
	dir             string
	prefix          string
	currentFile     *os.File
	currentDate     string
	logRotationDays int
	nowFunc         func() time.Time
}

// NewDailyRotator creates a new DailyRotator that writes daily log files into
// dir named "{prefix}-{YYYY-MM-DD}.log". Logs older than logRotationDays are
// automatically deleted.
func NewDailyRotator(dir, prefix string, logRotationDays int) (*DailyRotator, error) {
	return newDailyRotator(dir, prefix, logRotationDays, time.Now)
}

// newDailyRotator is the internal constructor that accepts a custom time
// source. Tests use it to inject a fake clock; the exported NewDailyRotator
// always passes time.Now.
func newDailyRotator(dir, prefix string, logRotationDays int, nowFn func() time.Time) (*DailyRotator, error) {
	if logRotationDays <= 0 {
		logRotationDays = DefaultLogRotationDays
	}
	if nowFn == nil {
		nowFn = time.Now
	}

	r := &DailyRotator{
		dir:             dir,
		prefix:          prefix,
		logRotationDays: logRotationDays,
		nowFunc:         nowFn,
	}

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}

	// Compress any leftover .log files from previous days.
	if err := r.compressStaleFiles(); err != nil {
		return nil, fmt.Errorf("compress stale logs: %w", err)
	}

	// Open or create today's active log file.
	if err := r.openActive(); err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	// Clean up expired logs on startup.
	r.cleanup()

	return r, nil
}

func (r *DailyRotator) today() string {
	return r.nowFunc().Format(dateFormat)
}

func (r *DailyRotator) pathForDate(date string) string {
	return filepath.Join(r.dir, fmt.Sprintf("%s-%s%s", r.prefix, date, logFileExt))
}

func (r *DailyRotator) compressedPathForDate(date string) string {
	return r.pathForDate(date) + ".gz"
}

func (r *DailyRotator) activePath() string {
	return r.pathForDate(r.today())
}

func (r *DailyRotator) openActive() error {
	f, err := os.OpenFile(r.activePath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
	if err != nil {
		return err
	}
	r.currentFile = f
	r.currentDate = r.today()
	return nil
}

// compressStaleFiles finds uncompressed log files in dir whose date is not
// today and compresses them. This handles the case where the process exited
// before its last-day file could be compressed.
func (r *DailyRotator) compressStaleFiles() error {
	pattern := filepath.Join(r.dir, fmt.Sprintf("%s-*%s", r.prefix, logFileExt))
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	today := r.today()
	for _, path := range matches {
		date, ok := r.parseDateFromPath(path)
		if !ok || date == today {
			continue
		}
		r.compressAndRemove(path, date)
	}
	return nil
}

// Write implements zapcore.WriteSyncer. It checks for a date boundary
// crossing before each write and rotates the log file if needed.
func (r *DailyRotator) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentFile == nil {
		return 0, fmt.Errorf("write to closed log rotator")
	}

	if r.today() != r.currentDate {
		if err := r.rotateLocked(); err != nil {
			// Log the error but continue writing to the current file.
			zap.L().Error("log rotation failed", zap.Error(err))
		}
	}

	if r.currentFile == nil {
		return 0, fmt.Errorf("log file unavailable after failed rotation")
	}

	return r.currentFile.Write(p)
}

// Sync flushes the current log file to disk.
func (r *DailyRotator) Sync() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currentFile == nil {
		return nil
	}
	return r.currentFile.Sync()
}

// Close syncs and closes the underlying file.
func (r *DailyRotator) Close() error {
	r.mu.Lock()
	if r.currentFile == nil {
		r.mu.Unlock()
		return nil
	}
	err := r.currentFile.Sync()
	closeErr := r.currentFile.Close()
	r.currentFile = nil
	r.mu.Unlock()

	if err != nil {
		return err
	}
	return closeErr
}

// rotateLocked performs the actual rotation. Must be called with mu held.
func (r *DailyRotator) rotateLocked() error {
	oldDate := r.currentDate
	oldPath := r.pathForDate(oldDate)

	if r.currentFile != nil {
		if err := r.currentFile.Close(); err != nil {
			return fmt.Errorf("close current log: %w", err)
		}
		r.currentFile = nil
	}

	if err := r.openActive(); err != nil {
		return fmt.Errorf("open new log after rotation: %w", err)
	}

	// Compress the old file and clean up expired logs in the background.
	r.compressAndRemove(oldPath, oldDate)
	r.cleanup()

	return nil
}

// compressAndRemove gzip-compresses a rotated log file and removes the original.
// The source file is explicitly closed before removal to avoid file-locking
// issues on Windows.
func (r *DailyRotator) compressAndRemove(srcPath, date string) {
	dstPath := r.compressedPathForDate(date)

	if err := r.compressFile(srcPath, dstPath); err != nil {
		zap.L().Error("compress rotated log", zap.Error(err),
			zap.String("src", srcPath), zap.String("dst", dstPath))
		return
	}

	if err := os.Remove(srcPath); err != nil {
		zap.L().Error("remove rotated log after compression", zap.Error(err), zap.String("path", srcPath))
	}
}

// compressFile gzip-compresses src into dst. Both files are fully closed
// before this function returns (required for Windows file deletion).
func (r *DailyRotator) compressFile(srcPath, dstPath string) (retErr error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer func() {
		if err := src.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("close source: %w", err)
		}
	}()

	dst, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("create destination: %w", err)
	}
	defer func() {
		if err := dst.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("close destination: %w", err)
		}
		// If compression failed, clean up the partial file.
		if retErr != nil {
			_ = os.Remove(dstPath)
		}
	}()

	gw := gzip.NewWriter(dst)
	if _, err := io.Copy(gw, src); err != nil {
		return fmt.Errorf("gzip copy: %w", err)
	}

	if err := gw.Close(); err != nil {
		return fmt.Errorf("gzip finalize: %w", err)
	}

	return nil
}

// parseDateFromPath extracts the YYYY-MM-DD date from a rotated log filename.
// Accepts both "{prefix}-{date}.log" and "{prefix}-{date}.log.gz".
func (r *DailyRotator) parseDateFromPath(path string) (string, bool) {
	base := filepath.Base(path)
	base = strings.TrimSuffix(base, ".gz")
	base = strings.TrimSuffix(base, logFileExt)
	dateStr := strings.TrimPrefix(base, r.prefix+"-")
	if _, err := time.Parse(dateFormat, dateStr); err != nil {
		return "", false
	}
	return dateStr, true
}

// cleanup removes rotated and compressed log files older than the retention period.
func (r *DailyRotator) cleanup() {
	cutoff := r.nowFunc().AddDate(0, 0, -r.logRotationDays)

	compressedMatches, _ := filepath.Glob(filepath.Join(r.dir, fmt.Sprintf("%s-*%s.gz", r.prefix, logFileExt)))
	uncompressedMatches, _ := filepath.Glob(filepath.Join(r.dir, fmt.Sprintf("%s-*%s", r.prefix, logFileExt)))

	allMatches := make([]string, 0, len(compressedMatches)+len(uncompressedMatches))
	allMatches = append(allMatches, compressedMatches...)
	allMatches = append(allMatches, uncompressedMatches...)

	active := r.activePath()

	for _, path := range allMatches {
		if path == active {
			continue
		}

		dateStr, ok := r.parseDateFromPath(path)
		if !ok {
			continue
		}
		logDate, err := time.Parse(dateFormat, dateStr)
		if err != nil {
			continue
		}

		if logDate.Before(cutoff) {
			if err := os.Remove(path); err != nil {
				zap.L().Error("remove expired log", zap.Error(err), zap.String("path", path))
			}
		}
	}
}
