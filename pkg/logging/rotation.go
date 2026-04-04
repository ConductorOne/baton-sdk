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
	// DefaultRetentionDays is the default number of days to keep compressed log files.
	DefaultRetentionDays = 10
	dateFormat           = "2006-01-02"
)

// nowFunc can be overridden in tests to control time.
var nowFunc = time.Now

func today() string {
	return nowFunc().Format(dateFormat)
}

// DailyRotator is a zapcore.WriteSyncer that rotates log files daily,
// compresses rotated logs with gzip, and removes logs older than the
// configured retention period.
//
// File naming convention:
//
//	Active:     {baseName}{ext}            e.g. baton.log
//	Rotated:    {baseName}-{date}{ext}     e.g. baton-2026-03-29.log
//	Compressed: {baseName}-{date}{ext}.gz  e.g. baton-2026-03-29.log.gz
type DailyRotator struct {
	mu            sync.Mutex
	dir           string
	baseName      string // filename without extension (e.g. "baton")
	ext           string // file extension including dot (e.g. ".log")
	currentFile   *os.File
	currentDate   string
	retentionDays int
}

// NewDailyRotator creates a new DailyRotator that writes to the given file path.
// Rotated logs are stored in the same directory with a date suffix.
// Logs older than retentionDays are automatically deleted.
func NewDailyRotator(filePath string, retentionDays int) (*DailyRotator, error) {
	if retentionDays <= 0 {
		retentionDays = DefaultRetentionDays
	}

	dir := filepath.Dir(filePath)
	base := filepath.Base(filePath)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)

	r := &DailyRotator{
		dir:           dir,
		baseName:      name,
		ext:           ext,
		retentionDays: retentionDays,
	}

	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}

	// If active log file exists and is from a previous day, rotate it.
	if err := r.rotateStaleFile(); err != nil {
		return nil, fmt.Errorf("rotate stale log: %w", err)
	}

	// Open or create the active log file.
	if err := r.openActive(); err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	// Clean up expired logs on startup.
	r.cleanup()

	return r, nil
}

func (r *DailyRotator) activePath() string {
	return filepath.Join(r.dir, r.baseName+r.ext)
}

func (r *DailyRotator) rotatedPath(date string) string {
	return filepath.Join(r.dir, fmt.Sprintf("%s-%s%s", r.baseName, date, r.ext))
}

func (r *DailyRotator) compressedPath(date string) string {
	return filepath.Join(r.dir, fmt.Sprintf("%s-%s%s.gz", r.baseName, date, r.ext))
}

func (r *DailyRotator) openActive() error {
	f, err := os.OpenFile(r.activePath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
	if err != nil {
		return err
	}
	r.currentFile = f
	r.currentDate = today()
	return nil
}

// rotateStaleFile checks if the active log file exists and is from a previous
// day. If so, it renames the file with a date suffix and compresses it.
func (r *DailyRotator) rotateStaleFile() error {
	info, err := os.Stat(r.activePath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	fileDate := info.ModTime().Format(dateFormat)
	if fileDate == today() {
		return nil
	}

	rotated := r.rotatedPath(fileDate)
	if err := os.Rename(r.activePath(), rotated); err != nil {
		return err
	}

	go r.compressAndRemove(rotated, fileDate)
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

	if today() != r.currentDate {
		if err := r.rotateLocked(); err != nil {
			// Log the error but continue writing to the current file.
			zap.L().Error("log rotation failed", zap.Error(err))
		}
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
	defer r.mu.Unlock()

	if r.currentFile == nil {
		return nil
	}
	err := r.currentFile.Sync()
	closeErr := r.currentFile.Close()
	r.currentFile = nil
	if err != nil {
		return err
	}
	return closeErr
}

// rotateLocked performs the actual rotation. Must be called with mu held.
func (r *DailyRotator) rotateLocked() error {
	oldDate := r.currentDate

	if r.currentFile != nil {
		if err := r.currentFile.Close(); err != nil {
			return fmt.Errorf("close current log: %w", err)
		}
		r.currentFile = nil
	}

	rotated := r.rotatedPath(oldDate)
	if err := os.Rename(r.activePath(), rotated); err != nil {
		// If rename fails, try to reopen the active file.
		_ = r.openActive()
		return fmt.Errorf("rename log for rotation: %w", err)
	}

	if err := r.openActive(); err != nil {
		return fmt.Errorf("open new log after rotation: %w", err)
	}

	// Compress the old file and clean up expired logs in the background.
	go func() {
		r.compressAndRemove(rotated, oldDate)
		r.cleanup()
	}()

	return nil
}

// compressAndRemove gzip-compresses a rotated log file and removes the original.
// The source file is explicitly closed before removal to avoid file-locking
// issues on Windows.
func (r *DailyRotator) compressAndRemove(srcPath, date string) {
	dstPath := r.compressedPath(date)

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

// cleanup removes rotated and compressed log files older than the retention period.
func (r *DailyRotator) cleanup() {
	cutoff := nowFunc().AddDate(0, 0, -r.retentionDays)

	// Match compressed rotated logs: baton-*.log.gz
	compressedPattern := filepath.Join(r.dir, fmt.Sprintf("%s-*%s.gz", r.baseName, r.ext))
	compressedMatches, _ := filepath.Glob(compressedPattern)

	// Match uncompressed rotated logs: baton-*.log (in case compression failed)
	uncompressedPattern := filepath.Join(r.dir, fmt.Sprintf("%s-*%s", r.baseName, r.ext))
	uncompressedMatches, _ := filepath.Glob(uncompressedPattern)

	// Collect all matches without mutating either source slice (gocritic: appendAssign).
	allMatches := make([]string, 0, len(compressedMatches)+len(uncompressedMatches))
	allMatches = append(allMatches, compressedMatches...)
	allMatches = append(allMatches, uncompressedMatches...)

	active := r.activePath()

	for _, path := range allMatches {
		if path == active {
			continue
		}

		// Extract date from filename.
		base := filepath.Base(path)
		base = strings.TrimSuffix(base, ".gz")
		dateStr := strings.TrimPrefix(base, r.baseName+"-")
		dateStr = strings.TrimSuffix(dateStr, r.ext)

		logDate, err := time.Parse(dateFormat, dateStr)
		if err != nil {
			continue // not our naming convention
		}

		if logDate.Before(cutoff) {
			if err := os.Remove(path); err != nil {
				zap.L().Error("remove expired log", zap.Error(err), zap.String("path", path))
			}
		}
	}
}
