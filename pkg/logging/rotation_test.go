package logging

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const testPrefix = "app"

// fakeClock returns a now-function backed by a pointer the test can mutate
// to advance simulated time.
func fakeClock(t *time.Time) func() time.Time {
	return func() time.Time { return *t }
}

func activeLogPath(dir, date string) string {
	return filepath.Join(dir, testPrefix+"-"+date+".log")
}

// --- Basic operation tests ---

func TestDailyRotator_WritesToActiveFile(t *testing.T) {
	dir := t.TempDir()

	r, err := NewDailyRotator(dir, testPrefix, 10)
	if err != nil {
		t.Fatalf("NewDailyRotator: %v", err)
	}
	defer r.Close()

	msg := "hello world\n"
	n, err := r.Write([]byte(msg))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(msg) {
		t.Fatalf("Write returned %d, want %d", n, len(msg))
	}

	if err := r.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	logPath := activeLogPath(dir, time.Now().Format(dateFormat))
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != msg {
		t.Fatalf("got %q, want %q", data, msg)
	}
}

func TestDailyRotator_MultipleWritesAppend(t *testing.T) {
	dir := t.TempDir()

	r, err := NewDailyRotator(dir, testPrefix, 10)
	if err != nil {
		t.Fatalf("NewDailyRotator: %v", err)
	}
	defer r.Close()

	for i := 0; i < 100; i++ {
		if _, err := r.Write([]byte("line\n")); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := r.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	logPath := activeLogPath(dir, time.Now().Format(dateFormat))
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Count(string(data), "line\n")
	if lines != 100 {
		t.Fatalf("expected 100 lines, got %d", lines)
	}
}

// --- Rotation tests ---

func TestDailyRotator_RotatesOnDateChange(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)
	r, err := newDailyRotator(dir, testPrefix, 10, fakeClock(&now))
	if err != nil {
		t.Fatalf("newDailyRotator: %v", err)
	}
	defer r.Close()

	if _, err := r.Write([]byte("day1 log\n")); err != nil {
		t.Fatalf("Write day1: %v", err)
	}
	if err := r.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Advance to day 2 — triggers rotation on next write.
	now = time.Date(2026, 3, 29, 10, 0, 0, 0, time.UTC)

	if _, err := r.Write([]byte("day2 log\n")); err != nil {
		t.Fatalf("Write day2: %v", err)
	}
	if err := r.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Day 2 file should contain only day2 content.
	day2 := activeLogPath(dir, "2026-03-29")
	data, err := os.ReadFile(day2)
	if err != nil {
		t.Fatalf("ReadFile day2: %v", err)
	}
	if string(data) != "day2 log\n" {
		t.Fatalf("day2 file: got %q, want %q", data, "day2 log\n")
	}

	// Day 1 file should exist either uncompressed or compressed.
	day1 := activeLogPath(dir, "2026-03-28")
	compressed := day1 + ".gz"
	if _, err := os.Stat(day1); err != nil {
		if _, err2 := os.Stat(compressed); err2 != nil {
			t.Fatalf("neither day1 nor compressed file exists: %v / %v", err, err2)
		}
	}
}

func TestDailyRotator_MultipleRotations(t *testing.T) {
	dir := t.TempDir()

	base := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	current := base

	r, err := newDailyRotator(dir, testPrefix, 30, fakeClock(&current))
	if err != nil {
		t.Fatalf("newDailyRotator: %v", err)
	}
	defer r.Close()

	// Write across 5 consecutive days.
	for day := 0; day < 5; day++ {
		current = base.AddDate(0, 0, day)
		if _, err := r.Write([]byte("log\n")); err != nil {
			t.Fatalf("Write day %d: %v", day, err)
		}
	}
	if err := r.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Last day's file should have content.
	lastDate := base.AddDate(0, 0, 4).Format(dateFormat)
	data, err := os.ReadFile(activeLogPath(dir, lastDate))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != "log\n" {
		t.Fatalf("last day: got %q, want %q", data, "log\n")
	}

	// We should have 4 rotated files (day 0 through day 3) either compressed or not.
	for day := 0; day < 4; day++ {
		date := base.AddDate(0, 0, day).Format(dateFormat)
		rotated := activeLogPath(dir, date)
		compressed := rotated + ".gz"
		if _, err := os.Stat(rotated); err != nil {
			if _, err2 := os.Stat(compressed); err2 != nil {
				t.Fatalf("day %d: neither %s nor %s found", day, rotated, compressed)
			}
		}
	}
}

// --- Compression tests ---

func TestDailyRotator_CompressesRotatedFile(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 3, 28, 10, 0, 0, 0, time.UTC)
	r, err := newDailyRotator(dir, testPrefix, 10, fakeClock(&now))
	if err != nil {
		t.Fatalf("newDailyRotator: %v", err)
	}
	defer r.Close()

	content := "day1 content for compression test\n"
	if _, err := r.Write([]byte(content)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := r.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Advance to day 2, triggering rotation.
	now = time.Date(2026, 3, 29, 10, 0, 0, 0, time.UTC)

	if _, err := r.Write([]byte("day2\n")); err != nil {
		t.Fatalf("Write day2: %v", err)
	}

	// Wait for background compression goroutine to finish.
	compressed := activeLogPath(dir, "2026-03-28") + ".gz"
	waitForFile(t, compressed, 5*time.Second)

	// Verify the compressed file contains the original content.
	assertGzipContents(t, compressed, content)

	// Uncompressed rotated file should have been removed.
	rotated := activeLogPath(dir, "2026-03-28")
	waitForFileRemoval(t, rotated, 5*time.Second)
}

// --- Cleanup / retention tests ---

func TestDailyRotator_CleansUpExpiredLogs(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)

	// Expired: 15 days ago with 10-day retention.
	oldDate := now.AddDate(0, 0, -15).Format(dateFormat)
	oldFile := activeLogPath(dir, oldDate) + ".gz"
	if err := os.WriteFile(oldFile, []byte("old"), 0600); err != nil {
		t.Fatalf("WriteFile old: %v", err)
	}

	// Not expired: 5 days ago.
	recentDate := now.AddDate(0, 0, -5).Format(dateFormat)
	recentFile := activeLogPath(dir, recentDate) + ".gz"
	if err := os.WriteFile(recentFile, []byte("recent"), 0600); err != nil {
		t.Fatalf("WriteFile recent: %v", err)
	}

	// Expired uncompressed file (compression may have failed previously).
	oldUncompressedDate := now.AddDate(0, 0, -12).Format(dateFormat)
	oldUncompressed := activeLogPath(dir, oldUncompressedDate)
	if err := os.WriteFile(oldUncompressed, []byte("old-uncompressed"), 0600); err != nil {
		t.Fatalf("WriteFile old uncompressed: %v", err)
	}

	// newDailyRotator runs cleanup on startup.
	r, err := newDailyRotator(dir, testPrefix, 10, fakeClock(&now))
	if err != nil {
		t.Fatalf("newDailyRotator: %v", err)
	}
	defer r.Close()

	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Fatalf("old compressed file should have been cleaned up")
	}
	// The old uncompressed file is first compressed by compressStaleFiles, then
	// the compressed result is older than retention and should be cleaned up.
	if _, err := os.Stat(oldUncompressed); !os.IsNotExist(err) {
		t.Fatalf("old uncompressed file should have been cleaned up")
	}
	if _, err := os.Stat(oldUncompressed + ".gz"); !os.IsNotExist(err) {
		t.Fatalf("compressed-from-old-uncompressed file should have been cleaned up")
	}
	if _, err := os.Stat(recentFile); err != nil {
		t.Fatalf("recent file should still exist: %v", err)
	}
}

func TestDailyRotator_CleanupIgnoresUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()

	now := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)

	// Create a file that matches the glob but has a non-date name.
	unrelated := filepath.Join(dir, testPrefix+"-config.log")
	if err := os.WriteFile(unrelated, []byte("config"), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r, err := newDailyRotator(dir, testPrefix, 1, fakeClock(&now))
	if err != nil {
		t.Fatalf("newDailyRotator: %v", err)
	}
	defer r.Close()

	// Unrelated file should not be deleted.
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatalf("unrelated file should not have been cleaned up: %v", err)
	}
}

// --- Stale file startup tests ---

func TestDailyRotator_CompressesStaleFileOnStartup(t *testing.T) {
	dir := t.TempDir()

	// Pre-existing log file for an earlier day.
	stale := activeLogPath(dir, "2026-03-29")
	if err := os.WriteFile(stale, []byte("stale content\n"), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	todayTime := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)

	r, err := newDailyRotator(dir, testPrefix, 10, fakeClock(&todayTime))
	if err != nil {
		t.Fatalf("newDailyRotator: %v", err)
	}
	defer r.Close()

	if _, err := r.Write([]byte("fresh\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := r.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Today's file should contain only fresh content.
	today := activeLogPath(dir, "2026-03-30")
	data, err := os.ReadFile(today)
	if err != nil {
		t.Fatalf("ReadFile today: %v", err)
	}
	if strings.Contains(string(data), "stale") {
		t.Fatalf("today's file should not contain stale content: %q", data)
	}

	// Stale file should have been compressed.
	compressed := stale + ".gz"
	waitForFile(t, compressed, 5*time.Second)
	waitForFileRemoval(t, stale, 5*time.Second)
}

// --- Default / edge case tests ---

func TestDailyRotator_DefaultRetention(t *testing.T) {
	dir := t.TempDir()

	r, err := NewDailyRotator(dir, testPrefix, 0)
	if err != nil {
		t.Fatalf("NewDailyRotator: %v", err)
	}
	defer r.Close()

	if r.logRotationDays != DefaultLogRotationDays {
		t.Fatalf("logRotationDays: got %d, want %d", r.logRotationDays, DefaultLogRotationDays)
	}
}

func TestDailyRotator_NegativeRetention(t *testing.T) {
	dir := t.TempDir()

	r, err := NewDailyRotator(dir, testPrefix, -5)
	if err != nil {
		t.Fatalf("NewDailyRotator: %v", err)
	}
	defer r.Close()

	if r.logRotationDays != DefaultLogRotationDays {
		t.Fatalf("logRotationDays: got %d, want %d", r.logRotationDays, DefaultLogRotationDays)
	}
}

func TestDailyRotator_WriteAfterClose(t *testing.T) {
	dir := t.TempDir()

	r, err := NewDailyRotator(dir, testPrefix, 10)
	if err != nil {
		t.Fatalf("NewDailyRotator: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Write after Close should return an error, not panic.
	_, err = r.Write([]byte("should fail\n"))
	if err == nil {
		t.Fatal("Write after Close should return an error")
	}
}

func TestDailyRotator_DoubleClose(t *testing.T) {
	dir := t.TempDir()

	r, err := NewDailyRotator(dir, testPrefix, 10)
	if err != nil {
		t.Fatalf("NewDailyRotator: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	// Second close should be a no-op, not an error.
	if err := r.Close(); err != nil {
		t.Fatalf("Close 2: %v", err)
	}
}

func TestDailyRotator_SyncAfterClose(t *testing.T) {
	dir := t.TempDir()

	r, err := NewDailyRotator(dir, testPrefix, 10)
	if err != nil {
		t.Fatalf("NewDailyRotator: %v", err)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Sync after close is a no-op.
	if err := r.Sync(); err != nil {
		t.Fatalf("Sync after Close should not error: %v", err)
	}
}

func TestDailyRotator_CreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "a", "b", "c")

	r, err := NewDailyRotator(nested, testPrefix, 10)
	if err != nil {
		t.Fatalf("NewDailyRotator: %v", err)
	}
	defer r.Close()

	if _, err := os.Stat(nested); err != nil {
		t.Fatalf("nested directory should have been created: %v", err)
	}
}

// --- Concurrency test ---

func TestDailyRotator_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()

	r, err := NewDailyRotator(dir, testPrefix, 10)
	if err != nil {
		t.Fatalf("NewDailyRotator: %v", err)
	}
	defer r.Close()

	var wg sync.WaitGroup
	writers := 10
	writesPerWriter := 100
	msg := "concurrent line\n"

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < writesPerWriter; j++ {
				if _, err := r.Write([]byte(msg)); err != nil {
					t.Errorf("concurrent Write: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if err := r.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	logPath := activeLogPath(dir, time.Now().Format(dateFormat))
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Count(string(data), msg)
	expected := writers * writesPerWriter
	if lines != expected {
		t.Fatalf("expected %d lines, got %d", expected, lines)
	}
}

// --- Init integration test ---

func TestInit_WithLogRotation(t *testing.T) {
	dir := t.TempDir()

	ctx, err := Init(
		context.Background(),
		WithLogFormat(LogFormatJSON),
		WithLogLevel("info"),
		WithLogRotation(dir, "test", 5),
		WithFileOnly(true),
	)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	_ = ctx

	// Clean up the rotator created by Init.
	activeMu.Lock()
	rotator := activeRotator
	activeRotator = nil
	activeMu.Unlock()
	if rotator != nil {
		defer func() {
			if err := rotator.Close(); err != nil {
				t.Logf("close activeRotator: %v", err)
			}
		}()
	}

	// The log file should have been created.
	logPath := filepath.Join(dir, "test-"+time.Now().Format(dateFormat)+".log")
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("log file should exist: %v", err)
	}
}

func TestInit_WithoutRotation_Regression(t *testing.T) {
	// Ensure the non-rotation path still works (backwards compat).
	ctx, err := Init(
		context.Background(),
		WithLogFormat(LogFormatJSON),
		WithLogLevel("info"),
	)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	_ = ctx
}

func TestInit_EmptyLogDir_NoOp(t *testing.T) {
	// WithLogRotation with empty dir should be a no-op.
	ctx, err := Init(
		context.Background(),
		WithLogFormat(LogFormatJSON),
		WithLogLevel("info"),
		WithLogRotation("", "baton", 10),
	)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	_ = ctx
}

// --- Helpers ---

func assertGzipContents(t *testing.T, path, expected string) {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open compressed: %v", err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer gr.Close()

	data, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(data) != expected {
		t.Fatalf("decompressed: got %q, want %q", data, expected)
	}
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("file %s did not appear within %v", path, timeout)
}

func waitForFileRemoval(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("file %s was not removed within %v", path, timeout)
}
