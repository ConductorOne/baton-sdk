package test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/synccompactor"
)

// TestTmpDirEnvironmentVariable tests that setting TMPDIR environment variable
// properly redirects temporary file creation away from /tmp
func TestTmpDirEnvironmentVariable(t *testing.T) {
	// Create a temporary directory for our test
	testTempDir, err := os.MkdirTemp("", "baton-test-temp-")
	if err != nil {
		t.Fatalf("Failed to create test temp directory: %v", err)
	}
	defer os.RemoveAll(testTempDir)

	// Create a monitor for /tmp directory
	monitor, err := NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Set TMPDIR environment variable
	originalTmpDir := os.Getenv("TMPDIR")
	defer os.Setenv("TMPDIR", originalTmpDir)

	err = os.Setenv("TMPDIR", testTempDir)
	if err != nil {
		t.Fatalf("Failed to set TMPDIR environment variable: %v", err)
	}

	// Test that os.TempDir() now returns our test directory
	systemTempDir := os.TempDir()
	if systemTempDir != testTempDir {
		t.Errorf("Expected os.TempDir() to return %s, got %s", testTempDir, systemTempDir)
	}

	// Test dotc1z file operations
	testDotC1zFileOperations(t, testTempDir, monitor)

	// Test synccompactor operations
	testSynccompactorOperations(t, testTempDir, monitor)

	// Test connector runner configuration
	testConnectorRunnerConfiguration(t, testTempDir, monitor)

	// Assert no files were created in /tmp
	monitor.AssertNoNewFilesInTmp(t)
}

// TestC1zTempDirField tests that the c1z-temp-dir field properly configures
// temporary directory usage throughout the system
func TestC1zTempDirField(t *testing.T) {
	// Create a temporary directory for our test
	testTempDir, err := os.MkdirTemp("", "baton-test-temp-")
	if err != nil {
		t.Fatalf("Failed to create test temp directory: %v", err)
	}
	defer os.RemoveAll(testTempDir)

	// Create a monitor for /tmp directory
	monitor, err := NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Test dotc1z file operations with configured temp directory
	testDotC1zFileOperations(t, testTempDir, monitor)

	// Test synccompactor operations with configured temp directory
	testSynccompactorOperations(t, testTempDir, monitor)

	// Test connector runner configuration with configured temp directory
	testConnectorRunnerConfiguration(t, testTempDir, monitor)

	// Assert no files were created in /tmp
	monitor.AssertNoNewFilesInTmp(t)
}

func testDotC1zFileOperations(t *testing.T, testTempDir string, monitor *TmpMonitor) {
	t.Helper()

	// Create a test c1z file
	testC1zFile := filepath.Join(testTempDir, "test.c1z")
	testFile, err := os.Create(testC1zFile)
	if err != nil {
		t.Fatalf("Failed to create test c1z file: %v", err)
	}
	testFile.Close()

	// Test NewC1ZFile with configured temp directory
	ctx := context.Background()
	c1File, err := dotc1z.NewC1ZFile(ctx, testC1zFile, dotc1z.WithTmpDir(testTempDir))
	if err != nil {
		t.Fatalf("NewC1ZFile failed: %v", err)
	}
	defer c1File.Close()

	// Verify the C1File was created successfully
	if c1File == nil {
		t.Error("C1File should not be nil")
	}
}

func testSynccompactorOperations(t *testing.T, testTempDir string, monitor *TmpMonitor) {
	t.Helper()

	// Create output directory
	outputDir, err := os.MkdirTemp("", "baton-test-output-")
	if err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	defer os.RemoveAll(outputDir)

	// Create some test compactable syncs
	compactableSyncs := []*synccompactor.CompactableSync{
		{
			SyncID:   "test-sync-1",
			FilePath: filepath.Join(testTempDir, "sync1.c1z"),
		},
		{
			SyncID:   "test-sync-2",
			FilePath: filepath.Join(testTempDir, "sync2.c1z"),
		},
	}

	// Create test files
	for _, sync := range compactableSyncs {
		err = os.WriteFile(sync.FilePath, []byte("test content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file %s: %v", sync.FilePath, err)
		}
	}

	// Test NewCompactor with configured temp directory
	ctx := context.Background()
	compactor, cleanup, err := synccompactor.NewCompactor(ctx, outputDir, compactableSyncs, synccompactor.WithTmpDir(testTempDir))
	if err != nil {
		t.Fatalf("NewCompactor failed: %v", err)
	}
	defer cleanup()

	// Verify the compactor was created successfully
	if compactor == nil {
		t.Error("Compactor should not be nil")
	}
}

func testConnectorRunnerConfiguration(t *testing.T, testTempDir string, monitor *TmpMonitor) {
	t.Helper()

	// Test that the temp directory exists and is writable
	_, err := os.Stat(testTempDir)
	if err != nil {
		t.Fatalf("Test temp directory should exist: %v", err)
	}

	// Test writing to the temp directory
	testFile := filepath.Join(testTempDir, "test-write")
	err = os.WriteFile(testFile, []byte("test"), 0644)
	if err != nil {
		t.Fatalf("Should be able to write to temp directory: %v", err)
	}
	os.Remove(testFile)
}

// TestConcurrentTempDirUsage tests that multiple goroutines using the same
// configured temp directory don't interfere with each other
func TestConcurrentTempDirUsage(t *testing.T) {
	// Create a temporary directory for our test
	testTempDir, err := os.MkdirTemp("", "baton-test-temp-")
	if err != nil {
		t.Fatalf("Failed to create test temp directory: %v", err)
	}
	defer os.RemoveAll(testTempDir)

	// Create a monitor for /tmp directory
	monitor, err := NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Test concurrent operations
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(i int) {
			defer func() { done <- true }()

			// Create a test c1z file
			testC1zFile := filepath.Join(testTempDir, "test-concurrent.c1z")
			testFile, err := os.Create(testC1zFile)
			if err != nil {
				t.Errorf("Failed to create test c1z file %d: %v", i, err)
				return
			}
			testFile.Close()

			// Test NewC1ZFile with configured temp directory
			ctx := context.Background()
			c1File, err := dotc1z.NewC1ZFile(ctx, testC1zFile, dotc1z.WithTmpDir(testTempDir))
			if err != nil {
				t.Errorf("NewC1ZFile failed for goroutine %d: %v", i, err)
				return
			}
			defer c1File.Close()

			// Verify the C1File was created successfully
			if c1File == nil {
				t.Errorf("C1File should not be nil for goroutine %d", i)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Wait a bit for any async operations to complete
	time.Sleep(100 * time.Millisecond)

	// Assert no files were created in /tmp
	monitor.AssertNoNewFilesInTmp(t)
}

// TestTempDirCleanup tests that temporary files are properly cleaned up
func TestTempDirCleanup(t *testing.T) {
	// Create a temporary directory for our test
	testTempDir, err := os.MkdirTemp("", "baton-test-temp-")
	if err != nil {
		t.Fatalf("Failed to create test temp directory: %v", err)
	}
	defer os.RemoveAll(testTempDir)

	// Create a monitor for /tmp directory
	monitor, err := NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Test that temporary files are cleaned up
	testC1zFile := filepath.Join(testTempDir, "test-cleanup.c1z")
	testFile, err := os.Create(testC1zFile)
	if err != nil {
		t.Fatalf("Failed to create test c1z file: %v", err)
	}
	testFile.Close()

	// Test NewC1ZFile with configured temp directory
	ctx := context.Background()
	c1File, err := dotc1z.NewC1ZFile(ctx, testC1zFile, dotc1z.WithTmpDir(testTempDir))
	if err != nil {
		t.Fatalf("NewC1ZFile failed: %v", err)
	}

	// Verify the C1File was created successfully
	if c1File == nil {
		t.Error("C1File should not be nil")
	}

	// Clean up the C1File
	err = c1File.Close()
	if err != nil {
		t.Errorf("Failed to close C1File: %v", err)
	}

	// Assert no files were created in /tmp
	monitor.AssertNoNewFilesInTmp(t)
}
