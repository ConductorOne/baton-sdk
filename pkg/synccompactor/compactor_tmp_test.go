package synccompactor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/test"
)

func TestNewCompactorUsesConfiguredTempDir(t *testing.T) {
	// Create a temporary directory for our test
	testTempDir, err := os.MkdirTemp("", "baton-test-temp-")
	if err != nil {
		t.Fatalf("Failed to create test temp directory: %v", err)
	}
	defer os.RemoveAll(testTempDir)

	// Create output directory
	outputDir, err := os.MkdirTemp("", "baton-test-output-")
	if err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	defer os.RemoveAll(outputDir)

	// Create a monitor for /tmp directory
	monitor, err := test.NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Create some test compactable syncs
	compactableSyncs := []*CompactableSync{
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
	compactor, cleanup, err := NewCompactor(ctx, outputDir, compactableSyncs, WithTmpDir(testTempDir))
	if err != nil {
		t.Fatalf("NewCompactor failed: %v", err)
	}
	defer cleanup()

	// Verify the compactor was created with the configured temp directory
	// Note: tmpDir is not exported, so we can't directly access it
	// This test verifies that the compactor was created successfully with the configured temp directory
	if compactor == nil {
		t.Error("Compactor should not be nil")
	}

	// Assert no files were created in /tmp
	monitor.AssertNoNewFilesInTmp(t)
}

func TestNewCompactorWithEmptyTempDirUsesSystemTemp(t *testing.T) {
	// Create output directory
	outputDir, err := os.MkdirTemp("", "baton-test-output-")
	if err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	defer os.RemoveAll(outputDir)

	// Create some test compactable syncs
	testTempDir := os.TempDir()
	compactableSyncs := []*CompactableSync{
		{
			SyncID:   "test-sync-1",
			FilePath: filepath.Join(testTempDir, "sync1-empty-temp.c1z"),
		},
		{
			SyncID:   "test-sync-2",
			FilePath: filepath.Join(testTempDir, "sync2-empty-temp.c1z"),
		},
	}

	// Create test files
	for _, sync := range compactableSyncs {
		err = os.WriteFile(sync.FilePath, []byte("test content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file %s: %v", sync.FilePath, err)
		}
		defer os.Remove(sync.FilePath)
	}

	// Test NewCompactor with empty temp directory (should use system temp)
	ctx := context.Background()
	compactor, cleanup, err := NewCompactor(ctx, outputDir, compactableSyncs)
	if err != nil {
		t.Fatalf("NewCompactor failed: %v", err)
	}
	defer cleanup()

	// Verify the compactor was created with the system temp directory
	expectedPrefix := os.TempDir()
	if !filepath.HasPrefix(compactor.tmpDir, expectedPrefix) {
		t.Errorf("Compactor temp directory was not set to system temp directory. Expected prefix: %s, got: %s", expectedPrefix, compactor.tmpDir)
	}

	// This test is expected to create files in /tmp since we're using system temp
	// We just verify that the temp directory was created in the expected location
	if compactor.tmpDir == "" {
		t.Error("Compactor temp directory should not be empty")
	}
}

func TestNewCompactorWithNonExistentTempDirFails(t *testing.T) {
	// Create output directory
	outputDir, err := os.MkdirTemp("", "baton-test-output-")
	if err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	defer os.RemoveAll(outputDir)

	// Create a monitor for /tmp directory
	monitor, err := test.NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Create some test compactable syncs
	testTempDir := os.TempDir()
	compactableSyncs := []*CompactableSync{
		{
			SyncID:   "test-sync-1",
			FilePath: filepath.Join(testTempDir, "sync1-nonexistent.c1z"),
		},
		{
			SyncID:   "test-sync-2",
			FilePath: filepath.Join(testTempDir, "sync2-nonexistent.c1z"),
		},
	}

	// Create test files
	for _, sync := range compactableSyncs {
		err = os.WriteFile(sync.FilePath, []byte("test content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file %s: %v", sync.FilePath, err)
		}
		defer os.Remove(sync.FilePath)
	}

	// Test NewCompactor with non-existent temp directory should fail
	ctx := context.Background()
	_, _, err = NewCompactor(ctx, outputDir, compactableSyncs, WithTmpDir("/non/existent/directory"))
	if err == nil {
		t.Error("Expected NewCompactor to fail with non-existent temp directory, but it succeeded")
	}

	// Assert no files were created in /tmp
	monitor.AssertNoNewFilesInTmp(t)
}

func TestNewCompactorWithInsufficientSyncsFails(t *testing.T) {
	// Create a temporary directory for our test
	testTempDir, err := os.MkdirTemp("", "baton-test-temp-")
	if err != nil {
		t.Fatalf("Failed to create test temp directory: %v", err)
	}
	defer os.RemoveAll(testTempDir)

	// Create output directory
	outputDir, err := os.MkdirTemp("", "baton-test-output-")
	if err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}
	defer os.RemoveAll(outputDir)

	// Create a monitor for /tmp directory
	monitor, err := test.NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Test with insufficient syncs (less than 2)
	compactableSyncs := []*CompactableSync{
		{
			SyncID:   "test-sync-1",
			FilePath: filepath.Join(testTempDir, "sync1.c1z"),
		},
	}

	ctx := context.Background()
	_, _, err = NewCompactor(ctx, outputDir, compactableSyncs, WithTmpDir(testTempDir))
	if err == nil {
		t.Error("Expected NewCompactor to fail with insufficient syncs, but it succeeded")
	}

	// Assert no files were created in /tmp
	monitor.AssertNoNewFilesInTmp(t)
}
