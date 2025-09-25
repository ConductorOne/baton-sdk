package dotc1z

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/test"
)

func TestLoadC1zUsesConfiguredTempDir(t *testing.T) {
	// Create a temporary directory for our test
	testTempDir, err := os.MkdirTemp("", "baton-test-temp-")
	if err != nil {
		t.Fatalf("Failed to create test temp directory: %v", err)
	}
	defer os.RemoveAll(testTempDir)

	// Create a monitor for /tmp directory
	monitor, err := test.NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Create a test c1z file
	testC1zFile := filepath.Join(testTempDir, "test.c1z")
	testFile, err := os.Create(testC1zFile)
	if err != nil {
		t.Fatalf("Failed to create test c1z file: %v", err)
	}
	testFile.Close()

	// Test loadC1z with configured temp directory
	dbPath, err := loadC1z(testC1zFile, testTempDir)
	if err != nil {
		t.Fatalf("loadC1z failed: %v", err)
	}
	defer os.RemoveAll(filepath.Dir(dbPath))

	// Verify the database file was created in the configured temp directory
	if !filepath.HasPrefix(dbPath, testTempDir) {
		t.Errorf("Database file was not created in configured temp directory. Expected prefix: %s, got: %s", testTempDir, dbPath)
	}

	// Assert no files were created in /tmp
	monitor.AssertNoNewFilesInTmp(t)
}

func TestLoadC1zWithEmptyTempDirUsesSystemTemp(t *testing.T) {
	// Create a test c1z file
	testC1zFile := filepath.Join(os.TempDir(), "test-empty-temp.c1z")
	_, err := os.Create(testC1zFile)
	if err != nil {
		t.Fatalf("Failed to create test c1z file: %v", err)
	}
	defer os.Remove(testC1zFile)

	// Test loadC1z with empty temp directory (should use system temp)
	dbPath, err := loadC1z(testC1zFile, "")
	if err != nil {
		t.Fatalf("loadC1z failed: %v", err)
	}
	defer os.RemoveAll(filepath.Dir(dbPath))

	// Verify the database file was created in the system temp directory
	expectedPrefix := os.TempDir()
	if !filepath.HasPrefix(dbPath, expectedPrefix) {
		t.Errorf("Database file was not created in system temp directory. Expected prefix: %s, got: %s", expectedPrefix, dbPath)
	}

	// This test is expected to create files in /tmp since we're using empty temp dir
	// We just verify that the file was created in the expected location
	if dbPath == "" {
		t.Error("Database path should not be empty")
	}
}

func TestLoadC1zWithNonExistentTempDirFails(t *testing.T) {
	// Create a monitor for /tmp directory
	monitor, err := test.NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Create a test c1z file
	testC1zFile := filepath.Join(os.TempDir(), "test-nonexistent.c1z")
	_, err = os.Create(testC1zFile)
	if err != nil {
		t.Fatalf("Failed to create test c1z file: %v", err)
	}
	defer os.Remove(testC1zFile)

	// Test loadC1z with non-existent temp directory
	nonExistentDir := "/non/existent/directory"
	_, err = loadC1z(testC1zFile, nonExistentDir)
	if err == nil {
		t.Error("Expected loadC1z to fail with non-existent temp directory, but it succeeded")
	}

	// Assert no files were created in /tmp
	monitor.AssertNoNewFilesInTmp(t)
}
