package local

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/test"
)

func TestCopyFileToTmpUsesConfiguredTempDir(t *testing.T) {
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

	// Create a test file
	testFile := filepath.Join(testTempDir, "test-file.c1z")
	testFileContent := []byte("test content")
	err = os.WriteFile(testFile, testFileContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create localManager with configured temp directory
	manager := &localManager{
		filePath: testFile,
		tmpDir:   testTempDir,
	}

	// Test copyFileToTmp
	ctx := context.Background()
	err = manager.copyFileToTmp(ctx)
	if err != nil {
		t.Fatalf("copyFileToTmp failed: %v", err)
	}

	// Verify the temp file was created in the configured temp directory
	if !filepath.HasPrefix(manager.tmpPath, testTempDir) {
		t.Errorf("Temp file was not created in configured temp directory. Expected prefix: %s, got: %s", testTempDir, manager.tmpPath)
	}

	// Verify the temp file exists and has correct content
	content, err := os.ReadFile(manager.tmpPath)
	if err != nil {
		t.Fatalf("Failed to read temp file: %v", err)
	}
	if string(content) != string(testFileContent) {
		t.Errorf("Temp file content mismatch. Expected: %s, got: %s", string(testFileContent), string(content))
	}

	// Clean up
	os.Remove(manager.tmpPath)

	// Assert no files were created in /tmp
	monitor.AssertNoNewFilesInTmp(t)
}

func TestCopyFileToTmpWithEmptyTempDirUsesSystemTemp(t *testing.T) {

	// Create a test file
	testFile := filepath.Join(os.TempDir(), "test-file-empty-temp.c1z")
	testFileContent := []byte("test content")
	err := os.WriteFile(testFile, testFileContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(testFile)

	// Create localManager with empty temp directory (should use system temp)
	manager := &localManager{
		filePath: testFile,
		tmpDir:   "", // Empty temp dir should use system temp
	}

	// Test copyFileToTmp
	ctx := context.Background()
	err = manager.copyFileToTmp(ctx)
	if err != nil {
		t.Fatalf("copyFileToTmp failed: %v", err)
	}
	defer os.Remove(manager.tmpPath)

	// Verify the temp file was created in the system temp directory
	expectedPrefix := os.TempDir()
	if !filepath.HasPrefix(manager.tmpPath, expectedPrefix) {
		t.Errorf("Temp file was not created in system temp directory. Expected prefix: %s, got: %s", expectedPrefix, manager.tmpPath)
	}

	// This test is expected to create files in /tmp since we're using empty temp dir
	// We just verify that the file was created in the expected location
	if manager.tmpPath == "" {
		t.Error("Temp path should not be empty")
	}
}

func TestCopyFileToTmpWithNonExistentTempDirFails(t *testing.T) {
	// Create a monitor for /tmp directory
	monitor, err := test.NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Create a test file
	testFile := filepath.Join(os.TempDir(), "test-file-nonexistent.c1z")
	testFileContent := []byte("test content")
	err = os.WriteFile(testFile, testFileContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(testFile)

	// Create localManager with non-existent temp directory
	manager := &localManager{
		filePath: testFile,
		tmpDir:   "/non/existent/directory",
	}

	// Test copyFileToTmp should fail
	ctx := context.Background()
	err = manager.copyFileToTmp(ctx)
	if err == nil {
		t.Error("Expected copyFileToTmp to fail with non-existent temp directory, but it succeeded")
	}

	// Assert no files were created in /tmp
	monitor.AssertNoNewFilesInTmp(t)
}

func TestNewLocalManagerWithTmpDirOption(t *testing.T) {
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

	// Create a test file
	testFile := filepath.Join(testTempDir, "test-file.c1z")
	testFileContent := []byte("test content")
	err = os.WriteFile(testFile, testFileContent, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create localManager using New with WithTmpDir option
	ctx := context.Background()
	manager, err := New(ctx, testFile, WithTmpDir(testTempDir))
	if err != nil {
		t.Fatalf("Failed to create local manager: %v", err)
	}

	// Test copyFileToTmp
	err = manager.copyFileToTmp(ctx)
	if err != nil {
		t.Fatalf("copyFileToTmp failed: %v", err)
	}
	defer os.Remove(manager.tmpPath)

	// Verify the temp file was created in the configured temp directory
	if !filepath.HasPrefix(manager.tmpPath, testTempDir) {
		t.Errorf("Temp file was not created in configured temp directory. Expected prefix: %s, got: %s", testTempDir, manager.tmpPath)
	}

	// Assert no files were created in /tmp
	monitor.AssertNoNewFilesInTmp(t)
}
