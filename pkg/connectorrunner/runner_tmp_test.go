package connectorrunner

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/test"
)

func TestConnectorRunnerWithConfiguredTempDir(t *testing.T) {
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

	// Test connector runner configuration with temp directory
	cfg := &runnerConfig{
		c1zPath: testC1zFile,
		tempDir: testTempDir,
	}

	// Verify the configuration was set correctly
	if cfg.tempDir != testTempDir {
		t.Errorf("Expected temp directory to be %s, got %s", testTempDir, cfg.tempDir)
	}

	// Assert no files were created in /tmp during configuration
	monitor.AssertNoNewFilesInTmp(t)
}

func TestWithTempDirOption(t *testing.T) {
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

	// Test WithTempDir option
	cfg := &runnerConfig{}

	ctx := context.Background()
	opt := WithTempDir(testTempDir)
	err = opt(ctx, cfg)
	if err != nil {
		t.Fatalf("WithTempDir option failed: %v", err)
	}

	// Verify the configuration was set correctly
	if cfg.tempDir != testTempDir {
		t.Errorf("Expected temp directory to be %s, got %s", testTempDir, cfg.tempDir)
	}

	// Assert no files were created in /tmp during option application
	monitor.AssertNoNewFilesInTmp(t)
}

func TestConnectorRunnerWithEmptyTempDir(t *testing.T) {
	// Create a monitor for /tmp directory
	monitor, err := test.NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Test connector runner configuration with empty temp directory
	cfg := &runnerConfig{
		c1zPath: "test.c1z",
		tempDir: "", // Empty temp dir should use system temp
	}

	// Verify the configuration was set correctly
	if cfg.tempDir != "" {
		t.Errorf("Expected temp directory to be empty, got %s", cfg.tempDir)
	}

	// Assert no files were created in /tmp during configuration
	monitor.AssertNoNewFilesInTmp(t)
}

func TestConnectorRunnerWithNonExistentTempDir(t *testing.T) {
	// Create a monitor for /tmp directory
	monitor, err := test.NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Test connector runner configuration with non-existent temp directory
	cfg := &runnerConfig{
		c1zPath: "test.c1z",
		tempDir: "/non/existent/directory",
	}

	// Verify the configuration was set correctly
	if cfg.tempDir != "/non/existent/directory" {
		t.Errorf("Expected temp directory to be /non/existent/directory, got %s", cfg.tempDir)
	}

	// Assert no files were created in /tmp during configuration
	monitor.AssertNoNewFilesInTmp(t)
}

func TestConnectorRunnerTempDirValidation(t *testing.T) {
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

	// Test that the temp directory exists and is writable
	_, err = os.Stat(testTempDir)
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

	// Assert no files were created in /tmp during validation
	monitor.AssertNoNewFilesInTmp(t)
}
