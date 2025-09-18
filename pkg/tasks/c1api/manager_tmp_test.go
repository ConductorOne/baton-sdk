package c1api

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/test"
)

func TestC1ApiTaskManagerWithConfiguredTempDir(t *testing.T) {
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

	// Create a mock task manager with configured temp directory
	manager := &c1ApiTaskManager{
		tempDir: testTempDir,
	}

	// Test GetTempDir method
	tempDir := manager.GetTempDir()
	if tempDir != testTempDir {
		t.Errorf("Expected temp directory to be %s, got %s", testTempDir, tempDir)
	}

	// Assert no files were created in /tmp during manager creation
	monitor.AssertNoNewFilesInTmp(t)
}

func TestC1ApiTaskManagerWithEmptyTempDir(t *testing.T) {
	// Create a monitor for /tmp directory
	monitor, err := test.NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Create a mock task manager with empty temp directory
	manager := &c1ApiTaskManager{
		tempDir: "", // Empty temp dir should use system temp
	}

	// Test GetTempDir method
	tempDir := manager.GetTempDir()
	if tempDir != "" {
		t.Errorf("Expected temp directory to be empty, got %s", tempDir)
	}

	// Assert no files were created in /tmp during manager creation
	monitor.AssertNoNewFilesInTmp(t)
}

func TestC1ApiTaskManagerWithSystemTempDir(t *testing.T) {
	// Create a monitor for /tmp directory
	monitor, err := test.NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Create a mock task manager with system temp directory
	systemTempDir := os.TempDir()
	manager := &c1ApiTaskManager{
		tempDir: systemTempDir,
	}

	// Test GetTempDir method
	tempDir := manager.GetTempDir()
	if tempDir != systemTempDir {
		t.Errorf("Expected temp directory to be %s, got %s", systemTempDir, tempDir)
	}

	// Assert no files were created in /tmp during manager creation
	monitor.AssertNoNewFilesInTmp(t)
}

func TestNewC1TaskManagerWithTempDir(t *testing.T) {
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

	// Test NewC1TaskManager with configured temp directory
	ctx := context.Background()
	manager, err := NewC1TaskManager(ctx, "test-client-id", "test-client-secret", testTempDir, false, "", "", nil)
	if err != nil {
		t.Fatalf("NewC1TaskManager failed: %v", err)
	}

	// Verify the temp directory was set correctly
	if manager.GetTempDir() != testTempDir {
		t.Errorf("Expected temp directory to be %s, got %s", testTempDir, manager.GetTempDir())
	}

	// Assert no files were created in /tmp during manager creation
	monitor.AssertNoNewFilesInTmp(t)
}

func TestNewC1TaskManagerWithEmptyTempDir(t *testing.T) {
	// Create a monitor for /tmp directory
	monitor, err := test.NewTmpMonitor()
	if err != nil {
		t.Fatalf("Failed to create tmp monitor: %v", err)
	}

	// Test NewC1TaskManager with empty temp directory
	ctx := context.Background()
	manager, err := NewC1TaskManager(ctx, "test-client-id", "test-client-secret", "", false, "", "", nil)
	if err != nil {
		t.Fatalf("NewC1TaskManager failed: %v", err)
	}

	// Verify the temp directory was set correctly
	if manager.GetTempDir() != "" {
		t.Errorf("Expected temp directory to be empty, got %s", manager.GetTempDir())
	}

	// Assert no files were created in /tmp during manager creation
	monitor.AssertNoNewFilesInTmp(t)
}

func TestC1ApiTaskManagerTempDirConsistency(t *testing.T) {
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

	// Create a mock task manager
	manager := &c1ApiTaskManager{
		tempDir: testTempDir,
	}

	// Test that GetTempDir returns the same value multiple times
	tempDir1 := manager.GetTempDir()
	tempDir2 := manager.GetTempDir()

	if tempDir1 != tempDir2 {
		t.Errorf("GetTempDir should return consistent values. First call: %s, second call: %s", tempDir1, tempDir2)
	}

	if tempDir1 != testTempDir {
		t.Errorf("Expected temp directory to be %s, got %s", testTempDir, tempDir1)
	}

	// Assert no files were created in /tmp during consistency test
	monitor.AssertNoNewFilesInTmp(t)
}

func TestC1ApiTaskManagerTempDirValidation(t *testing.T) {
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

	// Create manager with validated temp directory
	manager := &c1ApiTaskManager{
		tempDir: testTempDir,
	}

	// Verify the temp directory is correctly set
	if manager.GetTempDir() != testTempDir {
		t.Errorf("Expected temp directory to be %s, got %s", testTempDir, manager.GetTempDir())
	}

	// Assert no files were created in /tmp during validation
	monitor.AssertNoNewFilesInTmp(t)
}
