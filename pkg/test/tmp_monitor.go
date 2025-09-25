package test

import (
	"os"
	"sort"
	"testing"
	"time"
)

// TmpMonitor helps monitor file creation in the /tmp directory during tests
type TmpMonitor struct {
	initialFiles map[string]os.FileInfo
	tmpDir       string
}

// NewTmpMonitor creates a new monitor for the /tmp directory
func NewTmpMonitor() (*TmpMonitor, error) {
	tmpDir := os.TempDir()

	// Get initial list of files in /tmp
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		return nil, err
	}

	initialFiles := make(map[string]os.FileInfo)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		initialFiles[entry.Name()] = info
	}

	return &TmpMonitor{
		initialFiles: initialFiles,
		tmpDir:       tmpDir,
	}, nil
}

// CheckForNewFiles checks if any new files were created in /tmp and returns them
func (m *TmpMonitor) CheckForNewFiles() ([]string, error) {
	entries, err := os.ReadDir(m.tmpDir)
	if err != nil {
		return nil, err
	}

	var newFiles []string
	for _, entry := range entries {
		if _, exists := m.initialFiles[entry.Name()]; !exists {
			newFiles = append(newFiles, entry.Name())
		}
	}

	sort.Strings(newFiles)
	return newFiles, nil
}

// AssertNoNewFilesInTmp asserts that no new files were created in /tmp
func (m *TmpMonitor) AssertNoNewFilesInTmp(t *testing.T) {
	t.Helper()

	newFiles, err := m.CheckForNewFiles()
	if err != nil {
		t.Fatalf("Failed to check for new files in /tmp: %v", err)
	}

	if len(newFiles) > 0 {
		t.Errorf("Found %d new files created in /tmp directory: %v", len(newFiles), newFiles)
		t.Logf("This indicates that the code is not properly using the configured temporary directory")
		t.Logf("Expected all temporary files to be created in the configured temp directory, not /tmp")
	}
}

// WaitForFilesAndAssert waits for a short period and then asserts no new files in /tmp
// This is useful for async operations that might create files
func (m *TmpMonitor) WaitForFilesAndAssert(t *testing.T, waitTime time.Duration) {
	t.Helper()

	time.Sleep(waitTime)
	m.AssertNoNewFilesInTmp(t)
}

// GetTmpDir returns the monitored /tmp directory path
func (m *TmpMonitor) GetTmpDir() string {
	return m.tmpDir
}
