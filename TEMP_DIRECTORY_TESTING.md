# Temporary Directory Testing

This document describes the comprehensive testing approach implemented to ensure that the baton-sdk does not write files to the `/tmp` directory and instead uses the configured fast disk mount.

## Problem Statement

The baton-sdk was writing files to the `/tmp` directory instead of using the configured fast disk mount (e.g., `/c1-tenant-datastore`). This testing suite ensures that all temporary file operations use the properly configured temporary directory.

## Testing Strategy

### 1. TmpMonitor Utility

The `pkg/test/tmp_monitor.go` file provides a utility to monitor file creation in the `/tmp` directory during tests:

```go
// Create a monitor for /tmp directory
monitor, err := test.NewTmpMonitor()
if err != nil {
    t.Fatalf("Failed to create tmp monitor: %v", err)
}

// Run your test operations...

// Assert no files were created in /tmp
monitor.AssertNoNewFilesInTmp(t)
```

### 2. Test Coverage

The following components are tested to ensure they use the configured temporary directory:

#### A. dotc1z Package (`pkg/dotc1z/file_tmp_test.go`)
- Tests the `loadC1z` function to ensure it uses the configured temp directory
- Verifies that empty temp directory falls back to system temp
- Ensures non-existent temp directories fail appropriately

#### B. dotc1z Manager (`pkg/dotc1z/manager/local/local_tmp_test.go`)
- Tests the `copyFileToTmp` function in the local manager
- Verifies temp directory configuration through options
- Ensures proper error handling for invalid temp directories

#### C. Sync Compactor (`pkg/synccompactor/compactor_tmp_test.go`)
- Tests the `NewCompactor` function with temp directory configuration
- Verifies that compactable syncs use the configured temp directory
- Ensures proper cleanup of temporary files

#### D. Connector Runner (`pkg/connectorrunner/runner_tmp_test.go`)
- Tests the `WithTempDir` option configuration
- Verifies temp directory validation
- Ensures proper configuration handling

#### E. C1API Task Manager (`pkg/tasks/c1api/manager_tmp_test.go`)
- Tests the `GetTempDir` method
- Verifies temp directory consistency
- Ensures proper configuration passing

#### F. Integration Tests (`pkg/test/integration_tmp_test.go`)
- Tests environment variable `TMPDIR` configuration
- Tests the `c1z-temp-dir` field configuration
- Tests concurrent operations
- Tests cleanup procedures

### 3. Configuration Methods

The tests verify that temporary directories can be configured through:

1. **Environment Variable**: `TMPDIR` environment variable
2. **CLI Field**: `c1z-temp-dir` field in the configuration
3. **Programmatic Options**: Various `WithTmpDir` options in different packages

### 4. Test Execution

#### Running Individual Tests

```bash
# Run all temp directory tests
go test -v ./pkg/test/... -run "TestTmpDir"

# Run specific component tests
go test -v ./pkg/dotc1z/... -run "TestLoadC1z"
go test -v ./pkg/synccompactor/... -run "TestNewCompactor"
```

#### Running the Test Suite

```bash
# Run the comprehensive test script
./test_tmp_usage.sh
```

#### Manual Verification

To manually verify that no files are created in `/tmp`:

1. Set up a monitor directory:
   ```bash
   export TMPDIR=/c1-tenant-datastore/temp
   ```

2. Run your baton-sdk operations

3. Check `/tmp` for new files:
   ```bash
   find /tmp -type f -newer /c1-tenant-datastore/temp
   ```

### 5. Key Test Patterns

#### Pattern 1: Configured Temp Directory
```go
func TestUsesConfiguredTempDir(t *testing.T) {
    // Create test temp directory
    testTempDir, err := os.MkdirTemp("", "baton-test-temp-")
    if err != nil {
        t.Fatalf("Failed to create test temp directory: %v", err)
    }
    defer os.RemoveAll(testTempDir)

    // Create monitor
    monitor, err := test.NewTmpMonitor()
    if err != nil {
        t.Fatalf("Failed to create tmp monitor: %v", err)
    }

    // Test with configured temp directory
    // ... test operations ...

    // Assert no files in /tmp
    monitor.AssertNoNewFilesInTmp(t)
}
```

#### Pattern 2: Environment Variable Testing
```go
func TestTmpDirEnvironmentVariable(t *testing.T) {
    // Set TMPDIR environment variable
    originalTmpDir := os.Getenv("TMPDIR")
    defer os.Setenv("TMPDIR", originalTmpDir)
    
    err := os.Setenv("TMPDIR", testTempDir)
    if err != nil {
        t.Fatalf("Failed to set TMPDIR: %v", err)
    }

    // Test operations should use the new TMPDIR
    // ... test operations ...
}
```

#### Pattern 3: Concurrent Operations
```go
func TestConcurrentTempDirUsage(t *testing.T) {
    // Test multiple goroutines using the same temp directory
    // Ensure no race conditions or /tmp usage
    // ... concurrent test operations ...
}
```

### 6. Expected Behavior

#### ✅ Correct Behavior
- All temporary files created in the configured temp directory
- No files created in `/tmp` when temp directory is configured
- Proper fallback to system temp when no temp directory is configured
- Proper error handling for invalid temp directories
- Cleanup of temporary files after operations

#### ❌ Incorrect Behavior
- Files created in `/tmp` when temp directory is configured
- Using system temp when temp directory is explicitly configured
- Silent failures when temp directory is invalid
- Temporary files not cleaned up after operations

### 7. Integration with CI/CD

These tests should be run in your CI/CD pipeline to ensure that:

1. New code changes don't introduce `/tmp` usage
2. Configuration changes work correctly
3. Environment variable handling is robust
4. Concurrent operations are safe

### 8. Troubleshooting

If tests fail with files being created in `/tmp`:

1. Check that the `c1z-temp-dir` field is being set correctly
2. Verify that the `TMPDIR` environment variable is being respected
3. Ensure that all `os.MkdirTemp` and `os.CreateTemp` calls use the configured directory
4. Check that the `os.TempDir()` function is not being used when a temp directory is configured

### 9. Future Enhancements

- Add performance benchmarks for temp directory operations
- Add tests for edge cases (disk full, permissions, etc.)
- Add monitoring for temp directory usage in production
- Add metrics for temp directory operations

## Conclusion

This comprehensive testing suite ensures that the baton-sdk properly uses the configured temporary directory and never writes files to `/tmp` when a fast disk mount is available. The tests cover all major components and provide both unit-level and integration-level verification.
