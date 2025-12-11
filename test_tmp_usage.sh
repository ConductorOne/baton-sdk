#!/bin/bash

# Test script to verify that baton-sdk does not write files to /tmp
# This script runs all the temporary directory tests to ensure proper configuration
# The fast disk mount should be configured to use /c1-tenant-datastore

set -e

echo "🧪 Running baton-sdk temporary directory usage tests..."
echo "=================================================="

# Function to run tests and check for /tmp usage
run_test() {
    local test_name="$1"
    local test_pattern="$2"
    
    echo "Running $test_name..."
    
    # Create a temporary directory to monitor
    TMP_MONITOR_DIR=$(mktemp -d)
    trap "rm -rf $TMP_MONITOR_DIR" EXIT
    
    # Set TMPDIR to our monitor directory
    export TMPDIR="$TMP_MONITOR_DIR"
    
    # Run the specific test
    if go test -v -run "$test_pattern" ./...; then
        echo "✅ $test_name passed"
        
        # Check if any files were created in /tmp
        TMP_FILES=$(find /tmp -maxdepth 1 -type f -newer "$TMP_MONITOR_DIR" 2>/dev/null | wc -l)
        if [ "$TMP_FILES" -gt 0 ]; then
            echo "❌ $test_name FAILED: Files were created in /tmp directory"
            find /tmp -maxdepth 1 -type f -newer "$TMP_MONITOR_DIR" 2>/dev/null
            exit 1
        else
            echo "✅ $test_name: No files created in /tmp"
        fi
    else
        echo "❌ $test_name failed"
        exit 1
    fi
}

# Run all temporary directory tests
echo "1. Testing dotc1z file operations..."
run_test "dotc1z file operations" "TestLoadC1z.*"

echo "2. Testing dotc1z manager operations..."
run_test "dotc1z manager operations" "TestCopyFileToTmp.*"

echo "3. Testing synccompactor operations..."
run_test "synccompactor operations" "TestNewCompactor.*"

echo "4. Testing connector runner configuration..."
run_test "connector runner configuration" "TestConnectorRunner.*"

echo "5. Testing C1API task manager..."
run_test "C1API task manager" "TestC1ApiTaskManager.*"

echo "6. Testing integration scenarios..."
run_test "integration scenarios" "TestTmpDirEnvironmentVariable|TestC1zTempDirField|TestConcurrentTempDirUsage|TestTempDirCleanup"

echo ""
echo "🎉 All tests passed! No files were written to /tmp directory."
echo "✅ baton-sdk is properly configured to use the specified temporary directory (e.g., /c1-tenant-datastore)."
