#!/bin/bash

# Test script that only shows failed tests
# Usage: ./test_tmp_failures_only.sh

echo "🧪 Running baton-sdk temp directory tests (failures only)..."
echo "=========================================================="

# Run tests and capture output
output=$(go test -v ./... -run ".*[Tt]mp.*" 2>&1)

# Check if there were any failures
if echo "$output" | grep -q "FAIL"; then
    echo "❌ FAILED TESTS FOUND:"
    echo "====================="
    echo "$output" | grep -A 5 -B 5 "FAIL"
    exit 1
else
    echo "✅ All temp directory tests passed!"
    exit 0
fi
