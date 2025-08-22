package pebble

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestObservabilityManager(t *testing.T) {
	ctx := context.Background()
	
	tempDir, err := os.MkdirTemp("", "pebble_observability_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)
	
	// Create engine with observability enabled
	logger := &DefaultLogger{}
	pe, err := NewPebbleEngine(ctx, tempDir,
		WithMetricsEnabled(true),
		WithLogger(logger),
		WithMetricsInterval(100*time.Millisecond),
	)
	require.NoError(t, err)
	defer pe.Close()
	
	t.Run("ObservabilityManager", func(t *testing.T) {
		testObservabilityManager(t, ctx, pe)
	})
	
	t.Run("SystemStats", func(t *testing.T) {
		testSystemStats(t, ctx, pe)
	})
	
	t.Run("HealthStatus", func(t *testing.T) {
		testHealthStatus(t, ctx, pe)
	})
	
	t.Run("EventLogging", func(t *testing.T) {
		testEventLogging(t, ctx, pe)
	})
}

func testObservabilityManager(t *testing.T, ctx context.Context, pe *PebbleEngine) {
	om := pe.GetObservabilityManager()
	require.NotNil(t, om)
	
	// Record some operations
	om.RecordOperation("test_operation", 50*time.Millisecond, nil)
	om.RecordOperation("test_operation", 150*time.Millisecond, nil)
	om.RecordOperation("failed_operation", 25*time.Millisecond, 
		&TestError{Message: "test error"})
	
	// Record a slow query
	om.RecordSlowQuery("slow_list", 2*time.Second, map[string]interface{}{
		"table": "resources",
		"count": 10000,
	})
}

func testSystemStats(t *testing.T, ctx context.Context, pe *PebbleEngine) {
	stats, err := pe.GetSystemStats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	
	// Check expected fields
	require.Contains(t, stats, "uptime_seconds")
	require.Contains(t, stats, "database_path")
	
	// Uptime should be positive
	uptime, ok := stats["uptime_seconds"].(float64)
	require.True(t, ok)
	require.Greater(t, uptime, 0.0)
	
	// Database path should match
	dbPath, ok := stats["database_path"].(string)
	require.True(t, ok)
	require.Contains(t, dbPath, "pebble.db")
}

func testHealthStatus(t *testing.T, ctx context.Context, pe *PebbleEngine) {
	health, err := pe.GetHealthStatus(ctx)
	require.NoError(t, err)
	require.NotNil(t, health)
	
	// Check required fields
	require.Contains(t, health, "status")
	require.Contains(t, health, "timestamp")
	require.Contains(t, health, "issues")
	
	// Status should be healthy for a new engine
	status, ok := health["status"].(string)
	require.True(t, ok)
	require.Contains(t, []string{"healthy", "degraded"}, status)
	
	// Timestamp should be recent
	timestamp, ok := health["timestamp"].(int64)
	require.True(t, ok)
	require.Greater(t, timestamp, time.Now().Unix()-10) // Within last 10 seconds
	
	// Issues should be a slice
	issues, ok := health["issues"].([]string)
	require.True(t, ok)
	_ = issues // Issues may or may not be empty for a new engine
}

func testEventLogging(t *testing.T, ctx context.Context, pe *PebbleEngine) {
	om := pe.GetObservabilityManager()
	require.NotNil(t, om)
	
	// Log some database events
	om.LogDatabaseEvent("sync_started", map[string]interface{}{
		"sync_id":    "test-sync-123",
		"sync_type":  "full",
		"items":      1000,
	})
	
	om.LogDatabaseEvent("sync_completed", map[string]interface{}{
		"sync_id":     "test-sync-123",
		"duration_ms": 5000,
		"items":       1000,
	})
	
	om.LogDatabaseEvent("compaction_triggered", map[string]interface{}{
		"reason":      "manual",
		"size_before": 1024*1024*100, // 100MB
	})
}

func TestObservabilityWithoutLogger(t *testing.T) {
	ctx := context.Background()
	
	tempDir, err := os.MkdirTemp("", "pebble_no_logger_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)
	
	// Create engine without logger (should use NoOpLogger)
	pe, err := NewPebbleEngine(ctx, tempDir,
		WithMetricsEnabled(false),
	)
	require.NoError(t, err)
	defer pe.Close()
	
	// Should still work without errors
	om := pe.GetObservabilityManager()
	require.NotNil(t, om)
	
	om.RecordOperation("test", 10*time.Millisecond, nil)
	
	stats, err := pe.GetSystemStats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	
	health, err := pe.GetHealthStatus(ctx)
	require.NoError(t, err)
	require.NotNil(t, health)
}

func TestLoggerImplementations(t *testing.T) {
	// Test DefaultLogger doesn't panic
	logger := &DefaultLogger{}
	logger.Debug("debug message", LogField{"key", "value"})
	logger.Info("info message", LogField{"count", 42})
	logger.Warn("warn message", LogField{"warning", true})
	logger.Error("error message", LogField{"error", "test error"})
	
	// Test NoOpLogger doesn't panic
	noopLogger := &NoOpLogger{}
	noopLogger.Debug("debug message", LogField{"key", "value"})
	noopLogger.Info("info message", LogField{"count", 42})
	noopLogger.Warn("warn message", LogField{"warning", true})
	noopLogger.Error("error message", LogField{"error", "test error"})
}

// TestError is a simple error implementation for testing
type TestError struct {
	Message string
}

func (e *TestError) Error() string {
	return e.Message
}

func TestPeriodicReporting(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping periodic reporting test in short mode")
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	
	tempDir, err := os.MkdirTemp("", "pebble_periodic_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)
	
	logger := &DefaultLogger{}
	pe, err := NewPebbleEngine(ctx, tempDir,
		WithMetricsEnabled(true),
		WithLogger(logger),
		WithMetricsInterval(100*time.Millisecond),
	)
	require.NoError(t, err)
	defer pe.Close()
	
	// Let periodic reporting run for a bit
	time.Sleep(300 * time.Millisecond)
	
	// Should not panic or error
}