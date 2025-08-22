package pebble

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var observabilityTracer = otel.Tracer("baton-sdk/pkg.dotc1z.engine.pebble.observability")

// ObservabilityManager provides monitoring, logging, and metrics collection for the Pebble engine.
type ObservabilityManager struct {
	engine     *PebbleEngine
	logger     Logger
	startTime  time.Time
	mu         sync.RWMutex
	metrics    map[string]interface{}
	
	// OpenTelemetry metrics (if available)
	meter           metric.Meter
	operationCount  metric.Int64Counter
	operationTime   metric.Float64Histogram
	dbSize          metric.Int64Gauge
	compactionStats metric.Int64Counter
}

// Logger interface for flexible logging backends
type Logger interface {
	Debug(msg string, fields ...LogField)
	Info(msg string, fields ...LogField)
	Warn(msg string, fields ...LogField)
	Error(msg string, fields ...LogField)
}

// LogField represents a structured logging field
type LogField struct {
	Key   string
	Value interface{}
}

// NewObservabilityManager creates a new observability manager
func NewObservabilityManager(engine *PebbleEngine, logger Logger) *ObservabilityManager {
	om := &ObservabilityManager{
		engine:    engine,
		logger:    logger,
		startTime: time.Now(),
		metrics:   make(map[string]interface{}),
	}

	// Initialize OpenTelemetry metrics if available
	om.initializeMetrics()

	return om
}

// initializeMetrics sets up OpenTelemetry metrics
func (om *ObservabilityManager) initializeMetrics() {
	om.meter = otel.GetMeterProvider().Meter(
		"baton-sdk.pebble-engine",
		metric.WithInstrumentationVersion("1.0.0"),
	)

	// Initialize counters and histograms
	var err error
	
	om.operationCount, err = om.meter.Int64Counter(
		"pebble_operations_total",
		metric.WithDescription("Total number of operations performed"),
	)
	if err != nil {
		if om.logger != nil {
			om.logger.Warn("Failed to create operation counter", LogField{"error", err.Error()})
		}
	}

	om.operationTime, err = om.meter.Float64Histogram(
		"pebble_operation_duration_seconds",
		metric.WithDescription("Duration of operations in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		if om.logger != nil {
			om.logger.Warn("Failed to create operation duration histogram", LogField{"error", err.Error()})
		}
	}

	om.dbSize, err = om.meter.Int64Gauge(
		"pebble_database_size_bytes",
		metric.WithDescription("Current database size in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		if om.logger != nil {
			om.logger.Warn("Failed to create database size gauge", LogField{"error", err.Error()})
		}
	}

	om.compactionStats, err = om.meter.Int64Counter(
		"pebble_compactions_total",
		metric.WithDescription("Total number of compactions"),
	)
	if err != nil {
		if om.logger != nil {
			om.logger.Warn("Failed to create compaction counter", LogField{"error", err.Error()})
		}
	}
}

// RecordOperation records an operation with timing and result
func (om *ObservabilityManager) RecordOperation(operation string, duration time.Duration, err error) {
	if om.logger != nil {
		fields := []LogField{
			{"operation", operation},
			{"duration_ms", duration.Milliseconds()},
		}
		
		if err != nil {
			fields = append(fields, LogField{"error", err.Error()})
			om.logger.Error("Operation failed", fields...)
		} else {
			om.logger.Debug("Operation completed", fields...)
		}
	}

	// Record OpenTelemetry metrics
	if om.operationCount != nil {
		attrs := []attribute.KeyValue{
			attribute.String("operation", operation),
		}
		if err != nil {
			attrs = append(attrs, attribute.String("status", "error"))
		} else {
			attrs = append(attrs, attribute.String("status", "success"))
		}
		om.operationCount.Add(context.Background(), 1, metric.WithAttributes(attrs...))
	}

	if om.operationTime != nil {
		attrs := []attribute.KeyValue{
			attribute.String("operation", operation),
		}
		om.operationTime.Record(context.Background(), duration.Seconds(), metric.WithAttributes(attrs...))
	}
}

// RecordSlowQuery logs a slow query with details
func (om *ObservabilityManager) RecordSlowQuery(operation string, duration time.Duration, details map[string]interface{}) {
	if om.logger == nil {
		return
	}

	fields := []LogField{
		{"operation", operation},
		{"duration_ms", duration.Milliseconds()},
		{"slow_query", true},
	}

	for k, v := range details {
		fields = append(fields, LogField{k, v})
	}

	om.logger.Warn("Slow query detected", fields...)
}

// GetSystemStats returns current system statistics
func (om *ObservabilityManager) GetSystemStats(ctx context.Context) (map[string]interface{}, error) {
	ctx, span := observabilityTracer.Start(ctx, "ObservabilityManager.GetSystemStats")
	defer span.End()

	stats := make(map[string]interface{})

	// Basic system info
	stats["uptime_seconds"] = time.Since(om.startTime).Seconds()
	stats["database_path"] = om.engine.dbPath

	// Database statistics
	if om.engine.db != nil {
		// Get database size estimate
		dbStats := om.engine.db.Metrics()
		stats["wal_files"] = dbStats.WAL.Files
		stats["wal_size_bytes"] = dbStats.WAL.Size
		stats["levels"] = len(dbStats.Levels)
		
		var totalSize uint64
		for _, level := range dbStats.Levels {
			totalSize += uint64(level.Size)
		}
		stats["total_size_bytes"] = totalSize

		// Record size as a gauge metric
		if om.dbSize != nil {
			om.dbSize.Record(context.Background(), int64(totalSize))
		}

		// Compaction statistics
		stats["compactions"] = dbStats.Compact.Count
		stats["compaction_bytes"] = dbStats.Compact.EstimatedDebt
	}

	// Performance metrics
	if om.engine.metrics != nil {
		perfStats := om.engine.metrics.GetStats()
		for k, v := range perfStats {
			stats["perf_"+k] = v
		}
	}

	return stats, nil
}

// GetHealthStatus returns the health status of the engine
func (om *ObservabilityManager) GetHealthStatus(ctx context.Context) (map[string]interface{}, error) {
	ctx, span := observabilityTracer.Start(ctx, "ObservabilityManager.GetHealthStatus")
	defer span.End()

	health := make(map[string]interface{})
	health["status"] = "healthy"
	health["timestamp"] = time.Now().Unix()

	var issues []string

	// Check if database is open
	if !om.engine.isOpen() {
		issues = append(issues, "database_not_open")
		health["status"] = "unhealthy"
	}

	// Check disk space (basic check)
	stats, err := om.GetSystemStats(ctx)
	if err != nil {
		issues = append(issues, "stats_unavailable")
	} else {
		if totalSize, ok := stats["total_size_bytes"].(uint64); ok {
			if totalSize > 10*1024*1024*1024 { // 10GB warning threshold
				issues = append(issues, "large_database_size")
				health["status"] = "degraded"
			}
		}
	}

	// Check for performance issues
	if om.engine.metrics != nil {
		perfStats := om.engine.metrics.GetStats()
		if slowQueries, ok := perfStats["slow_queries"].(int64); ok && slowQueries > 100 {
			issues = append(issues, "frequent_slow_queries")
			health["status"] = "degraded"
		}
	}

	health["issues"] = issues
	return health, nil
}

// LogDatabaseEvent logs significant database events
func (om *ObservabilityManager) LogDatabaseEvent(event string, details map[string]interface{}) {
	if om.logger == nil {
		return
	}

	fields := []LogField{
		{"event", event},
		{"timestamp", time.Now().Unix()},
	}

	for k, v := range details {
		fields = append(fields, LogField{k, v})
	}

	om.logger.Info("Database event", fields...)
}

// StartPeriodicReporting starts periodic reporting of statistics
func (om *ObservabilityManager) StartPeriodicReporting(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stats, err := om.GetSystemStats(ctx)
				if err != nil {
					if om.logger != nil {
						om.logger.Error("Failed to collect periodic stats", LogField{"error", err.Error()})
					}
					continue
				}

				if om.logger != nil {
					fields := make([]LogField, 0, len(stats))
					for k, v := range stats {
						fields = append(fields, LogField{k, v})
					}
					om.logger.Info("Periodic statistics", fields...)
				}
			}
		}
	}()
}

// DefaultLogger provides a basic logging implementation
type DefaultLogger struct{}

// Debug implements Logger interface
func (dl *DefaultLogger) Debug(msg string, fields ...LogField) {
	fmt.Printf("[DEBUG] %s %v\n", msg, fields)
}

// Info implements Logger interface
func (dl *DefaultLogger) Info(msg string, fields ...LogField) {
	fmt.Printf("[INFO] %s %v\n", msg, fields)
}

// Warn implements Logger interface
func (dl *DefaultLogger) Warn(msg string, fields ...LogField) {
	fmt.Printf("[WARN] %s %v\n", msg, fields)
}

// Error implements Logger interface
func (dl *DefaultLogger) Error(msg string, fields ...LogField) {
	fmt.Printf("[ERROR] %s %v\n", msg, fields)
}

// NoOpLogger provides a no-operation logging implementation
type NoOpLogger struct{}

// Debug implements Logger interface (no-op)
func (nol *NoOpLogger) Debug(msg string, fields ...LogField) {}

// Info implements Logger interface (no-op)
func (nol *NoOpLogger) Info(msg string, fields ...LogField) {}

// Warn implements Logger interface (no-op)
func (nol *NoOpLogger) Warn(msg string, fields ...LogField) {}

// Error implements Logger interface (no-op)
func (nol *NoOpLogger) Error(msg string, fields ...LogField) {}