package profiling

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	connectorwrapperV1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
)

// Profiler manages CPU and memory profiling.
type Profiler struct {
	cpuFile     *os.File
	cpuFilePath string
	memFilePath string
	cfg         *connectorwrapperV1.ProfileConfig
}

// New creates a new Profiler from the given configuration.
// Filenames are generated with a timestamp: cpu-YYYYMMDD-HHMMSS.prof and mem-YYYYMMDD-HHMMSS.prof
func New(cfg *connectorwrapperV1.ProfileConfig) *Profiler {
	if cfg == nil || (!cfg.EnableCpu && !cfg.EnableMem) {
		return nil
	}

	// Default to current working directory if not specified
	outputDir := cfg.OutputDir
	if outputDir == "" {
		wd, err := os.Getwd()
		if err != nil {
			// If we can't get CWD, return nil to disable profiling
			return nil
		}
		outputDir = wd
	}

	timestamp := time.Now().Format("20060102-150405")
	return &Profiler{
		cfg:         cfg,
		cpuFilePath: filepath.Join(outputDir, fmt.Sprintf("cpu-%s.prof", timestamp)),
		memFilePath: filepath.Join(outputDir, fmt.Sprintf("mem-%s.prof", timestamp)),
	}
}

// Start begins CPU profiling if configured. Returns an error if profiling fails to start.
func (p *Profiler) Start(ctx context.Context) error {
	if p == nil || !p.cfg.EnableCpu {
		return nil
	}

	l := ctxzap.Extract(ctx)

	// Ensure output directory exists
	outputDir := filepath.Dir(p.cpuFilePath)
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create profile output directory: %w", err)
	}

	// Start CPU profiling
	f, err := os.Create(p.cpuFilePath)
	if err != nil {
		return err
	}
	p.cpuFile = f

	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return err
	}

	l.Info("CPU profiling started", zap.String("output_path", p.cpuFilePath))
	return nil
}

// Stop stops CPU profiling.
func (p *Profiler) Stop(ctx context.Context) error {
	if p == nil || p.cpuFile == nil {
		return nil
	}

	l := ctxzap.Extract(ctx)

	// Stop CPU profiling
	pprof.StopCPUProfile()
	if err := p.cpuFile.Close(); err != nil {
		l.Error("failed to close CPU profile file", zap.Error(err))
	} else {
		l.Info("CPU profile written", zap.String("path", p.cpuFilePath))
	}
	p.cpuFile = nil
	return nil
}

// WriteMemProfile writes a memory profile to disk. Should be called when you want to
// capture memory state (e.g., after main work completes but before cleanup).
func (p *Profiler) WriteMemProfile(ctx context.Context) error {
	if p == nil || !p.cfg.EnableMem {
		return nil
	}

	l := ctxzap.Extract(ctx)

	f, err := os.Create(p.memFilePath)
	if err != nil {
		l.Error("failed to create memory profile file", zap.Error(err))
		return err
	}
	defer f.Close()

	// Write heap profile without forcing GC - we want to see actual memory state
	if err := pprof.WriteHeapProfile(f); err != nil {
		l.Error("failed to write memory profile", zap.Error(err))
		return err
	}

	l.Info("Memory profile written", zap.String("path", p.memFilePath))
	return nil
}
