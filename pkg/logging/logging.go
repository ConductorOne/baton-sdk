package logging

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	LogFormatJSON    = "json"
	LogFormatConsole = "console"
)

// logConfig holds both the standard zap configuration and optional
// file rotation settings.
type logConfig struct {
	zapConfig       zap.Config
	logRotationDir  string
	logPrefix       string
	logRotationDays int
	fileOnly        bool
}

// Option configures the logger created by Init.
type Option func(*logConfig)

var (
	activeMu      sync.RWMutex
	activeLevel   *zap.AtomicLevel
	activeRotator *DailyRotator
)

func WithLogLevel(level string) Option {
	return func(c *logConfig) {
		ll, err := ParseLogLevel(level)
		if err != nil {
			return
		}
		c.zapConfig.Level.SetLevel(ll)
	}
}

func ParseLogLevel(level string) (zapcore.Level, error) {
	level = strings.TrimSpace(level)
	if level == "" {
		level = "info"
	}
	var parsed zapcore.Level
	if err := parsed.Set(level); err != nil {
		return zapcore.InfoLevel, fmt.Errorf("invalid log level %q: %w", level, err)
	}
	return parsed, nil
}

func NormalizeLogLevel(level string) (string, error) {
	parsed, err := ParseLogLevel(level)
	if err != nil {
		return "", err
	}
	return parsed.String(), nil
}

func SetLogLevel(level string) error {
	parsed, err := ParseLogLevel(level)
	if err != nil {
		return err
	}

	activeMu.RLock()
	levelHandle := activeLevel
	activeMu.RUnlock()
	if levelHandle == nil {
		return nil
	}
	levelHandle.SetLevel(parsed)
	return nil
}

func WithLogFormat(format string) Option {
	return func(c *logConfig) {
		switch format {
		case LogFormatJSON:
			c.zapConfig.Encoding = LogFormatJSON
		case LogFormatConsole:
			c.zapConfig.Encoding = LogFormatConsole
			c.zapConfig.EncoderConfig = zap.NewDevelopmentEncoderConfig()
		default:
			c.zapConfig.Encoding = LogFormatJSON
		}
	}
}

func WithOutputPaths(paths []string) Option {
	return func(c *logConfig) {
		c.zapConfig.OutputPaths = paths
	}
}

// WithInitialFields allows the logger to be configured with static fields at creation time.
// This is useful for setting fields that are constant across all log messages.
func WithInitialFields(fields map[string]interface{}) Option {
	return func(c *logConfig) {
		c.zapConfig.InitialFields = fields
	}
}

// WithLogRotation enables daily log file rotation with gzip compression
// and automatic cleanup of logs older than logRotationDays. Active log
// files are written to "{dir}/{prefix}-{YYYY-MM-DD}.log".
// When enabled, logs are written to both the rotated file and stderr
// unless WithFileOnly is also set.
// If dir is empty, this option is a no-op.
func WithLogRotation(dir, prefix string, logRotationDays int) Option {
	return func(c *logConfig) {
		if dir == "" {
			return
		}
		c.logRotationDir = dir
		c.logPrefix = prefix
		c.logRotationDays = logRotationDays
	}
}

// WithFileOnly suppresses stderr output when file rotation is enabled.
// This is intended for Windows service mode where only file output is desired.
func WithFileOnly(fileOnly bool) Option {
	return func(c *logConfig) {
		c.fileOnly = fileOnly
	}
}

// Init creates a new zap logger and attaches it to the provided context.
// When log rotation is configured via WithLogRotation, it builds a custom
// logger core with daily rotation. Otherwise it falls back to the standard
// zap.Config.Build() path.
func Init(ctx context.Context, opts ...Option) (context.Context, error) {
	cfg := &logConfig{
		zapConfig: zap.NewProductionConfig(),
	}
	cfg.zapConfig.Sampling = nil
	cfg.zapConfig.DisableStacktrace = true

	for _, opt := range opts {
		opt(cfg)
	}

	var l *zap.Logger
	var err error

	if cfg.logRotationDir != "" {
		l, err = buildRotatingLogger(cfg)
	} else {
		l, err = cfg.zapConfig.Build()
	}
	if err != nil {
		return nil, err
	}
	activeMu.Lock()
	activeLevel = &cfg.zapConfig.Level
	activeMu.Unlock()
	zap.ReplaceGlobals(l)

	l.Debug("Logger created!", zap.String("log_level", cfg.zapConfig.Level.String()))

	return ctxzap.ToContext(ctx, l), nil
}

// buildRotatingLogger creates a zap.Logger backed by a DailyRotator.
func buildRotatingLogger(cfg *logConfig) (*zap.Logger, error) {
	// Close previous rotator if the logger is being reinitialized.
	activeMu.Lock()
	previous := activeRotator
	activeRotator = nil
	activeMu.Unlock()
	if previous != nil {
		if err := previous.Close(); err != nil {
			zap.L().Error("close previous log rotator", zap.Error(err))
		}
	}

	logRotationDays := cfg.logRotationDays
	if logRotationDays <= 0 {
		logRotationDays = DefaultLogRotationDays
	}

	prefix := cfg.logPrefix
	if prefix == "" {
		prefix = "baton"
	}

	rotator, err := NewDailyRotator(cfg.logRotationDir, prefix, logRotationDays)
	if err != nil {
		return nil, err
	}
	activeMu.Lock()
	activeRotator = rotator
	activeMu.Unlock()

	// Build encoder matching the configured format.
	var encoder zapcore.Encoder
	encoderCfg := cfg.zapConfig.EncoderConfig
	switch cfg.zapConfig.Encoding {
	case LogFormatConsole:
		encoder = zapcore.NewConsoleEncoder(encoderCfg)
	default:
		encoder = zapcore.NewJSONEncoder(encoderCfg)
	}

	level := cfg.zapConfig.Level

	// File core with daily rotation.
	fileCore := zapcore.NewCore(encoder, zapcore.AddSync(rotator), level)

	var core zapcore.Core
	if cfg.fileOnly {
		core = fileCore
	} else {
		// Tee to both the rotating file and stderr.
		stderrCore := zapcore.NewCore(encoder, zapcore.Lock(os.Stderr), level)
		core = zapcore.NewTee(fileCore, stderrCore)
	}

	// Carry over any initial fields from the config.
	var fields []zap.Field
	for k, v := range cfg.zapConfig.InitialFields {
		fields = append(fields, zap.Any(k, v))
	}

	return zap.New(core, zap.Fields(fields...)), nil
}
