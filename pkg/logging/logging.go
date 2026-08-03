package logging

import (
	"context"
	"fmt"
	"io"
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

// Option configures the zap.Config used to build the logger. Its signature is
// intentionally kept as func(*zap.Config) so downstream connectors that define
// custom options keep compiling. Optional file rotation is passed separately
// via InitWithRotation / InitWithCoreAndRotation rather than as an Option, to
// avoid changing this exported type.
type Option func(*zap.Config)

var (
	activeLevelMu sync.RWMutex
	activeLevel   *zap.AtomicLevel

	// activeRotators tracks the rotating file writers created by the most
	// recent Init call, so a later re-Init (e.g. a Windows service
	// re-initializing its logger) closes the previous handles instead of
	// leaking open file descriptors. Mirrors how activeLevel tracks the most
	// recent level handle.
	activeRotatorsMu sync.Mutex
	activeRotators   []io.Closer
)

func WithLogLevel(level string) Option {
	return func(c *zap.Config) {
		ll, err := ParseLogLevel(level)
		if err != nil {
			return
		}
		c.Level.SetLevel(ll)
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

// SetLogLevel sets the log level for the active logger.
// Currently only used by lambda connectors.
func SetLogLevel(level string) error {
	parsed, err := ParseLogLevel(level)
	if err != nil {
		return err
	}

	activeLevelMu.RLock()
	levelHandle := activeLevel
	activeLevelMu.RUnlock()
	if levelHandle == nil {
		return nil
	}
	levelHandle.SetLevel(parsed)
	return nil
}

func WithLogFormat(format string) Option {
	return func(c *zap.Config) {
		switch format {
		case LogFormatJSON:
			c.Encoding = LogFormatJSON
		case LogFormatConsole:
			c.Encoding = LogFormatConsole
			c.EncoderConfig = zap.NewDevelopmentEncoderConfig()
		default:
			c.Encoding = LogFormatJSON
		}
	}
}

func WithOutputPaths(paths []string) Option {
	return func(c *zap.Config) {
		c.OutputPaths = paths
	}
}

// WithInitialFields allows the logger to be configured with static fields at creation time.
// This is useful for setting fields that are constant across all log messages.
func WithInitialFields(fields map[string]interface{}) Option {
	return func(c *zap.Config) {
		c.InitialFields = fields
	}
}

// buildConfig returns the zap configuration used by the Init functions.
func buildConfig(opts ...Option) zap.Config {
	zc := zap.NewProductionConfig()
	zc.Sampling = nil
	zc.DisableStacktrace = true

	for _, opt := range opts {
		opt(&zc)
	}

	return zc
}

// encoderFromConfig returns the zapcore.Encoder matching zc's configured
// Encoding, so extra cores (rotation, event log, ...) format entries
// identically to the base logger.
func encoderFromConfig(zc zap.Config) zapcore.Encoder {
	if zc.Encoding == LogFormatConsole {
		return zapcore.NewConsoleEncoder(zc.EncoderConfig)
	}
	return zapcore.NewJSONEncoder(zc.EncoderConfig)
}

// isStdOutErr reports whether an OutputPaths entry refers to one of zap's
// built-in stdout/stderr sinks rather than a real file path.
func isStdOutErr(p string) bool {
	return p == "stdout" || p == "stderr"
}

// splitOutputPaths separates OutputPaths entries into the ones zap should keep
// handling itself (stdout/stderr) and real file paths that the caller wants
// rotated instead.
func splitOutputPaths(paths []string) ([]string, []string) {
	var kept, files []string
	for _, p := range paths {
		if isStdOutErr(p) {
			kept = append(kept, p)
		} else {
			files = append(files, p)
		}
	}
	return kept, files
}

// initFromConfig builds the base logger. When maxSizeMB > 0, real file paths in
// OutputPaths are pulled out and served by a rotating core instead of zap's
// plain file sink; maxSizeMB <= 0 leaves OutputPaths handling exactly as zap's
// default (no rotation, no behavior change).
func initFromConfig(ctx context.Context, zc zap.Config, maxSizeMB, maxBackups int) (context.Context, *zap.Logger, error) {
	var filePaths []string
	if maxSizeMB > 0 {
		kept, files := splitOutputPaths(zc.OutputPaths)
		if len(files) > 0 {
			zc.OutputPaths = kept
			filePaths = files
		}
	}

	l, err := zc.Build()
	if err != nil {
		return nil, nil, err
	}

	rotators := make([]io.Closer, 0, len(filePaths))
	for _, p := range filePaths {
		rw, rwErr := newRotatingWriter(p, maxSizeMB, maxBackups)
		if rwErr != nil {
			for _, c := range rotators {
				_ = c.Close()
			}
			return nil, nil, fmt.Errorf("failed to create rotating log writer for %q: %w", p, rwErr)
		}
		rotators = append(rotators, rw)

		rotateCore := zapcore.NewCore(encoderFromConfig(zc), zapcore.AddSync(rw), zc.Level)
		l = l.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return zapcore.NewTee(core, rotateCore)
		}))
	}

	activeLevelMu.Lock()
	activeLevel = &zc.Level
	activeLevelMu.Unlock()

	activeRotatorsMu.Lock()
	prevRotators := activeRotators
	activeRotators = rotators
	activeRotatorsMu.Unlock()

	zap.ReplaceGlobals(l)

	// Close the previous rotators only after the new logger has replaced the
	// global one, so no still-installed logger can write to a closed handle.
	for _, c := range prevRotators {
		_ = c.Close()
	}

	l.Debug("Logger created!", zap.String("log_level", zc.Level.String()))

	return ctxzap.ToContext(ctx, l), l, nil
}

// teeExtraCore builds an additional core via buildCore and tees it onto l,
// replacing the global logger. Shared by InitWithCore and
// InitWithCoreAndRotation.
func teeExtraCore(ctx context.Context, l *zap.Logger, buildCore func(zap.Config) zapcore.Core, zc zap.Config) context.Context {
	extraCore := buildCore(zc)
	l = l.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewTee(core, extraCore)
	}))
	zap.ReplaceGlobals(l)
	return ctxzap.ToContext(ctx, l)
}

// Init creates a new zap logger and attaches it to the provided context.
func Init(ctx context.Context, opts ...Option) (context.Context, error) {
	ctx, _, err := initFromConfig(ctx, buildConfig(opts...), 0, 0)
	return ctx, err
}

// InitWithRotation is like Init but also enables size-based rotation of any
// plain file paths in OutputPaths when maxSizeMB > 0. maxSizeMB <= 0 disables
// rotation entirely, behaving exactly like Init. maxBackups bounds the number
// of retained rotated files.
func InitWithRotation(ctx context.Context, maxSizeMB, maxBackups int, opts ...Option) (context.Context, error) {
	ctx, _, err := initFromConfig(ctx, buildConfig(opts...), maxSizeMB, maxBackups)
	return ctx, err
}

// InitWithCore creates a new zap logger and tees an additional core onto it.
// buildCore receives the logger config so the extra core uses the same level
// filtering and encoder settings as Init.
func InitWithCore(ctx context.Context, buildCore func(zap.Config) zapcore.Core, opts ...Option) (context.Context, error) {
	return InitWithCoreAndRotation(ctx, buildCore, 0, 0, opts...)
}

// InitWithCoreAndRotation combines InitWithCore's extra-core tee with
// InitWithRotation's size-based file rotation. maxSizeMB <= 0 disables rotation.
func InitWithCoreAndRotation(ctx context.Context, buildCore func(zap.Config) zapcore.Core, maxSizeMB, maxBackups int, opts ...Option) (context.Context, error) {
	zc := buildConfig(opts...)
	ctx, l, err := initFromConfig(ctx, zc, maxSizeMB, maxBackups)
	if err != nil {
		return nil, err
	}
	return teeExtraCore(ctx, l, buildCore, zc), nil
}
