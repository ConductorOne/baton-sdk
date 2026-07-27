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

// loggerOptions bundles the zap.Config every Option historically mutated
// with the (opt-in) rotation settings added by WithRotation. Keeping this as
// an unexported struct means the public Option signature can grow new,
// unrelated settings later without breaking existing callers - they only
// ever see the zero value (rotation disabled) unless they call WithRotation.
type loggerOptions struct {
	zc               zap.Config
	rotateMaxSizeMB  int
	rotateMaxBackups int
}

type Option func(*loggerOptions)

var (
	activeLevelMu sync.RWMutex
	activeLevel   *zap.AtomicLevel

	// activeRotators tracks the rotating file writers created by the most
	// recent Init/InitWithCore call, so a later re-Init (e.g. a Windows
	// service re-initializing its logger) closes the previous handles
	// instead of leaking open file descriptors. Mirrors how activeLevel
	// tracks the most recent level handle.
	activeRotatorsMu sync.Mutex
	activeRotators   []io.Closer
)

func WithLogLevel(level string) Option {
	return func(o *loggerOptions) {
		ll, err := ParseLogLevel(level)
		if err != nil {
			return
		}
		o.zc.Level.SetLevel(ll)
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
	return func(o *loggerOptions) {
		switch format {
		case LogFormatJSON:
			o.zc.Encoding = LogFormatJSON
		case LogFormatConsole:
			o.zc.Encoding = LogFormatConsole
			o.zc.EncoderConfig = zap.NewDevelopmentEncoderConfig()
		default:
			o.zc.Encoding = LogFormatJSON
		}
	}
}

func WithOutputPaths(paths []string) Option {
	return func(o *loggerOptions) {
		o.zc.OutputPaths = paths
	}
}

// WithInitialFields allows the logger to be configured with static fields at creation time.
// This is useful for setting fields that are constant across all log messages.
func WithInitialFields(fields map[string]interface{}) Option {
	return func(o *loggerOptions) {
		o.zc.InitialFields = fields
	}
}

// WithRotation opts into size-based rotation for any plain file paths
// configured via WithOutputPaths/OutputPaths (stdout/stderr entries are
// left untouched). maxSizeMB is the size a log file may reach before it is
// rotated; maxBackups is the number of rotated files to retain (<=0 keeps
// none).
//
// Rotation is off by default: without this option, OutputPaths files are
// opened exactly as before (plain, unrotated files), so existing callers
// see no behavior change.
func WithRotation(maxSizeMB, maxBackups int) Option {
	return func(o *loggerOptions) {
		o.rotateMaxSizeMB = maxSizeMB
		o.rotateMaxBackups = maxBackups
	}
}

// buildConfig returns the logger options (zap config plus any rotation
// settings) used by Init and InitWithCore.
func buildConfig(opts ...Option) loggerOptions {
	o := loggerOptions{
		zc: zap.NewProductionConfig(),
	}
	o.zc.Sampling = nil
	o.zc.DisableStacktrace = true

	for _, opt := range opts {
		opt(&o)
	}

	return o
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

// splitOutputPaths separates OutputPaths entries into the ones zap should
// keep handling itself (stdout/stderr) and real file paths that the caller
// wants rotated instead.
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

func initFromConfig(ctx context.Context, o loggerOptions) (context.Context, *zap.Logger, error) {
	zc := o.zc

	// When rotation is enabled, pull any real file paths out of
	// OutputPaths so zc.Build() doesn't also open them as plain,
	// unrotated files - they get their own rotating core below, teed
	// onto the base logger the same way InitWithCore tees the Windows
	// event log core.
	var filePaths []string
	if o.rotateMaxSizeMB > 0 {
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
		rw, rwErr := newRotatingWriter(p, o.rotateMaxSizeMB, o.rotateMaxBackups)
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

	// Swap in the new rotators before closing the previous ones: the new
	// logger is already built and about to become the global logger, so
	// there is no window where a live logger references a closed writer.
	activeRotatorsMu.Lock()
	prevRotators := activeRotators
	activeRotators = rotators
	activeRotatorsMu.Unlock()
	for _, c := range prevRotators {
		_ = c.Close()
	}

	zap.ReplaceGlobals(l)

	l.Debug("Logger created!", zap.String("log_level", zc.Level.String()))

	return ctxzap.ToContext(ctx, l), l, nil
}

// Init creates a new zap logger and attaches it to the provided context.
func Init(ctx context.Context, opts ...Option) (context.Context, error) {
	ctx, _, err := initFromConfig(ctx, buildConfig(opts...))
	return ctx, err
}

// InitWithCore creates a new zap logger and tees an additional core onto it.
// buildCore receives the logger config so the extra core uses the same level
// filtering and encoder settings as Init.
func InitWithCore(ctx context.Context, buildCore func(zap.Config) zapcore.Core, opts ...Option) (context.Context, error) {
	o := buildConfig(opts...)

	ctx, l, err := initFromConfig(ctx, o)
	if err != nil {
		return nil, err
	}

	extraCore := buildCore(o.zc)
	l = l.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewTee(core, extraCore)
	}))
	zap.ReplaceGlobals(l)

	return ctxzap.ToContext(ctx, l), nil
}
