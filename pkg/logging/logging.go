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
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	LogFormatJSON    = "json"
	LogFormatConsole = "console"

	// DefaultRetentionDays is the default number of days to keep rotated logs.
	DefaultRetentionDays = 10
	// DefaultMaxSizeMB is the default size, in megabytes, at which the active
	// log file is rotated.
	DefaultMaxSizeMB = 100
	// DefaultMaxBackups is the default number of rotated files to keep. With
	// DefaultMaxSizeMB this bounds rotated history to ~1 GB (plus the active
	// file, so ~1.1 GB worst case on disk).
	DefaultMaxBackups = 10
)

// logConfig holds both the standard zap configuration and optional
// file rotation settings.
type logConfig struct {
	zapConfig     zap.Config
	filePath      string
	maxSizeMB     int
	maxBackups    int
	retentionDays int
	fileOnly      bool
	compress      bool
}

// Option configures the logger created by Init.
type Option func(*logConfig)

var (
	// activeMu serializes logger reconfiguration: Init swaps activeLevel and
	// activeRotator under the write lock; SetLogLevel reads activeLevel under
	// the read lock. On re-Init the previous rotator is closed so its open file
	// handle is released. This does not retract loggers already extracted from
	// earlier contexts — a retained zap logger still references the old core, and
	// writing through it will reopen the file; use the logger from the latest Init.
	activeMu      sync.RWMutex
	activeLevel   *zap.AtomicLevel
	activeRotator *lumberjack.Logger
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

// WithFileRotation enables log file rotation backed by lumberjack: the active
// file is rotated once it exceeds maxSizeMB, rotated files are gzip-compressed,
// and old files are removed once they exceed retentionDays (age) or maxBackups
// (count). When enabled, logs are written to both the file and stderr unless
// WithFileOnly is also set.
// If filePath is empty, this option is a no-op. Non-positive maxSizeMB or
// retentionDays fall back to the package defaults; maxBackups <= 0 keeps all
// files within the age limit.
func WithFileRotation(filePath string, maxSizeMB, maxBackups, retentionDays int) Option {
	return func(c *logConfig) {
		if filePath == "" {
			return
		}
		c.filePath = filePath
		c.maxSizeMB = maxSizeMB
		c.maxBackups = maxBackups
		c.retentionDays = retentionDays
	}
}

// WithFileOnly suppresses stderr output when file rotation is enabled.
// This is intended for Windows service mode where only file output is desired.
func WithFileOnly(fileOnly bool) Option {
	return func(c *logConfig) {
		c.fileOnly = fileOnly
	}
}

// WithFileCompression controls gzip compression of rotated log files. It
// defaults to true. Disabling it is mainly useful in tests: lumberjack performs
// compression in a background goroutine, which can outlive a test and race
// t.TempDir cleanup; a test that only needs to observe rotation can turn it off
// for deterministic teardown.
func WithFileCompression(compress bool) Option {
	return func(c *logConfig) {
		c.compress = compress
	}
}

// Init creates a new zap logger and attaches it to the provided context.
// When file rotation is configured via WithFileRotation, it builds a logger
// core backed by lumberjack. Otherwise it falls back to the standard
// zap.Config.Build() path.
func Init(ctx context.Context, opts ...Option) (context.Context, error) {
	cfg := &logConfig{
		zapConfig: zap.NewProductionConfig(),
	}
	cfg.zapConfig.Sampling = nil
	cfg.zapConfig.DisableStacktrace = true
	cfg.compress = true // default; overridable via WithFileCompression

	for _, opt := range opts {
		opt(cfg)
	}

	// Reconfigure the active logger state atomically: closing any previous
	// rotator, building the new logger, and publishing activeLevel/activeRotator
	// must not interleave with a concurrent Init (or a SetLogLevel read).
	activeMu.Lock()
	defer activeMu.Unlock()

	// Release any rotator installed by a previous Init before building the new
	// logger, regardless of whether the new configuration uses rotation, so its
	// open file handle is never leaked on a rotation -> no-rotation switch.
	if activeRotator != nil {
		if err := activeRotator.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "baton: log rotation: close previous rotator: %v\n", err)
		}
		activeRotator = nil
	}

	var l *zap.Logger
	var err error

	if cfg.filePath != "" {
		l, err = buildRotatingLogger(cfg)
	} else {
		l, err = cfg.zapConfig.Build()
	}
	if err != nil {
		return nil, err
	}

	activeLevel = &cfg.zapConfig.Level
	zap.ReplaceGlobals(l)

	l.Debug("Logger created!", zap.String("log_level", cfg.zapConfig.Level.String()))

	return ctxzap.ToContext(ctx, l), nil
}

// InitWithCore behaves like Init but tees an additional core onto the logger —
// used, e.g., to also emit to the Windows event log. The extra core is built
// from the resolved zap.Config so it inherits the encoder and level. Any file
// rotation configured via WithFileRotation still applies to the base logger.
func InitWithCore(ctx context.Context, buildCore func(zap.Config) zapcore.Core, opts ...Option) (context.Context, error) {
	cfg := &logConfig{
		zapConfig: zap.NewProductionConfig(),
	}
	cfg.zapConfig.Sampling = nil
	cfg.zapConfig.DisableStacktrace = true
	cfg.compress = true // default; overridable via WithFileCompression

	for _, opt := range opts {
		opt(cfg)
	}

	activeMu.Lock()
	defer activeMu.Unlock()

	if activeRotator != nil {
		if err := activeRotator.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "baton: log rotation: close previous rotator: %v\n", err)
		}
		activeRotator = nil
	}

	var l *zap.Logger
	var err error
	if cfg.filePath != "" {
		l, err = buildRotatingLogger(cfg)
	} else {
		l, err = cfg.zapConfig.Build()
	}
	if err != nil {
		return nil, err
	}

	if buildCore != nil {
		extra := buildCore(cfg.zapConfig)
		l = l.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
			return zapcore.NewTee(c, extra)
		}))
	}

	activeLevel = &cfg.zapConfig.Level
	zap.ReplaceGlobals(l)

	return ctxzap.ToContext(ctx, l), nil
}

// buildRotatingLogger creates a zap.Logger backed by a lumberjack.Logger.
// The caller must hold activeMu and must already have closed any previous
// rotator; this publishes the new one to the activeRotator global.
func buildRotatingLogger(cfg *logConfig) (*zap.Logger, error) {
	maxSizeMB := cfg.maxSizeMB
	if maxSizeMB <= 0 {
		maxSizeMB = DefaultMaxSizeMB
	}
	retentionDays := cfg.retentionDays
	if retentionDays <= 0 {
		retentionDays = DefaultRetentionDays
	}

	rotator := &lumberjack.Logger{
		Filename:   cfg.filePath,
		MaxSize:    maxSizeMB,      // megabytes before the active file is rotated
		MaxBackups: cfg.maxBackups, // max rotated files to keep (0 = no count limit)
		MaxAge:     retentionDays,  // days to retain rotated files
		Compress:   cfg.compress,   // gzip rotated files (default true; see WithFileCompression)
		// LocalTime is deliberately false (UTC). lumberjack formats backup
		// timestamps with LocalTime but always parses them as UTC when enforcing
		// MaxAge, so local time would skew age-based retention by the host's tz
		// offset. UTC keeps format and parse consistent.
		LocalTime: false,
	}

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

	// File core backed by the rotating writer.
	fileCore := zapcore.NewCore(encoder, zapcore.AddSync(rotator), level)

	var core zapcore.Core
	if cfg.fileOnly {
		core = fileCore
	} else {
		// Tee to both the rotating file and stderr.
		stderrCore := zapcore.NewCore(encoder, zapcore.Lock(os.Stderr), level)
		core = zapcore.NewTee(fileCore, stderrCore)
	}

	// Re-apply InitialFields to our core. Build attaches them to the core it
	// constructs, which WrapCore then discards, so without this WithInitialFields
	// would be silently dropped in rotation mode. (Caller annotation, error
	// output, and stacktrace are logger-level options and survive the core swap.)
	if len(cfg.zapConfig.InitialFields) > 0 {
		fields := make([]zap.Field, 0, len(cfg.zapConfig.InitialFields))
		for k, v := range cfg.zapConfig.InitialFields {
			fields = append(fields, zap.Any(k, v))
		}
		core = core.With(fields)
	}

	// Build constructs a core from zapConfig.OutputPaths, but WrapCore below
	// discards that core entirely — real output only ever goes through the
	// rotating core above. Point OutputPaths at zap's implicit discard sink
	// (empty == io.Discard) so Build never opens a real file or stderr for the
	// throwaway core, e.g. if a caller also passed WithOutputPaths. This is
	// independent of fileOnly: nothing is written here, so it can't duplicate
	// output to stderr. ErrorOutputPaths is left at its default for zap's own
	// internal error reporting.
	cfg.zapConfig.OutputPaths = nil

	// Build through zapConfig and swap in the rotating core via WrapCore so the
	// logger-level options Build applies (caller annotation, error output,
	// stacktrace) are preserved rather than dropped via a bare zap.New.
	l, err := cfg.zapConfig.Build(zap.WrapCore(func(zapcore.Core) zapcore.Core {
		return core
	}))
	if err != nil {
		// Don't leave a dangling, open rotator behind a failed Init.
		_ = rotator.Close()
		return nil, err
	}

	activeRotator = rotator
	return l, nil
}
