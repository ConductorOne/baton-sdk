package logging

import (
	"context"
	"fmt"
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

type Option func(*zap.Config)

var (
	activeLevelMu sync.RWMutex
	activeLevel   *zap.AtomicLevel
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

// buildConfig returns the zap configuration used by Init and InitWithCore.
func buildConfig(opts ...Option) zap.Config {
	zc := zap.NewProductionConfig()
	zc.Sampling = nil
	zc.DisableStacktrace = true

	for _, opt := range opts {
		opt(&zc)
	}

	return zc
}

func initFromConfig(ctx context.Context, zc zap.Config) (context.Context, *zap.Logger, error) {
	l, err := zc.Build()
	if err != nil {
		return nil, nil, err
	}
	activeLevelMu.Lock()
	activeLevel = &zc.Level
	activeLevelMu.Unlock()
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
	zc := buildConfig(opts...)

	ctx, l, err := initFromConfig(ctx, zc)
	if err != nil {
		return nil, err
	}

	extraCore := buildCore(zc)
	l = l.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewTee(core, extraCore)
	}))
	zap.ReplaceGlobals(l)

	return ctxzap.ToContext(ctx, l), nil
}
