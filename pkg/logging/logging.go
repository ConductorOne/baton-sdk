package logging

import (
	"context"
	"io"
	"net/url"
	"os"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golift.io/rotatorr"
	"golift.io/rotatorr/timerotator"
)

const (
	LogFormatJSON    = "json"
	LogFormatConsole = "console"
)

type Option func(*zap.Config)

func WithLogLevel(level string) Option {
	return func(c *zap.Config) {
		ll := zapcore.DebugLevel
		_ = ll.Set(level)
		c.Level.SetLevel(ll)
	}
}

func WithLogFormat(format string) Option {
	return func(c *zap.Config) {
		switch format {
		case LogFormatJSON:
			c.Encoding = LogFormatJSON
		case LogFormatConsole:
			c.Encoding = LogFormatConsole
		default:
			c.Encoding = LogFormatJSON
		}
	}
}

const rotatorrScheme = "rotatorr"

func WithOutputPaths(paths []string) Option {
	return func(c *zap.Config) {
		p := make([]string, 0, len(paths))
		for _, path := range paths {
			switch path {
			case "stdout", "stderr":
				p = append(p, path)
			default:
				u := &url.URL{Scheme: rotatorrScheme, Path: path}
				p = append(p, u.String())
			}
		}
		c.OutputPaths = p
	}
}

type zapSink struct {
	*rotatorr.Logger
}

func (z *zapSink) Sync() error {
	return nil
}

type pathRegistry struct {
	sync.Map
}

func (p *pathRegistry) Register(path string) (zap.Sink, error) {
	if sink, ok := p.Load(path); ok {
		return sink.(zap.Sink), nil
	}

	rr, err := rotatorr.New(&rotatorr.Config{
		FileSize: 1024 * 1024 * 10, // 10 megabytes
		Filepath: path,
		Rotatorr: &timerotator.Layout{FileCount: 10},
	})
	if err != nil {
		return nil, err
	}

	sink := &zapSink{Logger: rr}
	p.Store(path, sink)
	return sink, nil
}

var pr = &pathRegistry{}

func WriterForPath(path string) (io.Writer, error) {
	switch path {
	case "stdout":
		return os.Stdout, nil
	case "stderr":
		return os.Stderr, nil
	default:
		return pr.Register(path)
	}
}

func init() {
	err := zap.RegisterSink(rotatorrScheme, func(u *url.URL) (zap.Sink, error) {
		return pr.Register(u.Path)
	})

	if err != nil {
		panic(err)
	}
}

// Init creates a new zap logger and attaches it to the provided context.
func Init(ctx context.Context, opts ...Option) (context.Context, error) {
	zc := zap.NewProductionConfig()
	zc.Sampling = nil
	zc.DisableStacktrace = true

	for _, opt := range opts {
		opt(&zc)
	}

	l, err := zc.Build()
	if err != nil {
		return nil, err
	}
	zap.ReplaceGlobals(l)

	l.Debug("Logger created!", zap.String("log_level", zc.Level.String()))

	return ctxzap.ToContext(ctx, l), nil
}
