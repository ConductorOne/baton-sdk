package dotc1z

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine"
	v1 "github.com/conductorone/baton-sdk/pkg/dotc1z/v1"
	v2 "github.com/conductorone/baton-sdk/pkg/dotc1z/v2"
)

var tracer = otel.Tracer("baton-sdk/pkg.dotc1z")

type pragma struct {
	name  string
	value string
}
type c1zOptions struct {
	tmpDir  string
	pragmas []pragma
	format  C1ZFormat
	engine  string
}
type C1ZOption func(*c1zOptions)

func WithTmpDir(tmpDir string) C1ZOption {
	return func(o *c1zOptions) {
		o.tmpDir = tmpDir
	}
}

func WithPragma(name string, value string) C1ZOption {
	return func(o *c1zOptions) {
		o.pragmas = append(o.pragmas, pragma{name, value})
	}
}

func WithFormat(format C1ZFormat) C1ZOption {
	return func(o *c1zOptions) {
		o.format = format
	}
}

func WithEngine(engine string) C1ZOption {
	return func(o *c1zOptions) {
		o.engine = engine
	}
}

// Returns a new C1File instance with its state stored at the provided filename.
func NewC1ZFile(ctx context.Context, outputFilePath string, opts ...C1ZOption) (engine.StorageEngine, error) {
	ctx, span := tracer.Start(ctx, "NewC1ZFile")
	defer span.End()

	options := &c1zOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Determine if the c1z exists and what format version it is.
	if stat, err := os.Stat(outputFilePath); err == nil && stat.Size() != 0 {
		c1zFile, err := os.Open(outputFilePath)
		if err != nil {
			return nil, err
		}
		defer c1zFile.Close()

		options.format, err = ReadHeader(c1zFile)
		if err != nil {
			return nil, fmt.Errorf("c1z: failed to read header: %w", err)
		}
	}

	switch GetFormat(options.format) {
	case C1ZFormatV1:
		v1Opts := []v1.V1C1ZOption{v1.WithTmpDir(options.tmpDir)}
		for _, p := range options.pragmas {
			v1Opts = append(v1Opts, v1.WithPragma(p.name, p.value))
		}
		return v1.NewV1C1ZFile(ctx, outputFilePath, v1Opts...)
	case C1ZFormatV2:
		v2Opts := []v2.V2C1ZOption{v2.WithTmpDir(options.tmpDir), v2.WithEngine(options.engine)}
		for _, p := range options.pragmas {
			v2Opts = append(v2Opts, v2.WithPragma(p.name, p.value))
		}
		return v2.NewV2C1ZFile(ctx, outputFilePath)
	default:
		return nil, fmt.Errorf("c1z: unknown format")
	}
}

func NewExternalC1FileReader(ctx context.Context, tmpDir string, externalResourceC1ZPath string) (connectorstore.Reader, error) {
	return NewC1ZFile(ctx, externalResourceC1ZPath, WithTmpDir(tmpDir))
}
