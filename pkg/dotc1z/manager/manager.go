package manager

import (
	"context"
	"io"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/manager/local"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Manager interface {
	LoadRaw(ctx context.Context) (io.ReadCloser, error)
	LoadC1Z(ctx context.Context) (*dotc1z.C1File, error)
	SaveC1Z(ctx context.Context) error
	Close(ctx context.Context) error
}

type managerOptions struct {
	tmpDir         string
	decoderOptions []dotc1z.DecoderOption
}

type ManagerOption func(*managerOptions)

func WithTmpDir(tmpDir string) ManagerOption {
	return func(o *managerOptions) {
		o.tmpDir = tmpDir
	}
}

func WithDecoderOptions(opts ...dotc1z.DecoderOption) ManagerOption {
	return func(o *managerOptions) {
		o.decoderOptions = opts
	}
}

// Given a file path, return a Manager that can read and write files to that path.
//
// Previously, this supported S3. This was never used, and has been removed.
func New(ctx context.Context, filePath string, opts ...ManagerOption) (Manager, error) {
	options := &managerOptions{}

	for _, opt := range opts {
		opt(options)
	}

	switch {
	case strings.HasPrefix(filePath, "s3://"):
		return nil, status.Errorf(codes.Unimplemented, "s3 support is not implemented")
	default:
		var localOpts []local.Option
		if options.tmpDir != "" {
			localOpts = append(localOpts, local.WithTmpDir(options.tmpDir))
		}
		if len(options.decoderOptions) > 0 {
			localOpts = append(localOpts, local.WithDecoderOptions(options.decoderOptions...))
		}
		return local.New(ctx, filePath, localOpts...)
	}
}
