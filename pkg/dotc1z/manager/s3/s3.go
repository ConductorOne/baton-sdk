package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/aws/smithy-go"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/uotel"
	"github.com/conductorone/baton-sdk/pkg/us3"
)

var tracer = otel.Tracer("baton-sdk/pkg.dotc1z.manager.s3")

// s3PutClient is the subset of us3.S3Client the manager uses.
// Production callers continue to pass a concrete *us3.S3Client into
// NewS3Manager (it satisfies the interface); tests inject a fake that
// records the call without standing up an S3 fake.
type s3PutClient interface {
	PutWithVerify(ctx context.Context, key string, r io.Reader, expectedSize int64, contentType string) error
	Get(ctx context.Context, key string) (io.Reader, error)
}

type s3Manager struct {
	client         s3PutClient
	fileName       string
	tmpFile        string
	tmpDir         string
	decoderOptions []dotc1z.DecoderOption
}

type Option func(*s3Manager)

func WithTmpDir(tmpDir string) Option {
	return func(o *s3Manager) {
		o.tmpDir = tmpDir
	}
}

func WithDecoderOptions(opts ...dotc1z.DecoderOption) Option {
	return func(o *s3Manager) {
		o.decoderOptions = opts
	}
}

func (s *s3Manager) copyToTempFile(ctx context.Context, r io.Reader) error {
	_, span := tracer.Start(ctx, "s3Manager.copyToTempFile")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	f, err := os.CreateTemp(s.tmpDir, "sync-*.c1z")
	if err != nil {
		return err
	}
	defer f.Close()

	s.tmpFile = f.Name()

	if r != nil {
		_, err = io.Copy(f, r)
		if err != nil {
			_ = f.Close()
			return err
		}

		if err := f.Sync(); err != nil {
			_ = f.Close()
			return fmt.Errorf("failed to sync temp file: %w", err)
		}
	}

	return nil
}

// LoadRaw loads the file from S3 and returns an io.Reader for the contents.
func (s *s3Manager) LoadRaw(ctx context.Context) (io.ReadCloser, error) {
	ctx, span := tracer.Start(ctx, "s3Manager.LoadRaw")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	out, err := s.client.Get(ctx, s.fileName)
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			switch ae.ErrorCode() {
			case "NotFound":
				return nil, err
			default:
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	err = s.copyToTempFile(ctx, out)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(s.tmpFile)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// LoadC1Z gets a file from the AWS S3 bucket and copies it to a temp file.
func (s *s3Manager) LoadC1Z(ctx context.Context) (*dotc1z.C1File, error) {
	ctx, span := tracer.Start(ctx, "s3Manager.LoadC1Z")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)

	out, err := s.client.Get(ctx, s.fileName)
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			switch ae.ErrorCode() {
			case "NotFound":
				l.Info("c1z was not found in s3 -- creating empty c1z", zap.String("file_path", s.fileName))
			default:
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	err = s.copyToTempFile(ctx, out)
	if err != nil {
		return nil, err
	}

	opts := []dotc1z.C1ZOption{
		dotc1z.WithTmpDir(s.tmpDir),
	}
	if len(s.decoderOptions) > 0 {
		opts = append(opts, dotc1z.WithDecoderOptions(s.decoderOptions...))
	}
	return dotc1z.NewC1ZFile(ctx, s.tmpFile, opts...)
}

// SaveC1Z saves a file to the AWS S3 bucket.
//
// Stats the local tmp file up-front so the underlying us3 client can
// verify both that the source read produced the expected byte count
// (catches truncated local reads) and that the committed S3 object
// matches that same count (catches truncated multipart uploads).
func (s *s3Manager) SaveC1Z(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "s3Manager.SaveC1Z")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if s.client == nil {
		return fmt.Errorf("attempting to save to s3 without a valid client")
	}

	if s.fileName == "" {
		return fmt.Errorf("attempting to save to s3 without a valid file path specified")
	}

	f, err := os.Open(s.tmpFile)
	if err != nil {
		return err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("s3Manager.SaveC1Z: stat tmp file: %w", err)
	}
	expectedSize := stat.Size()

	err = s.client.PutWithVerify(ctx, s.fileName, f, expectedSize, "application/c1z")
	if err != nil {
		return err
	}

	return nil
}

func (s *s3Manager) Close(ctx context.Context) error {
	_, span := tracer.Start(ctx, "s3Manager.Close")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	err = os.Remove(s.tmpFile)
	if err != nil {
		return err
	}

	return nil
}

// NewS3Manager returns a new `s3Manager` that uses the given `s3Uri`.
func NewS3Manager(ctx context.Context, s3Uri string, opts ...Option) (*s3Manager, error) {
	l := ctxzap.Extract(ctx)

	fileName, s3Client, err := us3.NewClientFromURI(ctx, s3Uri)
	if err != nil {
		return nil, err
	}

	manager := &s3Manager{
		client:   s3Client,
		fileName: fileName,
	}

	for _, opt := range opts {
		opt(manager)
	}

	l.Debug("created new s3 file manager", zap.String("filename", fileName))

	return manager, nil
}
