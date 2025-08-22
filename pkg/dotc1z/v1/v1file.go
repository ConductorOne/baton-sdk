package v1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"go.opentelemetry.io/otel"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/sqlite"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

var tracer = otel.Tracer("baton-sdk/pkg.dotc1z.v1")

type pragma struct {
	name  string
	value string
}

type V1File struct {
	pragmas        []pragma
	workingDir     string
	outputFilePath string

	engine *sqlite.SQLite
}

var _ engine.StorageEngine = (*V1File)(nil)

type V1FileOption func(*V1File)

func WithC1FPragma(name string, value string) V1FileOption {
	return func(o *V1File) {
		o.pragmas = append(o.pragmas, pragma{name, value})
	}
}

// Returns a V1File instance for the given db filepath.
func NewV1File(ctx context.Context, workingDir string, opts ...V1FileOption) (*V1File, error) {
	c1File := &V1File{
		workingDir: workingDir,
	}

	for _, opt := range opts {
		opt(c1File)
	}

	sqliteOpts := make([]sqlite.SQLiteOption, 0)
	for _, p := range c1File.pragmas {
		sqliteOpts = append(sqliteOpts, sqlite.WithPragma(p.name, p.value))
	}

	e, err := sqlite.NewSQLite(ctx, workingDir, sqliteOpts...)
	if err != nil {
		return nil, err
	}
	c1File.engine = e

	return c1File, nil
}

type v1C1ZOptions struct {
	tmpDir  string
	pragmas []pragma
}
type V1C1ZOption func(*v1C1ZOptions)

func WithPragma(name string, value string) V1C1ZOption {
	return func(o *v1C1ZOptions) {
		o.pragmas = append(o.pragmas, pragma{name, value})
	}
}

func WithTmpDir(tmpDir string) V1C1ZOption {
	return func(o *v1C1ZOptions) {
		o.tmpDir = tmpDir
	}
}

// Returns a new V1File instance with its state stored at the provided filename.
func NewV1C1ZFile(ctx context.Context, outputFilePath string, opts ...V1C1ZOption) (*V1File, error) {
	ctx, span := tracer.Start(ctx, "NewV1C1ZFile")
	defer span.End()

	options := &v1C1ZOptions{}
	for _, opt := range opts {
		opt(options)
	}

	workingDir, err := loadC1z(outputFilePath, options.tmpDir)
	if err != nil {
		return nil, err
	}

	var c1fopts []V1FileOption
	for _, pragma := range options.pragmas {
		c1fopts = append(c1fopts, WithC1FPragma(pragma.name, pragma.value))
	}

	c1File, err := NewV1File(ctx, workingDir, c1fopts...)
	if err != nil {
		return nil, err
	}

	c1File.outputFilePath = outputFilePath

	return c1File, nil
}

// Close ensures that the sqlite database is flushed to disk, and if any changes were made we update the original database
// with our changes.
func (c *V1File) Close() error {
	if c.engine == nil {
		return nil
	}

	err := c.engine.Close()
	if err != nil {
		return err
	}

	if c.engine.Dirty() {
		err = saveC1z(c.workingDir, c.outputFilePath)
		if err != nil {
			return err
		}
	}

	// Cleanup the database filepath. This should always be a file within a temp directory, so we remove the entire dir.
	err = os.RemoveAll(c.workingDir)
	if err != nil {
		return err
	}

	return nil
}

func (c *V1File) AttachFile(other engine.StorageEngine, dbName string) (engine.AttachedStorageEngine, error) {
	// FIXME(morgabra): What a glorious hack.
	v1f, ok := other.(*V1File)
	if ok {
		return c.engine.AttachFile(v1f.engine, dbName)
	}
	return c.engine.AttachFile(other, dbName)
}

func (c *V1File) Dirty() bool {
	return c.engine.Dirty()
}

func (c *V1File) Stats(ctx context.Context) (map[string]int64, error) {
	return c.engine.Stats(ctx)
}

func (c *V1File) CloneSync(ctx context.Context, syncID string) (string, error) {
	return c.engine.CloneSync(ctx, syncID)
}

func (c *V1File) GenerateSyncDiff(ctx context.Context, baseSyncID string, appliedSyncID string) (string, error) {
	return c.engine.GenerateSyncDiff(ctx, baseSyncID, appliedSyncID)
}

func (c *V1File) ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*engine.SyncRun, string, error) {
	return c.engine.ListSyncRuns(ctx, pageToken, pageSize)
}

func (c *V1File) OutputFilepath() (string, error) {
	if c.outputFilePath == "" {
		return "", fmt.Errorf("v1file: output file path is empty")
	}
	return c.outputFilePath, nil
}

func (c *V1File) ViewSync(ctx context.Context, syncID string) error {
	return c.engine.ViewSync(ctx, syncID)
}

// NewV1FileReader returns a connectorstore.Reader implementation for the given sqlite db file path.
func NewV1FileReader(ctx context.Context, dbFilePath string) (connectorstore.Reader, error) {
	return NewV1File(ctx, dbFilePath)
}

// NewC1ZFileDecoder wraps a given .c1z io.Reader that validates the .c1z and decompresses/decodes the underlying file.
// Defaults: 32MiB max memory and 2GiB max decoded size
// You must close the resulting io.ReadCloser when you are done, do not forget to close the given io.Reader if necessary.
func NewC1ZFileDecoder(f io.Reader, opts ...DecoderOption) (io.ReadCloser, error) {
	return NewDecoder(f, opts...)
}

// V1FileCheckHeader reads len(C1ZFileHeader) bytes from the given io.ReadSeeker and compares them to C1ZFileHeader.
// Returns true if the header is valid. Returns any errors from Read() or Seek().
// If a nil error is returned, the given io.ReadSeeker will be pointing to the first byte of the stream, and is suitable
// to be passed to NewC1ZFileDecoder.
func V1FileCheckHeader(f io.ReadSeeker) (bool, error) {
	// Read header
	err := ReadHeader(f)

	// Seek back to start
	_, seekErr := f.Seek(0, 0)
	if seekErr != nil {
		return false, err
	}

	if err != nil {
		if errors.Is(err, ErrInvalidFile) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func NewExternalV1FileReader(ctx context.Context, tmpDir string, externalResourceC1ZPath string) (connectorstore.Reader, error) {
	dbFilePath, err := loadC1z(externalResourceC1ZPath, tmpDir)
	if err != nil {
		return nil, fmt.Errorf("error loading external resource c1z file: %w", err)
	}

	return NewV1File(ctx, dbFilePath)
}
