package v2

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"go.opentelemetry.io/otel"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/sqlite"
)

var tracer = otel.Tracer("baton-sdk/pkg.dotc1z.v2")

type pragma struct {
	name  string
	value string
}

type v2FileOpts struct {
	pragmas []pragma
	engine  string
}

type V2File struct {
	opts           *v2FileOpts
	workingDir     string
	workingDirFS   *os.Root
	outputFilePath string
	manifest       *Manifest
	engine         engine.StorageEngine
}

var _ engine.StorageEngine = (*V2File)(nil)

type V2FileOption func(*v2FileOpts)

func WithV2FilePragma(name string, value string) V2FileOption {
	return func(o *v2FileOpts) {
		o.pragmas = append(o.pragmas, pragma{name, value})
	}
}

func WithV2FileEngine(engine string) V2FileOption {
	return func(o *v2FileOpts) {
		o.engine = engine
	}
}

func NewV2File(ctx context.Context, workingDir string, opts ...V2FileOption) (*V2File, error) {
	_, span := tracer.Start(ctx, "NewV2File")
	defer span.End()

	v2Opts := &v2FileOpts{}
	for _, opt := range opts {
		opt(v2Opts)
	}

	rootFS, err := os.OpenRoot(workingDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open working directory: %w", err)
	}

	f := &V2File{
		opts:         v2Opts,
		workingDir:   workingDir,
		workingDirFS: rootFS,
	}

	err = f.initialize(ctx)
	if err != nil {
		_ = rootFS.Close()
		return nil, fmt.Errorf("c1z: failed to initialize backend: %w", err)
	}

	return f, nil
}

type v1C1ZOptions struct {
	tmpDir  string
	pragmas []pragma
	engine  string
}
type V2C1ZOption func(*v1C1ZOptions)

func WithPragma(name string, value string) V2C1ZOption {
	return func(o *v1C1ZOptions) {
		o.pragmas = append(o.pragmas, pragma{name, value})
	}
}

func WithTmpDir(tmpDir string) V2C1ZOption {
	return func(o *v1C1ZOptions) {
		o.tmpDir = tmpDir
	}
}

func WithEngine(engine string) V2C1ZOption {
	return func(o *v1C1ZOptions) {
		o.engine = engine
	}
}

// Returns a new V2File instance with its state stored at the provided filename.
func NewV2C1ZFile(ctx context.Context, outputFilePath string, opts ...V2C1ZOption) (*V2File, error) {
	ctx, span := tracer.Start(ctx, "NewV2C1ZFile")
	defer span.End()

	options := &v1C1ZOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Load the c1z file to a temporary directory
	workingDir, err := loadC1zV2(outputFilePath, options.tmpDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load c1z file: %w", err)
	}

	// Create V2File options from the provided options
	c1fopts := []V2FileOption{}
	if options.engine != "" {
		c1fopts = append(c1fopts, WithV2FileEngine(options.engine))
	}
	for _, p := range options.pragmas {
		c1fopts = append(c1fopts, WithV2FilePragma(p.name, p.value))
	}

	// Create the V2File instance
	c1File, err := NewV2File(ctx, workingDir, c1fopts...)
	if err != nil {
		// Clean up working directory on error
		if removeErr := os.RemoveAll(workingDir); removeErr != nil {
			err = errors.Join(err, removeErr)
		}
		return nil, err
	}
	c1File.outputFilePath = outputFilePath

	return c1File, nil
}

// initialize reads the manifest and initializes the appropriate storage engine.
// TODO(morgabra): This is terrible do some kind of backend registry thing.
func (c *V2File) initialize(ctx context.Context) error {
	// Try to read the manifest
	manifest, err := c.ReadManifest()
	if err != nil {
		// If manifest doesn't exist, create a default one
		if errors.Is(err, fs.ErrNotExist) {
			engineType := c.opts.engine
			if engineType == "" {
				engineType = "sqlite" // Default to SQLite
			}
			manifest = CreateDefaultManifest(engineType)
			err = c.WriteManifest(manifest)
			if err != nil {
				return fmt.Errorf("failed to write default manifest: %w", err)
			}
		} else {
			return fmt.Errorf("failed to read manifest: %w", err)
		}
	}
	c.manifest = manifest

	// Find the primary backend (for now, use the first one or look for a default)
	if len(manifest.Backends) != 1 {
		return fmt.Errorf("c1z: expected 1 backend in manifest but got %d", len(manifest.Backends))
	}
	var primaryBackend *BackendInfo
	for _, backend := range manifest.Backends {
		primaryBackend = &backend
		break
	}

	if primaryBackend.Path == "" {
		return fmt.Errorf("c1z: invalid backend found in manifest, missing path")
	}

	switch primaryBackend.Engine {
	case "sqlite":
		sqliteOpts := make([]sqlite.SQLiteOption, 0)
		for _, p := range c.opts.pragmas {
			sqliteOpts = append(sqliteOpts, sqlite.WithPragma(p.name, p.value))
		}
		c.engine, err = sqlite.NewSQLite(ctx, filepath.Join(c.workingDir, primaryBackend.Path))
		if err != nil {
			return fmt.Errorf("failed to initialize sqlite backend: %w", err)
		}
	default:
		return fmt.Errorf("c1z: unsupported storage engine %s", primaryBackend.Engine)
	}

	return nil
}

// Close ensures that the storage engine is flushed to disk, and if any changes were made we update the original file
// with our changes, then cleans up temporary resources.
func (c *V2File) Close() error {
	var err error

	// Close the underlying engine if it exists
	if c.engine != nil {
		if closeErr := c.engine.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close engine: %w", closeErr))
		}
	}

	// Save changes if the engine is dirty and we have an output file path
	if c.engine != nil && c.engine.Dirty() && c.outputFilePath != "" {
		if saveErr := saveC1zV2(c.workingDir, c.outputFilePath); saveErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to save changes: %w", saveErr))
		}
	}

	// Clean up the working directory
	if c.workingDirFS != nil {
		closeErr := c.workingDirFS.Close()
		if closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close working dir: %w", closeErr))
		}
	}
	if c.workingDir != "" {
		if removeErr := os.RemoveAll(c.workingDir); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to clean up working directory: %w", removeErr))
		}
	}

	// Clear references
	c.engine = nil
	c.workingDir = ""
	c.workingDirFS = nil

	return err
}

func (c *V2File) AttachFile(other engine.StorageEngine, dbName string) (engine.AttachedStorageEngine, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.AttachFile(other, dbName)
}

func (c *V2File) Dirty() bool {
	if c.engine == nil {
		return false
	}
	return c.engine.Dirty()
}

func (c *V2File) Stats(ctx context.Context) (map[string]int64, error) {
	if c.engine == nil {
		return nil, fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.Stats(ctx)
}

func (c *V2File) CloneSync(ctx context.Context, syncID string) (string, error) {
	if c.engine == nil {
		return "", fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.CloneSync(ctx, syncID)
}

func (c *V2File) GenerateSyncDiff(ctx context.Context, baseSyncID string, appliedSyncID string) (string, error) {
	if c.engine == nil {
		return "", fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.GenerateSyncDiff(ctx, baseSyncID, appliedSyncID)
}

func (c *V2File) ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*engine.SyncRun, string, error) {
	if c.engine == nil {
		return nil, "", fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.ListSyncRuns(ctx, pageToken, pageSize)
}

func (c *V2File) OutputFilepath() (string, error) {
	if c.outputFilePath == "" {
		return "", fmt.Errorf("v2file: output file path is empty")
	}
	return c.outputFilePath, nil
}

func (c *V2File) ViewSync(ctx context.Context, syncID string) error {
	if c.engine == nil {
		return fmt.Errorf("v2file: engine not initialized")
	}
	return c.engine.ViewSync(ctx, syncID)
}
