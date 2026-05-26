package dotc1z

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// StorePragma is a SQLite pragma forwarded through StoreOptions. Non-SQLite
// engines may ignore it.
type StorePragma struct {
	Name  string
	Value string
}

// StoreOptions is the engine-neutral form of C1ZOption values passed to a
// registered engine driver.
type StoreOptions struct {
	TmpDir             string
	Pragmas            []StorePragma
	DecoderOptions     []DecoderOption
	ReadOnly           bool
	EncoderConcurrency int
	SyncLimit          int
	SkipCleanup        bool
	V2GrantsWriter     bool
	Engine             Engine

	// PayloadEncoding selects the v3 envelope payload framing for
	// engines that produce a v3 envelope (currently Pebble). Zero
	// value means "engine default" (PayloadEncodingTarZstd for Pebble).
	PayloadEncoding PayloadEncoding
}

// EngineDriver opens a .c1z file for a specific storage engine. Drivers live
// outside the dotc1z package when they carry optional dependencies. The default
// SQLite driver is registered by this package; Pebble registers from
// pkg/dotc1z/engine/pebble when callers explicitly import and register it.
type EngineDriver interface {
	Engine() Engine
	Format() C1ZFormat
	OpenStore(ctx context.Context, outputFilePath string, opts StoreOptions) (connectorstore.Writer, error)
}

type engineRegistry struct {
	mu       sync.RWMutex
	byEngine map[Engine]EngineDriver
}

var defaultEngineRegistry = &engineRegistry{
	byEngine: map[Engine]EngineDriver{
		EngineSQLite: sqliteDriver{},
	},
}

// RegisterEngine registers a storage engine driver with the process-global
// dotc1z engine registry.
func RegisterEngine(driver EngineDriver) error {
	return defaultEngineRegistry.register(driver)
}

// EngineDriverFor returns the registered driver for engine.
func EngineDriverFor(engine Engine) (EngineDriver, bool) {
	return defaultEngineRegistry.driverForEngine(engine)
}

func (r *engineRegistry) register(driver EngineDriver) error {
	if driver == nil {
		return errors.New("dotc1z: cannot register nil engine driver")
	}
	engine := driver.Engine()
	if engine == "" {
		return errors.New("dotc1z: cannot register engine driver with empty engine")
	}
	format := driver.Format()
	if format == C1ZFormatUnknown {
		return errors.New("dotc1z: cannot register engine driver with unknown format")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.byEngine[engine]; ok {
		return fmt.Errorf("dotc1z: engine %q already registered by %T", engine, existing)
	}
	r.byEngine[engine] = driver
	return nil
}

func (r *engineRegistry) driverForEngine(engine Engine) (EngineDriver, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	driver, ok := r.byEngine[engine]
	return driver, ok
}

// NewStore opens outputFilePath through the registered engine registry. It is
// the engine-neutral constructor for callers that may opt into non-default
// engines. NewC1ZFile remains the concrete SQLite constructor for legacy
// callers that need *C1File.
func NewStore(ctx context.Context, outputFilePath string, opts ...C1ZOption) (connectorstore.Writer, error) {
	options, err := buildC1ZOptions(opts...)
	if err != nil {
		return nil, err
	}
	driver, err := selectStoreDriver(outputFilePath, options)
	if err != nil {
		return nil, err
	}
	storeOptions := storeOptionsFromC1ZOptions(options)
	storeOptions.Engine = driver.Engine()
	return driver.OpenStore(ctx, outputFilePath, storeOptions)
}

func buildC1ZOptions(opts ...C1ZOption) (*c1zOptions, error) {
	options := &c1zOptions{
		encoderConcurrency: 1,
	}
	for _, opt := range opts {
		opt(options)
	}
	if options.encoderConcurrency < 0 {
		return nil, fmt.Errorf("encoder concurrency must not be negative: %d", options.encoderConcurrency)
	}
	return options, nil
}

func storeOptionsFromC1ZOptions(options *c1zOptions) StoreOptions {
	out := StoreOptions{
		TmpDir:             options.tmpDir,
		DecoderOptions:     append([]DecoderOption(nil), options.decoderOptions...),
		ReadOnly:           options.readOnly,
		EncoderConcurrency: options.encoderConcurrency,
		SyncLimit:          options.syncLimit,
		SkipCleanup:        options.skipCleanup,
		V2GrantsWriter:     options.v2GrantsWriter,
		Engine:             options.engine,
		PayloadEncoding:    options.payloadEncoding,
	}
	if out.Engine == "" {
		out.Engine = EngineSQLite
	}
	out.Pragmas = make([]StorePragma, 0, len(options.pragmas))
	for _, p := range options.pragmas {
		out.Pragmas = append(out.Pragmas, StorePragma{Name: p.name, Value: p.value})
	}
	return out
}

func selectStoreDriver(outputFilePath string, options *c1zOptions) (EngineDriver, error) {
	engine := options.engine
	if engine == "" {
		engine = EngineSQLite
	}

	stat, err := os.Stat(outputFilePath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return requireEngineDriver(engine)
	case err != nil:
		return nil, err
	case stat.Size() == 0:
		return requireEngineDriver(engine)
	}

	// Existing files dispatch by file header/manifest, independent of
	// WithEngine. That preserves the existing read-any-format semantics.
	f, err := os.Open(outputFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	format, err := ReadHeaderFormat(f)
	if err != nil {
		return nil, err
	}
	switch format {
	case C1ZFormatV1:
		return requireEngineDriver(EngineSQLite)
	case C1ZFormatV3:
		if _, err := f.Seek(0, 0); err != nil {
			return nil, err
		}
		env, err := formatv3.ReadEnvelope(f)
		if err != nil {
			return nil, err
		}
		defer env.Close()
		return requireEngineDriver(Engine(env.Manifest.GetEngine()))
	default:
		return nil, ErrInvalidFile
	}
}

func requireEngineDriver(engine Engine) (EngineDriver, error) {
	driver, ok := EngineDriverFor(engine)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrEngineNotAvailable, engine)
	}
	return driver, nil
}

type sqliteDriver struct{}

func (sqliteDriver) Engine() Engine    { return EngineSQLite }
func (sqliteDriver) Format() C1ZFormat { return C1ZFormatV1 }

func (sqliteDriver) OpenStore(ctx context.Context, outputFilePath string, opts StoreOptions) (connectorstore.Writer, error) {
	c1zOpts := []C1ZOption{
		WithEngine(EngineSQLite),
		WithEncoderConcurrency(opts.EncoderConcurrency),
	}
	if opts.TmpDir != "" {
		c1zOpts = append(c1zOpts, WithTmpDir(opts.TmpDir))
	}
	if len(opts.DecoderOptions) > 0 {
		c1zOpts = append(c1zOpts, WithDecoderOptions(opts.DecoderOptions...))
	}
	if opts.ReadOnly {
		c1zOpts = append(c1zOpts, WithReadOnly(true))
	}
	for _, p := range opts.Pragmas {
		c1zOpts = append(c1zOpts, WithPragma(p.Name, p.Value))
	}
	if opts.SyncLimit > 0 {
		c1zOpts = append(c1zOpts, WithSyncLimit(opts.SyncLimit))
	}
	if opts.SkipCleanup {
		c1zOpts = append(c1zOpts, WithSkipCleanup(true))
	}
	if opts.V2GrantsWriter {
		c1zOpts = append(c1zOpts, WithV2GrantsWriter(true))
	}
	return NewC1ZFile(ctx, outputFilePath, c1zOpts...)
}
