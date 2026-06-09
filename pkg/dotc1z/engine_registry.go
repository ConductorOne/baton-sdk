package dotc1z

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

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

// EngineDriver opens a .c1z file for a specific storage engine. The SQLite
// and Pebble drivers are both registered statically by this package;
// RegisterEngine exists for additional engines.
type EngineDriver interface {
	Engine() Engine
	Format() C1ZFormat
	OpenStore(ctx context.Context, outputFilePath string, opts StoreOptions) (C1ZStore, error)
}

type engineRegistry struct {
	mu       sync.RWMutex
	byEngine map[Engine]EngineDriver
}

var defaultEngineRegistry = &engineRegistry{
	byEngine: map[Engine]EngineDriver{
		EngineSQLite: sqliteDriver{},
		EnginePebble: pebbleDriver{},
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
func NewStore(ctx context.Context, outputFilePath string, opts ...C1ZOption) (C1ZStore, error) {
	options, err := buildC1ZOptions(opts...)
	if err != nil {
		return nil, err
	}
	driver, err := selectStoreDriver(ctx, outputFilePath, options)
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

// selectStoreDriver picks the engine driver for outputFilePath.
//
// Dispatch policy (in order):
//
//  1. If the file doesn't exist or is empty, honor the caller's
//     `WithEngine(...)` choice (defaulting to EngineSQLite when
//     unset). The about-to-be-written file gets the requested format.
//  2. If the file exists with content, dispatch by the on-disk magic
//     byte — v1 → SQLite, v3 → whatever engine name the manifest
//     records. The caller's `WithEngine` choice is overridden in this
//     case because we can't re-encode an existing file at open time;
//     the on-disk format is authoritative. This preserves the
//     read-any-format semantics that pre-dates the engine option.
//
// When the caller's WithEngine disagrees with the on-disk format we
// log a warning so the divergence is observable. Callers that want
// to *fail* on engine mismatch (vs silently switching) should stat
// the file and read the header themselves before calling NewStore.
func selectStoreDriver(ctx context.Context, outputFilePath string, options *c1zOptions) (EngineDriver, error) {
	l := ctxzap.Extract(ctx)
	requested := options.engine
	if requested == "" {
		requested = EngineSQLite
	}

	stat, err := os.Stat(outputFilePath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return requireEngineDriver(requested)
	case err != nil:
		return nil, err
	case stat.Size() == 0:
		return requireEngineDriver(requested)
	}

	f, err := os.Open(outputFilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	format, err := ReadHeaderFormat(f)
	if err != nil {
		return nil, err
	}

	var fileEngine Engine
	switch format {
	case C1ZFormatV1:
		if shouldConvertSQLiteToPebble(requested, options.readOnly, true) {
			l.Debug("converting existing v1 c1z to pebble", zap.String("output_file_path", outputFilePath))
			if err := convertExistingV1C1ZFile(ctx, outputFilePath, pebbleOpenOptionsFromC1Z(options)); err != nil {
				return nil, fmt.Errorf("select-store-driver: convert existing v1 c1z to pebble: %w", err)
			}
			l.Debug("converted existing v1 c1z to pebble", zap.String("output_file_path", outputFilePath))
			return requireEngineDriver(EnginePebble)
		}
		fileEngine = EngineSQLite
	case C1ZFormatV3:
		if _, err := f.Seek(0, 0); err != nil {
			return nil, err
		}
		env, err := formatv3.ReadEnvelope(f)
		if err != nil {
			return nil, err
		}
		defer env.Close()
		fileEngine = Engine(env.Manifest.GetEngine())
	default:
		return nil, ErrInvalidFile
	}

	if options.engine != "" && options.engine != fileEngine {
		l.Warn("dotc1z: WithEngine overridden by on-disk file format; using engine recorded in the file",
			zap.String("path", outputFilePath),
			zap.String("requested_engine", string(options.engine)),
			zap.String("file_engine", string(fileEngine)),
			zap.String("file_format", format.String()),
		)
	}
	return requireEngineDriver(fileEngine)
}

func requireEngineDriver(engine Engine) (EngineDriver, error) {
	driver, ok := EngineDriverFor(engine)
	if !ok {
		return nil, fmt.Errorf("require-engine-driver: %w: %s", ErrEngineNotAvailable, engine)
	}
	return driver, nil
}

type sqliteDriver struct{}

func (sqliteDriver) Engine() Engine    { return EngineSQLite }
func (sqliteDriver) Format() C1ZFormat { return C1ZFormatV1 }

func (sqliteDriver) OpenStore(ctx context.Context, outputFilePath string, opts StoreOptions) (C1ZStore, error) {
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
