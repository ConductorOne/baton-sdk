package dotc1z

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"

	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// ErrStoreNotFound is returned by OpenStore when the c1z file does not exist.
var ErrStoreNotFound = errors.New("dotc1z: store file not found")

// ErrStoreEmpty is returned by OpenStore when the file exists but is empty
// (zero bytes), indicating no c1z has been written to that path yet.
var ErrStoreEmpty = errors.New("dotc1z: store file is empty")

// ErrStoreCorrupt is the sentinel matched by errors.Is when OpenStore
// determines that a c1z file exists but cannot be decoded (invalid magic,
// truncated header, unreadable v3 manifest, etc.). The full error wraps
// ErrStoreCorrupt and preserves the low-level cause via errors.As /
// errors.Unwrap.
var ErrStoreCorrupt = errors.New("dotc1z: store file is corrupt or invalid")

// StoreCorruptError is the concrete type returned by OpenStore on corruption.
// errors.Is(err, ErrStoreCorrupt) returns true for values of this type.
type StoreCorruptError struct {
	Cause error
}

func (e *StoreCorruptError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", ErrStoreCorrupt, e.Cause)
	}
	return ErrStoreCorrupt.Error()
}
func (e *StoreCorruptError) Unwrap() error { return e.Cause }
func (e *StoreCorruptError) Is(target error) bool {
	return target == ErrStoreCorrupt
}

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
	SkipVacuum         bool
	BulkLoad           bool
	V2GrantsWriter     bool
	Engine             Engine

	// PayloadEncoding selects the v3 envelope payload framing for
	// engines that produce a v3 envelope (currently Pebble). Zero
	// value means "engine default" (PayloadEncodingIndexedZstd for
	// Pebble).
	PayloadEncoding PayloadEncoding

	// DecoderPool optionally scopes v3 payload-decoder reuse to the
	// caller's operation (see WithDecoderPool). Nil means a one-shot
	// decoder per open.
	DecoderPool *EnvelopeDecoderPool

	// MaxDecodedPayloadBytes optionally caps v3 envelope payload extraction.
	// Zero means use BATON_DECODER_MAX_DECODED_SIZE_MB or the v3 default.
	MaxDecodedPayloadBytes uint64

	// MaxDecoderMemoryBytes optionally caps zstd decoder memory for v3 envelope
	// payload extraction. Zero means use BATON_DECODER_MAX_MEMORY_MB or the v3
	// default.
	MaxDecoderMemoryBytes uint64
}

// EnvelopeDecoderPool re-exports the v3 envelope payload decoder pool
// so callers (e.g. the sync compactor) can scope decoder reuse to one
// operation without importing the format package. See
// formatv3.DecoderPool for semantics.
type EnvelopeDecoderPool = formatv3.DecoderPool

// NewEnvelopeDecoderPool returns an empty pool; the caller owns its
// lifetime and must Close it when the operation completes.
func NewEnvelopeDecoderPool() *EnvelopeDecoderPool {
	return formatv3.NewDecoderPool()
}

// WithDecoderPool threads a caller-owned envelope decoder pool through
// a store open. Useful only for callers that open MANY v3 c1z files in
// one operation (the compactor opens one envelope per source per
// merge); each open draws its zstd payload decoder from the pool
// instead of constructing a fresh one. The caller must Close the pool
// when the operation finishes — that deterministic release is the
// point of scoping the pool instead of sharing one process-wide.
// Ignored by the SQLite (v1) engine and harmless when nil.
func WithDecoderPool(p *EnvelopeDecoderPool) C1ZOption {
	return func(o *c1zOptions) {
		o.decoderPool = p
	}
}

// EngineDriver opens or creates a .c1z file for a specific storage engine.
// The SQLite and Pebble drivers are both registered statically by this
// package; RegisterEngine exists for additional engines.
type EngineDriver interface {
	Engine() Engine
	Format() C1ZFormat
	// OpenStore opens outputFilePath using the driver's historical
	// open-or-create semantics.
	//
	// Deprecated: implement EngineLifecycleDriver to distinguish strict
	// existing-file opens from create-new-store calls.
	OpenStore(ctx context.Context, outputFilePath string, opts StoreOptions) (C1ZStore, error)
}

// EngineLifecycleDriver is the explicit lifecycle extension for EngineDriver.
// Implementers can distinguish strict opens from creates while old EngineDriver
// implementations keep compiling via the deprecated OpenStore fallback.
type EngineLifecycleDriver interface {
	EngineDriver
	// OpenExistingStore opens a pre-existing c1z at outputFilePath.
	OpenExistingStore(ctx context.Context, outputFilePath string, opts StoreOptions) (C1ZStore, error)
	// CreateNewStore creates a fresh c1z at outputFilePath, which must not
	// already contain a valid c1z.
	CreateNewStore(ctx context.Context, outputFilePath string, opts StoreOptions) (C1ZStore, error)
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

// OpenStore opens a .c1z file at path and returns a C1ZStore. The file must
// already contain a valid c1z; empty placeholder files are rejected with
// ErrStoreEmpty. If path exists and is non-empty, OpenStore dispatches by the
// file's on-disk format header; the caller's WithEngine option is ignored (the
// file's format is authoritative).
//
// Sentinel errors:
//   - ErrStoreNotFound — path does not exist.
//   - ErrStoreEmpty    — path exists but is empty.
//   - ErrStoreCorrupt  — file exists but cannot be decoded as a valid c1z
//     (invalid magic, truncated header, unreadable v3 manifest, etc.).
//     Use errors.As to unwrap the StoreCorruptError and inspect the cause.
//
// I/O and system failures are returned as-is and do not satisfy
// errors.Is(err, ErrStoreCorrupt). Only known decode/format failures are
// classified as corruption.
func OpenStore(ctx context.Context, path string, opts ...C1ZOption) (C1ZStore, error) {
	stat, err := os.Stat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, ErrStoreNotFound
	case err != nil:
		return nil, err
	}

	options, err := buildC1ZOptions(opts...)
	if err != nil {
		return nil, err
	}

	if stat.Size() == 0 {
		return nil, ErrStoreEmpty
	}

	driver, err := selectExistingStoreDriver(ctx, path, options)
	if err != nil {
		return nil, err
	}

	storeOptions := storeOptionsFromC1ZOptions(options)
	storeOptions.Engine = driver.Engine()
	return openExistingStoreWithDriver(ctx, driver, path, storeOptions)
}

// CreateStore creates a new .c1z file at path and returns a C1ZStore. The
// path must not already contain data; a missing path or existing empty
// placeholder file is accepted. WithEngine selects the storage engine,
// defaulting to SQLite for backward compatibility.
//
// Callers working with Pebble or mixed-format inputs should pass an explicit
// engine option rather than relying on the SQLite default.
func CreateStore(ctx context.Context, path string, opts ...C1ZOption) (C1ZStore, error) {
	stat, err := os.Stat(path)
	switch {
	case err == nil && stat.Size() > 0:
		return nil, fmt.Errorf("dotc1z: CreateStore: path already exists: %s", path)
	case err == nil:
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	}

	options, err := buildC1ZOptions(opts...)
	if err != nil {
		return nil, err
	}

	requested := options.engine
	if requested == "" {
		requested = EngineSQLite
	}

	driver, err := requireEngineDriver(requested)
	if err != nil {
		return nil, err
	}

	storeOptions := storeOptionsFromC1ZOptions(options)
	storeOptions.Engine = driver.Engine()
	return createNewStoreWithDriver(ctx, driver, path, storeOptions)
}

// NewStore opens outputFilePath through the registered engine registry. If the
// file does not exist or is empty a new store is created; otherwise the
// existing file is opened by its on-disk format.
//
// Deprecated: Use OpenStore to open an existing c1z or CreateStore to create a
// new one. NewStore's open-or-create semantics make the caller's intent
// ambiguous; the replacement constructors enforce it explicitly.
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

	stat, statErr := os.Stat(outputFilePath)
	if statErr != nil || stat.Size() == 0 {
		return createNewStoreWithDriver(ctx, driver, outputFilePath, storeOptions)
	}
	return openExistingStoreWithDriver(ctx, driver, outputFilePath, storeOptions)
}

func openExistingStoreWithDriver(ctx context.Context, driver EngineDriver, path string, opts StoreOptions) (C1ZStore, error) {
	if lifecycle, ok := driver.(EngineLifecycleDriver); ok {
		return lifecycle.OpenExistingStore(ctx, path, opts)
	}
	store, err := driver.OpenStore(ctx, path, opts)
	if err != nil {
		return nil, wrapOpenExistingStoreError(err)
	}
	return store, nil
}

func createNewStoreWithDriver(ctx context.Context, driver EngineDriver, path string, opts StoreOptions) (C1ZStore, error) {
	if lifecycle, ok := driver.(EngineLifecycleDriver); ok {
		return lifecycle.CreateNewStore(ctx, path, opts)
	}
	return driver.OpenStore(ctx, path, opts)
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
	if _, err := applyDecoderOptions(options.decoderOptions...); err != nil {
		return nil, err
	}
	return options, nil
}

func storeOptionsFromC1ZOptions(options *c1zOptions) StoreOptions {
	maxDecodedPayloadBytes, _ := explicitMaxDecodedSizeForDecoderOptions(options.decoderOptions...)
	maxDecoderMemoryBytes, _ := explicitMaxMemorySizeForDecoderOptions(options.decoderOptions...)
	out := StoreOptions{
		TmpDir:                 options.tmpDir,
		DecoderOptions:         append([]DecoderOption(nil), options.decoderOptions...),
		ReadOnly:               options.readOnly,
		EncoderConcurrency:     options.encoderConcurrency,
		SyncLimit:              options.syncLimit,
		SkipCleanup:            options.skipCleanup,
		SkipVacuum:             options.skipVacuum,
		BulkLoad:               options.bulkLoad,
		V2GrantsWriter:         options.v2GrantsWriter,
		Engine:                 options.engine,
		PayloadEncoding:        options.payloadEncoding,
		DecoderPool:            options.decoderPool,
		MaxDecodedPayloadBytes: maxDecodedPayloadBytes,
		MaxDecoderMemoryBytes:  maxDecoderMemoryBytes,
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
	defer func() {
		if f != nil {
			_ = f.Close()
		}
	}()

	format, err := ReadHeaderFormat(f)
	if err != nil {
		return nil, err
	}

	var fileEngine Engine
	switch format {
	case C1ZFormatV1:
		// Maybe error if the file is read-only?
		if requested == EnginePebble && !options.readOnly {
			// Close our header-read handle before converting: the conversion
			// renames a temp file over outputFilePath, which fails on Windows
			// if any handle to the destination is still open. Nil out f so
			// the deferred close doesn't double-close.
			closeErr := f.Close()
			f = nil
			if closeErr != nil {
				return nil, closeErr
			}
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
		// Engine dispatch only needs the small manifest header. Avoiding the
		// descriptor closure unmarshal cut the clean skewed-500 overlay profile
		// from ~3.64M to ~1.74M allocs/op; ReadManifestHeader (vs
		// ReadEnvelopeHeader) additionally skips constructing a zstd payload
		// decoder that dispatch never reads from — one wasted
		// window-allocation + worker spin-up per open.
		m, err := formatv3.ReadManifestHeader(f)
		if err != nil {
			return nil, err
		}
		fileEngine = Engine(m.GetEngine())
		if fileEngine == PebbleManifestEngine { // single-sync manifest name; same driver
			fileEngine = EnginePebble
		}
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

// selectExistingStoreDriver reads the header of an existing file and returns
// the driver for its format. Unlike selectStoreDriver it never creates a new
// store, never auto-converts SQLite to Pebble, and wraps header-read errors
// in StoreCorruptError so callers can distinguish corruption from transient I/O.
//
// The caller is responsible for ensuring the file exists and is non-empty
// before calling this function.
func selectExistingStoreDriver(ctx context.Context, outputFilePath string, options *c1zOptions) (EngineDriver, error) {
	f, err := os.Open(outputFilePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if f != nil {
			_ = f.Close()
		}
	}()

	format, err := ReadHeaderFormat(f)
	if err != nil {
		return nil, &StoreCorruptError{Cause: err}
	}

	var fileEngine Engine
	switch format {
	case C1ZFormatV1:
		fileEngine = EngineSQLite
	case C1ZFormatV3:
		if _, err := f.Seek(0, 0); err != nil {
			return nil, err
		}
		m, err := formatv3.ReadManifestHeader(f)
		if err != nil {
			return nil, &StoreCorruptError{Cause: err}
		}
		fileEngine = Engine(m.GetEngine())
		if fileEngine == PebbleManifestEngine {
			fileEngine = EnginePebble
		}
	default:
		return nil, &StoreCorruptError{Cause: ErrInvalidFile}
	}

	return requireEngineDriver(fileEngine)
}

type sqliteDriver struct{}

func (sqliteDriver) Engine() Engine    { return EngineSQLite }
func (sqliteDriver) Format() C1ZFormat { return C1ZFormatV1 }

func (d sqliteDriver) OpenStore(ctx context.Context, outputFilePath string, opts StoreOptions) (C1ZStore, error) {
	return d.openOrCreate(ctx, outputFilePath, opts)
}

// OpenExistingStore opens a pre-existing SQLite c1z by decompressing its
// zstd envelope and returning a *C1File wrapping the unpacked database.
func (d sqliteDriver) OpenExistingStore(ctx context.Context, outputFilePath string, opts StoreOptions) (C1ZStore, error) {
	store, err := d.openOrCreate(ctx, outputFilePath, opts)
	if err != nil {
		return nil, wrapOpenExistingStoreError(err)
	}
	return store, nil
}

// CreateNewStore creates a fresh SQLite c1z at outputFilePath. If the path
// does not exist or is empty, a new empty database is initialised.
func (d sqliteDriver) CreateNewStore(ctx context.Context, outputFilePath string, opts StoreOptions) (C1ZStore, error) {
	return d.openOrCreate(ctx, outputFilePath, opts)
}

// openOrCreate translates StoreOptions to C1ZOptions and delegates to
// NewC1ZFile, which handles both the empty-file (new store) and existing-file
// (decompress-and-open) cases.
func (sqliteDriver) openOrCreate(ctx context.Context, outputFilePath string, opts StoreOptions) (C1ZStore, error) {
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
	if opts.SkipVacuum {
		c1zOpts = append(c1zOpts, WithSkipVacuum(true))
	}
	if opts.BulkLoad {
		c1zOpts = append(c1zOpts, WithBulkLoad(true))
	}
	if opts.V2GrantsWriter {
		c1zOpts = append(c1zOpts, WithV2GrantsWriter(true))
	}
	return newC1ZFile(ctx, outputFilePath, c1zOpts...)
}

func wrapOpenExistingStoreError(err error) error {
	if err == nil {
		return nil
	}
	if isStoreCorruptionError(err) {
		return &StoreCorruptError{Cause: err}
	}
	return err
}

func isStoreCorruptionError(err error) bool {
	return errors.Is(err, ErrInvalidFile) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, formatv3.ErrInvalidV3Magic) ||
		errors.Is(err, formatv3.ErrEnvelopeTruncated) ||
		errors.Is(err, zstd.ErrMagicMismatch) ||
		errors.Is(err, zstd.ErrReservedBlockType) ||
		errors.Is(err, zstd.ErrCompressedSizeTooBig) ||
		errors.Is(err, zstd.ErrWindowSizeTooSmall) ||
		errors.Is(err, zstd.ErrFrameSizeMismatch) ||
		errors.Is(err, zstd.ErrCRCMismatch)
}
