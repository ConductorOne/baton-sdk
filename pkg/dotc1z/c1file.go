package dotc1z

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	// NOTE: required to register the dialect for goqu.
	//
	// If you remove this import, goqu.Dialect("sqlite3") will
	// return a copy of the default dialect, which is not what we want,
	// and allocates a ton of memory.
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"

	_ "modernc.org/sqlite"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

var ErrDbNotOpen = errors.New("c1file: database has not been opened")

type pragma struct {
	name  string
	value string
}

type C1File struct {
	rawDb              *sql.DB
	db                 *goqu.Database
	currentSyncID      string
	viewSyncID         string
	outputFilePath     string
	dbFilePath         string
	dbUpdated          bool
	tempDir            string
	pragmas            []pragma
	readOnly           bool
	encoderConcurrency int
	closed             bool
	closedMu           sync.Mutex

	// Cached sync run for listConnectorObjects (avoids N+1 queries)
	cachedViewSyncRun *SyncRun
	cachedViewSyncMu  sync.Mutex
	cachedViewSyncErr error

	// Slow query tracking
	slowQueryLogTimes     map[string]time.Time
	slowQueryLogTimesMu   sync.Mutex
	slowQueryThreshold    time.Duration
	slowQueryLogFrequency time.Duration

	// Sync cleanup settings
	syncLimit   int
	skipCleanup bool
	skipVacuum  bool

	// See WithC1FV2GrantsWriter.
	v2GrantsWriter bool

	// engine is the storage engine to use for newly created files.
	// Reads dispatch on magic byte regardless of this value. Default
	// is EngineSQLite (v1 .c1z format).
	engine Engine

	// payloadEncoding selects the v3 envelope payload framing for
	// Pebble-written files. Zero value = PayloadEncodingTarZstd
	// (default). Ignored by the SQLite engine.
	payloadEncoding PayloadEncoding
}

// *C1File satisfies connectorstore.Writer (the connector-facing contract),
// connectorstore.LatestFinishedSyncIDFetcher (narrow optional capability
// added in PR #774), and dotc1z.C1ZStore (the internal sync-pipeline
// contract asserted in c1file_store.go alongside the sub-store assertions).
var (
	_ connectorstore.Writer                      = (*C1File)(nil)
	_ connectorstore.LatestFinishedSyncIDFetcher = (*C1File)(nil)
)

type C1FOption func(*C1File)

// WithC1FTmpDir sets the temporary directory to use when cloning a sync.
// If not provided, os.TempDir() will be used.
func WithC1FTmpDir(tempDir string) C1FOption {
	return func(o *C1File) {
		o.tempDir = tempDir
	}
}

// WithC1FPragma sets a sqlite pragma for the c1z file.
func WithC1FPragma(name string, value string) C1FOption {
	return func(o *C1File) {
		o.pragmas = append(o.pragmas, pragma{name, value})
	}
}

// WithC1FReadOnly opens the c1file in read only mode.
// Write operations will return an error.
// Read only mode is faster, as it disables journaling and synchronous writes.
func WithC1FReadOnly(readOnly bool) C1FOption {
	return func(o *C1File) {
		o.readOnly = readOnly
	}
}

// WithC1FEncoderConcurrency sets the number of created encoders.
// Default is 1, which disables async encoding/concurrency.
// 0 uses GOMAXPROCS.
func WithC1FEncoderConcurrency(concurrency int) C1FOption {
	return func(o *C1File) {
		o.encoderConcurrency = concurrency
	}
}

// WithC1FSkipCleanup skips cleanup of old syncs when set to true.
func WithC1FSkipCleanup(skip bool) C1FOption {
	return func(o *C1File) {
		o.skipCleanup = skip
	}
}

// WithC1FSkipVacuum skips the VACUUM step at the end of Cleanup() when set to
// true. The old-sync delete and WAL truncate inside Cleanup still run.
//
// Low-level option for callers operating on a [C1File] directly. Most callers
// should use [WithSkipVacuum] on [NewC1ZFile] instead, which propagates this
// setting through the C1Z constructor.
//
// Trade-off: skipping VACUUM leaves freed pages on the SQLite freelist instead
// of reclaiming them, so the c1z file on disk grows across syncs with no upper
// bound until a real VACUUM runs. Use when the file is consumed immediately
// and re-encoded (e.g. iterative compaction); avoid when the c1z is intended
// to sit at rest or be read repeatedly. See also [WithC1FSkipCleanup] to skip
// the whole Cleanup step instead.
func WithC1FSkipVacuum(skip bool) C1FOption {
	return func(o *C1File) {
		o.skipVacuum = skip
	}
}

// WithC1FSyncCountLimit sets the number of syncs to keep during cleanup.
// If not set, defaults to 2 (or BATON_KEEP_SYNC_COUNT env var if set).
func WithC1FSyncCountLimit(limit int) C1FOption {
	return func(o *C1File) {
		o.syncLimit = limit
	}
}

// WithC1FEngine selects the storage engine for new .c1z files. The
// default is EngineSQLite, which keeps the legacy v1 file format and
// behavior. EnginePebble selects the v3 engine introduced by the
// storage-engine-v4 RFC.
//
// Engine selection only affects newly created files. Existing files
// dispatch on their magic byte; readers handle both v1 and v3
// regardless of this option.
func WithC1FEngine(engine Engine) C1FOption {
	return func(o *C1File) {
		o.engine = engine
	}
}

// WithC1FPayloadEncoding selects the v3 envelope payload encoding
// (TAR_ZSTD default, TAR uncompressed). No-op for SQLite engines.
func WithC1FPayloadEncoding(enc PayloadEncoding) C1FOption {
	return func(o *C1File) {
		o.payloadEncoding = enc
	}
}

// WithC1FV2GrantsWriter strips Grant.Entitlement and Grant.Principal
// from the serialized data blob at write time. Readers rebuild them
// as identity-only stubs (Id + nested Resource.Id) from the grants
// row's columns. Stubs carry no DisplayName, Annotations, Purpose,
// Slug, or traits; callers that need those must fetch the Entitlement
// or Resource directly. Readers accept both shapes regardless of this
// flag, so old and new rows coexist. Default false.
//
// Per-grant escape hatch: see unsafeForSlim. Grants carrying
// InsertResourceGrants or any ExternalResourceMatch* annotation stay
// full-blob — those code paths read non-identity fields off the
// embedded Resource / Principal.
func WithC1FV2GrantsWriter(enabled bool) C1FOption {
	return func(o *C1File) {
		o.v2GrantsWriter = enabled
	}
}

// Returns a C1File instance for the given db filepath.
func NewC1File(ctx context.Context, dbFilePath string, opts ...C1FOption) (*C1File, error) {
	ctx, span := tracer.Start(ctx, "NewC1File")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	rawDB, err := sql.Open("sqlite", dbFilePath)
	if err != nil {
		return nil, fmt.Errorf("new-c1-file: error opening raw db: %w", err)
	}
	l := ctxzap.Extract(ctx)
	l.Debug("new-c1-file: opened raw db",
		zap.String("db_file_path", dbFilePath),
	)

	// Limit to a single connection so idle pool connections don't hold WAL
	// read locks that prevent PRAGMA wal_checkpoint(TRUNCATE) from completing
	// all frames. Without this, saveC1z() can read an incomplete main db file
	// because uncheckpointed WAL frames are invisible to raw file I/O.
	rawDB.SetMaxOpenConns(1)

	db := goqu.New("sqlite3", rawDB)

	c1File := &C1File{
		rawDb:                 rawDB,
		db:                    db,
		dbFilePath:            dbFilePath,
		pragmas:               []pragma{},
		slowQueryLogTimes:     make(map[string]time.Time),
		slowQueryThreshold:    5 * time.Second,
		slowQueryLogFrequency: 1 * time.Minute,
		encoderConcurrency:    1,
	}

	for _, opt := range opts {
		opt(c1File)
	}

	// Normalize the engine zero value so downstream switch/if-eq
	// checks treat an unset engine as EngineSQLite.
	if c1File.engine == "" {
		c1File.engine = EngineSQLite
	}

	err = c1File.validateDb(ctx)
	if err != nil {
		return nil, err
	}

	err = c1File.init(ctx)
	if err != nil {
		return nil, fmt.Errorf("new-c1-file: error initializing c1file: %w", err)
	}

	return c1File, nil
}

type c1zOptions struct {
	tmpDir             string
	pragmas            []pragma
	decoderOptions     []DecoderOption
	readOnly           bool
	encoderConcurrency int
	syncLimit          int
	skipCleanup        bool
	skipVacuum         bool
	v2GrantsWriter     bool

	// engine is the storage engine to use for newly created files.
	// Reads dispatch on magic byte regardless. Default EngineSQLite.
	engine Engine

	// payloadEncoding controls the v3 envelope payload framing. Only
	// honored when engine == EnginePebble (the v3 path). Allowed
	// values: PayloadEncodingTarZstd (default), PayloadEncodingTar.
	// Zero value means PayloadEncodingTarZstd.
	payloadEncoding PayloadEncoding
}

type C1ZOption func(*c1zOptions)

// WithTmpDir sets the temporary directory to extract the c1z file to.
// If not provided, os.TempDir() will be used.
func WithTmpDir(tmpDir string) C1ZOption {
	return func(o *c1zOptions) {
		o.tmpDir = tmpDir
	}
}

// WithPragma sets a sqlite pragma for the c1z file.
func WithPragma(name string, value string) C1ZOption {
	return func(o *c1zOptions) {
		o.pragmas = append(o.pragmas, pragma{name, value})
	}
}

func WithDecoderOptions(opts ...DecoderOption) C1ZOption {
	return func(o *c1zOptions) {
		o.decoderOptions = opts
	}
}

// WithReadOnly opens the c1z file in read only mode. Modifying the c1z will result in an error on close.
func WithReadOnly(readOnly bool) C1ZOption {
	return func(o *c1zOptions) {
		o.readOnly = readOnly
	}
}

// WithEncoderConcurrency sets the number of created encoders.
// Default is 1, which disables async encoding/concurrency.
// 0 uses GOMAXPROCS.
func WithEncoderConcurrency(concurrency int) C1ZOption {
	return func(o *c1zOptions) {
		o.encoderConcurrency = concurrency
	}
}

// WithSkipCleanup skips cleanup of old syncs when set to true.
func WithSkipCleanup(skip bool) C1ZOption {
	return func(o *c1zOptions) {
		o.skipCleanup = skip
	}
}

// WithSkipVacuum skips the VACUUM step at the end of Cleanup() when set to
// true. The old-sync delete and WAL truncate inside Cleanup still run.
//
// Trade-off: skipping VACUUM leaves freed pages on the SQLite freelist instead
// of reclaiming them, so the c1z file on disk grows across syncs with no upper
// bound until a real VACUUM runs. Use when the file is consumed immediately
// and re-encoded (e.g. iterative compaction where the output is supplanted on
// the next iteration); avoid when the c1z is intended to sit at rest or be
// read repeatedly. No effect when [WithSkipCleanup] is also set, since
// Cleanup returns early in that case.
func WithSkipVacuum(skip bool) C1ZOption {
	return func(o *c1zOptions) {
		o.skipVacuum = skip
	}
}

// WithSyncLimit sets the number of syncs to keep during cleanup.
// If not set, defaults to 2 (or BATON_KEEP_SYNC_COUNT env var if set).
func WithSyncLimit(limit int) C1ZOption {
	return func(o *c1zOptions) {
		o.syncLimit = limit
	}
}

// WithEngine selects the storage engine for newly created .c1z files.
// Default is EngineSQLite (v1 format). EnginePebble enables the v3
// engine.
//
// Reading existing files dispatches on the file's magic byte and is
// independent of this option.
func WithEngine(engine Engine) C1ZOption {
	return func(o *c1zOptions) {
		o.engine = engine
	}
}

// WithV2GrantsWriter toggles the slim-blob writer path for grants.
// See WithC1FV2GrantsWriter for details.
func WithV2GrantsWriter(enabled bool) C1ZOption {
	return func(o *c1zOptions) {
		o.v2GrantsWriter = enabled
	}
}

// WithPayloadEncoding selects the c1z v3 envelope payload encoding
// for newly created files written by the Pebble engine. Default is
// PayloadEncodingTarZstd. PayloadEncodingTar skips the outer zstd
// compression — useful when Pebble's L5/L6 SSTs are already
// zstd-compressed at the engine layer, or when the storage target
// (S3 with Content-Encoding negotiation, etc.) compresses in transit.
//
// No-op for SQLite engines; the encoding selector applies only to
// the v3 envelope written by Pebble.
func WithPayloadEncoding(enc PayloadEncoding) C1ZOption {
	return func(o *c1zOptions) {
		o.payloadEncoding = enc
	}
}

// Returns a new C1File instance with its state stored at the provided filename.
func NewC1ZFile(ctx context.Context, outputFilePath string, opts ...C1ZOption) (*C1File, error) {
	ctx, span := tracer.Start(ctx, "NewC1ZFile")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	options, err := buildC1ZOptions(opts...)
	if err != nil {
		return nil, err
	}

	dbFilePath, _, err := decompressC1z(outputFilePath, options.tmpDir, options.decoderOptions...)
	if err != nil {
		return nil, err
	}
	l := ctxzap.Extract(ctx)
	l.Debug("new-c1z-file: decompressed c1z",
		zap.String("db_file_path", dbFilePath),
		zap.String("output_file_path", outputFilePath),
	)

	var c1fopts []C1FOption
	if options.tmpDir != "" {
		c1fopts = append(c1fopts, WithC1FTmpDir(options.tmpDir))
	}
	for _, pragma := range options.pragmas {
		c1fopts = append(c1fopts, WithC1FPragma(pragma.name, pragma.value))
	}
	if options.readOnly {
		c1fopts = append(c1fopts, WithC1FReadOnly(true))
	}
	c1fopts = append(c1fopts, WithC1FEncoderConcurrency(options.encoderConcurrency))
	if options.syncLimit > 0 {
		c1fopts = append(c1fopts, WithC1FSyncCountLimit(options.syncLimit))
	}
	if options.skipCleanup {
		c1fopts = append(c1fopts, WithC1FSkipCleanup(true))
	}
	if options.skipVacuum {
		c1fopts = append(c1fopts, WithC1FSkipVacuum(true))
	}
	if options.v2GrantsWriter {
		c1fopts = append(c1fopts, WithC1FV2GrantsWriter(true))
	}
	if options.engine != "" {
		c1fopts = append(c1fopts, WithC1FEngine(options.engine))
	}
	if options.payloadEncoding != PayloadEncodingUnspecified {
		c1fopts = append(c1fopts, WithC1FPayloadEncoding(options.payloadEncoding))
	}

	c1File, err := NewC1File(ctx, dbFilePath, c1fopts...)
	if err != nil {
		return nil, cleanupDbDir(dbFilePath, err)
	}

	c1File.outputFilePath = outputFilePath

	return c1File, nil
}

func cleanupDbDir(dbFilePath string, err error) error {
	// Stat dbFilePath to make sure it's a file, not a directory.
	stat, statErr := os.Stat(dbFilePath)
	if statErr != nil {
		if errors.Is(statErr, os.ErrNotExist) {
			// If the file doesn't exist, we can't clean up the directory.
			return err
		}
		return errors.Join(err, fmt.Errorf("cleanupDbDir: error statting dbFilePath %s: %w", dbFilePath, statErr))
	}
	if stat.IsDir() {
		// If the file is a directory, don't try to clean up the parent directory.
		return errors.Join(err, fmt.Errorf("cleanupDbDir: dbFilePath %s is a directory, not a file: %w", dbFilePath, statErr))
	}

	cleanupErr := os.RemoveAll(filepath.Dir(dbFilePath))
	if cleanupErr != nil {
		err = errors.Join(err, cleanupErr)
	}
	return err
}

var ErrReadOnly = errors.New("c1z: read only mode")

// Close ensures that the sqlite database is flushed to disk, and if any
// changes were made we update the original database with our changes.
//
// When there is real finalize work to do (rawDb open, dbUpdated, not
// read-only), the WAL checkpoint, raw-db close, and saveC1z all run on a
// detached context bounded by FinalizeTimeout(). Caller cancellation
// cannot truncate the c1z mid-finalize; the new-root finalize span links
// back to the caller's trace for navigability without bloating the
// parent trace.
//
//nolint:nonamedreturns // named return required so the deferred span captures all early-return error paths
func (c *C1File) Close(ctx context.Context) (retErr error) {
	ctx, span := tracer.Start(ctx, "C1File.Close")
	defer func() { uotel.EndSpanWithError(span, retErr) }()

	l := ctxzap.Extract(ctx)

	c.closedMu.Lock()
	defer c.closedMu.Unlock()
	if c.closed {
		l.Debug("close called on already-closed c1file", zap.String("db_path", c.dbFilePath))
		return nil
	}

	span.SetAttributes(
		attribute.Bool("read_only", c.readOnly),
		attribute.Bool("db_updated", c.dbUpdated),
		attribute.String("db_path", c.dbFilePath),
	)

	// Cheap paths (no save-to-disk work) keep the caller's context and
	// span topology. Read-only opens never write the c1z back, even if
	// the c1z was opened without being read; the same is true when no
	// mutations occurred. Every cheap-path branch closes c.rawDb (when
	// open) before returning so a misuse like opening read-only and
	// then dirtying via an attached-db mutation still releases the
	// SQLite handle and any FDs/goroutines it owns.
	if !c.dbUpdated || c.readOnly {
		if c.rawDb != nil {
			if err := c.closeRawDB(ctx); err != nil {
				return cleanupDbDir(c.dbFilePath, err)
			}
		}
		if c.dbUpdated && c.readOnly {
			c.closed = true
			return cleanupDbDir(c.dbFilePath, ErrReadOnly)
		}
		if err := cleanupDbDir(c.dbFilePath, nil); err != nil {
			return err
		}
		c.closed = true
		return nil
	}

	if err := c.finalize(ctx); err != nil {
		return err
	}
	c.closed = true
	return nil
}

// finalize runs the WAL-checkpoint → close-raw-db → saveC1z sequence on a
// context that is detached from the caller's cancellation. The detached
// context is bounded by FinalizeTimeout() for the steps that observe
// ctx (truncateWAL, closeRawDB); saveC1z itself does not take ctx, so
// its encoder phase is not cancellable today and the upper bound on
// total finalize wall-clock is effectively FinalizeTimeout +
// saveC1z-encoder-time. The finalize span is a new root linked to the
// caller's trace so very long syncs don't end up with the upload
// subtree inflating their span counts.
//
// If the caller propagated a parent ctx error label via
// uotel.WithParentCtxErrLabel (e.g. syncer.Close did so before
// detaching), that label wins over re-classifying ctx.Err() — useful
// because by the time we reach here the caller's ctx may already be
// the syncer's detached finalizeCtx, which always reports "nil".
func (c *C1File) finalize(ctx context.Context) error {
	parentCtxErrLabel := uotel.ClassifyCtxErr(ctx.Err())
	if propagated, ok := uotel.ParentCtxErrLabel(ctx); ok {
		parentCtxErrLabel = propagated
	}

	timeout := FinalizeTimeout()
	finalizeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	finalizeCtx, finalizeSpan := uotel.StartWithLink(finalizeCtx, tracer, "C1File.finalize")
	var finalizeErr error
	defer func() { uotel.EndSpanWithError(finalizeSpan, finalizeErr) }()
	finalizeSpan.SetAttributes(
		attribute.Bool("c1z.finalize.cancel_observed", parentCtxErrLabel != "nil"),
		attribute.String("c1z.finalize.parent_ctx_err", parentCtxErrLabel),
		attribute.Int64("c1z.finalize.timeout_seconds", int64(timeout.Seconds())),
		attribute.String("c1z.finalize.db_path", c.dbFilePath),
	)

	l := ctxzap.Extract(finalizeCtx)

	// Only WAL-checkpoint and close the raw DB if a handle is open.
	// Some callers (notably TestC1ZDecoder) manually close c.rawDb to
	// force a checkpoint before calling Close — that path skips both
	// operations here and proceeds directly to saveC1z.
	if c.rawDb != nil {
		// CRITICAL: Force a full WAL checkpoint before closing the database.
		// This ensures all WAL data is written back to the main database file
		// and the writes are synced to disk. Without this, on filesystems with
		// aggressive caching (like ZFS with large ARC), the subsequent saveC1z()
		// read could see stale data because the checkpoint writes may still be
		// in kernel buffers. TRUNCATE mode checkpoints as many frames as possible
		// then truncates the WAL file to zero bytes.
		busy, log, checkpointed, err := c.truncateWAL(finalizeCtx)
		if err != nil {
			finalizeErr = fmt.Errorf("c1z: WAL checkpoint failed: %w", err)
			l.Error("WAL checkpoint failed before close",
				zap.Error(err),
				zap.String("db_path", c.dbFilePath))
			if closeErr := c.closeRawDB(finalizeCtx); closeErr != nil {
				l.Error("error closing raw db", zap.Error(closeErr))
			}
			return cleanupDbDir(c.dbFilePath, finalizeErr)
		}
		if busy != 0 || (log >= 0 && checkpointed < log) {
			finalizeErr = fmt.Errorf("c1z: WAL checkpoint incomplete: busy=%d log=%d checkpointed=%d", busy, log, checkpointed)
			l.Error("WAL checkpoint incomplete before close",
				zap.Int("busy", busy),
				zap.Int("log", log),
				zap.Int("checkpointed", checkpointed),
				zap.String("db_path", c.dbFilePath))
			if closeErr := c.closeRawDB(finalizeCtx); closeErr != nil {
				l.Error("error closing raw db", zap.Error(closeErr))
			}
			return cleanupDbDir(c.dbFilePath, finalizeErr)
		}

		if err := c.closeRawDB(finalizeCtx); err != nil {
			finalizeErr = err
			return cleanupDbDir(c.dbFilePath, err)
		}
	}

	// Verify WAL was fully checkpointed. If it still has data, saveC1z
	// would create a c1z missing the WAL contents since it only reads
	// the main database file.
	walPath := c.dbFilePath + "-wal"
	if walInfo, statErr := os.Stat(walPath); statErr == nil && walInfo.Size() > 0 {
		finalizeErr = fmt.Errorf("c1z: WAL file not empty after close (size=%d) - refusing to save incomplete data", walInfo.Size())
		return cleanupDbDir(c.dbFilePath, finalizeErr)
	}

	saveCtx, saveSpan := tracer.Start(finalizeCtx, "C1File.saveC1z")
	saveSpan.SetAttributes(
		attribute.String("db_path", c.dbFilePath),
		attribute.String("output_path", c.outputFilePath),
		attribute.Int("encoder_concurrency", c.encoderConcurrency),
	)
	if dbInfo, statErr := os.Stat(c.dbFilePath); statErr == nil {
		saveSpan.SetAttributes(attribute.Int64("input_bytes", dbInfo.Size()))
		finalizeSpan.SetAttributes(attribute.Int64("c1z.finalize.input_bytes", dbInfo.Size()))
	}
	_ = saveCtx // saveC1z doesn't currently take ctx; saveCtx is kept so any future span inside saveC1z attaches to saveSpan, not finalizeSpan.
	saveErr := saveC1z(c.dbFilePath, c.outputFilePath, c.encoderConcurrency)
	uotel.EndSpanWithError(saveSpan, saveErr)
	if saveErr != nil {
		finalizeErr = saveErr
		return cleanupDbDir(c.dbFilePath, saveErr)
	}
	if outInfo, statErr := os.Stat(c.outputFilePath); statErr == nil {
		finalizeSpan.SetAttributes(attribute.Int64("c1z.finalize.output_bytes", outInfo.Size()))
	}

	if err := cleanupDbDir(c.dbFilePath, nil); err != nil {
		finalizeErr = err
		return err
	}
	return nil
}

// closeRawDB wraps c.rawDb.Close with a span and drops the handle
// references on the C1File so callers do not have to repeat the
// nil-out. Returns the error from rawDb.Close so error paths can
// still propagate or log it.
func (c *C1File) closeRawDB(ctx context.Context) error {
	_, span := tracer.Start(ctx, "C1File.closeRawDB")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()
	err = c.rawDb.Close()
	c.rawDb = nil
	c.db = nil
	return err
}

// truncateWAL truncates the WAL file.
// Returns the busy, log, and checkpointed values.
func (c *C1File) truncateWAL(ctx context.Context) (int, int, int, error) {
	ctx, span := tracer.Start(ctx, "C1File.truncateWAL")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	// Use QueryRowContext to read the (busy, log, checkpointed) result.
	// ExecContext silently discards these values, making partial
	// checkpoints undetectable — the PRAGMA returns nil error even when
	// it can't checkpoint all frames due to concurrent readers.
	var busy, log, checkpointed int
	row := c.rawDb.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	if err := row.Scan(&busy, &log, &checkpointed); err != nil {
		return 0, 0, 0, err
	}
	// TODO: Return an error here?
	if busy != 0 || (log >= 0 && checkpointed < log) {
		ctxzap.Extract(ctx).Info("WAL checkpoint incomplete",
			zap.Int("busy", busy),
			zap.Int("log", log),
			zap.Int("checkpointed", checkpointed),
			zap.String("db_path", c.dbFilePath))
	}
	return busy, log, checkpointed, nil
}

// init ensures that the database has all of the required schema.
func (c *C1File) init(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "C1File.init")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	l := ctxzap.Extract(ctx)

	err = c.validateDb(ctx)
	if err != nil {
		return err
	}

	shouldOptimize, err := c.InitTables(ctx)
	if err != nil {
		l.Error("c1file-init: error initializing tables", zap.Error(err))
		return err
	}
	l.Debug("c1file-init: initialized tables",
		zap.String("db_file_path", c.dbFilePath),
	)

	// // Checkpoint the WAL after migrations. Migrations like backfillGrantExpansionColumn
	// // can update many rows, filling the WAL. Without a checkpoint, subsequent reads are
	// // slow because SQLite must scan the WAL hash table for every page read.
	if _, err = c.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		l.Warn("c1file-init: WAL checkpoint after init failed", zap.Error(err))
	}

	// Optimize DB. Desired after running migrations to improve performance.
	if shouldOptimize {
		l.Debug("c1file-init: optimizing database", zap.String("db_file_path", c.dbFilePath))
		startTime := time.Now()
		_, err = c.db.ExecContext(ctx, "PRAGMA optimize")
		if err != nil {
			return err
		}
		l.Debug("c1file-init: optimized database",
			zap.Duration("time_taken", time.Since(startTime)),
			zap.String("db_file_path", c.dbFilePath),
		)
	}

	if c.readOnly {
		// Disable journaling in read only mode, since we're not writing to the database.
		_, err = c.db.ExecContext(ctx, "PRAGMA journal_mode = OFF")
		if err != nil {
			return err
		}
		// Disable synchronous writes in read only mode, since we're not writing to the database.
		_, err = c.db.ExecContext(ctx, "PRAGMA synchronous = OFF")
		if err != nil {
			return err
		}
	} else {
		// Use NORMAL synchronous mode instead of FULL (default).
		// Normal is faster and only unsafe on old filesystems.
		_, err = c.db.ExecContext(ctx, "PRAGMA synchronous = NORMAL")
		if err != nil {
			return err
		}

		// Use TRUNCATE journal mode instead of DELETE (default).
		// User-specified pragmas such as journal_mode=WAL will override this.
		_, err = c.db.ExecContext(ctx, "PRAGMA journal_mode = TRUNCATE")
		if err != nil {
			return err
		}
	}

	hasLockingPragma := false
	for _, pragma := range c.pragmas {
		pragmaName := strings.ToLower(pragma.name)
		if pragmaName == "main.locking_mode" || pragmaName == "locking_mode" {
			hasLockingPragma = true
			break
		}
	}
	if !hasLockingPragma {
		l.Debug("c1file-init: setting locking mode to EXCLUSIVE", zap.String("db_file_path", c.dbFilePath))
		_, err = c.db.ExecContext(ctx, "PRAGMA main.locking_mode = EXCLUSIVE")
		if err != nil {
			return fmt.Errorf("c1file-init: error setting locking mode to EXCLUSIVE: %w", err)
		}
	}

	for _, pragma := range c.pragmas {
		_, err := c.db.ExecContext(ctx, fmt.Sprintf("PRAGMA %s = %s", pragma.name, pragma.value))
		if err != nil {
			return fmt.Errorf("c1file-init: error setting pragma %s = %s: %w", pragma.name, pragma.value, err)
		}
	}

	return nil
}

func getSchemaVersion(ctx context.Context, db *goqu.Database) (int, error) {
	rows, err := db.QueryContext(ctx, "SELECT schema_version FROM pragma_schema_version;")
	if err != nil {
		return 0, fmt.Errorf("c1file-init-tables: error getting schema version: %w", err)
	}
	defer rows.Close()

	var schemaVersion int
	for rows.Next() {
		err = rows.Scan(&schemaVersion)
		if err != nil {
			return 0, fmt.Errorf("c1file-init-tables: error scanning schema version: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("c1file-init-tables: error iterating schema version rows: %w", err)
	}

	return schemaVersion, nil
}

// InitTables initializes the tables in the database.
// Returns true if the any migrations were run, false otherwise.
func (c *C1File) InitTables(ctx context.Context) (bool, error) {
	ctx, span := tracer.Start(ctx, "C1File.InitTables")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	shouldOptimize := false
	err = c.validateDb(ctx)
	if err != nil {
		return false, err
	}

	l := ctxzap.Extract(ctx).With(zap.String("db_file_path", c.dbFilePath))

	// Get schema version before creating tables/indexes/running migrations.
	schemaVersion, err := getSchemaVersion(ctx, c.db)
	if err != nil {
		return false, fmt.Errorf("c1file-init-tables: error getting schema version: %w", err)
	}

	for _, t := range allTableDescriptors {
		query, args := t.Schema()

		startTime := time.Now()
		_, err = c.db.ExecContext(ctx, fmt.Sprintf(query, args...))
		if err != nil {
			l.Error("c1file-init-tables: error initializing table schema", zap.Error(err), zap.String("table_name", t.Name()))
			return false, fmt.Errorf("c1file-init-tables: error initializing table %s: %w", t.Name(), err)
		}
		l.Debug("c1file-init-tables: initialized table",
			zap.String("table_name", t.Name()),
			zap.Duration("time_taken", time.Since(startTime)),
		)

		startTime = time.Now()
		migrated, err := t.Migrations(ctx, c.db)
		if err != nil {
			l.Error("c1file-init-tables: error running migrations", zap.Error(err), zap.String("table_name", t.Name()))
			return false, fmt.Errorf("c1file-init-tables: error running migrations for table %s: %w", t.Name(), err)
		}
		l.Debug("c1file-init-tables: ran migrations",
			zap.String("table_name", t.Name()),
			zap.Duration("time_taken", time.Since(startTime)),
			zap.Bool("migrated", migrated),
		)
		if migrated {
			shouldOptimize = true
		}
	}

	if !shouldOptimize {
		newSchemaVersion, err := getSchemaVersion(ctx, c.db)
		if err != nil {
			return false, fmt.Errorf("c1file-init-tables: error getting schema version: %w", err)
		}
		if newSchemaVersion > schemaVersion {
			shouldOptimize = true
		}
	}

	return shouldOptimize, nil
}

func statsToMap(stats *reader_v2.SyncStats, syncType connectorstore.SyncType) map[string]int64 {
	out := map[string]int64{
		"resource_types": stats.GetResourceTypes(),
	}
	if syncType != connectorstore.SyncTypeResourcesOnly {
		out["entitlements"] = stats.GetEntitlements()
		out["grants"] = stats.GetGrants()
	}
	for rt, n := range stats.GetResourcesByResourceType() {
		out[rt] = n
	}
	return out
}

// Stats introspects the database and returns the count of objects for the given sync run.
// If syncId is empty, it will use the latest sync run of the given type.
func (c *C1File) Stats(ctx context.Context, syncType connectorstore.SyncType, syncId string) (map[string]int64, error) {
	sync, stats, err := c.stats(ctx, syncType, syncId, false)
	if err != nil {
		return nil, err
	}
	return statsToMap(stats, connectorstore.SyncType(sync.GetSyncType())), nil
}

func (c *C1File) stats(ctx context.Context, syncType connectorstore.SyncType, syncId string, forceRefresh bool) (*reader_v2.SyncRun, *reader_v2.SyncStats, error) {
	ctx, span := tracer.Start(ctx, "C1File.Stats")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if syncId == "" {
		syncId, err = c.LatestSyncID(ctx, syncType)
		if err != nil {
			return nil, nil, err
		}
	}
	resp, err := c.GetSync(ctx, reader_v2.SyncsReaderServiceGetSyncRequest_builder{SyncId: syncId}.Build())
	if err != nil {
		return nil, nil, err
	}
	if resp == nil || !resp.HasSync() {
		return nil, nil, status.Errorf(codes.NotFound, "sync '%s' not found", syncId)
	}
	sync := resp.GetSync()
	if syncType != connectorstore.SyncTypeAny && syncType != connectorstore.SyncType(sync.GetSyncType()) {
		return nil, nil, status.Errorf(codes.InvalidArgument, "sync '%s' is not of type '%s'", syncId, syncType)
	}
	syncType = connectorstore.SyncType(sync.GetSyncType())

	stats := sync.GetStats()
	if stats != nil && stats.GetResourceTypes() > 0 && !forceRefresh {
		return sync, stats, nil
	}

	// Slow path: Calculate sync stats and save them to the sync run so subsequent stats calls
	stats = &reader_v2.SyncStats{
		ResourceTypes:           0,
		Resources:               0,
		Entitlements:            0,
		Grants:                  0,
		ResourcesByResourceType: make(map[string]int64),
		GrantsByResourceType:    make(map[string]int64),
	}

	var rtStats []*v2.ResourceType
	pageToken := ""
	for {
		resp, err := c.ListResourceTypes(ctx, v2.ResourceTypesServiceListResourceTypesRequest_builder{
			PageToken:    pageToken,
			ActiveSyncId: syncId,
		}.Build())
		if err != nil {
			return nil, nil, err
		}

		rtStats = append(rtStats, resp.GetList()...)

		if resp.GetNextPageToken() == "" {
			break
		}

		pageToken = resp.GetNextPageToken()
	}
	stats.ResourceTypes = int64(len(rtStats))

	// Pre-populate every known resource type with 0 so the result map
	// always carries an entry per resource type, even if the sync has no
	// rows of that type. The GROUP BY below only emits resource_type_ids
	// that have at least one row.
	for _, rt := range rtStats {
		stats.ResourcesByResourceType[rt.GetId()] = 0
	}
	resourceCounts, err := c.countBySyncAndResourceType(ctx, resources.Name(), syncId)
	if err != nil {
		return nil, nil, err
	}
	for rtID, n := range resourceCounts {
		// Only surface counts for resource types in the catalog —
		// matches the prior per-type loop, which only wrote keys it
		// iterated over. Stray resource_type_ids in the table (which
		// shouldn't normally exist) are intentionally ignored here.
		if _, known := stats.ResourcesByResourceType[rtID]; known {
			stats.ResourcesByResourceType[rtID] = n
			stats.Resources += n
		}
	}

	if syncType != connectorstore.SyncTypeResourcesOnly {
		entitlementsCount, err := c.db.From(entitlements.Name()).
			Where(goqu.C("sync_id").Eq(syncId)).
			CountContext(ctx)
		if err != nil {
			return nil, nil, err
		}
		stats.Entitlements = entitlementsCount

		grantCounts, err := c.countBySyncAndResourceType(ctx, grants.Name(), syncId)
		if err != nil {
			return nil, nil, fmt.Errorf("count grants: %w", err)
		}
		stats.GrantsByResourceType = grantCounts

		for _, grantsCount := range grantCounts {
			stats.Grants += grantsCount
		}
	}

	// If sync is ended and c1z is not read-only, save stats to the database.
	if sync.GetEndedAt() != nil && !c.readOnly {
		statsJSON, err := json.Marshal(stats)
		if err != nil {
			return nil, nil, fmt.Errorf("c1file-stats: error marshalling stats: %w", err)
		}
		q := c.db.Update(syncRuns.Name()).Set(goqu.Record{
			"stats": string(statsJSON),
		}).Where(goqu.C("sync_id").Eq(syncId))
		query, args, err := q.ToSQL()
		if err != nil {
			return nil, nil, fmt.Errorf("c1file-stats: error building SQL: %w", err)
		}
		_, err = c.db.ExecContext(ctx, query, args...)
		if err != nil {
			return nil, nil, fmt.Errorf("c1file-stats: error saving stats: %w", err)
		}
		c.dbUpdated = true
	}

	return sync, stats, nil
}

// grantStats introspects the database and returns the count of grants for the given sync run.
// If syncId is empty, it will use the latest sync run of the given type.
func (c *C1File) grantStats(ctx context.Context, syncType connectorstore.SyncType, syncId string) (map[string]int64, error) {
	ctx, span := tracer.Start(ctx, "C1File.GrantStats")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	_, stats, err := c.stats(ctx, syncType, syncId, false)
	if err != nil {
		return nil, err
	}

	statsMap := map[string]int64{}
	for rt, n := range stats.GetGrantsByResourceType() {
		statsMap[rt] = n
	}

	return statsMap, nil
}

// countBySyncAndResourceType issues a single GROUP BY query that returns
// per-resource-type row counts for a sync. It replaces what used to be
// a per-resource-type loop of N COUNT(*) queries.
//
// We chose this over adding a (sync_id, resource_type_id) covering index
// because grants tables can hold tens of millions of rows and the index
// maintenance cost on every grant insert isn't worth a stats-call
// speedup that's a small fraction of overall sync runtime. The single
// GROUP BY collapses N round-trips through goqu/sql/driver into one
// without changing the schema or insert hot path.
//
// The returned map only contains entries for resource_type_ids that
// actually appear in the table. Callers that want zero-rows for missing
// types must pre-populate them; this keeps the helper simple and
// independent of the resource-type catalog.
func (c *C1File) countBySyncAndResourceType(
	ctx context.Context, tableName string, syncID string,
) (map[string]int64, error) {
	q := c.db.From(tableName).
		Where(goqu.C("sync_id").Eq(syncID)).
		Select(goqu.C("resource_type_id"), goqu.COUNT("*")).
		GroupBy(goqu.C("resource_type_id"))

	query, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]int64)
	for rows.Next() {
		var rtID string
		var n int64
		if err := rows.Scan(&rtID, &n); err != nil {
			return nil, err
		}
		out[rtID] = n
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// validateDb ensures that the database has been opened.
func (c *C1File) validateDb(ctx context.Context) error {
	if c.db == nil {
		return ErrDbNotOpen
	}

	return nil
}

// validateSyncDb ensures that there is a sync currently running, and that the database has been opened.
func (c *C1File) validateSyncDb(ctx context.Context) error {
	if c.currentSyncID == "" {
		return fmt.Errorf("c1file: sync is not active")
	}

	return c.validateDb(ctx)
}

func (c *C1File) OutputFilepath() (string, error) {
	if c.outputFilePath == "" {
		return "", fmt.Errorf("c1file: output file path is empty")
	}
	return c.outputFilePath, nil
}

// Metadata describes the storage backing this c1z. C1File is the
// SQLite-engine implementation; the v1 file format is implied by
// the engine. No I/O is performed.
func (c *C1File) Metadata() connectorstore.StoreMetadata {
	engine := c.engine
	if engine == "" {
		engine = EngineSQLite
	}
	return connectorstore.StoreMetadata{
		Engine: string(engine),
		Format: C1ZFormatV1.String(),
		// PayloadEncoding is v3-only; v1 has no envelope framing.
	}
}

// CurrentDBSizeBytes returns the current total on-disk size of the underlying
// uncompressed sqlite database, including the write-ahead log if present.
// Used by operational tooling (e.g. the grant-expansion progress logger) to
// observe c1z growth during long in-process writes without waiting for
// saveC1z to land a new frame.
//
// The WAL file holds writes that have not yet been checkpointed into the main
// database file; with journal_mode=WAL the main file may stay stable for long
// stretches while the WAL grows into the hundreds of MB. Summing both gives a
// representative "bytes written so far" figure during active expansion.
//
// This is the *uncompressed* size. The post-saveC1z c1z file size (compressed)
// is smaller; for that, see the `c1z: saved` log line emitted by saveC1z.
func (c *C1File) CurrentDBSizeBytes() (int64, error) {
	if c.dbFilePath == "" {
		return 0, fmt.Errorf("c1file: db file path is empty")
	}
	fi, err := os.Stat(c.dbFilePath)
	if err != nil {
		return 0, err
	}
	total := fi.Size()
	// Add the WAL sidecar if it exists. `os.ErrNotExist` is expected (no WAL
	// or journal_mode != WAL). Any *other* error — permission, EIO, stale
	// handle, etc. — we surface: a silently-underreported WAL would defeat
	// the growth-visibility purpose of this method (could hide hundreds of
	// MB of pending writes).
	switch wal, err := os.Stat(c.dbFilePath + "-wal"); {
	case err == nil:
		total += wal.Size()
	case errors.Is(err, os.ErrNotExist):
		// no WAL — fine.
	default:
		return 0, fmt.Errorf("c1file: stat wal sidecar: %w", err)
	}
	return total, nil
}

// Compile-time assertion that *C1File satisfies the DBSizeProvider capability
// that ProgressLog.LogExpandProgress type-asserts against. Catches signature
// drift (e.g. if someone adds a ctx parameter to CurrentDBSizeBytes) at
// compile time instead of silently turning off the expand-log size fields.
var _ connectorstore.DBSizeProvider = (*C1File)(nil)

func (c *C1File) AttachFile(other *C1File, dbName string) (*C1FileAttached, error) {
	_, err := c.db.Exec(`ATTACH DATABASE ? AS ?`, other.dbFilePath, dbName)
	if err != nil {
		return nil, err
	}

	return &C1FileAttached{
		safe: true,
		file: c,
	}, nil
}

func (c *C1FileAttached) DetachFile(dbName string) (*C1FileAttached, error) {
	_, err := c.file.db.Exec(`DETACH DATABASE ?`, dbName)
	if err != nil {
		return nil, err
	}

	return &C1FileAttached{
		safe: false,
		file: c.file,
	}, nil
}
