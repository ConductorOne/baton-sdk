package dotc1z

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// pebbleDriver is the EngineDriver for the Pebble v3 engine.
type pebbleDriver struct{}

var _ c1zstore.Store = (*pebbleStore)(nil)
var _ connectorstore.Writer = (*pebbleStore)(nil)

// Local mirrors of the optional capabilities the c1z sanitizer probes on
// the source/destination store (pkg/c1zsanitize keeps those interfaces
// unexported). The assertions below make a refactor that drops one of these
// methods from either engine break the build here, rather than silently
// disarming the sanitizer's sync-graph-metadata preservation.
type sanitizeSyncLinkWriter interface {
	SetSyncLink(ctx context.Context, syncID string, linkedSyncID string) error
}
type sanitizeSupportsDiffWriter interface {
	SetSupportsDiff(ctx context.Context, syncID string) error
}
type sanitizeSyncRunMetadataReader interface {
	ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
}

var (
	_ sanitizeSyncLinkWriter        = (*pebbleStore)(nil)
	_ sanitizeSupportsDiffWriter    = (*pebbleStore)(nil)
	_ sanitizeSyncRunMetadataReader = (*pebbleStore)(nil)
	_ sanitizeSyncLinkWriter        = (*C1File)(nil)
	_ sanitizeSupportsDiffWriter    = (*C1File)(nil)
	_ sanitizeSyncRunMetadataReader = (*C1File)(nil)
)

func (pebbleDriver) Engine() c1zstore.Engine { return c1zstore.EnginePebble }
func (pebbleDriver) Format() C1ZFormat       { return C1ZFormatV3 }

func (pebbleDriver) OpenStore(ctx context.Context, outputFilePath string, opts StoreOptions) (c1zstore.Store, error) {
	tmpDir, err := os.MkdirTemp(opts.TmpDir, "c1z-pebble")
	if err != nil {
		return nil, err
	}
	cleanupOnError := func(e error) error {
		if removeErr := os.RemoveAll(tmpDir); removeErr != nil {
			e = errors.Join(e, removeErr)
		}
		return e
	}

	dbDir := filepath.Join(tmpDir, "db")
	reuse, fileEncoding, foldDeadBytes, err := unpackExistingPebbleC1Z(outputFilePath, dbDir, opts.MaxDecodedPayloadBytes, opts.MaxDecoderMemoryBytes, opts.DecoderPool)
	if err != nil {
		return nil, cleanupOnError(err)
	}

	if opts.ReadOnly {
		// A read-only open of a missing or empty c1z must fail loudly (as it
		// does on main via pebble's ErrDBDoesNotExist), not silently create
		// an empty DB in the temp dir: the writable migration pre-open below
		// would otherwise mint a fresh store and mask a mistyped path as
		// "file has no syncs". unpackExistingPebbleC1Z creates dbDir exactly
		// when it actually unpacked a payload.
		if _, statErr := os.Stat(dbDir); statErr != nil {
			return nil, cleanupOnError(fmt.Errorf("pebble: read-only open: %s does not exist or is empty", outputFilePath))
		}
		// Match SQLite read-only semantics: the source c1z is immutable, but the
		// unpacked temp DB may be migrated so current read paths see the latest
		// layout. Reopen read-only afterwards so callers that reach the engine
		// directly still get read-only write barriers.
		migratingEngine, err := pebble.Open(ctx, dbDir)
		if err != nil {
			return nil, cleanupOnError(err)
		}
		if err := migratingEngine.Close(); err != nil {
			return nil, cleanupOnError(err)
		}
	}

	engineOpts := []pebble.Option{pebble.WithReadOnly(opts.ReadOnly)}
	if opts.DisableGrantDigestIndex {
		engineOpts = append(engineOpts, pebble.WithGrantDigestIndex(false))
	}
	e, err := pebble.Open(ctx, dbDir, engineOpts...)
	if err != nil {
		return nil, cleanupOnError(err)
	}
	encoding := opts.PayloadEncoding
	if encoding == c1zstore.PayloadEncodingUnspecified {
		encoding = fileEncoding
	}

	err = e.InitCurrentSync(ctx)
	if err != nil {
		// Close the engine before removing its directory: a live pebble DB
		// holds open fds and background goroutines that would otherwise
		// leak for the life of the process.
		if closeErr := e.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
		return nil, cleanupOnError(err)
	}

	store := &pebbleStore{
		Engine:          e,
		outputFilePath:  outputFilePath,
		tmpDir:          tmpDir,
		readOnly:        opts.ReadOnly,
		payloadEncoding: encoding,
		payloadReuse:    reuse,
		foldDeadBytes:   foldDeadBytes,
		syncLimit:       opts.SyncLimit,
		skipCleanup:     opts.SkipCleanup,
		// A writable open that ran the in-place id-index migration must
		// save the migrated layout back into the c1z even if the caller
		// never writes, or every subsequent open re-pays the O(rows)
		// migration.
		dirty: !opts.ReadOnly && e.MigratedOnOpen(),
	}
	store.closeCond = sync.NewCond(&store.closeMu)
	return store, nil
}

func unpackExistingPebbleC1Z(
	outputFilePath string,
	dbDir string,
	maxDecodedPayloadBytes uint64,
	maxDecoderMemoryBytes uint64,
	pool *EnvelopeDecoderPool,
) (*formatv3.PayloadReuse, c1zstore.PayloadEncoding, int64, error) {
	stat, err := os.Stat(outputFilePath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, c1zstore.PayloadEncodingUnspecified, 0, nil
	case err != nil:
		return nil, c1zstore.PayloadEncodingUnspecified, 0, err
	case stat.Size() == 0:
		return nil, c1zstore.PayloadEncodingUnspecified, 0, nil
	}

	f, err := os.Open(outputFilePath)
	if err != nil {
		return nil, c1zstore.PayloadEncodingUnspecified, 0, err
	}
	defer f.Close()

	header, err := formatv3.ReadManifestHeader(f)
	if err != nil {
		return nil, c1zstore.PayloadEncodingUnspecified, 0, err
	}
	if e := c1zstore.Engine(header.GetEngine()); e != c1zstore.EnginePebble && e != c1zstore.PebbleManifestEngine && e != c1zstore.PebbleManifestEngineV2 {
		return nil, c1zstore.PayloadEncodingUnspecified, 0, fmt.Errorf("%w: %s", pebble.ErrUnknownEngine, header.GetEngine())
	}
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return nil, c1zstore.PayloadEncodingUnspecified, 0, err
	}
	manifest, reuse, err := formatv3.ExtractEnvelopePayload(f, dbDir,
		formatv3.WithMaxDecodedPayloadBytes(maxDecodedPayloadBytes),
		formatv3.WithMaxDecoderMemoryBytes(maxDecoderMemoryBytes),
		formatv3.WithPayloadDecoderPool(pool),
	)
	if err != nil {
		return nil, c1zstore.PayloadEncodingUnspecified, 0, err
	}
	// fold_dead_bytes is inherited from the source file so the waste
	// accounting survives arbitrary open/save cycles, not just fold
	// compactions; a fresh file starts at zero.
	return reuse, payloadEncodingFromProto(manifest.GetPayloadEncoding()), header.GetFoldDeadBytes(), nil
}

func payloadEncodingFromProto(enc c1zv3.PayloadEncoding) c1zstore.PayloadEncoding {
	switch enc {
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR:
		return c1zstore.PayloadEncodingTar
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD:
		return c1zstore.PayloadEncodingIndexedZstd
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD:
		return c1zstore.PayloadEncodingTarZstd
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_UNSPECIFIED:
		return c1zstore.PayloadEncodingUnspecified
	default:
		return c1zstore.PayloadEncodingUnspecified
	}
}

type pebbleStore struct {
	// Deliberately a named field, not an embedded one. Embedding promoted
	// every engine mutator onto the store, where it bypassed withMutation
	// silently; see pebble_store_reads.go. Reads are forwarded explicitly
	// there, mutators go through the wrappers below, and nothing else on
	// the engine is reachable as a store method.
	Engine          *pebble.Engine
	outputFilePath  string
	tmpDir          string
	readOnly        bool
	payloadEncoding c1zstore.PayloadEncoding
	payloadReuse    *formatv3.PayloadReuse
	// foldDeadBytes is the cumulative fold-waste counter carried in
	// the envelope manifest (C1ZManifestV3.fold_dead_bytes): seeded
	// from the file this store was opened from, optionally bumped by
	// RunPebbleFoldMutation during a fold compaction, and written back at
	// save. Guarded by closeMu (writes happen on the compactor's
	// single merge goroutine; the lock just pairs it with save/Close).
	foldDeadBytes int64

	// syncLimit and skipCleanup mirror StoreOptions and feed into
	// Cleanup. The Adapter intentionally has no awareness of these
	// — retention policy is an envelope-writer concern, not an
	// engine-keyspace concern.
	syncLimit   int
	skipCleanup bool

	closeMu   sync.Mutex
	closeCond *sync.Cond
	admission pebbleStoreAdmissionState
	// activeWrites counts admitted mutations; a teardown drains them to zero
	// before touching the engine. closeAttempt distinguishes successive
	// teardown attempts so a waiter can tell "the attempt I was waiting on
	// finished" from "a later one started".
	activeWrites int
	closeAttempt uint64
	dirty        bool

	// mutationAdmissionHook is test-only. It runs after admission and
	// immediately before the underlying mutation, without closeMu held.
	mutationAdmissionHook func()

	// drainWarnInterval overrides how often a blocked teardown reports
	// itself. Zero means pebbleStoreDrainWarnInterval; tests shorten it.
	drainWarnInterval time.Duration
}

type pebbleStoreAdmissionState uint8

const (
	pebbleStoreOpen pebbleStoreAdmissionState = iota
	pebbleStoreClosing
	pebbleStoreClosed
)

func (s *pebbleStore) cond() *sync.Cond {
	if s.closeCond == nil {
		s.closeCond = sync.NewCond(&s.closeMu)
	}
	return s.closeCond
}

// beginClose takes ownership of a teardown, closing admission and draining
// in-flight mutations before returning. It reports false when the store is
// already closed and there is nothing left to do.
//
// A caller whose attempt fails must reopenAdmission so a waiter can take over.
// Waiters then retry the teardown on their own behalf rather than adopting the
// failed attempt's error: the two callers may be closing for unrelated reasons
// (CloseEngineOnly's dirty refusal is not a reason for Close to skip its save),
// and inheriting a foreign error let one caller's refusal silently cancel
// another caller's save.
//
// The drain is deliberately not cancellable. Abandoning it would leave admitted
// writes running against an engine the caller is about to close, which is the
// failure mode admission control exists to prevent.
func (s *pebbleStore) beginClose() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	for {
		switch s.admission {
		case pebbleStoreClosed:
			return false
		case pebbleStoreOpen:
			s.admission = pebbleStoreClosing
			s.closeAttempt++
			for s.activeWrites > 0 {
				s.cond().Wait()
			}
			return true
		case pebbleStoreClosing:
			attempt := s.closeAttempt
			for s.admission == pebbleStoreClosing && s.closeAttempt == attempt {
				s.cond().Wait()
			}
		}
	}
}

// pebbleStoreDrainWarnInterval is how often a teardown blocked in beginClose
// announces itself.
const pebbleStoreDrainWarnInterval = 30 * time.Second

// warnWhileBlocked reports a teardown that has been waiting long enough to be
// suspicious, and returns a func that stops the reporting.
//
// beginClose's wait is deliberately uncancellable, and a single admission can
// span an entire compaction (compactPebble and compactPebbleFold each hold one
// for the whole merge). A wedged merge or a frozen Lambda therefore turns
// Close into an unbounded block with no ctx.Err() to surface and nothing in
// the logs — diagnosable today only from a stack dump. Naming activeWrites and
// the elapsed time makes it visible in the places an operator actually looks.
func (s *pebbleStore) warnWhileBlocked(ctx context.Context) func() {
	interval := s.drainWarnInterval
	if interval <= 0 {
		interval = pebbleStoreDrainWarnInterval
	}
	l := ctxzap.Extract(ctx)
	start := time.Now()
	stop := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				s.closeMu.Lock()
				active := s.activeWrites
				s.closeMu.Unlock()
				waitingOn := "another teardown attempt"
				if active > 0 {
					waitingOn = "in-flight mutations"
				}
				l.Warn("pebble store close: still waiting to tear down",
					zap.Duration("elapsed", time.Since(start)),
					zap.Int("active_writes", active),
					zap.String("waiting_on", waitingOn),
				)
			}
		}
	}()
	return func() {
		close(stop)
		<-stopped
	}
}

// isDirty reports whether the store has unsaved writes. Callers that own a
// teardown can trust the answer to stay put: admission is already shut and
// beginClose drained the in-flight mutations, so nothing can set it after this.
func (s *pebbleStore) isDirty() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	return s.dirty
}

// reopenAdmission abandons a teardown attempt, readmitting mutations and waking
// any caller waiting to take over.
func (s *pebbleStore) reopenAdmission() {
	s.closeMu.Lock()
	s.admission = pebbleStoreOpen
	s.cond().Broadcast()
	s.closeMu.Unlock()
}

// finishClose publishes the terminal state and releases waiters.
func (s *pebbleStore) finishClose() {
	s.closeMu.Lock()
	s.admission = pebbleStoreClosed
	s.cond().Broadcast()
	s.closeMu.Unlock()
}

// withMutation admits one synchronous mutation. Admission and release only
// hold closeMu briefly; admitted mutations execute concurrently. Dirty is set
// before invoking fn because an error may follow a partially committed write.
func (s *pebbleStore) withMutation(fn func(*pebble.Engine) error) error {
	return s.admitMutation(true, fn)
}

// admitMutation is the shared admission body. dirtyOnAdmission is true for
// ordinary mutations, where a returned error can still follow a committed
// write, so the envelope must be saved either way. The fold path passes false
// and marks dirty itself once the fold has fully landed: a failed fold's
// destination is a throwaway copy under the compactor's temp dir, and saving it
// would spend a full envelope write on an artifact about to be discarded.
func (s *pebbleStore) admitMutation(dirtyOnAdmission bool, fn func(*pebble.Engine) error) error {
	if s == nil || s.Engine == nil {
		return pebble.ErrEngineClosing
	}
	s.closeMu.Lock()
	if s.admission != pebbleStoreOpen {
		s.closeMu.Unlock()
		return pebble.ErrEngineClosing
	}
	s.activeWrites++
	if dirtyOnAdmission && !s.readOnly {
		s.dirty = true
	}
	hook := s.mutationAdmissionHook
	s.closeMu.Unlock()

	defer func() {
		s.closeMu.Lock()
		s.activeWrites--
		if s.activeWrites == 0 {
			s.cond().Broadcast()
		}
		s.closeMu.Unlock()
	}()
	if hook != nil {
		hook()
	}
	return fn(s.Engine)
}

// RunPebbleMutation implements pebble's guarded direct-engine mutation
// callback. The engine is explicit so callers cannot accidentally mutate via
// promoted pebbleStore methods outside admission.
func (s *pebbleStore) RunPebbleMutation(ctx context.Context, fn func(context.Context, *pebble.Engine) error) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return fn(ctx, e)
	})
}

// Compile-time guard: a Pebble store satisfies the full C1ZStore
// contract — connectorstore.Writer (via Adapter) plus the three
// sub-store methods (Grants, SyncMeta, FileOps) and the C1ZStore
// Close(ctx) signature. Lets callers route Pebble stores through
// pkg/sync.NewSyncer's WithConnectorStore option the same way they
// route SQLite *C1File handles today.
var _ c1zstore.Store = (*pebbleStore)(nil)

// FileOps overrides the Adapter-level FileOps for two reasons:
//
//   - CloneSync threads the pebbleStore's configured payload encoding
//     into the destination c1z (otherwise clone output would always
//     use the default TAR_ZSTD); and
//   - GenerateSyncDiff writes a NEW sync into THIS store, so it must
//     flip the dirty bit — without it, Close would skip the envelope
//     save and the diff sync would exist only in the discarded temp
//     directory.
func (s *pebbleStore) FileOps() c1zstore.FileOps {
	engine := s.Engine
	return pebbleStoreFileOps{inner: engine.FileOpsWithEncoding(s.payloadEncoding), store: s}
}

// pebbleStoreFileOps wraps the Adapter-level FileOps to route the one
// mutating-in-place method (GenerateSyncDiff) through the store's
// dirty-marking path. CloneSync writes a separate file and passes
// through unchanged.
type pebbleStoreFileOps struct {
	inner c1zstore.FileOps
	store *pebbleStore
}

func (f pebbleStoreFileOps) CloneSync(ctx context.Context, outPath string, syncID string, opts ...c1zstore.CloneSyncOption) error {
	return f.inner.CloneSync(ctx, outPath, syncID, opts...)
}

func (f pebbleStoreFileOps) CopyIsolateSync(ctx context.Context, outPath string, syncID string, opts ...c1zstore.CloneSyncOption) error {
	return f.inner.CopyIsolateSync(ctx, outPath, syncID, opts...)
}

func (f pebbleStoreFileOps) GenerateSyncDiff(ctx context.Context, baseSyncID, appliedSyncID string) (string, error) {
	var diffSyncID string
	err := f.store.withMutation(func(_ *pebble.Engine) error {
		var err error
		diffSyncID, err = f.inner.GenerateSyncDiff(ctx, baseSyncID, appliedSyncID)
		return err
	})
	return diffSyncID, err
}

// SyncMeta overrides the Adapter-level SyncMeta so the MUTATING
// metadata methods flip the store's dirty bit — Close only saves the
// envelope when dirty, so a standalone metadata stamp on a reopened
// c1z (e.g. MarkIngestInvariantsVerified after the engine sealed)
// would otherwise be silently dropped with the discarded temp dir.
// Same pattern as Grants() and FileOps(); the production Sync() path
// never noticed because EndSync sets dirty right before the stamps.
func (s *pebbleStore) SyncMeta() c1zstore.SyncMeta {
	return pebbleStoreSyncMeta{inner: s.Engine.SyncMeta(), store: s}
}

type pebbleStoreSyncMeta struct {
	inner c1zstore.SyncMeta
	store *pebbleStore
}

// The engine-level SyncMeta implements the verification writer; the
// dirty-marking wrapper must keep exposing it.
var _ c1zstore.IngestInvariantVerificationWriter = pebbleStoreSyncMeta{}

func (m pebbleStoreSyncMeta) MarkSyncSupportsDiff(ctx context.Context, syncID string) error {
	return m.store.withMutation(func(_ *pebble.Engine) error {
		return m.inner.MarkSyncSupportsDiff(ctx, syncID)
	})
}

func (m pebbleStoreSyncMeta) MarkIngestInvariantsVerified(ctx context.Context, syncID string, verification c1zstore.IngestInvariantVerification) error {
	w, ok := m.inner.(c1zstore.IngestInvariantVerificationWriter)
	if !ok {
		return errors.New("pebble sync meta: engine SyncMeta does not implement IngestInvariantVerificationWriter")
	}
	return m.store.withMutation(func(_ *pebble.Engine) error {
		return w.MarkIngestInvariantsVerified(ctx, syncID, verification)
	})
}

func (m pebbleStoreSyncMeta) ClearIngestInvariantVerification(ctx context.Context, syncID string) error {
	w, ok := m.inner.(c1zstore.IngestInvariantVerificationWriter)
	if !ok {
		return errors.New("pebble sync meta: engine SyncMeta does not implement IngestInvariantVerificationWriter")
	}
	return m.store.withMutation(func(_ *pebble.Engine) error {
		return w.ClearIngestInvariantVerification(ctx, syncID)
	})
}

func (m pebbleStoreSyncMeta) RecalculateStats(ctx context.Context, syncID string) error {
	return m.store.withMutation(func(_ *pebble.Engine) error {
		return m.inner.RecalculateStats(ctx, syncID)
	})
}

func (m pebbleStoreSyncMeta) LatestFullSync(ctx context.Context) (*c1zstore.SyncRun, error) {
	return m.inner.LatestFullSync(ctx)
}

func (m pebbleStoreSyncMeta) LatestFinishedSyncOfAnyType(ctx context.Context) (*c1zstore.SyncRun, error) {
	return m.inner.LatestFinishedSyncOfAnyType(ctx)
}

func (m pebbleStoreSyncMeta) Stats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (map[string]int64, error) {
	return m.inner.Stats(ctx, syncType, syncID)
}

func (m pebbleStoreSyncMeta) StatsV2(ctx context.Context, syncType connectorstore.SyncType, syncID string) (*reader_v2.SyncStats, error) {
	return m.inner.StatsV2(ctx, syncType, syncID)
}

// Metadata extends the embedded Adapter's Metadata with this store's
// configured payload encoding. Encoding lives on the pebbleStore
// (not the inner Adapter) because it's a writer-side option threaded
// through the envelope, not a property of the Pebble engine itself.
//
// Unspecified is resolved to the engine's effective default (IndexedZstd
// — see pebble.BuildManifest). Callers see the value the writer
// will actually use, not the literal option supplied.
func (s *pebbleStore) Metadata() connectorstore.StoreMetadata {
	md := s.Engine.Metadata()
	enc := s.payloadEncoding
	if enc == c1zstore.PayloadEncodingUnspecified {
		enc = c1zstore.PayloadEncodingIndexedZstd
	}
	md.PayloadEncoding = enc.String()
	return md
}

// PebbleEngine implements pebble.AsEngine's accessor with an explicit
// nil-receiver guard; the promoted Adapter method would panic on a nil
// *pebbleStore.
func (s *pebbleStore) PebbleEngine() *pebble.Engine {
	if s == nil {
		return nil
	}
	return s.Engine
}

// CloseEngineOnly closes the Pebble engine without removing the
// store's unpacked temp directory, refusing to discard a dirty
// writable store. Consumed by the compactor's chunk lifecycle via
// pebble.CloseEngineOnly, where a caller owns a parent temp directory
// and does one bulk cleanup after closing many read-only sources.
func (s *pebbleStore) CloseEngineOnly() error {
	if s == nil || s.Engine == nil {
		return nil
	}
	if !s.beginClose() {
		return nil
	}
	if !s.readOnly && s.isDirty() {
		s.reopenAdmission()
		return errors.New("pebble CloseEngineOnly: refusing to discard dirty writable store")
	}
	err := s.Engine.Close()
	s.finishClose()
	return err
}

// NormalizeForFixtureSave flushes and compacts the single sync, then
// marks the store dirty so Close writes a fresh c1z envelope.
// Intentionally narrow (consumed by benchmark fixture generation via
// pebble.NormalizeForFixtureSave): fixtures should not measure WAL
// replay or un-compacted LSM shape left over from generation. The
// syncID arg is accepted for signature parity but ignored — the file
// holds one sync and CompactAllRanges covers the whole keyspace.
func (s *pebbleStore) NormalizeForFixtureSave(ctx context.Context, syncID string) error {
	if s == nil || s.Engine == nil {
		return nil
	}
	if s.readOnly {
		return errors.New("pebble NormalizeForFixtureSave: store is read-only")
	}
	return s.withMutation(func(e *pebble.Engine) error {
		if err := e.Flush(ctx); err != nil {
			return err
		}
		return e.CompactAllRanges(ctx)
	})
}

// RunPebbleFoldMutation keeps fold-dead-byte manifest metadata in the same
// admission as the direct engine mutations that produced it: activeWrites is
// still held here, so a concurrent Close is parked in its drain and cannot
// save an envelope whose counter is only half updated.
//
// Nothing is recorded on failure. A fold either lands whole or its output is
// discarded, so a failed attempt must leave the store exactly as it found it:
// dirty stays put (Close then skips the O(base) save of a doomed artifact) and
// the waste counter keeps describing only folds that actually shadowed bytes.
func (s *pebbleStore) RunPebbleFoldMutation(ctx context.Context, fn func(context.Context, *pebble.Engine) (int64, error)) error {
	return s.admitMutation(false, func(e *pebble.Engine) error {
		n, err := fn(ctx, e)
		if err != nil {
			return err
		}
		s.closeMu.Lock()
		if !s.readOnly {
			s.dirty = true
		}
		if n > 0 {
			s.foldDeadBytes += n
		}
		s.closeMu.Unlock()
		return nil
	})
}

// StartNewSync begins a sync on the Pebble v3 store. INVARIANT: a v3 Pebble
// c1z holds exactly ONE sync — StartNewSync replaces any prior sync in place
// (the engine's ResetForNewSync wipes every sync-scoped keyspace). There is no
// multi-sync Pebble file. Callers that copy N source syncs into a Pebble
// destination (e.g. the c1z sanitizer) must reject N>1 up front, since each
// StartNewSync after the first would discard the previous sync's records.
// Future engine authors relying on this contract should preserve it here.
func (s *pebbleStore) StartNewSync(ctx context.Context, syncType connectorstore.SyncType, parentSyncID string) (string, error) {
	var syncID string
	err := s.withMutation(func(e *pebble.Engine) error {
		var err error
		syncID, err = e.StartNewSync(ctx, syncType, parentSyncID)
		return err
	})
	return syncID, err
}

func (s *pebbleStore) StartNewSyncWithID(ctx context.Context, syncType connectorstore.SyncType, syncID, parentSyncID string) (string, error) {
	var id string
	err := s.withMutation(func(e *pebble.Engine) error {
		var err error
		id, err = e.StartNewSyncWithID(ctx, syncType, syncID, parentSyncID)
		return err
	})
	return id, err
}

func (s *pebbleStore) ResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, error) {
	var id string
	err := s.withMutation(func(e *pebble.Engine) error {
		var err error
		id, err = e.ResumeSync(ctx, syncType, syncID)
		return err
	})
	return id, err
}

func (s *pebbleStore) StartOrResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, bool, error) {
	var id string
	var started bool
	err := s.withMutation(func(e *pebble.Engine) error {
		var err error
		id, started, err = e.StartOrResumeSync(ctx, syncType, syncID)
		return err
	})
	return id, started, err
}

func (s *pebbleStore) SetCurrentSync(ctx context.Context, syncID string) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.SetCurrentSync(ctx, syncID)
	})
}

func (s *pebbleStore) CheckpointSync(ctx context.Context, syncToken string) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.CheckpointSync(ctx, syncToken)
	})
}

func (s *pebbleStore) EndSync(ctx context.Context) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.EndSync(ctx)
	})
}

// Cleanup is a no-op for the Pebble v3 engine. A c1z holds exactly one
// sync by contract — StartNewSync replaces any prior sync in place (see
// Engine.ResetForNewSync) — so there is never stale sync data to prune.
// The SDK retention policy (SelectSyncsToDelete, WithSyncLimit,
// BATON_KEEP_SYNC_COUNT) applies only to the multi-sync SQLite engine;
// those options are accepted on the Pebble store but inert. The method
// stays to satisfy connectorstore.Writer.
func (s *pebbleStore) Cleanup(ctx context.Context) error {
	return nil
}

func (s *pebbleStore) PutAsset(ctx context.Context, assetRef *v2.AssetRef, contentType string, data []byte) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.PutAsset(ctx, assetRef, contentType, data)
	})
}

// SetSupportsDiff marks the given sync as diff-capable, matching the
// SQLite engine's sync_runs.supports_diff column. The c1z sanitizer
// carries this marker from a source sync to its sanitized copy so the
// output remains usable wherever the source was. Delegates to the
// SyncMeta sub-store's MarkSyncSupportsDiff.
func (s *pebbleStore) SetSupportsDiff(ctx context.Context, syncID string) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.SyncMeta().MarkSyncSupportsDiff(ctx, syncID)
	})
}

// SetSyncLink records linkedSyncID as the diff partner of syncID on the
// sync-run record (v3 linked_sync_id), matching the SQLite engine.
//
// This is implemented for connectorstore.Writer parity but is NOT
// reached by the c1z sanitizer's Pebble path: a v3 Pebble c1z holds
// exactly one sync, so there is never a second sync to link to. Cross-
// file linkage is unpreservable on either engine regardless, because
// sanitize mints fresh destination sync ids.
func (s *pebbleStore) SetSyncLink(ctx context.Context, syncID string, linkedSyncID string) error {
	if syncID == "" {
		return fmt.Errorf("SetSyncLink: empty syncID")
	}
	return s.withMutation(func(e *pebble.Engine) error {
		r, err := e.GetSyncRunRecord(ctx, syncID)
		if err != nil {
			return fmt.Errorf("SetSyncLink: get: %w", err)
		}
		r.SetLinkedSyncId(linkedSyncID)
		if err := e.PutSyncRunRecord(ctx, r); err != nil {
			return fmt.Errorf("SetSyncLink: put: %w", err)
		}
		return nil
	})
}

func (s *pebbleStore) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.PutGrants(ctx, grants...)
	})
}

// UnsafePutUniqueGrants is the trusted-import write path (no
// read-before-write, no dedup, parallel encode). Do not use it for live
// connector output. Caller must guarantee unique external_ids across the whole
// destination sync. See pebble.Adapter.UnsafePutUniqueGrants.
func (s *pebbleStore) UnsafePutUniqueGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.UnsafePutUniqueGrants(ctx, grants...)
	})
}

func (s *pebbleStore) PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.PutResourceTypes(ctx, resourceTypes...)
	})
}

func (s *pebbleStore) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.PutResources(ctx, resources...)
	})
}

func (s *pebbleStore) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.PutEntitlements(ctx, entitlements...)
	})
}

func (s *pebbleStore) DeleteGrant(ctx context.Context, grantID string) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.DeleteGrant(ctx, grantID)
	})
}

// DeleteGrantByRefs is the exact grant delete for callers holding the full
// grant: identity derives from the structured refs, never the lossy id
// string. The syncer prefers this when available.
func (s *pebbleStore) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.DeleteGrantByRefs(ctx, grant)
	})
}

// DeleteResourceRecord removes a resource and marks the envelope dirty so an
// explicit reconciliation performed by the syncer is persisted on Close.
func (s *pebbleStore) DeleteResourceRecord(ctx context.Context, resourceTypeID, resourceID string) error {
	return s.withMutation(func(e *pebble.Engine) error {
		return e.DeleteResourceRecord(ctx, resourceTypeID, resourceID)
	})
}

// DeleteEntitlementByRefs removes one exact entitlement identity and preserves
// the mutation when the envelope is closed.
func (s *pebbleStore) DeleteEntitlementByRefs(ctx context.Context, entitlement *v2.Entitlement) error {
	resourceID := entitlement.GetResource().GetId()
	return s.withMutation(func(e *pebble.Engine) error {
		return e.DeleteEntitlementRecordByIdentity(
			ctx,
			resourceID.GetResourceType(),
			resourceID.GetResource(),
			entitlement.GetId(),
		)
	})
}

// Grants overrides Adapter.Grants() so the returned GrantStore
// routes StoreExpandedGrants through the pebbleStore's dirty-marking
// path. The Adapter-level wrapper calls Adapter.PutGrants directly,
// which skips the dirty flag.
func (s *pebbleStore) Grants() c1zstore.GrantStore {
	return pebbleStoreGrants{inner: s.Engine.Grants(), store: s}
}

// pebbleStoreGrants wraps the Adapter-level grant store and overrides
// only StoreExpandedGrants (the lone mutating method) to flip the
// dirty bit. Read-only methods pass through.
type pebbleStoreGrants struct {
	inner c1zstore.GrantStore
	store *pebbleStore
}

var pebbleStoreExpandedGrantImmutableAnnotationAny = func() *anypb.Any {
	a, err := anypb.New(&v2.GrantImmutable{})
	if err != nil {
		panic(fmt.Errorf("dotc1z: marshal GrantImmutable annotation: %w", err))
	}
	return a
}()

func (g pebbleStoreGrants) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	return g.store.withMutation(func(_ *pebble.Engine) error {
		return g.inner.StoreExpandedGrants(ctx, grants...)
	})
}

func (g pebbleStoreGrants) StoreNewExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	if fast, ok := g.inner.(interface {
		StoreNewExpandedGrants(context.Context, ...*v2.Grant) error
	}); ok {
		return g.store.withMutation(func(_ *pebble.Engine) error {
			return fast.StoreNewExpandedGrants(ctx, grants...)
		})
	}
	return g.store.withMutation(func(_ *pebble.Engine) error {
		return g.inner.StoreExpandedGrants(ctx, grants...)
	})
}

func (g pebbleStoreGrants) StoreNewExpandedGrantContributions(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error {
	if fast, ok := g.inner.(interface {
		StoreNewExpandedGrantContributions(context.Context, *v2.Entitlement, []*v3.PrincipalRef, []batonGrant.Sources) error
	}); ok {
		return g.store.withMutation(func(_ *pebble.Engine) error {
			return fast.StoreNewExpandedGrantContributions(ctx, dest, principals, sources)
		})
	}
	grants := make([]*v2.Grant, 0, len(principals))
	for i, principalRef := range principals {
		principal := newPebbleStorePrincipalResource(principalRef)
		grant, err := newPebbleStoreExpandedGrant(dest, principal, sources[i])
		if err != nil {
			return err
		}
		grants = append(grants, grant)
	}
	return g.store.withMutation(func(_ *pebble.Engine) error {
		return g.inner.StoreExpandedGrants(ctx, grants...)
	})
}

// pebbleStoreGrantLayerStorer is the layer-scoped layer session surface the
// engine-level grant store may implement (see the pebble adapter). Kept as a
// local interface so the wrapper can pass sessions through with the dirty bit.
type pebbleStoreGrantLayerStorer interface {
	BeginExpandedGrantLayer(ctx context.Context) (bool, error)
	AddExpandedGrantLayerContributions(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error
	FinishExpandedGrantLayer(ctx context.Context) error
	AbortExpandedGrantLayer(ctx context.Context) error
}

func (g pebbleStoreGrants) BeginExpandedGrantLayer(ctx context.Context) (bool, error) {
	if fast, ok := g.inner.(pebbleStoreGrantLayerStorer); ok {
		var started bool
		err := g.store.withMutation(func(_ *pebble.Engine) error {
			var err error
			started, err = fast.BeginExpandedGrantLayer(ctx)
			return err
		})
		return started, err
	}
	return false, nil
}

func (g pebbleStoreGrants) AddExpandedGrantLayerContributions(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error {
	fast, ok := g.inner.(pebbleStoreGrantLayerStorer)
	if !ok {
		return fmt.Errorf("expanded grant layer: store does not support layer sessions")
	}
	return g.store.withMutation(func(_ *pebble.Engine) error {
		return fast.AddExpandedGrantLayerContributions(ctx, dest, principals, sources)
	})
}

func (g pebbleStoreGrants) FinishExpandedGrantLayer(ctx context.Context) error {
	fast, ok := g.inner.(pebbleStoreGrantLayerStorer)
	if !ok {
		return fmt.Errorf("expanded grant layer: store does not support layer sessions")
	}
	return g.store.withMutation(func(_ *pebble.Engine) error {
		return fast.FinishExpandedGrantLayer(ctx)
	})
}

func (g pebbleStoreGrants) AbortExpandedGrantLayer(ctx context.Context) error {
	fast, ok := g.inner.(pebbleStoreGrantLayerStorer)
	if !ok {
		return nil
	}
	return g.store.withMutation(func(_ *pebble.Engine) error {
		return fast.AbortExpandedGrantLayer(ctx)
	})
}

func newPebbleStorePrincipalResource(ref *v3.PrincipalRef) *v2.Resource {
	if ref == nil {
		return nil
	}
	var parent *v2.ResourceId
	if ref.GetParentResourceId() != "" {
		parent = v2.ResourceId_builder{
			ResourceType: ref.GetParentResourceTypeId(),
			Resource:     ref.GetParentResourceId(),
		}.Build()
	}
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: ref.GetResourceTypeId(),
			Resource:     ref.GetResourceId(),
		}.Build(),
		ParentResourceId: parent,
	}.Build()
}

func newPebbleStoreExpandedGrant(dest *v2.Entitlement, principal *v2.Resource, sources batonGrant.Sources) (*v2.Grant, error) {
	if len(sources) == 0 {
		return nil, fmt.Errorf("new expanded grant: empty sources")
	}
	if dest == nil || dest.GetResource() == nil {
		return nil, fmt.Errorf("new expanded grant: entitlement has no resource")
	}
	if principal == nil {
		return nil, fmt.Errorf("new expanded grant: principal is nil")
	}
	sourceMap := make(map[string]*v2.GrantSources_GrantSource, len(sources))
	for _, src := range sources {
		sourceMap[src.EntitlementID] = &v2.GrantSources_GrantSource{IsDirect: src.IsDirect}
	}
	return v2.Grant_builder{
		Id:          batonGrant.NewGrantID(principal, dest),
		Entitlement: dest,
		Principal:   principal,
		Sources:     v2.GrantSources_builder{Sources: sourceMap}.Build(),
		Annotations: []*anypb.Any{pebbleStoreExpandedGrantImmutableAnnotationAny},
	}.Build(), nil
}

func (g pebbleStoreGrants) PendingExpansionPage(ctx context.Context, pageToken string) ([]c1zstore.PendingExpansion, string, error) {
	return g.inner.PendingExpansionPage(ctx, pageToken)
}

func (g pebbleStoreGrants) PendingExpansion(ctx context.Context) iter.Seq2[c1zstore.PendingExpansion, error] {
	return g.inner.PendingExpansion(ctx)
}

func (g pebbleStoreGrants) ListWithAnnotationsPage(ctx context.Context, pageToken string) ([]c1zstore.GrantAnnotation, string, error) {
	return g.inner.ListWithAnnotationsPage(ctx, pageToken)
}

func (g pebbleStoreGrants) ListWithAnnotationsForResourcePage(
	ctx context.Context, resource *v2.Resource, syncID string, pageToken string, pageSize uint32,
) ([]c1zstore.GrantAnnotation, string, error) {
	return g.inner.ListWithAnnotationsForResourcePage(ctx, resource, syncID, pageToken, pageSize)
}

func (g pebbleStoreGrants) ListWithAnnotations(ctx context.Context) iter.Seq2[c1zstore.GrantAnnotation, error] {
	return g.inner.ListWithAnnotations(ctx)
}

// Close saves the envelope when the store is dirty, then tears it down. It is
// idempotent: once the store is closed, later calls report nil per io.Closer
// convention, so a deferred Close following an explicit one does not re-report
// a failure the caller already handled.
func (s *pebbleStore) Close(ctx context.Context) error {
	stopWarning := s.warnWhileBlocked(ctx)
	owned := s.beginClose()
	stopWarning()
	if !owned {
		return nil
	}

	if !s.readOnly && s.isDirty() {
		if err := s.save(ctx); err != nil {
			// Tear NOTHING down: the unpacked DB under tmpDir is the only copy
			// of the synced data, and save failures are frequently transient
			// (target-path permissions, disk space for the envelope). Leave the
			// store open so the caller can fix the condition and Close again;
			// if the process exits instead, the temp dir survives on disk for
			// manual recovery rather than being deleted out from under a failed
			// save.
			s.reopenAdmission()
			return fmt.Errorf("pebble store close: save failed, store left open and unsaved data preserved under %s: %w", s.tmpDir, err)
		}
		s.closeMu.Lock()
		s.dirty = false
		s.closeMu.Unlock()
	}

	var retErr error
	if err := s.Engine.Close(); err != nil {
		retErr = errors.Join(retErr, err)
	}
	if removeErr := os.RemoveAll(s.tmpDir); removeErr != nil {
		retErr = errors.Join(retErr, removeErr)
	}
	s.finishClose()
	return retErr
}

func (s *pebbleStore) save(ctx context.Context) error {
	if s.outputFilePath == "" {
		return fmt.Errorf("pebble engine: output file path is empty")
	}
	saveStart := time.Now()
	checkpointDir := filepath.Join(s.tmpDir, "checkpoint")
	// A previous failed save can leave this dir behind, and pebble's
	// Checkpoint refuses an existing destination — without this, the
	// "fix the condition and Close again" recovery path advertised by
	// Close would fail forever with ErrExist.
	if err := os.RemoveAll(checkpointDir); err != nil {
		return fmt.Errorf("pebble save: clear stale checkpoint dir: %w", err)
	}
	engine := s.Engine
	if err := engine.CheckpointTo(ctx, checkpointDir); err != nil {
		return err
	}
	checkpointDur := time.Since(saveStart)

	tmpPath := s.outputFilePath + ".tmp"
	out, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if out != nil {
			_ = out.Close()
		}
		if !success {
			_ = os.Remove(tmpPath)
		}
	}()

	manifest, err := pebble.BuildManifestWithSyncRuns(ctx, s.Engine, s.payloadEncoding)
	if err != nil {
		return err
	}
	s.closeMu.Lock()
	foldDeadBytes := s.foldDeadBytes
	s.closeMu.Unlock()
	if foldDeadBytes > 0 {
		manifest.SetFoldDeadBytes(foldDeadBytes)
	}
	encodeStart := time.Now()
	spliceStats, err := formatv3.WriteEnvelopeWithReuse(out, manifest, checkpointDir, s.payloadReuse)
	if err != nil {
		return err
	}
	l := ctxzap.Extract(ctx)
	if spliceStats.ReuseMissingPath != "" {
		// Frame splicing was configured but its source envelope was gone, so
		// this save recompressed the entire payload instead of copying the
		// unchanged frames. On a whale-scale file that is the difference
		// between seconds and many minutes, and nothing else reports it.
		l.Warn("pebble save: splice source missing, recompressed the whole payload",
			zap.String("splice_source", spliceStats.ReuseMissingPath),
			zap.Int("encoded_frames", spliceStats.EncodedFrames),
		)
	}
	l.Debug("pebble save: envelope written",
		zap.Duration("checkpoint", checkpointDur),
		zap.Duration("envelope_encode", time.Since(encodeStart)),
		zap.Int("spliced_frames", spliceStats.SplicedFrames),
		zap.Int64("spliced_bytes", spliceStats.SplicedBytes),
		zap.Int("encoded_frames", spliceStats.EncodedFrames),
		zap.Int64("encoded_bytes", spliceStats.EncodedBytes),
	)
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	out = nil
	if err := os.Rename(tmpPath, s.outputFilePath); err != nil {
		return err
	}
	success = true
	return nil
}
