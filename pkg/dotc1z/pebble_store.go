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

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// pebbleDriver is the EngineDriver for the Pebble v3 engine.
type pebbleDriver struct{}

var _ C1ZStore = (*pebbleStore)(nil)
var _ connectorstore.Writer = (*pebbleStore)(nil)

func (pebbleDriver) Engine() Engine    { return EnginePebble }
func (pebbleDriver) Format() C1ZFormat { return C1ZFormatV3 }

func (pebbleDriver) OpenStore(ctx context.Context, outputFilePath string, opts StoreOptions) (C1ZStore, error) {
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

	e, err := pebble.Open(ctx, dbDir, pebble.WithReadOnly(opts.ReadOnly))
	if err != nil {
		return nil, cleanupOnError(err)
	}
	encoding := opts.PayloadEncoding
	if encoding == PayloadEncodingUnspecified {
		encoding = fileEncoding
	}

	adapter := pebble.NewAdapter(e)
	err = adapter.InitCurrentSync(ctx)
	if err != nil {
		return nil, cleanupOnError(err)
	}

	return &pebbleStore{
		Adapter:         adapter,
		engine:          e,
		outputFilePath:  outputFilePath,
		tmpDir:          tmpDir,
		readOnly:        opts.ReadOnly,
		payloadEncoding: encoding,
		payloadReuse:    reuse,
		foldDeadBytes:   foldDeadBytes,
		syncLimit:       opts.SyncLimit,
		skipCleanup:     opts.SkipCleanup,
	}, nil
}

func unpackExistingPebbleC1Z(
	outputFilePath string,
	dbDir string,
	maxDecodedPayloadBytes uint64,
	maxDecoderMemoryBytes uint64,
	pool *EnvelopeDecoderPool,
) (*formatv3.PayloadReuse, PayloadEncoding, int64, error) {
	stat, err := os.Stat(outputFilePath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, PayloadEncodingUnspecified, 0, nil
	case err != nil:
		return nil, PayloadEncodingUnspecified, 0, err
	case stat.Size() == 0:
		return nil, PayloadEncodingUnspecified, 0, nil
	}

	f, err := os.Open(outputFilePath)
	if err != nil {
		return nil, PayloadEncodingUnspecified, 0, err
	}
	defer f.Close()

	header, err := formatv3.ReadManifestHeader(f)
	if err != nil {
		return nil, PayloadEncodingUnspecified, 0, err
	}
	if e := Engine(header.GetEngine()); e != EnginePebble && e != PebbleManifestEngine {
		return nil, PayloadEncodingUnspecified, 0, fmt.Errorf("%w: %s", pebble.ErrUnknownEngine, header.GetEngine())
	}
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return nil, PayloadEncodingUnspecified, 0, err
	}
	manifest, reuse, err := formatv3.ExtractEnvelopePayload(f, dbDir,
		formatv3.WithMaxDecodedPayloadBytes(maxDecodedPayloadBytes),
		formatv3.WithMaxDecoderMemoryBytes(maxDecoderMemoryBytes),
		formatv3.WithPayloadDecoderPool(pool),
	)
	if err != nil {
		return nil, PayloadEncodingUnspecified, 0, err
	}
	// fold_dead_bytes is inherited from the source file so the waste
	// accounting survives arbitrary open/save cycles, not just fold
	// compactions; a fresh file starts at zero.
	return reuse, payloadEncodingFromProto(manifest.GetPayloadEncoding()), header.GetFoldDeadBytes(), nil
}

func payloadEncodingFromProto(enc c1zv3.PayloadEncoding) PayloadEncoding {
	switch enc {
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR:
		return PayloadEncodingTar
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_INDEXED_ZSTD:
		return PayloadEncodingIndexedZstd
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD:
		return PayloadEncodingTarZstd
	case c1zv3.PayloadEncoding_PAYLOAD_ENCODING_UNSPECIFIED:
		return PayloadEncodingUnspecified
	default:
		return PayloadEncodingUnspecified
	}
}

type pebbleStore struct {
	*pebble.Adapter
	engine          *pebble.Engine
	outputFilePath  string
	tmpDir          string
	readOnly        bool
	payloadEncoding PayloadEncoding
	payloadReuse    *formatv3.PayloadReuse
	// foldDeadBytes is the cumulative fold-waste counter carried in
	// the envelope manifest (C1ZManifestV3.fold_dead_bytes): seeded
	// from the file this store was opened from, optionally bumped by
	// AddFoldDeadBytes during a fold compaction, and written back at
	// save. Guarded by closeMu (writes happen on the compactor's
	// single merge goroutine; the lock just pairs it with save/Close).
	foldDeadBytes int64

	// syncLimit and skipCleanup mirror StoreOptions and feed into
	// Cleanup. The Adapter intentionally has no awareness of these
	// — retention policy is an envelope-writer concern, not an
	// engine-keyspace concern.
	syncLimit   int
	skipCleanup bool

	closeMu sync.Mutex
	closed  bool
	dirty   bool
}

// Compile-time guard: a Pebble store satisfies the full C1ZStore
// contract — connectorstore.Writer (via Adapter) plus the three
// sub-store methods (Grants, SyncMeta, FileOps) and the C1ZStore
// Close(ctx) signature. Lets callers route Pebble stores through
// pkg/sync.NewSyncer's WithConnectorStore option the same way they
// route SQLite *C1File handles today.
var _ C1ZStore = (*pebbleStore)(nil)

// FileOps overrides the Adapter-level FileOps for two reasons:
//
//   - CloneSync threads the pebbleStore's configured payload encoding
//     into the destination c1z (otherwise clone output would always
//     use the default TAR_ZSTD); and
//   - GenerateSyncDiff writes a NEW sync into THIS store, so it must
//     flip the dirty bit — without it, Close would skip the envelope
//     save and the diff sync would exist only in the discarded temp
//     directory.
func (s *pebbleStore) FileOps() FileOps {
	return pebbleStoreFileOps{inner: s.FileOpsWithEncoding(s.payloadEncoding), store: s}
}

// pebbleStoreFileOps wraps the Adapter-level FileOps to route the one
// mutating-in-place method (GenerateSyncDiff) through the store's
// dirty-marking path. CloneSync writes a separate file and passes
// through unchanged.
type pebbleStoreFileOps struct {
	inner FileOps
	store *pebbleStore
}

func (f pebbleStoreFileOps) CloneSync(ctx context.Context, outPath string, syncID string, opts ...CloneSyncOption) error {
	return f.inner.CloneSync(ctx, outPath, syncID, opts...)
}

func (f pebbleStoreFileOps) CopyIsolateSync(ctx context.Context, outPath string, syncID string, opts ...CloneSyncOption) error {
	return f.inner.CopyIsolateSync(ctx, outPath, syncID, opts...)
}

func (f pebbleStoreFileOps) GenerateSyncDiff(ctx context.Context, baseSyncID, appliedSyncID string) (string, error) {
	diffSyncID, err := f.inner.GenerateSyncDiff(ctx, baseSyncID, appliedSyncID)
	if err != nil {
		return "", err
	}
	return diffSyncID, f.store.markDirty(nil)
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
	md := s.Adapter.Metadata()
	enc := s.payloadEncoding
	if enc == PayloadEncodingUnspecified {
		enc = PayloadEncodingIndexedZstd
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
	return s.engine
}

// CloseEngineOnly closes the Pebble engine without removing the
// store's unpacked temp directory, refusing to discard a dirty
// writable store. Consumed by the compactor's chunk lifecycle via
// pebble.CloseEngineOnly, where a caller owns a parent temp directory
// and does one bulk cleanup after closing many read-only sources.
func (s *pebbleStore) CloseEngineOnly() error {
	if s == nil || s.engine == nil {
		return nil
	}
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	if !s.readOnly && s.dirty {
		s.closeMu.Unlock()
		return errors.New("pebble CloseEngineOnly: refusing to discard dirty writable store")
	}
	s.closed = true
	s.closeMu.Unlock()
	return s.engine.Close()
}

// NormalizeForFixtureSave flushes and compacts the single sync, then
// marks the store dirty so Close writes a fresh c1z envelope.
// Intentionally narrow (consumed by benchmark fixture generation via
// pebble.NormalizeForFixtureSave): fixtures should not measure WAL
// replay or un-compacted LSM shape left over from generation. The
// syncID arg is accepted for signature parity but ignored — the file
// holds one sync and CompactAllRanges covers the whole keyspace.
func (s *pebbleStore) NormalizeForFixtureSave(ctx context.Context, syncID string) error {
	if s == nil || s.engine == nil {
		return nil
	}
	if s.readOnly {
		return errors.New("pebble NormalizeForFixtureSave: store is read-only")
	}
	if err := s.engine.Flush(ctx); err != nil {
		return err
	}
	if err := s.engine.CompactAllRanges(ctx); err != nil {
		return err
	}
	s.closeMu.Lock()
	if !s.closed {
		s.dirty = true
	}
	s.closeMu.Unlock()
	return nil
}

func (s *pebbleStore) MarkDirty() {
	if s == nil {
		return
	}
	s.closeMu.Lock()
	if !s.closed {
		s.dirty = true
	}
	s.closeMu.Unlock()
}

// AddFoldDeadBytes bumps the cumulative fold-waste counter persisted
// in the envelope manifest at save. Called by the fold compactor with
// the raw bytes its merge shadowed in the base keyspace (see
// pebble.AddFoldDeadBytes for the Writer-level accessor).
func (s *pebbleStore) AddFoldDeadBytes(n int64) {
	if s == nil || n <= 0 {
		return
	}
	s.closeMu.Lock()
	if !s.closed {
		s.foldDeadBytes += n
	}
	s.closeMu.Unlock()
}

func (s *pebbleStore) markDirty(err error) error {
	if err == nil {
		s.MarkDirty()
	}
	return err
}

func (s *pebbleStore) StartNewSync(ctx context.Context, syncType connectorstore.SyncType, parentSyncID string) (string, error) {
	syncID, err := s.Adapter.StartNewSync(ctx, syncType, parentSyncID)
	if err == nil {
		s.closeMu.Lock()
		s.dirty = true
		s.closeMu.Unlock()
	}
	return syncID, err
}

func (s *pebbleStore) StartOrResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, bool, error) {
	id, started, err := s.Adapter.StartOrResumeSync(ctx, syncType, syncID)
	if err == nil && started {
		s.closeMu.Lock()
		s.dirty = true
		s.closeMu.Unlock()
	}
	return id, started, err
}

func (s *pebbleStore) CheckpointSync(ctx context.Context, syncToken string) error {
	return s.markDirty(s.Adapter.CheckpointSync(ctx, syncToken))
}

func (s *pebbleStore) EndSync(ctx context.Context) error {
	return s.markDirty(s.Adapter.EndSync(ctx))
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
	return s.markDirty(s.Adapter.PutAsset(ctx, assetRef, contentType, data))
}

func (s *pebbleStore) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.markDirty(s.Adapter.PutGrants(ctx, grants...))
}

// UnsafePutUniqueGrants is the trusted-import write path (no
// read-before-write, no dedup, parallel encode). Do not use it for live
// connector output. Caller must guarantee unique external_ids across the whole
// destination sync. See pebble.Adapter.UnsafePutUniqueGrants.
func (s *pebbleStore) UnsafePutUniqueGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.markDirty(s.Adapter.UnsafePutUniqueGrants(ctx, grants...))
}

func (s *pebbleStore) PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error {
	return s.markDirty(s.Adapter.PutResourceTypes(ctx, resourceTypes...))
}

func (s *pebbleStore) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	return s.markDirty(s.Adapter.PutResources(ctx, resources...))
}

func (s *pebbleStore) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	return s.markDirty(s.Adapter.PutEntitlements(ctx, entitlements...))
}

func (s *pebbleStore) DeleteGrant(ctx context.Context, grantID string) error {
	return s.markDirty(s.Adapter.DeleteGrant(ctx, grantID))
}

// Grants overrides Adapter.Grants() so the returned GrantStore
// routes StoreExpandedGrants through the pebbleStore's dirty-marking
// path. The Adapter-level wrapper calls Adapter.PutGrants directly,
// which skips the dirty flag.
func (s *pebbleStore) Grants() GrantStore {
	return pebbleStoreGrants{inner: s.Adapter.Grants(), store: s}
}

// pebbleStoreGrants wraps the Adapter-level grant store and overrides
// only StoreExpandedGrants (the lone mutating method) to flip the
// dirty bit. Read-only methods pass through.
type pebbleStoreGrants struct {
	inner GrantStore
	store *pebbleStore
}

func (g pebbleStoreGrants) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	return g.store.markDirty(g.inner.StoreExpandedGrants(ctx, grants...))
}

func (g pebbleStoreGrants) PendingExpansionPage(ctx context.Context, pageToken string) ([]PendingExpansion, string, error) {
	return g.inner.PendingExpansionPage(ctx, pageToken)
}

func (g pebbleStoreGrants) PendingExpansion(ctx context.Context) iter.Seq2[PendingExpansion, error] {
	return g.inner.PendingExpansion(ctx)
}

func (g pebbleStoreGrants) ListWithAnnotationsPage(ctx context.Context, pageToken string) ([]GrantAnnotation, string, error) {
	return g.inner.ListWithAnnotationsPage(ctx, pageToken)
}

func (g pebbleStoreGrants) ListWithAnnotationsForResourcePage(
	ctx context.Context, resource *v2.Resource, syncID string, pageToken string, pageSize uint32,
) ([]GrantAnnotation, string, error) {
	return g.inner.ListWithAnnotationsForResourcePage(ctx, resource, syncID, pageToken, pageSize)
}

func (g pebbleStoreGrants) ListWithAnnotations(ctx context.Context) iter.Seq2[GrantAnnotation, error] {
	return g.inner.ListWithAnnotations(ctx)
}

func (s *pebbleStore) Close(ctx context.Context) (retErr error) {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	defer func() {
		if removeErr := os.RemoveAll(s.tmpDir); removeErr != nil {
			retErr = errors.Join(retErr, removeErr)
		}
	}()

	if !s.readOnly && s.dirty {
		if err := s.save(ctx); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}
	if err := s.engine.Close(); err != nil {
		retErr = errors.Join(retErr, err)
	}
	return retErr
}

func (s *pebbleStore) save(ctx context.Context) error {
	if s.outputFilePath == "" {
		return fmt.Errorf("pebble engine: output file path is empty")
	}
	saveStart := time.Now()
	checkpointDir := filepath.Join(s.tmpDir, "checkpoint")
	if err := s.engine.CheckpointTo(ctx, checkpointDir); err != nil {
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

	manifest, err := pebble.BuildManifestWithSyncRuns(ctx, s.engine, s.payloadEncoding)
	if err != nil {
		return err
	}
	if s.foldDeadBytes > 0 {
		manifest.SetFoldDeadBytes(s.foldDeadBytes)
	}
	encodeStart := time.Now()
	if _, err := formatv3.WriteEnvelopeWithReuse(out, manifest, checkpointDir, s.payloadReuse); err != nil {
		return err
	}
	ctxzap.Extract(ctx).Debug("pebble save: envelope written",
		zap.Duration("checkpoint", checkpointDur),
		zap.Duration("envelope_encode", time.Since(encodeStart)),
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
