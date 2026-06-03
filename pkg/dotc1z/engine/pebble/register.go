package pebble

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// Register installs the Pebble engine into dotc1z's process-global engine
// registry. Callers opt into Pebble dependencies by importing this package and
// calling Register before using dotc1z.NewStore with EnginePebble, or before
// opening an existing v3/Pebble .c1z through NewStore.
func Register() error {
	if existing, ok := dotc1z.EngineDriverFor(dotc1z.EnginePebble); ok {
		if _, ok := existing.(driver); ok {
			return nil
		}
	}
	return dotc1z.RegisterEngine(driver{})
}

type driver struct{}

func (driver) Engine() dotc1z.Engine    { return dotc1z.EnginePebble }
func (driver) Format() dotc1z.C1ZFormat { return dotc1z.C1ZFormatV3 }

func (driver) OpenStore(ctx context.Context, outputFilePath string, opts dotc1z.StoreOptions) (connectorstore.Writer, error) {
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
	if err := unpackExisting(outputFilePath, dbDir); err != nil {
		return nil, cleanupOnError(err)
	}

	e, err := Open(ctx, dbDir, WithReadOnly(opts.ReadOnly))
	if err != nil {
		return nil, cleanupOnError(err)
	}
	return &registeredStore{
		Adapter:         NewAdapter(e),
		engine:          e,
		outputFilePath:  outputFilePath,
		tmpDir:          tmpDir,
		readOnly:        opts.ReadOnly,
		payloadEncoding: opts.PayloadEncoding,
		syncLimit:       opts.SyncLimit,
		skipCleanup:     opts.SkipCleanup,
	}, nil
}

func unpackExisting(outputFilePath string, dbDir string) error {
	stat, err := os.Stat(outputFilePath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	case stat.Size() == 0:
		return nil
	}

	f, err := os.Open(outputFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	env, err := formatv3.ReadEnvelope(f)
	if err != nil {
		return err
	}
	defer env.Close()
	if dotc1z.Engine(env.Manifest.GetEngine()) != dotc1z.EnginePebble {
		return fmt.Errorf("%w: %s", ErrUnknownEngine, env.Manifest.GetEngine())
	}
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return err
	}
	if err := formatv3.ExtractZstdTar(env.PayloadReader, dbDir); err != nil {
		return err
	}
	return nil
}

type registeredStore struct {
	*Adapter
	engine          *Engine
	outputFilePath  string
	tmpDir          string
	readOnly        bool
	payloadEncoding dotc1z.PayloadEncoding

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

// Compile-time guard: a registered Pebble store satisfies the full
// dotc1z.C1ZStore contract — connectorstore.Writer (via Adapter)
// plus the three sub-store methods (Grants, SyncMeta, FileOps) and
// the C1ZStore Close(ctx) signature. Lets callers route Pebble
// stores through pkg/sync.NewSyncer's WithConnectorStore option
// the same way they route SQLite *C1File handles today.
var _ dotc1z.C1ZStore = (*registeredStore)(nil)

// FileOps overrides the Adapter-level FileOps so CloneSync threads
// the registeredStore's configured payload encoding into the
// destination c1z. Without this, clone output would always use
// the default TAR_ZSTD even when the source store was opened with
// WithPayloadEncoding(PayloadEncodingTar).
func (s *registeredStore) FileOps() dotc1z.FileOps {
	return s.FileOpsWithEncoding(s.payloadEncoding)
}

// Metadata extends the embedded Adapter's Metadata with this store's
// configured payload encoding. Encoding lives on the registered store
// (not the inner Adapter) because it's a writer-side option threaded
// through the envelope, not a property of the Pebble engine itself.
//
// Unspecified is resolved to the engine's effective default (TarZstd
// — see payloadEncodingToProto). Callers see the value the writer
// will actually use, not the literal option supplied.
func (s *registeredStore) Metadata() connectorstore.StoreMetadata {
	md := s.Adapter.Metadata()
	enc := s.payloadEncoding
	if enc == dotc1z.PayloadEncodingUnspecified {
		enc = dotc1z.PayloadEncodingTarZstd
	}
	md.PayloadEncoding = enc.String()
	return md
}

func (s *registeredStore) markDirty(err error) error {
	if err == nil {
		s.closeMu.Lock()
		s.dirty = true
		s.closeMu.Unlock()
	}
	return err
}

func (s *registeredStore) StartNewSync(ctx context.Context, syncType connectorstore.SyncType, parentSyncID string) (string, error) {
	syncID, err := s.Adapter.StartNewSync(ctx, syncType, parentSyncID)
	if err == nil {
		s.closeMu.Lock()
		s.dirty = true
		s.closeMu.Unlock()
	}
	return syncID, err
}

func (s *registeredStore) StartOrResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, bool, error) {
	id, started, err := s.Adapter.StartOrResumeSync(ctx, syncType, syncID)
	if err == nil && started {
		s.closeMu.Lock()
		s.dirty = true
		s.closeMu.Unlock()
	}
	return id, started, err
}

func (s *registeredStore) CheckpointSync(ctx context.Context, syncToken string) error {
	return s.markDirty(s.Adapter.CheckpointSync(ctx, syncToken))
}

func (s *registeredStore) EndSync(ctx context.Context) error {
	return s.markDirty(s.Adapter.EndSync(ctx))
}

// Cleanup prunes old sync data per the SDK retention policy. The
// Adapter-level Cleanup is a no-op (it doesn't have access to the
// retention options); the registeredStore owns those options and
// drives the policy here.
//
// Mirrors (*dotc1z.C1File).Cleanup: gather sync_runs, apply
// dotc1z.SelectSyncsToDelete, range-delete every keyspace scoped
// to each pruned sync, then compact + flush so the next checkpoint
// sees the reclaimed bytes (the Pebble analogue of SQLite VACUUM).
//
// Cancellation model — three passes with different urgency:
//
//   - Pass 1 (logical deletions): once we've committed to a
//     toDelete list, we finish every DeleteSyncData call we can.
//     Each is microseconds (tombstones-only). Between syncs we
//     check ctx as a safety valve for pathological "thousands of
//     deletes + write stall" cases; if we bail mid-pass we return
//     ctx.Err() so syncer.go:531 marks the sync ErrSyncNotComplete
//     and reattempts on the next run.
//
//   - Pass 2 (compaction): purely opportunistic. Compact rewrites
//     SSTs (seconds to minutes) and pebble's background compactor
//     handles eventual disk reclamation regardless of whether we
//     run it here. We skip the whole pass on ctx cancel and log
//     per-sync failures as warnings — neither blocks Cleanup from
//     reporting success.
//
//   - Pass 3 (flush): also opportunistic. Skipped on ctx cancel
//     because the dirty flag we set up-front guarantees Close →
//     CheckpointTo will flush at the next safe boundary.
//
// Net effect: if the syncer's runDuration expires mid-Cleanup,
// every logical deletion we managed to start completes (so the
// next Cleanup re-selects against an accurate post-prune view),
// and we return ctx.Err() to signal the syncer to retry. If the
// budget expires only during the opportunistic passes, the syncer
// sees a successful Cleanup and the sync proceeds normally —
// pebble's background work catches up on the disk reclamation.
func (s *registeredStore) Cleanup(ctx context.Context) error {
	l := ctxzap.Extract(ctx)

	if s.skipCleanup {
		l.Info("skip_cleanup option is set, skipping cleanup of old syncs")
		return nil
	}
	if dotc1z.CleanupSkippedByEnv() {
		l.Info("BATON_SKIP_CLEANUP is set, skipping cleanup of old syncs")
		return nil
	}
	if s.readOnly {
		return nil
	}

	candidates, err := s.collectCleanupCandidates(ctx)
	if err != nil {
		return err
	}

	currentSyncID := s.currentSyncID()
	syncLimit := dotc1z.ResolveCleanupSyncLimit(s.syncLimit, currentSyncID != "")
	l.Debug("found syncs",
		zap.Int("candidate_count", len(candidates)),
		zap.Int("sync_limit", syncLimit))

	toDelete := dotc1z.SelectSyncsToDelete(candidates, currentSyncID, syncLimit)
	if len(toDelete) == 0 {
		return nil
	}
	// Mark dirty before any LSM mutation. A Cleanup that successfully
	// tombstoned one sync and then errored (context cancel, Compact
	// panic, Flush failure) must still drive Close → save →
	// CheckpointTo so the on-disk envelope reflects the in-memory
	// deletions. Otherwise reopening the c1z would resurrect the
	// pruned syncs.
	s.closeMu.Lock()
	s.dirty = true
	s.closeMu.Unlock()

	l.Info("Cleaning up old sync data...",
		zap.Int("delete_count", len(toDelete)),
		zap.Int("sync_limit", syncLimit))

	// === Pass 1: logical deletions (must complete) ===
	deleted := 0
	for _, id := range toDelete {
		if err := ctx.Err(); err != nil {
			l.Info("pebble Cleanup: interrupted mid-pass; remaining syncs deferred to next run",
				zap.Int("deleted", deleted),
				zap.Int("remaining", len(toDelete)-deleted),
				zap.Error(err))
			return err
		}
		if err := s.engine.DeleteSyncData(ctx, id); err != nil {
			return fmt.Errorf("pebble Cleanup: DeleteSyncData(%q): %w", id, err)
		}
		l.Info("Removed old sync data.", zap.String("sync_id", id))
		deleted++
	}

	// === Pass 2: opportunistic compaction ===
	// Skip on cancellation — pebble's background compactor will
	// reclaim the deleted bytes asynchronously, and a re-run of
	// Cleanup won't (and doesn't need to) reattempt compaction
	// because previously-deleted syncs aren't in the next
	// toDelete list. This is a soft permanent skip; the bg
	// compactor is the eventual cleanup path.
	compacted := 0
	for _, id := range toDelete {
		if ctx.Err() != nil {
			l.Info("pebble Cleanup: compaction pass interrupted; deferring to background compactor",
				zap.Int("compacted", compacted),
				zap.Int("remaining", len(toDelete)-compacted))
			break
		}
		if err := s.engine.CompactSyncRanges(ctx, id); err != nil {
			l.Warn("pebble Cleanup: CompactSyncRanges failed; tombstones will linger until background compaction",
				zap.String("sync_id", id),
				zap.Error(err))
		}
		compacted++
	}

	// === Pass 3: opportunistic flush ===
	// Skip on cancellation. Close → CheckpointTo Flushes anyway,
	// and the dirty flag we set up-front guarantees Close runs the
	// save path. The only failure mode skipping Flush opens is
	// "process crashes between Cleanup return and Close call" —
	// pebble's WAL recovery handles that, so the tombstones survive
	// either way.
	if ctx.Err() == nil {
		if err := s.engine.Flush(ctx); err != nil {
			l.Warn("pebble Cleanup: Flush failed; tombstones will flush at Close",
				zap.Error(err))
		}
	}
	return nil
}

// collectCleanupCandidates walks every sync_run record and projects
// it into the engine-neutral SyncCleanupCandidate shape, sorted
// oldest-first. SelectSyncsToDelete depends on this ordering so
// "drop the oldest overflow" trims the right end.
//
// We sort explicitly rather than trust IterateAllSyncRuns' order:
// the iterator walks by sync_id (KSUID), and KSUIDs only encode the
// timestamp to second resolution. Two syncs created in the same
// second sort by the 16-byte random tail rather than chronologically,
// which would silently pick the wrong sync to prune. Sorting on
// started_at (with sync_id tiebreaker) matches the SQLite engine's
// ListSyncRuns ORDER BY id ASC semantics.
func (s *registeredStore) collectCleanupCandidates(ctx context.Context) ([]dotc1z.SyncRun, error) {
	var out []dotc1z.SyncRun
	err := s.engine.IterateAllSyncRuns(ctx, func(r *v3.SyncRunRecord) bool {
		cand := dotc1z.SyncRun{
			ID:           r.GetSyncId(),
			Type:         syncTypeV3ToConnectorstore(r.GetType()),
			SyncToken:    r.GetSyncToken(),
			ParentSyncID: r.GetParentSyncId(),
			SupportsDiff: r.GetSupportsDiff(),
			LinkedSyncID: r.GetLinkedSyncId(),
		}
		if t := r.GetStartedAt(); t != nil {
			tt := t.AsTime()
			cand.StartedAt = &tt
		}
		if t := r.GetEndedAt(); t != nil {
			tt := t.AsTime()
			cand.EndedAt = &tt
		}
		out = append(out, cand)
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("pebble Cleanup: IterateAllSyncRuns: %w", err)
	}
	sort.SliceStable(out, func(i, j int) bool {
		ti, tj := out[i].StartedAt, out[j].StartedAt
		switch {
		case ti == nil && tj == nil:
			return out[i].ID < out[j].ID
		case ti == nil:
			return true
		case tj == nil:
			return false
		case ti.Equal(*tj):
			return out[i].ID < out[j].ID
		default:
			return ti.Before(*tj)
		}
	})
	return out, nil
}

func (s *registeredStore) PutAsset(ctx context.Context, assetRef *v2.AssetRef, contentType string, data []byte) error {
	return s.markDirty(s.Adapter.PutAsset(ctx, assetRef, contentType, data))
}

func (s *registeredStore) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.markDirty(s.Adapter.PutGrants(ctx, grants...))
}

// UnsafePutUniqueGrants is the trusted-import write path (no
// read-before-write, no dedup, parallel encode). Do not use it for live
// connector output. Caller must guarantee unique external_ids across the whole
// destination sync. See Adapter.UnsafePutUniqueGrants.
func (s *registeredStore) UnsafePutUniqueGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.markDirty(s.Adapter.UnsafePutUniqueGrants(ctx, grants...))
}

func (s *registeredStore) PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error {
	return s.markDirty(s.Adapter.PutResourceTypes(ctx, resourceTypes...))
}

func (s *registeredStore) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	return s.markDirty(s.Adapter.PutResources(ctx, resources...))
}

func (s *registeredStore) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	return s.markDirty(s.Adapter.PutEntitlements(ctx, entitlements...))
}

func (s *registeredStore) DeleteGrant(ctx context.Context, grantID string) error {
	return s.markDirty(s.Adapter.DeleteGrant(ctx, grantID))
}

// Grants overrides Adapter.Grants() so the returned GrantStore
// routes StoreExpandedGrants through the registered store's
// dirty-marking path. The Adapter-level wrapper calls
// Adapter.PutGrants directly, which skips the dirty flag.
func (s *registeredStore) Grants() dotc1z.GrantStore {
	return registeredStoreGrants{inner: s.Adapter.Grants(), store: s}
}

// registeredStoreGrants wraps the Adapter-level pebbleGrantStore
// and overrides only StoreExpandedGrants (the lone mutating method)
// to flip the dirty bit. Read-only methods pass through.
type registeredStoreGrants struct {
	inner dotc1z.GrantStore
	store *registeredStore
}

func (g registeredStoreGrants) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	return g.store.markDirty(g.inner.StoreExpandedGrants(ctx, grants...))
}

func (g registeredStoreGrants) PendingExpansionPage(ctx context.Context, pageToken string) ([]dotc1z.PendingExpansion, string, error) {
	return g.inner.PendingExpansionPage(ctx, pageToken)
}

func (g registeredStoreGrants) PendingExpansion(ctx context.Context) iter.Seq2[dotc1z.PendingExpansion, error] {
	return g.inner.PendingExpansion(ctx)
}

func (g registeredStoreGrants) ListWithAnnotationsPage(ctx context.Context, pageToken string) ([]dotc1z.GrantAnnotation, string, error) {
	return g.inner.ListWithAnnotationsPage(ctx, pageToken)
}

func (g registeredStoreGrants) ListWithAnnotationsForResourcePage(
	ctx context.Context, resource *v2.Resource, syncID string, pageToken string, pageSize uint32,
) ([]dotc1z.GrantAnnotation, string, error) {
	return g.inner.ListWithAnnotationsForResourcePage(ctx, resource, syncID, pageToken, pageSize)
}

func (g registeredStoreGrants) ListWithAnnotations(ctx context.Context) iter.Seq2[dotc1z.GrantAnnotation, error] {
	return g.inner.ListWithAnnotations(ctx)
}

func (s *registeredStore) Close(ctx context.Context) (retErr error) {
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

func (s *registeredStore) save(ctx context.Context) error {
	if s.outputFilePath == "" {
		return fmt.Errorf("pebble engine: output file path is empty")
	}
	checkpointDir := filepath.Join(s.tmpDir, "checkpoint")
	if err := s.engine.CheckpointTo(ctx, checkpointDir); err != nil {
		return err
	}

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

	manifest, err := s.manifest()
	if err != nil {
		return err
	}
	if err := formatv3.WriteEnvelope(out, manifest, checkpointDir); err != nil {
		return err
	}
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

func (s *registeredStore) manifest() (*c1zv3.C1ZManifestV3, error) {
	descriptors, err := formatv3.BuildDescriptorClosure()
	if err != nil {
		return nil, err
	}
	return c1zv3.C1ZManifestV3_builder{
		Engine:              string(dotc1z.EnginePebble),
		EngineSchemaVersion: uint32(SDKPebbleFormat),
		PayloadEncoding:     payloadEncodingToProto(s.payloadEncoding),
		Descriptors:         descriptors,
	}.Build(), nil
}

// payloadEncodingToProto maps the public dotc1z.PayloadEncoding to
// the c1zv3 enum. PayloadEncodingUnspecified means "engine default"
// for our purposes: TAR_ZSTD.
func payloadEncodingToProto(enc dotc1z.PayloadEncoding) c1zv3.PayloadEncoding {
	switch enc {
	case dotc1z.PayloadEncodingTar:
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR
	case dotc1z.PayloadEncodingTarZstd, dotc1z.PayloadEncodingUnspecified:
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD
	default:
		// Any non-enumerated value falls back to the default.
		// WriteEnvelope will reject any non-TAR/non-TAR_ZSTD value
		// before writing bytes, so a caller setting a bogus
		// encoding gets an error at save time.
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD
	}
}
