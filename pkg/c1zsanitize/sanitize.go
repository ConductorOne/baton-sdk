// Package c1zsanitize transforms a real .c1z snapshot into an
// identity-stripped copy whose graph topology, cardinalities, and
// annotation structure are preserved. The output is suitable for
// shipping to internal development environments where the original
// customer data must not appear.
//
// The whole transform is driven by a single per-c1z HMAC-SHA256
// secret. Same input → same output within one c1z so cross-references
// stay coherent; different across c1zs whose secrets differ so an
// attacker holding multiple sanitized outputs cannot correlate them.
//
// Sources may use any readable c1z engine. Sanitized output is Pebble,
// single-sync, and resumable through the page ledger.
package c1zsanitize

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// syncRunMetadataReader is the optional source capability for reading
// sync-run rows with the metadata fields the gRPC reader surface does
// not carry (supports_diff). *dotc1z.C1File implements it; sources
// without it skip metadata preservation with a log line rather than
// failing the run.
type syncRunMetadataReader interface {
	ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*c1zstore.SyncRun, string, error)
}

type currentSyncSetter interface {
	SetCurrentSync(ctx context.Context, syncID string) error
}

// supportsDiffWriter is the optional destination capability for
// carrying the supports_diff marker over. Despite the name, this has
// nothing to do with diff syncs (that feature was removed): supports_diff
// is a historically-named sync_runs column that means "grant expansion
// ran on this sync", and its only remaining consumer is
// `baton rollback-expansion`, which refuses syncs without it.
type supportsDiffWriter interface {
	SetSupportsDiff(ctx context.Context, syncID string) error
}

// Options configures a sanitization run.
type Options struct {
	// Secret is the per-c1z HMAC key. Must be at least MinSecretBytes.
	// The operator chooses whether to archive or discard it; the
	// sanitizer never persists it on its own.
	Secret []byte

	// TimestampAnchor is the wall-clock value the newest timestamp in
	// the source c1z lands on. All other timestamps shift by the same
	// delta so relative deltas are preserved. Defaults to time.Now()
	// when zero.
	TimestampAnchor time.Time

	// AllowUnknownAnnotations controls behavior when an annotation's
	// Any type URL is not in the handler registry. The zero value is
	// the safe default: unknown annotations are dropped and a log line
	// names the type URL, so a newly-added annotation type carrying
	// customer data can never pass through unsanitized. Set true to
	// pass unknown annotations through unchanged — convenient for
	// development against new annotation types, dangerous on real
	// customer data.
	AllowUnknownAnnotations bool

	// VerifyGrantCache turns on the off-by-default grant sub-cache
	// correctness guard: for a bounded sample of cache hits per sync, the
	// embedded entitlement/principal is re-transformed and compared
	// (proto.Equal) against the cached value, logging a Warn on any mismatch.
	// It catches a source that violates the one-object-per-id assumption the
	// cache relies on. Adds CPU; intended for diagnostics, not production runs.
	VerifyGrantCache bool
}

// Sanitize transforms the source's single sync into a resumable Pebble sync.
func Sanitize(ctx context.Context, src connectorstore.Reader, dst connectorstore.Writer, opts Options) error {
	if src == nil {
		return errors.New("c1zsanitize: src reader is nil")
	}
	if dst == nil {
		return errors.New("c1zsanitize: dst writer is nil")
	}
	if len(opts.Secret) < MinSecretBytes {
		return fmt.Errorf("c1zsanitize: secret too short: got %d bytes, want at least %d", len(opts.Secret), MinSecretBytes)
	}

	anchorExplicit := !opts.TimestampAnchor.IsZero()
	anchor := opts.TimestampAnchor
	if !anchorExplicit {
		anchor = time.Now().UTC()
	}

	srcSyncs, err := listAllSyncs(ctx, src)
	if err != nil {
		return fmt.Errorf("c1zsanitize: list source syncs: %w", err)
	}

	if dst.Metadata().Engine != string(c1zstore.EnginePebble) {
		return errors.New("c1zsanitize: destination must use the Pebble engine")
	}
	if len(srcSyncs) != 1 {
		return fmt.Errorf("c1zsanitize: Pebble sanitization requires exactly one source sync, got %d", len(srcSyncs))
	}
	if setter, ok := src.(currentSyncSetter); ok {
		if err := setter.SetCurrentSync(ctx, srcSyncs[0].GetId()); err != nil {
			return fmt.Errorf("c1zsanitize: select source sync: %w", err)
		}
	}

	tMax := findTMax(srcSyncs)

	s := &sanitizer{
		secret:                 opts.Secret,
		hmacPool:               newHMACPool(opts.Secret),
		domains:                newDomainMap(),
		shifter:                newTimestampShifter(anchor, tMax),
		dropUnknownAnnotations: !opts.AllowUnknownAnnotations,
		log:                    ctxzap.Extract(ctx),
		handlers:               defaultAnnotationHandlers(),
		syncIDMap:              map[string]string{},
		knownResourceTypes:     map[string]struct{}{},
		verifyGrantCache:       opts.VerifyGrantCache,
		warnedUndeclaredTypes:  map[string]struct{}{},
		droppedAnnotations:     map[string]uint64{},
		passedAnnotations:      map[string]uint64{},
		failedAnnotations:      map[string]uint64{},
		anchor:                 anchor,
		anchorExplicit:         anchorExplicit,
		tMax:                   tMax,
	}
	s.fingerprint = s.secretFingerprint()

	// One structured summary line per run instead of a log line per dropped
	// annotation / missing asset (which fired tens of millions of times on
	// whale-scale files). Deferred so the partial counts are still emitted on
	// an error or panic exit — an aborted run's telemetry is valid and wanted.
	defer s.logDropSummary()

	sourceSync := srcSyncs[0]
	_, err = s.runLedger(ctx, src, dst, sourceSync)
	if err != nil {
		return fmt.Errorf("c1zsanitize: sanitize sync %s: %w", sourceSync.GetId(), err)
	}

	s.completed = true
	return nil
}

type sanitizer struct {
	secret                 []byte
	hmacPool               *sync.Pool
	domains                *domainMap
	shifter                *timestampShifter
	dropUnknownAnnotations bool
	log                    *zap.Logger
	handlers               map[string]annotationHandler
	syncIDMap              map[string]string
	knownResourceTypes     map[string]struct{}

	// verifyGrantCache turns on the grant sub-cache correctness guard; see
	// Options.VerifyGrantCache.
	verifyGrantCache bool

	// warnedUndeclaredTypes dedups the undeclared-resource-type warning so
	// each such token is logged at most once per run.
	warnedUndeclaredTypes map[string]struct{}

	// statsMu guards the per-run counter maps, warnedUndeclaredTypes, and
	// missingAssets. The transform stage fans out across workers, so these
	// otherwise-tiny bookkeeping updates need a lock; the critical sections are
	// map increments and a once-per-key first-occurrence-log decision, so
	// contention is negligible relative to the HMAC/proto work outside it.
	statsMu sync.Mutex

	// Per-run annotation/asset drop counters. transformAnnotations and
	// copyAssets increment these instead of logging per item; logDropSummary
	// emits a single structured line at the end of the run. droppedAnnotations
	// and passedAnnotations are keyed by Any type URL; failedAnnotations counts
	// unmarshal/repack failures by type URL; missingAssets counts asset refs
	// not found in the source.
	droppedAnnotations map[string]uint64
	passedAnnotations  map[string]uint64
	failedAnnotations  map[string]uint64
	missingAssets      uint64

	// completed is set true just before Sanitize's normal return. The summary
	// is deferred, so it also fires on an error/panic exit; run_completed lets
	// a reader tell a full run from a partial one (partial counts are valid).
	completed bool

	fingerprint string

	anchor         time.Time
	anchorExplicit bool
	tMax           time.Time
}

func (s *sanitizer) secretFingerprint() string {
	h := s.hmacPool.Get().(hash.Hash)
	h.Reset()
	_, _ = h.Write([]byte("c1zsanitize-ledger-v1\x00secret-only"))
	sum := h.Sum(nil)
	s.hmacPool.Put(h)
	return idEncoding.EncodeToString(sum)
}

// recordAnnotation increments the per-type-URL counter under statsMu (the
// transform stage is concurrent) and reports whether this was the first
// occurrence, so the caller can emit a single first-occurrence log line per
// type URL outside the lock.
func (s *sanitizer) recordAnnotation(m map[string]uint64, typeURL string) bool {
	s.statsMu.Lock()
	first := m[typeURL] == 0
	m[typeURL]++
	s.statsMu.Unlock()
	return first
}

// logDropSummary emits exactly one structured line per run summarizing the
// annotations dropped/passed/failed (keyed by type URL) and the missing-asset
// count, replacing the former per-item log lines. Deferred in Sanitize, so it
// reports on success, error, and panic exits alike.
func (s *sanitizer) logDropSummary() {
	s.log.Info("c1zsanitize: run summary",
		zap.Bool("run_completed", s.completed),
		zap.Any("dropped_unknown_annotations", s.droppedAnnotations),
		zap.Any("passed_unknown_annotations", s.passedAnnotations),
		zap.Any("failed_annotations", s.failedAnnotations),
		zap.Uint64("missing_assets", s.missingAssets),
	)
}

// id is the per-sanitizer hot path. SanitizeID stays as the allocation-y
// reference implementation; this one borrows a pre-keyed hmac.Hash from a pool
// so the SHA-256 key schedule isn't redone every call, and so the transform
// stage can fan out across workers without sharing hash state. Output depends
// only on (secret, input): each call Resets a hasher it exclusively owns for
// the duration, so goroutine scheduling cannot affect the result.
func (s *sanitizer) id(input string) string {
	if input == "" {
		return ""
	}
	h := s.hmacPool.Get().(hash.Hash)
	h.Reset()
	_, _ = h.Write([]byte(input))
	sum := h.Sum(nil)
	s.hmacPool.Put(h)
	return idEncoding.EncodeToString(sum[:idTruncationBytes])
}

// newHMACPool returns a sync.Pool of HMAC-SHA256 hashers keyed on secret. The
// key schedule is paid once per pooled hasher, not per id() call, and pooling
// lets concurrent transform workers each hold their own hasher.
func newHMACPool(secret []byte) *sync.Pool {
	return &sync.Pool{New: func() any { return hmac.New(sha256.New, secret) }}
}

// preserveSupportsDiffMarkers carries the supports_diff marker — the one
// sync-run metadata field the proto reader surface cannot express — from
// src runs to their dst counterparts. The marker is unrelated to the
// removed diff-sync feature despite its historical name: it records that
// grant expansion ran, and dropping it would silently turn
// `baton rollback-expansion` into ErrSyncNotExpanded on the sanitized
// copy. Both sides are optional capabilities: when either store lacks
// them, the copy is skipped with a log line and the output remains
// valid, just without the marker.
func (s *sanitizer) preserveSupportsDiffMarkers(ctx context.Context, src connectorstore.Reader, dst connectorstore.Writer) error {
	mr, ok := src.(syncRunMetadataReader)
	if !ok {
		s.log.Debug("c1zsanitize: source does not expose sync-run metadata; skipping supports_diff preservation")
		return nil
	}
	dw, ok := dst.(supportsDiffWriter)
	if !ok {
		s.log.Debug("c1zsanitize: destination does not expose SetSupportsDiff; skipping supports_diff preservation")
		return nil
	}

	pageToken := ""
	for {
		runs, next, err := mr.ListSyncRuns(ctx, pageToken, 0)
		if err != nil {
			return fmt.Errorf("list source sync runs: %w", err)
		}
		for _, run := range runs {
			dstID := s.syncIDMap[run.ID]
			if dstID == "" {
				continue
			}
			if run.SupportsDiff {
				if err := dw.SetSupportsDiff(ctx, dstID); err != nil {
					return fmt.Errorf("set supports_diff %s: %w", dstID, err)
				}
			}
		}
		if next == "" {
			return nil
		}
		pageToken = next
	}
}

// listAllSyncs paginates the source SyncsReaderService and returns
// every sync run the source can see, ordered parent-before-child.
//
// The sanitize loop resolves each child's parent through the
// srcSyncID -> dstSyncID map, so a parent must be processed before its
// children or the child gets a dangling HMAC'd parent ref. We sort
// explicitly rather than trust the reader's order: it walks by sync id
// (KSUID), and KSUIDs only encode time to second resolution, so two
// syncs created in the same second can come back child-before-parent.
func listAllSyncs(ctx context.Context, src connectorstore.Reader) ([]*reader_v2.SyncRun, error) {
	var out []*reader_v2.SyncRun
	pageToken := ""
	for {
		req := reader_v2.SyncsReaderServiceListSyncsRequest_builder{
			PageToken: pageToken,
		}.Build()
		resp, err := src.ListSyncs(ctx, req)
		if err != nil {
			return nil, err
		}
		out = append(out, resp.GetSyncs()...)
		if resp.GetNextPageToken() == "" {
			return sortSyncsParentFirst(out), nil
		}
		pageToken = resp.GetNextPageToken()
	}
}

// sortSyncsParentFirst stably reorders syncs so every sync follows its
// parent_sync_id, preserving the reader's order wherever it is already
// valid. It is a stable topological refinement, not a re-sort: a child
// that already trails its in-set parent (the well-formed case, and any
// external-parent sync) keeps its position; only a child that precedes
// its in-set parent is moved to just after that parent. A cycle or
// otherwise unresolvable remainder is flushed in input order rather than
// dropped. Keeping the reader's order as the base means a multi-sync
// SQLite source whose runs are already id-asc is unchanged, while a
// same-second KSUID inversion that put a child before its parent is
// corrected.
func sortSyncsParentFirst(syncs []*reader_v2.SyncRun) []*reader_v2.SyncRun {
	if len(syncs) < 2 {
		return syncs
	}
	present := make(map[string]struct{}, len(syncs))
	for _, s := range syncs {
		present[s.GetId()] = struct{}{}
	}

	emitted := make(map[string]struct{}, len(syncs))
	out := make([]*reader_v2.SyncRun, 0, len(syncs))
	for len(out) < len(syncs) {
		progressed := false
		for _, s := range syncs {
			id := s.GetId()
			if _, done := emitted[id]; done {
				continue
			}
			parent := s.GetParentSyncId()
			_, parentPresent := present[parent]
			_, parentEmitted := emitted[parent]
			if parent == "" || !parentPresent || parentEmitted {
				out = append(out, s)
				emitted[id] = struct{}{}
				progressed = true
			}
		}
		if !progressed {
			for _, s := range syncs {
				if _, done := emitted[s.GetId()]; !done {
					out = append(out, s)
					emitted[s.GetId()] = struct{}{}
				}
			}
		}
	}
	return out
}

func findTMax(syncs []*reader_v2.SyncRun) time.Time {
	var tMax time.Time
	for _, sr := range syncs {
		if sr.HasStartedAt() {
			if t := sr.GetStartedAt().AsTime(); t.After(tMax) {
				tMax = t
			}
		}
		if sr.HasEndedAt() {
			if t := sr.GetEndedAt().AsTime(); t.After(tMax) {
				tMax = t
			}
		}
	}
	return tMax
}

// syncIDAnnotations returns the annotation slice that scopes a list
// request to a specific source sync. The reader resolves the sync ID
// from a SyncDetails annotation; see pkg/dotc1z/sql_helpers.go.
func syncIDAnnotations(srcSyncID string) []*anypb.Any {
	a := annotations.New(c1zpb.SyncDetails_builder{Id: srcSyncID}.Build())
	return a
}
