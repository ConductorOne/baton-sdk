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
// v0.1 reads and writes the v1/v2 sqlite-zstd .c1z format via
// connectorstore.Reader / Writer. v3 c1z3 output will land in v0.2
// once the storage-engine-v4 PR stack merges.
package c1zsanitize

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// syncRunMetadataReader is the optional source capability for reading
// sync-run rows with the linkage fields the gRPC reader surface does
// not carry (linked_sync_id, supports_diff). *dotc1z.C1File implements
// it; sources without it skip graph-metadata preservation with a log
// line rather than failing the run.
type syncRunMetadataReader interface {
	ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*dotc1z.SyncRun, string, error)
}

// syncLinkWriter is the optional destination capability for pairing
// diff syncs (partial_upserts <-> partial_deletions) after both runs
// exist.
type syncLinkWriter interface {
	SetSyncLink(ctx context.Context, syncID string, linkedSyncID string) error
}

// supportsDiffWriter is the optional destination capability for
// carrying the supports_diff marker over, so a sanitized c1z remains
// usable as a diff input wherever the source was.
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

// Sanitize copies records from src to dst, transforming identifiers,
// names, free text, emails, and timestamps under the per-c1z secret.
// One destination sync is opened per source sync; parent_sync_id
// linkage is preserved via a srcSyncID → dstSyncID map maintained for
// the duration of the call.
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

	anchor := opts.TimestampAnchor
	if anchor.IsZero() {
		anchor = time.Now().UTC()
	}

	srcSyncs, err := listAllSyncs(ctx, src)
	if err != nil {
		return fmt.Errorf("c1zsanitize: list source syncs: %w", err)
	}

	s := &sanitizer{
		secret:                 opts.Secret,
		idHmac:                 hmac.New(sha256.New, opts.Secret),
		domains:                newDomainMap(),
		shifter:                newTimestampShifter(anchor, findTMax(srcSyncs)),
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
	}

	for _, sr := range srcSyncs {
		if err := s.sanitizeSync(ctx, src, dst, sr); err != nil {
			return fmt.Errorf("c1zsanitize: sanitize sync %s: %w", sr.GetId(), err)
		}
	}

	if err := s.preserveSyncGraphMetadata(ctx, src, dst); err != nil {
		return fmt.Errorf("c1zsanitize: preserve sync graph metadata: %w", err)
	}

	// One structured summary line per run instead of a log line per dropped
	// annotation / missing asset (which fired tens of millions of times on
	// whale-scale files). Counters are accumulated cheaply during the walk.
	s.logDropSummary()
	return nil
}

type sanitizer struct {
	secret                 []byte
	idHmac                 hash.Hash
	domains                *domainMap
	shifter                *timestampShifter
	dropUnknownAnnotations bool
	log                    *zap.Logger
	handlers               map[string]annotationHandler
	syncIDMap              map[string]string
	knownResourceTypes     map[string]struct{}

	// verifyGrantCache turns on the grant sub-cache correctness guard; see
	// Options.VerifyGrantCache. knownResourceTypes is fully populated by
	// copyResourceTypes' buffering pre-pass before any transform reads it, so
	// there is no in-flight phase flag — the set is read-only by construction.
	verifyGrantCache bool

	// warnedUndeclaredTypes dedups the undeclared-resource-type warning so
	// each such token is logged at most once per run.
	warnedUndeclaredTypes map[string]struct{}

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
}

// logDropSummary emits exactly one structured line per run summarizing the
// annotations dropped/passed/failed (keyed by type URL) and the missing-asset
// count, replacing the former per-item log lines.
func (s *sanitizer) logDropSummary() {
	s.log.Info("c1zsanitize: run summary",
		zap.Any("dropped_unknown_annotations", s.droppedAnnotations),
		zap.Any("passed_unknown_annotations", s.passedAnnotations),
		zap.Any("failed_annotations", s.failedAnnotations),
		zap.Uint64("missing_assets", s.missingAssets),
	)
}

// id is the per-sanitizer hot path. SanitizeID stays as the
// allocation-y reference implementation; this one reuses a single
// hmac.Hash so the SHA-256 key schedule isn't redone every call.
// Sanitize runs single-threaded, so no locking is needed.
func (s *sanitizer) id(input string) string {
	if input == "" {
		return ""
	}
	s.idHmac.Reset()
	_, _ = s.idHmac.Write([]byte(input))
	sum := s.idHmac.Sum(nil)
	return idEncoding.EncodeToString(sum[:idTruncationBytes])
}

func (s *sanitizer) sanitizeSync(ctx context.Context, src connectorstore.Reader, dst connectorstore.Writer, sr *reader_v2.SyncRun) error {
	srcSyncID := sr.GetId()
	syncType := connectorstore.SyncType(sr.GetSyncType())
	if syncType == connectorstore.SyncTypeAny || syncType == "" {
		syncType = connectorstore.SyncTypeFull
	}

	parentDst := ""
	if parentSrc := sr.GetParentSyncId(); parentSrc != "" {
		parentDst = s.syncIDMap[parentSrc]
		if parentDst == "" {
			// The parent is not a sync in this c1z — a diff c1z built
			// by GenerateSyncDiffFromFile records the OTHER file's
			// base sync id as the pair's parent. HMAC the external
			// reference instead of dropping it: provenance structure
			// survives, the raw id does not, and two files sanitized
			// under the same secret still cross-reference.
			parentDst = s.id(parentSrc)
		}
	}

	dstSyncID, err := dst.StartNewSync(ctx, syncType, parentDst)
	if err != nil {
		return fmt.Errorf("start dst sync: %w", err)
	}
	s.syncIDMap[srcSyncID] = dstSyncID

	assetRefs := newAssetRefSet()

	// Per-sync memo cache for the embedded Entitlement/Principal transforms
	// in the grant loop. Reset each sync so memory is bounded by one sync's
	// distinct entitlements, not the whole file.
	grantCache := newGrantSubCache(s.verifyGrantCache)

	if err := s.copyResourceTypes(ctx, src, dst, srcSyncID, assetRefs); err != nil {
		return err
	}
	if err := s.copyResources(ctx, src, dst, srcSyncID, assetRefs); err != nil {
		return err
	}
	if err := s.copyEntitlements(ctx, src, dst, srcSyncID, assetRefs); err != nil {
		return err
	}
	if err := s.copyGrants(ctx, src, dst, srcSyncID, assetRefs, grantCache); err != nil {
		return err
	}
	if err := s.copyAssets(ctx, src, dst, assetRefs); err != nil {
		return err
	}

	if err := dst.EndSync(ctx); err != nil {
		return fmt.Errorf("end dst sync: %w", err)
	}
	return nil
}

// preserveSyncGraphMetadata carries the sync-run linkage the proto
// reader surface cannot express — the diff pair's bidirectional
// linked_sync_id and the supports_diff marker — from src runs to their
// dst counterparts. Both sides are optional capabilities: when either
// store lacks them, the copy is skipped with a log line and the output
// remains valid, just without the extra graph metadata.
func (s *sanitizer) preserveSyncGraphMetadata(ctx context.Context, src connectorstore.Reader, dst connectorstore.Writer) error {
	mr, ok := src.(syncRunMetadataReader)
	if !ok {
		s.log.Debug("c1zsanitize: source does not expose sync-run metadata; skipping link/supports_diff preservation")
		return nil
	}
	lw, hasLinkWriter := dst.(syncLinkWriter)
	dw, hasDiffWriter := dst.(supportsDiffWriter)
	if !hasLinkWriter && !hasDiffWriter {
		s.log.Debug("c1zsanitize: destination does not expose sync-run metadata writers; skipping link/supports_diff preservation")
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
			if run.LinkedSyncID != "" && hasLinkWriter {
				if dstLinked := s.syncIDMap[run.LinkedSyncID]; dstLinked != "" {
					if err := lw.SetSyncLink(ctx, dstID, dstLinked); err != nil {
						return fmt.Errorf("set sync link %s: %w", dstID, err)
					}
				}
			}
			if run.SupportsDiff && hasDiffWriter {
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
// every sync run the source can see. The SQLite-backed C1File emits
// in id-asc order which matches insertion order, which is parent-
// before-child for any well-formed sync chain.
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
			return out, nil
		}
		pageToken = resp.GetNextPageToken()
	}
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
