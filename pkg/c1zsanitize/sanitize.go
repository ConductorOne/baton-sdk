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
	"math"
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
		secret:                        opts.Secret,
		idHmac:                        hmac.New(sha256.New, opts.Secret),
		domains:                       newDomainMap(),
		shifter:                       newTimestampShifter(anchor, findTMax(srcSyncs)),
		dropUnknownAnnotations:        !opts.AllowUnknownAnnotations,
		log:                           ctxzap.Extract(ctx),
		handlers:                      defaultAnnotationHandlers(),
		syncIDMap:                     map[string]string{},
		knownResourceTypes:            map[string]struct{}{},
		droppedUnknownAnnotationTypes: map[string]uint64{},
		passedUnknownAnnotationTypes:  map[string]uint64{},
	}

	s.log.Info("c1zsanitize: starting", zap.Int("source_syncs", len(srcSyncs)))
	overallStart := time.Now()

	for _, sr := range srcSyncs {
		if err := s.sanitizeSync(ctx, src, dst, sr); err != nil {
			return fmt.Errorf("c1zsanitize: sanitize sync %s: %w", sr.GetId(), err)
		}
	}

	if err := s.preserveSyncGraphMetadata(ctx, src, dst); err != nil {
		return fmt.Errorf("c1zsanitize: preserve sync graph metadata: %w", err)
	}

	s.log.Info("c1zsanitize: complete",
		zap.Duration("elapsed", time.Since(overallStart)),
		zap.Uint64("dropped_unknown_annotations_total", s.droppedUnknownAnnotations),
		zap.Uint64("passed_unknown_annotations_total", s.passedUnknownAnnotations),
		zap.Any("dropped_unknown_annotation_types", s.droppedUnknownAnnotationTypes),
		zap.Any("passed_unknown_annotation_types", s.passedUnknownAnnotationTypes),
	)
	return nil
}

// syncStatsReader is the optional source capability the per-sync phase
// progress uses to resolve true N-of-M totals (resource_types,
// resources, entitlements, grants) before each phase starts.
// *dotc1z.C1File implements it — Stats reads the cached sync_runs row
// for a completed sync, so the call is O(1) and not a table scan.
// Sources that don't implement it (test fakes) fall through to
// running-count progress logging.
type syncStatsReader interface {
	Stats(ctx context.Context, syncType connectorstore.SyncType, syncId string) (map[string]int64, error)
}

// phaseTotals holds the up-front per-record-type counts the progress
// logger uses to emit N-of-M / percent / ETA. Zero values mean "not
// known" — the progress logger falls back to running-count + rate.
type phaseTotals struct {
	resourceTypes int64
	resources     int64
	entitlements  int64
	grants        int64
}

// resolvePhaseTotals returns the per-record-type totals for a source
// sync via the optional syncStatsReader capability. A failure or an
// uncapable reader returns zero totals; the progress logger then
// reports running-count rather than failing the run.
func (s *sanitizer) resolvePhaseTotals(ctx context.Context, src connectorstore.Reader, srcSyncID string, syncType connectorstore.SyncType) phaseTotals {
	sr, ok := src.(syncStatsReader)
	if !ok {
		s.log.Debug("c1zsanitize: source does not expose Stats; progress will be running-count only",
			zap.String("src_sync_id", srcSyncID))
		return phaseTotals{}
	}
	m, err := sr.Stats(ctx, syncType, srcSyncID)
	if err != nil {
		s.log.Warn("c1zsanitize: source Stats lookup failed; progress will be running-count only",
			zap.String("src_sync_id", srcSyncID), zap.Error(err))
		return phaseTotals{}
	}
	// Resources are returned as a per-resource-type breakdown plus the
	// well-known reserved keys. Sum the non-reserved entries to get the
	// resource grand total.
	totals := phaseTotals{
		resourceTypes: m["resource_types"],
		entitlements:  m["entitlements"],
		grants:        m["grants"],
	}
	for k, v := range m {
		switch k {
		case "resource_types", "entitlements", "grants":
			// reserved keys, not per-resource-type counts
		default:
			totals.resources += v
		}
	}
	return totals
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

	// Cumulative across the whole Sanitize() call. The per-phase
	// progress helper diffs these between phase start and end to
	// attribute the dropped/passed totals to the phase that incurred
	// them. Sanitize runs single-threaded, so no synchronization is
	// needed.
	droppedUnknownAnnotations     uint64
	droppedUnknownAnnotationTypes map[string]uint64
	passedUnknownAnnotations      uint64
	passedUnknownAnnotationTypes  map[string]uint64
}

// phaseProgress emits structured log lines for one copy* phase: a
// start marker, periodic running progress, and a done summary. When
// the per-record-type total is known up front (via the optional
// syncStatsReader capability on the source) progress includes
// percent-complete, items remaining, items/sec rate, and ETA. When
// the total is not known, progress falls back to a running count plus
// rate so an operator can still extrapolate "how much longer?"
//
// Periodic logs are throttled to one per tick, so a multi-hour
// sanitize emits dozens of progress lines, not millions — the failure
// mode the per-annotation log produced.
type phaseProgress struct {
	log            *zap.Logger
	phase          string
	srcSyncID      string
	total          int64
	started        time.Time
	lastLog        time.Time
	tick           time.Duration
	items          uint64
	pages          uint64
	droppedAtStart uint64
	passedAtStart  uint64
	s              *sanitizer
}

// phaseProgressTick is the throttle interval for the periodic
// per-phase progress log. 10 seconds is short enough that an operator
// watching the log sees regular advancement and long enough that a
// fast phase doesn't flood the log.
const phaseProgressTick = 10 * time.Second

func (s *sanitizer) startPhase(phase, srcSyncID string, total int64) *phaseProgress {
	now := time.Now()
	if total > 0 {
		s.log.Info("c1zsanitize: phase start",
			zap.String("phase", phase),
			zap.String("src_sync_id", srcSyncID),
			zap.Int64("total", total),
		)
	} else {
		s.log.Info("c1zsanitize: phase start",
			zap.String("phase", phase),
			zap.String("src_sync_id", srcSyncID),
		)
	}
	return &phaseProgress{
		log:            s.log,
		phase:          phase,
		srcSyncID:      srcSyncID,
		total:          total,
		started:        now,
		lastLog:        now,
		tick:           phaseProgressTick,
		droppedAtStart: s.droppedUnknownAnnotations,
		passedAtStart:  s.passedUnknownAnnotations,
		s:              s,
	}
}

// page increments the running count by the size of the page just
// processed and emits a periodic progress log line if the tick
// interval has elapsed since the last one. Called once per page
// boundary inside the copy* loops.
func (p *phaseProgress) page(n int) {
	if p == nil {
		return
	}
	p.pages++
	if n > 0 {
		p.items += uint64(n)
	}
	now := time.Now()
	if now.Sub(p.lastLog) < p.tick {
		return
	}
	p.emit(now, "c1zsanitize: phase progress")
	p.lastLog = now
}

// done emits the per-phase summary log line: total processed, total
// elapsed, average rate, dropped/passed annotation aggregates scoped
// to this phase (diff against the sanitizer-level counters).
func (p *phaseProgress) done() {
	if p == nil {
		return
	}
	p.emit(time.Now(), "c1zsanitize: phase done")
}

func (p *phaseProgress) emit(now time.Time, msg string) {
	elapsed := now.Sub(p.started)
	rate := 0.0
	if secs := elapsed.Seconds(); secs > 0 {
		rate = float64(p.items) / secs
	}
	fields := []zap.Field{
		zap.String("phase", p.phase),
		zap.String("src_sync_id", p.srcSyncID),
		zap.Uint64("processed", p.items),
		zap.Uint64("pages", p.pages),
		zap.Duration("elapsed", elapsed),
		zap.Float64("items_per_sec", rate),
		zap.Uint64("dropped_unknown_annotations", p.s.droppedUnknownAnnotations-p.droppedAtStart),
		zap.Uint64("passed_unknown_annotations", p.s.passedUnknownAnnotations-p.passedAtStart),
	}
	if p.total > 0 {
		// p.items is a uint64 counter that monotonically increases as
		// the loop pages; over-clamp to math.MaxInt64 before subtract
		// so the conversion can never wrap negative.
		var processed int64
		if p.items > math.MaxInt64 {
			processed = math.MaxInt64
		} else {
			processed = int64(p.items)
		}
		remaining := p.total - processed
		if remaining < 0 {
			remaining = 0
		}
		percent := 0.0
		if p.total > 0 {
			percent = float64(p.items) / float64(p.total) * 100.0
		}
		fields = append(fields,
			zap.Int64("total", p.total),
			zap.Int64("remaining", remaining),
			zap.Float64("percent_complete", percent),
		)
		// ETA only meaningful when we've seen some throughput AND we
		// don't already have everything. Truncate to a whole second
		// so the log value reads cleanly.
		if rate > 0 && remaining > 0 {
			etaSec := float64(remaining) / rate
			fields = append(fields, zap.Duration("eta", time.Duration(etaSec*float64(time.Second)).Round(time.Second)))
		}
	}
	p.log.Info(msg, fields...)
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

	totals := s.resolvePhaseTotals(ctx, src, srcSyncID, syncType)
	s.log.Info("c1zsanitize: sync start",
		zap.String("src_sync_id", srcSyncID),
		zap.String("dst_sync_id", dstSyncID),
		zap.String("sync_type", string(syncType)),
		zap.Int64("resource_types_total", totals.resourceTypes),
		zap.Int64("resources_total", totals.resources),
		zap.Int64("entitlements_total", totals.entitlements),
		zap.Int64("grants_total", totals.grants),
	)
	syncStart := time.Now()

	assetRefs := newAssetRefSet()

	if err := s.copyResourceTypes(ctx, src, dst, srcSyncID, assetRefs, totals.resourceTypes); err != nil {
		return err
	}
	if err := s.copyResources(ctx, src, dst, srcSyncID, assetRefs, totals.resources); err != nil {
		return err
	}
	if err := s.copyEntitlements(ctx, src, dst, srcSyncID, assetRefs, totals.entitlements); err != nil {
		return err
	}
	if err := s.copyGrants(ctx, src, dst, srcSyncID, assetRefs, totals.grants); err != nil {
		return err
	}
	if err := s.copyAssets(ctx, src, dst, assetRefs); err != nil {
		return err
	}

	s.log.Info("c1zsanitize: sync done",
		zap.String("src_sync_id", srcSyncID),
		zap.String("dst_sync_id", dstSyncID),
		zap.Duration("elapsed", time.Since(syncStart)),
	)

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
