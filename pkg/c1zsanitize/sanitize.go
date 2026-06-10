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
	"encoding/json"
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

	// Resumable enables checkpointing so an interrupted run continues instead
	// of restarting. Progress is recorded in each destination sync's
	// sync_token (a secret fingerprint, the persisted anchor, the source sync
	// id, the next phase, and the grant page token). On a later run against the
	// SAME destination file, completed phases are skipped and the grant phase
	// resumes from its last committed page. A destination carrying checkpoints
	// from a different Secret is rejected (fail-closed: never mix transforms
	// from two secrets). The anchor is persisted and adopted on resume when this
	// run passed a zero TimestampAnchor, so a default-anchor resumable run
	// resumes cleanly; a resume that passes a DIFFERENT explicit anchor is
	// rejected. The destination's durability between runs is the caller's
	// responsibility (Sanitize does not own the file lifecycle); resume only
	// finds progress that a prior run persisted to disk.
	//
	// Resumable requires the sqlite-backed destination (*dotc1z.C1File). A
	// destination engine that cannot enumerate checkpoints without mutating
	// sync state — currently the pebble engine, whose SetCurrentSync does not
	// rehydrate the persisted step — is rejected with a clear error rather than
	// silently restarting from scratch.
	Resumable bool
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

	anchorExplicit := !opts.TimestampAnchor.IsZero()
	anchor := opts.TimestampAnchor
	if !anchorExplicit {
		anchor = time.Now().UTC()
	}

	srcSyncs, err := listAllSyncs(ctx, src)
	if err != nil {
		return fmt.Errorf("c1zsanitize: list source syncs: %w", err)
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
		resumable:              opts.Resumable,
		anchor:                 anchor,
		anchorExplicit:         anchorExplicit,
		tMax:                   tMax,
	}
	s.fingerprint = s.checkpointFingerprint()

	// One structured summary line per run instead of a log line per dropped
	// annotation / missing asset (which fired tens of millions of times on
	// whale-scale files). Deferred so the partial counts are still emitted on
	// an error or panic exit — an aborted run's telemetry is valid and wanted.
	defer s.logDropSummary()

	// When resuming, read whatever progress a prior run committed to the
	// destination. A destination carrying checkpoints under a different secret
	// or anchor is rejected here — never mix transforms from two secrets.
	resume := map[string]*resumeState{}
	if s.resumable {
		resume, err = s.loadResumeStates(ctx, dst)
		if err != nil {
			return fmt.Errorf("c1zsanitize: load checkpoint: %w", err)
		}
	}

	for _, sr := range srcSyncs {
		rs := resume[sr.GetId()]
		if rs != nil && rs.ended {
			// This source sync was fully copied in a prior run; keep its dst
			// sync id for parent linkage and graph-metadata, skip the work.
			s.syncIDMap[sr.GetId()] = rs.dstSyncID
			continue
		}
		if err := s.sanitizeSync(ctx, src, dst, sr, rs); err != nil {
			return fmt.Errorf("c1zsanitize: sanitize sync %s: %w", sr.GetId(), err)
		}
	}

	if err := s.preserveSyncGraphMetadata(ctx, src, dst); err != nil {
		return fmt.Errorf("c1zsanitize: preserve sync graph metadata: %w", err)
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
	// Options.VerifyGrantCache. knownResourceTypes is fully populated by
	// copyResourceTypes' buffering pre-pass before any transform reads it, so
	// there is no in-flight phase flag — the set is read-only by construction.
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

	// resumable enables checkpointing; fingerprint binds a checkpoint to this
	// run's secret so a destination written under a different secret is never
	// resumed into. See Options.Resumable.
	resumable   bool
	fingerprint string

	// anchor is the timestamp anchor in force for this run; it is persisted in
	// every checkpoint token. anchorExplicit records whether the caller passed
	// it (vs. it defaulting): a resume that did not pass an explicit anchor
	// adopts the persisted one, while a resume that passed a DIFFERENT explicit
	// anchor is rejected. tMax is the source's newest timestamp, retained so the
	// shifter can be rebuilt if the anchor is adopted on resume.
	anchor         time.Time
	anchorExplicit bool
	tMax           time.Time
}

// Checkpoint phases recorded in a destination sync's token. The value names
// the NEXT phase to run, so resume skips everything before it. resource_types
// always re-runs (it is cheap and repopulates the known-type set the id
// transform needs), so it is not a checkpoint phase.
const (
	phaseResources    = "resources"
	phaseEntitlements = "entitlements"
	phaseGrants       = "grants"
	// phaseAssets is the terminal phase, written once grants complete. It marks
	// "all records written; only copyAssets + EndSync remain" so a crash after
	// grants resumes into copyAssets instead of re-running the whole grant phase
	// (a phaseGrants token with an empty page is indistinguishable from "grants
	// not started"). On resume at this phase the resource/entitlement walks still
	// run read-only to repopulate asset refs before copyAssets.
	phaseAssets = "assets"
)

// resumeState is the decoded checkpoint for one source sync's destination sync.
type resumeState struct {
	dstSyncID      string
	ended          bool   // dst sync was EndSync'd in a prior run → fully done
	phase          string // next phase to run
	grantPageToken string // resume point within the grant phase
}

// checkpointToken is the JSON payload stored in a destination sync's
// sync_token. Fingerprint binds it to the secret; SrcSyncID correlates the
// destination sync back to its source. Anchor records the timestamp anchor the
// run used so a later resume that did not pass an explicit anchor can adopt it
// (the anchor is not secret — it is the visible newest output timestamp — so
// persisting it is safe and is what makes a default-anchor run resumable).
type checkpointToken struct {
	Fingerprint string `json:"fp"`
	SrcSyncID   string `json:"src"`
	Phase       string `json:"phase"`
	GrantPage   string `json:"gpt,omitempty"`
	Anchor      string `json:"anchor,omitempty"`
}

// checkpointFingerprint derives a non-reversible binding of the secret. It
// never stores the secret; a different secret yields a different fingerprint,
// so loadResumeStates rejects a destination written under a different secret.
// The anchor is NOT bound here: it is persisted in the token and validated
// separately (adopt-if-zero, fail-closed on a different explicit anchor), so a
// run that let the anchor default can still resume.
func (s *sanitizer) checkpointFingerprint() string {
	h := s.hmacPool.Get().(hash.Hash)
	h.Reset()
	_, _ = h.Write([]byte("c1zsanitize-ckpt-v2\x00secret-only"))
	sum := h.Sum(nil)
	s.hmacPool.Put(h)
	return idEncoding.EncodeToString(sum)
}

// checkpoint records progress on the current destination sync. No-op unless
// resumable. phase names the next phase to run; gpt is the grant page to
// resume from (only meaningful for phaseGrants).
func (s *sanitizer) checkpoint(ctx context.Context, dst connectorstore.Writer, srcSyncID, phase, gpt string) error {
	if !s.resumable {
		return nil
	}
	tok, err := json.Marshal(checkpointToken{
		Fingerprint: s.fingerprint,
		SrcSyncID:   srcSyncID,
		Phase:       phase,
		GrantPage:   gpt,
		Anchor:      s.anchor.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return err
	}
	return dst.CheckpointSync(ctx, string(tok))
}

// dstSyncLister is the destination capability for enumerating sync runs with
// their persisted sync_token and ended_at WITHOUT mutating the current-sync
// pointer. *dotc1z.C1File (the sqlite engine) provides it. Reading checkpoints
// this way avoids the SetCurrentSync+CurrentSyncStep scan, which would leave
// the destination's current sync pointing at an already-ended sync — a state
// that previously caused StartNewSync to hand a fresh sync's records to an
// ended sync.
type dstSyncLister interface {
	ListSyncRuns(ctx context.Context, pageToken string, pageSize uint32) ([]*dotc1z.SyncRun, string, error)
}

// loadResumeStates scans the destination's existing syncs for checkpoint
// tokens written by a prior resumable run, reading them through ListSyncRuns so
// the destination's current-sync pointer is never mutated. A token whose
// fingerprint matches this run yields a resumeState; a token whose fingerprint
// does NOT match means the destination holds output from a different secret,
// which is rejected (fail-closed). The persisted anchor is adopted when this
// run did not pass an explicit one, and a different explicit anchor is rejected
// — so a default-anchor run resumes while a deliberate anchor change cannot
// silently mix two transforms. Destinations with no tokens yield an empty map
// and a clean full run.
//
// A destination that cannot enumerate sync tokens this way (e.g. the pebble
// engine, whose SetCurrentSync/CurrentSyncStep do not round-trip the persisted
// step) is rejected with a clear error rather than silently restarting from
// scratch; resumable runs require the sqlite-backed destination.
func (s *sanitizer) loadResumeStates(ctx context.Context, dst connectorstore.Writer) (map[string]*resumeState, error) {
	lister, ok := dst.(dstSyncLister)
	if !ok {
		return nil, fmt.Errorf("resumable runs require a destination that can enumerate checkpoints without mutating sync state (sqlite-backed); this destination cannot, so resume would silently restart")
	}

	out := map[string]*resumeState{}
	pageToken := ""
	for {
		runs, next, err := lister.ListSyncRuns(ctx, pageToken, 0)
		if err != nil {
			return nil, err
		}
		for _, ds := range runs {
			if ds.SyncToken == "" {
				continue
			}
			var ct checkpointToken
			if json.Unmarshal([]byte(ds.SyncToken), &ct) != nil {
				continue // not our token shape; ignore
			}
			if ct.Fingerprint == "" {
				continue
			}
			if ct.Fingerprint != s.fingerprint {
				return nil, fmt.Errorf("destination has a checkpoint for a different secret; clear the destination before resuming")
			}
			if err := s.reconcileAnchor(ct.Anchor); err != nil {
				return nil, err
			}
			out[ct.SrcSyncID] = &resumeState{
				dstSyncID:      ds.ID,
				ended:          ds.EndedAt != nil,
				phase:          ct.Phase,
				grantPageToken: ct.GrantPage,
			}
		}
		if next == "" {
			return out, nil
		}
		pageToken = next
	}
}

// reconcileAnchor handles the persisted checkpoint anchor. When the caller did
// not pass an explicit anchor, the persisted one is adopted and the timestamp
// shifter rebuilt so resumed output lands on the same anchor as the original
// run (without this, a default-anchor run could never resume — run 2's
// time.Now() anchor would not match run 1's). When the caller DID pass an
// explicit anchor, a mismatch is fail-closed: two different anchors must never
// be mixed into one destination. An empty persisted anchor (older token) is
// ignored.
func (s *sanitizer) reconcileAnchor(persisted string) error {
	if persisted == "" {
		return nil
	}
	pa, err := time.Parse(time.RFC3339Nano, persisted)
	if err != nil {
		return fmt.Errorf("checkpoint has an unparseable anchor %q: %w", persisted, err)
	}
	pa = pa.UTC()
	if s.anchorExplicit {
		if !s.anchor.Equal(pa) {
			return fmt.Errorf("destination was checkpointed with anchor %s but this run was given anchor %s; clear the destination or pass the original anchor", pa.Format(time.RFC3339Nano), s.anchor.Format(time.RFC3339Nano))
		}
		return nil
	}
	if s.anchor.Equal(pa) {
		return nil
	}
	s.anchor = pa
	s.shifter = newTimestampShifter(pa, s.tMax)
	return nil
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

func (s *sanitizer) sanitizeSync(ctx context.Context, src connectorstore.Reader, dst connectorstore.Writer, sr *reader_v2.SyncRun, rs *resumeState) error {
	srcSyncID := sr.GetId()
	syncType := connectorstore.SyncType(sr.GetSyncType())
	if syncType == connectorstore.SyncTypeAny || syncType == "" {
		syncType = connectorstore.SyncTypeFull
	}

	// Resume an unfinished destination sync from a prior run, or start a new
	// one. The phase to start at comes from the checkpoint; a fresh sync starts
	// at phaseResources.
	startPhase := phaseResources
	startGrantPage := ""
	var dstSyncID string
	if rs != nil {
		if err := dst.SetCurrentSync(ctx, rs.dstSyncID); err != nil {
			return fmt.Errorf("resume dst sync: %w", err)
		}
		dstSyncID = rs.dstSyncID
		startPhase = rs.phase
		startGrantPage = rs.grantPageToken
	} else {
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
		newID, err := dst.StartNewSync(ctx, syncType, parentDst)
		if err != nil {
			return fmt.Errorf("start dst sync: %w", err)
		}
		dstSyncID = newID
		// Write an initial checkpoint so an interruption before any phase
		// completes still correlates this dst sync to its source on resume.
		if err := s.checkpoint(ctx, dst, srcSyncID, phaseResources, ""); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}
	}
	s.syncIDMap[srcSyncID] = dstSyncID

	assetRefs := newAssetRefSet()

	// Per-sync memo cache for the embedded Entitlement/Principal transforms
	// in the grant loop. Reset each sync so memory is bounded by one sync's
	// distinct entitlements, not the whole file.
	grantCache := newGrantSubCache(s.verifyGrantCache)

	// resource_types always runs: it is cheap and repopulates knownResourceTypes
	// (which the id transform consults), so it must be rebuilt even when its
	// rows were already written in a prior run. PutResourceTypes upserts, so the
	// re-write is idempotent.
	if err := s.copyResourceTypes(ctx, src, dst, srcSyncID, assetRefs); err != nil {
		return err
	}

	// Resources and entitlements always walk so their trait icon/logo asset refs
	// land in assetRefs before copyAssets runs — the in-memory ref set does not
	// survive a process restart, so on resume the refs collected by a prior run
	// are gone and must be rebuilt. The WRITE is gated on the phase: a phase a
	// prior run already completed re-collects refs read-only instead of
	// re-writing rows that are already durable in the destination. This mirrors
	// copyResourceTypes, which always runs to rebuild knownResourceTypes.
	writeResources := phaseAtOrBefore(startPhase, phaseResources)
	if err := s.copyResources(ctx, src, dst, srcSyncID, assetRefs, writeResources); err != nil {
		return err
	}
	if writeResources {
		if err := s.checkpoint(ctx, dst, srcSyncID, phaseEntitlements, ""); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}
	}

	writeEntitlements := phaseAtOrBefore(startPhase, phaseEntitlements)
	if err := s.copyEntitlements(ctx, src, dst, srcSyncID, assetRefs, writeEntitlements); err != nil {
		return err
	}
	if writeEntitlements {
		if err := s.checkpoint(ctx, dst, srcSyncID, phaseGrants, ""); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}
		startGrantPage = "" // entitlements just finished; grants start at the top
	}

	// Grants run unless a prior run already finished them (checkpoint advanced
	// to the terminal assets phase). copyGrants writes the phaseAssets checkpoint
	// itself once its final page lands.
	if phaseAtOrBefore(startPhase, phaseGrants) {
		if err := s.copyGrants(ctx, src, dst, srcSyncID, assetRefs, grantCache, startGrantPage); err != nil {
			return err
		}
	}

	if err := s.copyAssets(ctx, src, dst, assetRefs); err != nil {
		return err
	}

	if err := dst.EndSync(ctx); err != nil {
		return fmt.Errorf("end dst sync: %w", err)
	}
	return nil
}

// phaseAtOrBefore reports whether the run should execute `target`, given the
// phase it is starting from. Phases are ordered resources < entitlements <
// grants; a start phase at or before the target means the target still needs
// to run.
func phaseAtOrBefore(start, target string) bool {
	return phaseRank(start) <= phaseRank(target)
}

func phaseRank(p string) int {
	switch p {
	case phaseResources:
		return 0
	case phaseEntitlements:
		return 1
	case phaseGrants:
		return 2
	case phaseAssets:
		return 3
	default:
		return 0 // unknown/empty → treat as the earliest phase (run everything)
	}
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
