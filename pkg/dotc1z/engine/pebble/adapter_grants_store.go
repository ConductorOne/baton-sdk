package pebble

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// Grants returns the GrantStore implementation backed by the Pebble
// adapter. Implements c1zstore.Store.Grants(); used by the
// expander, the c1-side fileClientWrapper, and the differ.
//
// needs_expansion is populated at PutGrants time: V2GrantToV3 extracts
// the GrantExpandable annotation and sets NeedsExpansion, which keys the
// idxGrantByNeedsExpansion index. The *Page methods walk that index and
// return real expandable rows (see TestExpansionAnnotationRoundtrip and
// the live PendingExpansion path).
func (e *Engine) Grants() c1zstore.GrantStore {
	return pebbleGrantStore{e: e}
}

type pebbleGrantStore struct {
	e *Engine
}

var expandedGrantImmutableAnnotationAny = func() *anypb.Any {
	a, err := anypb.New(&v2.GrantImmutable{})
	if err != nil {
		panic(fmt.Errorf("pebble: marshal GrantImmutable annotation: %w", err))
	}
	return a
}()

// StoreExpandedGrants writes a batch of grants that have already
// had their expansion annotations consumed by the expander. Matches
// the SQLite C1File.Grants().StoreExpandedGrants contract: the
// payload (display name, sources, etc.) is updated but the
// expansion side-state (Expansion proto + NeedsExpansion flag) is
// PRESERVED from the existing record.
//
// The SQLite path uses `grantUpsertModePreserveExpansion` which
// strips GrantExpandable from annotations but leaves the
// `expansion` and `needs_expansion` columns alone. Pebble has no
// equivalent mode on the upsert path, so we do the read-merge
// here: read the existing v3 record, translate the new v2 payload,
// copy the existing Expansion/NeedsExpansion onto the new record,
// then write. Idempotent on a non-existent grant (Expansion stays
// nil, NeedsExpansion stays false).
//
// Without this preservation, an expander rewrite would clobber
// the side-state and a subsequent PendingExpansion walk would
// silently miss the grant — breaking the expansion pipeline.
// Caught by TestStoreExpandedGrantsPreservesExpansion and the
// SQLite conformance test of the same name.
func (g pebbleGrantStore) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	syncID := g.e.CurrentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	if len(grants) == 0 {
		return nil
	}
	// Translate only. The expansion side-state preservation
	// (Expansion + NeedsExpansion + discovered_at carried forward from
	// the prior record) and the new-record discovered_at stamp are
	// performed inside PutExpandedGrantRecords, which already reads the
	// prior record to clean up its index entries — folding the two
	// reads the old path issued (this preservation Get plus the
	// index-cleanup Get in PutGrantRecords) into one. See its doc for
	// why expansion writes also commit NoSync.
	// Arena-allocate the v3 record/entitlement/principal structs in three
	// contiguous backing arrays instead of 3 heap allocs per grant — the same
	// batching the bulk PutGrants path uses. translateV2Grant is behaviorally
	// equivalent to V2GrantToV3 and (like it) leaves discovered_at unset:
	// PutExpandedGrantRecords stamps/preserves it. The returned pointers alias
	// the arena, which outlives this call's use of `merged`.
	merged := g.translateExpanded(syncID, grants)
	return g.e.PutExpandedGrantRecords(ctx, merged)
}

// StoreNewExpandedGrants is the fast path for synthesized expanded grants. The
// caller guarantees no existing grant has the same structured identity, so Pebble
// can skip the read-before-write Get used to preserve side-state and clean stale
// indexes for updates.
func (g pebbleGrantStore) StoreNewExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	syncID := g.e.CurrentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	if len(grants) == 0 {
		return nil
	}
	merged := g.translateExpanded(syncID, grants)
	return g.e.PutSynthesizedGrantRecords(ctx, merged)
}

// appendSynthesizedGrantRecords translates one destination's synthesized
// contributions into engine records, appending to records. Shared by the
// per-destination write path and the layer-scoped layer session.
func appendSynthesizedGrantRecords(records []synthesizedGrantRecord, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) ([]synthesizedGrantRecord, error) {
	if len(principals) != len(sources) {
		return records, fmt.Errorf("store new expanded grant contributions: principals/sources length mismatch")
	}
	entRef := entitlementToRef(dest)
	if entRef == nil {
		return records, fmt.Errorf("store new expanded grant contributions: entitlement is nil")
	}
	entID := entitlementIdentityFromParts(entRef.GetResourceTypeId(), entRef.GetResourceId(), entRef.GetEntitlementId())
	for i, principal := range principals {
		if principal == nil || principal.GetResourceTypeId() == "" || principal.GetResourceId() == "" {
			continue
		}
		if len(sources[i]) == 0 {
			return records, fmt.Errorf("store new expanded grant contributions: empty sources")
		}
		id := grantIdentity{
			entitlement:     entID,
			principalTypeID: principal.GetResourceTypeId(),
			principalID:     principal.GetResourceId(),
		}
		// The public external id (the legacy concat) is not materialized
		// here: the engine encoders build it directly on the wire from the
		// refs (appendSynthGrantExternalIDWire).
		records = append(records, synthesizedGrantRecord{
			id:          id,
			entitlement: entRef,
			principal:   principal,
			sources:     sources[i],
		})
	}
	return records, nil
}

func (g pebbleGrantStore) StoreNewExpandedGrantContributions(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error {
	if g.e.CurrentSyncID() == "" {
		return ErrNoCurrentSync
	}
	if len(principals) == 0 {
		return nil
	}
	// Pooled: the engine's Put/ingest paths copy everything they need out of
	// records before returning, so the backing array is safe to recycle.
	recordsPtr := getSynthRecords()
	records := *recordsPtr
	defer func() {
		*recordsPtr = records
		putSynthRecords(recordsPtr)
	}()
	records, err := appendSynthesizedGrantRecords(records, dest, principals, sources)
	if err != nil {
		return err
	}
	return g.e.PutSynthesizedGrantContributions(ctx, records)
}

// BeginExpandedGrantLayer opens a layer-scoped synthesized-grant layer session
// on the engine. Returns false when the engine cannot serve one (e.g. the
// by_principal index is not deferred); callers fall back to
// StoreNewExpandedGrantContributions.
func (g pebbleGrantStore) BeginExpandedGrantLayer(ctx context.Context) (bool, error) {
	if g.e.CurrentSyncID() == "" {
		return false, ErrNoCurrentSync
	}
	return g.e.BeginSynthesizedGrantLayer(ctx)
}

// AddExpandedGrantLayerContributions streams one destination's synthesized
// contributions into the open layer session. Rows become readable when
// FinishExpandedGrantLayer publishes the layer.
func (g pebbleGrantStore) AddExpandedGrantLayerContributions(ctx context.Context, dest *v2.Entitlement, principals []*v3.PrincipalRef, sources []batonGrant.Sources) error {
	if len(principals) == 0 {
		return nil
	}
	// Pooled: the engine encodes everything it needs out of records before
	// returning, so the backing array is safe to recycle.
	recordsPtr := getSynthRecords()
	records := *recordsPtr
	defer func() {
		*recordsPtr = records
		putSynthRecords(recordsPtr)
	}()
	records, err := appendSynthesizedGrantRecords(records, dest, principals, sources)
	if err != nil {
		return err
	}
	return g.e.AddSynthesizedGrantLayerContributions(ctx, records)
}

func (g pebbleGrantStore) FinishExpandedGrantLayer(ctx context.Context) error {
	return g.e.FinishSynthesizedGrantLayer(ctx)
}

func (g pebbleGrantStore) AbortExpandedGrantLayer(ctx context.Context) error {
	return g.e.AbortSynthesizedGrantLayer(ctx)
}

func (g pebbleGrantStore) translateExpanded(syncID string, grants []*v2.Grant) []*v3.GrantRecord {
	merged := make([]*v3.GrantRecord, 0, len(grants))
	arena := newGrantTranslateArena(len(grants))
	for _, gr := range grants {
		if gr == nil {
			continue
		}
		newRec := arena.translateV2Grant(syncID, gr)
		if newRec == nil {
			continue
		}
		// StoreExpandedGrants consumes expansion metadata before persisting.
		// Existing records get their prior Expansion/NeedsExpansion restored in
		// PutExpandedGrantRecords; new records must not become expandable just
		// because the caller left a residual GrantExpandable annotation.
		newRec.SetExpansion(nil)
		newRec.SetNeedsExpansion(false)
		merged = append(merged, newRec)
	}
	return merged
}

// PendingExpansionPage returns the next page of grants whose
// NeedsExpansion flag is set on the current sync. Backed by the
// engine's by_needs_expansion index keyspace (populated by
// writeGrantIndexes when r.NeedsExpansion=true and removed by
// deleteGrantIndexes on overwrite/delete). Equivalent to the
// SQLite partial index `WHERE needs_expansion = 1`.
//
// The materialized GrantRecord is reshaped into the
// PendingExpansion struct the syncer's ExpandGrants loop expects.
// A grant that lost its expansion annotation between Put and the
// index scan (e.g. partial overwrite) is skipped — same orphan
// semantic as the by_entitlement / by_principal indexes.
func (g pebbleGrantStore) PendingExpansionPage(ctx context.Context, pageToken string) ([]c1zstore.PendingExpansion, string, error) {
	syncID := g.e.CurrentSyncID()
	if syncID == "" {
		return nil, "", ErrNoCurrentSync
	}
	records, next, err := g.e.PaginateGrantsByNeedsExpansion(ctx, pageToken, DefaultPageSize)
	if err != nil {
		return nil, "", c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	out := make([]c1zstore.PendingExpansion, 0, len(records))
	for _, rec := range records {
		if rec.GetExpansion() == nil {
			// Index entry without expansion metadata on the
			// primary — defensive skip; mirrors the orphan path.
			continue
		}
		exp := rec.GetExpansion()
		ent := rec.GetEntitlement()
		princ := rec.GetPrincipal()
		// Raw pass-through: refs and expandable ids are the connector's own
		// strings; the graph, sources maps, and stored refs all share them.
		out = append(out, c1zstore.PendingExpansion{
			GrantExternalID:         rec.GetExternalId(),
			TargetEntitlementID:     ent.GetEntitlementId(),
			PrincipalResourceTypeID: princ.GetResourceTypeId(),
			PrincipalResourceID:     princ.GetResourceId(),
			Annotation: v2.GrantExpandable_builder{
				EntitlementIds:  exp.GetEntitlementIds(),
				Shallow:         exp.GetShallow(),
				ResourceTypeIds: exp.GetResourceTypeIds(),
			}.Build(),
			NeedsExpansion: true,
		})
	}
	return out, next, nil
}

// PendingExpansion walks PendingExpansionPage page-by-page.
// Iteration contract matches the SQLite GrantStore implementation:
// on error the sequence yields (zero, err) and terminates.
func (g pebbleGrantStore) PendingExpansion(ctx context.Context) iter.Seq2[c1zstore.PendingExpansion, error] {
	return func(yield func(c1zstore.PendingExpansion, error) bool) {
		pageToken := ""
		for {
			rows, next, err := g.PendingExpansionPage(ctx, pageToken)
			if err != nil {
				yield(c1zstore.PendingExpansion{}, err)
				return
			}
			for _, pe := range rows {
				if !yield(pe, nil) {
					return
				}
			}
			if next == "" {
				return
			}
			pageToken = next
		}
	}
}

// ListWithAnnotationsPage returns the next page of grants with their
// expansion annotations inline. The typed Annotation pointer is
// populated from the v3 GrantRecord's Expansion field (nil for
// non-expandable grants, matching the SQLite reader), so consumers
// that gate on `Annotation != nil` — the syncer's
// processGrantsWithExternalPrincipals and c1's fileClientWrapper —
// behave identically across engines.
func (g pebbleGrantStore) ListWithAnnotationsPage(ctx context.Context, pageToken string) ([]c1zstore.GrantAnnotation, string, error) {
	syncID := g.e.CurrentSyncID()
	if syncID == "" {
		return nil, "", ErrNoCurrentSync
	}
	records, next, err := g.e.PaginateGrants(ctx, pageToken, DefaultPageSize)
	if err != nil {
		return nil, "", c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	rows := make([]c1zstore.GrantAnnotation, 0, len(records))
	for _, rec := range records {
		ent := rec.GetEntitlement()
		princ := rec.GetPrincipal()
		rows = append(rows, c1zstore.GrantAnnotation{
			Grant:                   V3GrantToV2(rec),
			Annotation:              expansionRecordToV2(rec.GetExpansion()),
			GrantExternalID:         rec.GetExternalId(),
			TargetEntitlementID:     ent.GetEntitlementId(),
			PrincipalResourceTypeID: princ.GetResourceTypeId(),
			PrincipalResourceID:     princ.GetResourceId(),
			NeedsExpansion:          rec.GetNeedsExpansion(),
		})
	}
	return rows, next, nil
}

// ListWithAnnotationsForResourcePage filters by the entitlement-side
// resource of each grant — matches the SQLite `c1FileGrantStore`
// path and the GrantStore interface comment ("grants ON the given
// resource"). Used by the c1-side fileClientWrapper that emulates a
// connector from a c1z file and forwards a ListGrants RPC whose
// request has a Resource filter. Uses the by_entitlement_resource
// index for efficient lookup.
func (g pebbleGrantStore) ListWithAnnotationsForResourcePage(
	ctx context.Context,
	resource *v2.Resource,
	syncID string,
	pageToken string,
	pageSize uint32,
) ([]c1zstore.GrantAnnotation, string, error) {
	if resource == nil || resource.GetId() == nil {
		return nil, "", errors.New("ListWithAnnotationsForResourcePage: nil resource")
	}
	if syncID == "" {
		syncID = g.e.CurrentSyncID()
	}
	if syncID == "" {
		return nil, "", ErrNoCurrentSync
	}
	limit := clampPageSize(pageSize)
	records, next, err := g.e.PaginateGrantsByEntitlementResource(ctx,
		resource.GetId().GetResourceType(), resource.GetId().GetResource(),
		pageToken, limit)
	if err != nil {
		return nil, "", c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
	}
	rows := make([]c1zstore.GrantAnnotation, 0, len(records))
	for _, rec := range records {
		rows = append(rows, c1zstore.GrantAnnotation{
			Grant:                   V3GrantToV2(rec),
			Annotation:              expansionRecordToV2(rec.GetExpansion()),
			GrantExternalID:         rec.GetExternalId(),
			TargetEntitlementID:     rec.GetEntitlement().GetEntitlementId(),
			PrincipalResourceTypeID: rec.GetPrincipal().GetResourceTypeId(),
			PrincipalResourceID:     rec.GetPrincipal().GetResourceId(),
			NeedsExpansion:          rec.GetNeedsExpansion(),
		})
	}
	return rows, next, nil
}

// ListWithAnnotations walks all pages of ListWithAnnotationsPage.
// Yields each row; on error, yields (zero, err) and terminates.
func (g pebbleGrantStore) ListWithAnnotations(ctx context.Context) iter.Seq2[c1zstore.GrantAnnotation, error] {
	return func(yield func(c1zstore.GrantAnnotation, error) bool) {
		pageToken := ""
		for {
			rows, next, err := g.ListWithAnnotationsPage(ctx, pageToken)
			if err != nil {
				yield(c1zstore.GrantAnnotation{}, err)
				return
			}
			for _, r := range rows {
				if !yield(r, nil) {
					return
				}
			}
			if next == "" {
				return
			}
			pageToken = next
		}
	}
}
