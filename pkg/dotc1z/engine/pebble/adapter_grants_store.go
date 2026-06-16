package pebble

import (
	"context"
	"errors"
	"iter"

	"github.com/cockroachdb/pebble/v2"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
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
func (a *Adapter) Grants() c1zstore.GrantStore {
	return pebbleGrantStore{a: a}
}

type pebbleGrantStore struct {
	a *Adapter
}

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
	syncID := g.a.currentSyncID()
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
	return g.a.engine.PutExpandedGrantRecords(ctx, merged)
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
	syncID := g.a.currentSyncID()
	if syncID == "" {
		return nil, "", ErrNoCurrentSync
	}
	records, next, err := g.a.engine.PaginateGrantsByNeedsExpansion(ctx, pageToken, DefaultPageSize)
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
		out = append(out, c1zstore.PendingExpansion{
			GrantExternalID:         rec.GetExternalId(),
			TargetEntitlementID:     rec.GetEntitlement().GetEntitlementId(),
			PrincipalResourceTypeID: rec.GetPrincipal().GetResourceTypeId(),
			PrincipalResourceID:     rec.GetPrincipal().GetResourceId(),
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
	syncID := g.a.currentSyncID()
	if syncID == "" {
		return nil, "", ErrNoCurrentSync
	}
	records, next, err := g.a.engine.PaginateGrants(ctx, pageToken, DefaultPageSize)
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
		syncID = g.a.currentSyncID()
	}
	if syncID == "" {
		return nil, "", ErrNoCurrentSync
	}
	limit := clampPageSize(pageSize)
	records, next, err := g.a.engine.PaginateGrantsByEntitlementResource(ctx,
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
