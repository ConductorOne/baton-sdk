package pebble

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// Grants returns the GrantStore implementation backed by the Pebble
// adapter. Implements c1zstore.Store.Grants(); used by the
// expander, the c1-side fileClientWrapper, and the differ.
//
// Caveat: the *Page methods that filter on needs_expansion currently
// return no candidates because grant expansion (Stack 6 per RFC v4)
// is deferred. The methods are CORRECT (they don't break the syncer's
// expansion phase — they make it a no-op) but they don't actually
// find expandable grants. Once Stack 6 lands, the engine will start
// flagging needs_expansion at PutGrants and the index walk here will
// return real rows.
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
	merged := make([]*v3.GrantRecord, 0, len(grants))
	now := timestamppb.Now()
	for _, gr := range grants {
		if gr == nil {
			continue
		}
		newRec := V2GrantToV3(syncID, gr)
		if newRec == nil {
			continue
		}
		// Read the prior v3 record (if any) and preserve its
		// Expansion + NeedsExpansion onto the new record. The
		// payload (Annotations, Sources, identity refs) stays
		// from the new translation; only the expansion side-state
		// is carried forward.
		prior, err := g.a.engine.GetGrantRecord(ctx, syncID, gr.GetId())
		if err != nil {
			if !errors.Is(err, pebble.ErrNotFound) {
				return fmt.Errorf("StoreExpandedGrants: read prior %q: %w", gr.GetId(), err)
			}
			// No prior record: leave whatever V2GrantToV3 set
			// (Expansion is whatever extractV2Expansion produced
			// from the incoming annotations, which the expander
			// has already stripped).
		} else {
			newRec.SetExpansion(prior.GetExpansion())
			newRec.SetNeedsExpansion(prior.GetNeedsExpansion())
			// Preserve the original discovered_at, matching SQLite's
			// PreserveExpansion upsert which omits discovered_at from
			// the on-conflict update set (EXCLUDED.discovered_at is
			// ignored). An expander rewrite of an existing grant must
			// not re-stamp it to "now".
			newRec.SetDiscoveredAt(prior.GetDiscoveredAt())
		}
		// Stamp now only for a genuinely new record (no prior, or a
		// prior that somehow lacked a timestamp).
		if newRec.GetDiscoveredAt() == nil {
			newRec.SetDiscoveredAt(now)
		}
		merged = append(merged, newRec)
	}
	return g.a.engine.PutGrantRecords(ctx, merged...)
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
	records, next, err := g.a.engine.PaginateGrantsByNeedsExpansion(ctx, syncID, pageToken, DefaultPageSize)
	if err != nil {
		return nil, "", err
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
	records, next, err := g.a.engine.PaginateGrantsBySync(ctx, syncID, pageToken, DefaultPageSize)
	if err != nil {
		return nil, "", err
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
	records, next, err := g.a.engine.PaginateGrantsByEntitlementResource(ctx, syncID,
		resource.GetId().GetResourceType(), resource.GetId().GetResource(),
		pageToken, limit)
	if err != nil {
		return nil, "", err
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
