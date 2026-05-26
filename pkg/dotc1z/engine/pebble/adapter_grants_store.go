package pebble

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// Grants returns the GrantStore implementation backed by the Pebble
// adapter. Implements dotc1z.C1ZStore.Grants(); used by the
// expander, the c1-side fileClientWrapper, and the differ.
//
// Caveat: the *Page methods that filter on needs_expansion currently
// return no candidates because grant expansion (Stack 6 per RFC v4)
// is deferred. The methods are CORRECT (they don't break the syncer's
// expansion phase — they make it a no-op) but they don't actually
// find expandable grants. Once Stack 6 lands, the engine will start
// flagging needs_expansion at PutGrants and the index walk here will
// return real rows.
func (a *Adapter) Grants() dotc1z.GrantStore {
	return pebbleGrantStore{a: a}
}

type pebbleGrantStore struct {
	a *Adapter
}

// StoreExpandedGrants writes a batch of grants that have already had
// their expansion annotations consumed by the expander. Matches the
// SQLite C1File.Grants().StoreExpandedGrants contract.
//
// Today this delegates to PutGrants — Pebble doesn't re-extract
// expansion metadata at write time, so there's nothing to strip.
// Once Stack 6 wires expansion-metadata extraction into PutGrants,
// this method will diverge to skip that extraction.
func (g pebbleGrantStore) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	return g.a.PutGrants(ctx, grants...)
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
func (g pebbleGrantStore) PendingExpansionPage(ctx context.Context, pageToken string) ([]dotc1z.PendingExpansion, string, error) {
	syncID := g.a.currentSyncID()
	if syncID == "" {
		return nil, "", ErrNoCurrentSync
	}
	records, next, err := g.a.engine.PaginateGrantsByNeedsExpansion(ctx, syncID, pageToken, DefaultPageSize)
	if err != nil {
		return nil, "", err
	}
	out := make([]dotc1z.PendingExpansion, 0, len(records))
	for _, rec := range records {
		if rec.GetExpansion() == nil {
			// Index entry without expansion metadata on the
			// primary — defensive skip; mirrors the orphan path.
			continue
		}
		exp := rec.GetExpansion()
		out = append(out, dotc1z.PendingExpansion{
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
func (g pebbleGrantStore) PendingExpansion(ctx context.Context) iter.Seq2[dotc1z.PendingExpansion, error] {
	return func(yield func(dotc1z.PendingExpansion, error) bool) {
		pageToken := ""
		for {
			rows, next, err := g.PendingExpansionPage(ctx, pageToken)
			if err != nil {
				yield(dotc1z.PendingExpansion{}, err)
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
// expansion annotations inline. Currently returns grants with nil
// annotations because expansion metadata isn't extracted in v3 yet.
func (g pebbleGrantStore) ListWithAnnotationsPage(ctx context.Context, pageToken string) ([]dotc1z.GrantAnnotation, string, error) {
	syncID := g.a.currentSyncID()
	if syncID == "" {
		return nil, "", ErrNoCurrentSync
	}
	records, next, err := g.a.engine.PaginateGrantsBySync(ctx, syncID, pageToken, DefaultPageSize)
	if err != nil {
		return nil, "", err
	}
	rows := make([]dotc1z.GrantAnnotation, 0, len(records))
	for _, rec := range records {
		rows = append(rows, dotc1z.GrantAnnotation{
			Grant:                   V3GrantToV2(rec),
			Annotation:              nil, // Stack 6 fills this in
			GrantExternalID:         rec.GetExternalId(),
			TargetEntitlementID:     rec.GetEntitlement().GetEntitlementId(),
			PrincipalResourceTypeID: rec.GetPrincipal().GetResourceTypeId(),
			PrincipalResourceID:     rec.GetPrincipal().GetResourceId(),
			NeedsExpansion:          false,
		})
	}
	return rows, next, nil
}

// ListWithAnnotationsForResourcePage filters by principal resource.
// Uses the by_principal index for efficient lookup.
func (g pebbleGrantStore) ListWithAnnotationsForResourcePage(
	ctx context.Context,
	resource *v2.Resource,
	syncID string,
	pageToken string,
	pageSize uint32,
) ([]dotc1z.GrantAnnotation, string, error) {
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
	records, next, err := g.a.engine.PaginateGrantsByPrincipal(ctx, syncID,
		resource.GetId().GetResourceType(), resource.GetId().GetResource(),
		pageToken, limit)
	if err != nil {
		return nil, "", err
	}
	rows := make([]dotc1z.GrantAnnotation, 0, len(records))
	for _, rec := range records {
		rows = append(rows, dotc1z.GrantAnnotation{
			Grant:                   V3GrantToV2(rec),
			Annotation:              nil,
			GrantExternalID:         rec.GetExternalId(),
			TargetEntitlementID:     rec.GetEntitlement().GetEntitlementId(),
			PrincipalResourceTypeID: rec.GetPrincipal().GetResourceTypeId(),
			PrincipalResourceID:     rec.GetPrincipal().GetResourceId(),
			NeedsExpansion:          false,
		})
	}
	return rows, next, nil
}

// ListWithAnnotations walks all pages of ListWithAnnotationsPage.
// Yields each row; on error, yields (zero, err) and terminates.
func (g pebbleGrantStore) ListWithAnnotations(ctx context.Context) iter.Seq2[dotc1z.GrantAnnotation, error] {
	return func(yield func(dotc1z.GrantAnnotation, error) bool) {
		pageToken := ""
		for {
			rows, next, err := g.ListWithAnnotationsPage(ctx, pageToken)
			if err != nil {
				yield(dotc1z.GrantAnnotation{}, err)
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

// === *IfNewer adapter wires ===

// PutGrantsIfNewer writes grants only when their discovered_at is
// strictly after the existing record's discovered_at. Matches the
// SQLite engine's behavior for partial syncs.
func (a *Adapter) PutGrantsIfNewer(ctx context.Context, grants ...*v2.Grant) error {
	syncID := a.currentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	records := make([]*v3.GrantRecord, 0, len(grants))
	now := timestamppb.Now()
	for _, g := range grants {
		if g == nil {
			continue
		}
		rec := V2GrantToV3(syncID, g)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(now)
		}
		records = append(records, rec)
	}
	if err := a.engine.PutGrantRecordsIfNewer(ctx, records...); err != nil {
		return fmt.Errorf("PutGrantsIfNewer: %w", err)
	}
	return nil
}

// PutResourcesIfNewer mirrors PutGrantsIfNewer for resources.
func (a *Adapter) PutResourcesIfNewer(ctx context.Context, resources ...*v2.Resource) error {
	syncID := a.currentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	records := make([]*v3.ResourceRecord, 0, len(resources))
	now := timestamppb.Now()
	for _, r := range resources {
		if r == nil {
			continue
		}
		rec := V2ResourceToV3(syncID, r)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(now)
		}
		records = append(records, rec)
	}
	if err := a.engine.PutResourceRecordsIfNewer(ctx, records...); err != nil {
		return fmt.Errorf("PutResourcesIfNewer: %w", err)
	}
	return nil
}

// PutEntitlementsIfNewer mirrors PutGrantsIfNewer for entitlements.
func (a *Adapter) PutEntitlementsIfNewer(ctx context.Context, entitlements ...*v2.Entitlement) error {
	syncID := a.currentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	records := make([]*v3.EntitlementRecord, 0, len(entitlements))
	now := timestamppb.Now()
	for _, e := range entitlements {
		if e == nil {
			continue
		}
		rec := V2EntitlementToV3(syncID, e)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(now)
		}
		records = append(records, rec)
	}
	if err := a.engine.PutEntitlementRecordsIfNewer(ctx, records...); err != nil {
		return fmt.Errorf("PutEntitlementsIfNewer: %w", err)
	}
	return nil
}

// PutResourceTypesIfNewer mirrors PutGrantsIfNewer for resource_types.
func (a *Adapter) PutResourceTypesIfNewer(ctx context.Context, rts ...*v2.ResourceType) error {
	syncID := a.currentSyncID()
	if syncID == "" {
		return ErrNoCurrentSync
	}
	records := make([]*v3.ResourceTypeRecord, 0, len(rts))
	now := timestamppb.Now()
	for _, rt := range rts {
		if rt == nil {
			continue
		}
		rec := V2ResourceTypeToV3(syncID, rt)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(now)
		}
		records = append(records, rec)
	}
	if err := a.engine.PutResourceTypeRecordsIfNewer(ctx, records...); err != nil {
		return fmt.Errorf("PutResourceTypesIfNewer: %w", err)
	}
	return nil
}
