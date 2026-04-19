package dotc1z

import (
	"context"
	"iter"

	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

// Compile-time checks that *C1File satisfies C1ZStore and that the value
// wrapper structs satisfy each sub-interface. These assertions catch
// signature drift at build time rather than at the first runtime call.
var (
	_ C1ZStore   = (*C1File)(nil)
	_ GrantStore = c1FileGrantStore{}
	_ SyncMeta   = c1FileSyncMeta{}
	_ FileOps    = c1FileFileOps{}
)

// Grants returns the grant-store slice of this c1z.
func (c *C1File) Grants() GrantStore { return c1FileGrantStore{c} }

// SyncMeta returns the sync-metadata slice of this c1z.
func (c *C1File) SyncMeta() SyncMeta { return c1FileSyncMeta{c} }

// FileOps returns the file-operations slice of this c1z.
func (c *C1File) FileOps() FileOps { return c1FileFileOps{c} }

// -----------------------------------------------------------------------------
// GrantStore
// -----------------------------------------------------------------------------

// c1FileGrantStore adapts *C1File to GrantStore. It is a zero-allocation
// value wrapper so .Grants() does not allocate per-call.
type c1FileGrantStore struct{ c *C1File }

// StoreExpandedGrants implements GrantStore by delegating to the
// top-level *C1File.StoreExpandedGrants, which is where the actual
// implementation lives. See (*C1File).StoreExpandedGrants for semantics.
func (g c1FileGrantStore) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	return g.c.StoreExpandedGrants(ctx, grants...)
}

// StoreExpandedGrants persists grants produced by the expander. It strips
// any residual GrantExpandable annotation from each grant payload and
// then delegates to the internal UpsertGrants path using PreserveExpansion
// mode so existing expansion/needs_expansion columns are left untouched.
//
// The strip step is defensive: the expander's upstream callers consume
// the annotation before writing, but a residual annotation on the
// payload would disagree with the stored expansion columns. Stripping
// makes the method total regardless of caller discipline.
//
// This method is exposed on *C1File (not just via GrantStore) because
// test helpers in pkg/sync/expand construct a *C1File directly and
// pass it to NewExpander; putting it at the top level keeps those
// tests free of sub-store wiring.
func (c *C1File) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	ctx, span := tracer.Start(ctx, "C1File.StoreExpandedGrants")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	// Strip residual GrantExpandable annotations defensively. Cloning
	// only happens when there's something to strip, so grants without
	// an annotation incur no allocation.
	stripped := grants
	if len(grants) > 0 {
		stripped = make([]*v2.Grant, len(grants))
		for i, gr := range grants {
			if gr == nil || !hasGrantExpandable(gr) {
				stripped[i] = gr
				continue
			}
			clone := proto.Clone(gr).(*v2.Grant)
			_, _ = extractAndStripExpansion(clone)
			stripped[i] = clone
		}
	}

	// Delegate read-only check and empty-input handling to UpsertGrants.
	err = c.UpsertGrants(ctx, connectorstore.GrantUpsertOptions{
		Mode: connectorstore.GrantUpsertModePreserveExpansion,
	}, stripped...)
	return err
}

// PendingExpansionPage implements GrantStore. Thin wrapper over
// listExpandableGrantsInternal(Mode: ExpansionNeedsOnly) that reshapes
// the internal row struct into the exported PendingExpansion shape.
func (g c1FileGrantStore) PendingExpansionPage(ctx context.Context, pageToken string) ([]PendingExpansion, string, error) {
	defs, nextPageToken, err := g.c.listExpandableGrantsInternal(ctx, connectorstore.GrantListOptions{
		Mode:      connectorstore.GrantListModeExpansionNeedsOnly,
		PageToken: pageToken,
	})
	if err != nil {
		return nil, "", err
	}
	out := make([]PendingExpansion, 0, len(defs))
	for _, def := range defs {
		if def == nil {
			continue
		}
		out = append(out, PendingExpansion{
			GrantExternalID:         def.GrantExternalID,
			TargetEntitlementID:     def.TargetEntitlementID,
			PrincipalResourceTypeID: def.PrincipalResourceTypeID,
			PrincipalResourceID:     def.PrincipalResourceID,
			Annotation: v2.GrantExpandable_builder{
				EntitlementIds:  def.SourceEntitlementIDs,
				Shallow:         def.Shallow,
				ResourceTypeIds: def.ResourceTypeIDs,
			}.Build(),
			NeedsExpansion: def.NeedsExpansion,
		})
	}
	return out, nextPageToken, nil
}

// PendingExpansion implements GrantStore. Convenience iterator that
// walks every page via PendingExpansionPage.
//
// Early termination (break) stops the underlying paging — the iterator
// function returns as soon as the caller stops yielding.
func (g c1FileGrantStore) PendingExpansion(ctx context.Context) iter.Seq2[PendingExpansion, error] {
	return func(yield func(PendingExpansion, error) bool) {
		pageToken := ""
		for {
			page, nextPageToken, err := g.PendingExpansionPage(ctx, pageToken)
			if err != nil {
				_ = yield(PendingExpansion{}, err)
				return
			}
			for _, pe := range page {
				if !yield(pe, nil) {
					return
				}
			}
			if nextPageToken == "" {
				return
			}
			pageToken = nextPageToken
		}
	}
}

// ListWithAnnotationsPage implements GrantStore. Thin wrapper over
// listGrantsWithExpansionInternal that reshapes InternalGrantRow into
// GrantAnnotation.
//
// Identity fields on the returned GrantAnnotation are always populated
// from the underlying grant proto, regardless of whether the grant has
// an expansion annotation, so callers don't need to branch on
// Annotation-nil to get identity.
func (g c1FileGrantStore) ListWithAnnotationsPage(ctx context.Context, pageToken string) ([]GrantAnnotation, string, error) {
	resp, err := g.c.listGrantsWithExpansionInternal(ctx, connectorstore.GrantListOptions{
		Mode:      connectorstore.GrantListModePayloadWithExpansion,
		PageToken: pageToken,
	})
	if err != nil {
		return nil, "", err
	}
	out := make([]GrantAnnotation, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		if row == nil {
			continue
		}
		ga := GrantAnnotation{
			Grant:                   row.Grant,
			GrantExternalID:         row.Grant.GetId(),
			TargetEntitlementID:     row.Grant.GetEntitlement().GetId(),
			PrincipalResourceTypeID: row.Grant.GetPrincipal().GetId().GetResourceType(),
			PrincipalResourceID:     row.Grant.GetPrincipal().GetId().GetResource(),
		}
		if row.Expansion != nil {
			ga.Annotation = v2.GrantExpandable_builder{
				EntitlementIds:  row.Expansion.SourceEntitlementIDs,
				Shallow:         row.Expansion.Shallow,
				ResourceTypeIds: row.Expansion.ResourceTypeIDs,
			}.Build()
			ga.NeedsExpansion = row.Expansion.NeedsExpansion
		}
		out = append(out, ga)
	}
	return out, resp.NextPageToken, nil
}

// ListWithAnnotations implements GrantStore. Convenience iterator that
// walks every page via ListWithAnnotationsPage.
func (g c1FileGrantStore) ListWithAnnotations(ctx context.Context) iter.Seq2[GrantAnnotation, error] {
	return func(yield func(GrantAnnotation, error) bool) {
		pageToken := ""
		for {
			page, nextPageToken, err := g.ListWithAnnotationsPage(ctx, pageToken)
			if err != nil {
				_ = yield(GrantAnnotation{}, err)
				return
			}
			for _, ga := range page {
				if !yield(ga, nil) {
					return
				}
			}
			if nextPageToken == "" {
				return
			}
			pageToken = nextPageToken
		}
	}
}

// -----------------------------------------------------------------------------
// SyncMeta
// -----------------------------------------------------------------------------

type c1FileSyncMeta struct{ c *C1File }

// MarkSyncSupportsDiff implements SyncMeta. Thin rename over SetSupportsDiff.
func (s c1FileSyncMeta) MarkSyncSupportsDiff(ctx context.Context, syncID string) error {
	return s.c.SetSupportsDiff(ctx, syncID)
}

// LatestFullSync implements SyncMeta. Returns the most-recent finished
// SyncTypeFull run, or nil if none.
func (s c1FileSyncMeta) LatestFullSync(ctx context.Context) (*SyncRun, error) {
	run, err := s.c.getFinishedSync(ctx, 0, connectorstore.SyncTypeFull)
	if err != nil {
		return nil, err
	}
	return syncRunToExported(run), nil
}

// LatestFinishedSyncOfAnyType implements SyncMeta. Returns the most-recent
// finished sync of any type (including diff types), or nil if none.
func (s c1FileSyncMeta) LatestFinishedSyncOfAnyType(ctx context.Context) (*SyncRun, error) {
	run, err := s.c.getFinishedSync(ctx, 0, connectorstore.SyncTypeAny)
	if err != nil {
		return nil, err
	}
	return syncRunToExported(run), nil
}

// Stats implements SyncMeta. Signature matches *C1File.Stats exactly.
func (s c1FileSyncMeta) Stats(ctx context.Context, syncType connectorstore.SyncType, syncID string) (map[string]int64, error) {
	return s.c.Stats(ctx, syncType, syncID)
}

// syncRunToExported lifts an internal syncRun into the exported SyncRun shape.
// Returns nil if run is nil.
func syncRunToExported(run *syncRun) *SyncRun {
	if run == nil {
		return nil
	}
	return &SyncRun{
		ID:           run.ID,
		StartedAt:    run.StartedAt,
		EndedAt:      run.EndedAt,
		SyncToken:    run.SyncToken,
		Type:         run.Type,
		ParentSyncID: run.ParentSyncID,
		LinkedSyncID: run.LinkedSyncID,
		SupportsDiff: run.SupportsDiff,
	}
}

// -----------------------------------------------------------------------------
// FileOps
// -----------------------------------------------------------------------------

type c1FileFileOps struct{ c *C1File }

// CloneSync implements FileOps. Direct passthrough.
func (f c1FileFileOps) CloneSync(ctx context.Context, outPath string, syncID string) error {
	return f.c.CloneSync(ctx, outPath, syncID)
}

// GenerateSyncDiff implements FileOps. Direct passthrough.
func (f c1FileFileOps) GenerateSyncDiff(ctx context.Context, baseSyncID, appliedSyncID string) (string, error) {
	return f.c.GenerateSyncDiff(ctx, baseSyncID, appliedSyncID)
}
