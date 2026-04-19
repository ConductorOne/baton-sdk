package dotc1z

import (
	"context"
	"iter"

	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

// Compile-time check that *C1File satisfies C1ZStore (and the sub-interfaces
// via the sub-store getters below).
var _ C1ZStore = (*C1File)(nil)

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

// StoreExpandedGrants implements GrantStore.
//
// Strips any residual GrantExpandable annotation from the payload, then
// writes grants using the PreserveExpansion mode so existing
// expansion/needs_expansion columns are left untouched.
//
// The strip step exists because the expander's sole caller writes grants
// whose GrantExpandable annotation has already been consumed upstream.
// If a caller accidentally passes a grant with a residual annotation,
// the persisted `data` blob would disagree with the stored `expansion`
// column (the RFC §4.8 invariant). Stripping defensively makes the
// method total.
func (g c1FileGrantStore) StoreExpandedGrants(ctx context.Context, grants ...*v2.Grant) error {
	ctx, span := tracer.Start(ctx, "C1File.StoreExpandedGrants")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if len(grants) == 0 {
		return nil
	}

	stripped := make([]*v2.Grant, len(grants))
	for i, gr := range grants {
		if gr == nil {
			stripped[i] = nil
			continue
		}
		if !hasGrantExpandable(gr) {
			stripped[i] = gr
			continue
		}
		// Only clone when there is something to strip.
		clone := proto.Clone(gr).(*v2.Grant)
		_, _ = extractAndStripExpansion(clone)
		stripped[i] = clone
	}

	err = g.c.UpsertGrants(ctx, connectorstore.GrantUpsertOptions{
		Mode: connectorstore.GrantUpsertModePreserveExpansion,
	}, stripped...)
	return err
}

// PendingExpansion implements GrantStore. Wraps the existing
// listExpandableGrantsInternal page loop in an iter.Seq2, so callers range
// over PendingExpansion values and pagination is hidden.
//
// Early termination (break) stops the underlying paging — the iterator
// function returns as soon as the caller stops yielding.
func (g c1FileGrantStore) PendingExpansion(ctx context.Context) iter.Seq2[PendingExpansion, error] {
	return func(yield func(PendingExpansion, error) bool) {
		pageToken := ""
		for {
			defs, nextPageToken, err := g.c.listExpandableGrantsInternal(ctx, connectorstore.GrantListOptions{
				Mode:      connectorstore.GrantListModeExpansionNeedsOnly,
				PageToken: pageToken,
			})
			if err != nil {
				_ = yield(PendingExpansion{}, err)
				return
			}
			for _, def := range defs {
				if def == nil {
					continue
				}
				pe := PendingExpansion{
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
				}
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

// ListWithAnnotations implements GrantStore. Wraps the existing
// listGrantsWithExpansionInternal page loop in an iter.Seq2.
func (g c1FileGrantStore) ListWithAnnotations(ctx context.Context) iter.Seq2[GrantAnnotation, error] {
	return func(yield func(GrantAnnotation, error) bool) {
		pageToken := ""
		for {
			resp, err := g.c.listGrantsWithExpansionInternal(ctx, connectorstore.GrantListOptions{
				Mode:      connectorstore.GrantListModePayloadWithExpansion,
				PageToken: pageToken,
			})
			if err != nil {
				_ = yield(GrantAnnotation{}, err)
				return
			}
			for _, row := range resp.Rows {
				if row == nil {
					continue
				}
				ga := GrantAnnotation{Grant: row.Grant}
				if row.Expansion != nil {
					ga.Annotation = v2.GrantExpandable_builder{
						EntitlementIds:  row.Expansion.SourceEntitlementIDs,
						Shallow:         row.Expansion.Shallow,
						ResourceTypeIds: row.Expansion.ResourceTypeIDs,
					}.Build()
					ga.GrantExternalID = row.Expansion.GrantExternalID
					ga.TargetEntitlementID = row.Expansion.TargetEntitlementID
					ga.PrincipalResourceTypeID = row.Expansion.PrincipalResourceTypeID
					ga.PrincipalResourceID = row.Expansion.PrincipalResourceID
					ga.NeedsExpansion = row.Expansion.NeedsExpansion
				}
				if !yield(ga, nil) {
					return
				}
			}
			if resp.NextPageToken == "" {
				return
			}
			pageToken = resp.NextPageToken
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
