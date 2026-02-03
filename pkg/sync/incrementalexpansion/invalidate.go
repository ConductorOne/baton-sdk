package incrementalexpansion

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

type InvalidationStore interface {
	// Optional, but needed for dotc1z.C1File.DeleteGrant scoping.
	SetSyncID(ctx context.Context, syncID string) error

	ListGrantsForEntitlement(ctx context.Context, req *reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementResponse, error)
	PutGrants(ctx context.Context, grants ...*v2.Grant) error
	DeleteGrant(ctx context.Context, grantId string) error
}

// InvalidateRemovedEdges removes only the specific source keys implied by removed edges.
//
// For each removed edge srcE->dstE (source entitlement to destination entitlement):
// - list grants G for entitlement dstE (filtered by edge principal resource types)
// - remove sources[srcE] from those grants
// - if a grant G becomes sourceless and is GrantImmutable, delete it
// - otherwise, persist the updated sources map.
func InvalidateRemovedEdges(ctx context.Context, store InvalidationStore, targetSyncID string, delta *EdgeDelta) error {
	if delta == nil || len(delta.Removed) == 0 {
		return nil
	}
	if err := store.SetSyncID(ctx, targetSyncID); err != nil {
		return err
	}

	// Batch updates to reduce write overhead.
	const chunkSize = 10000
	updates := make([]*v2.Grant, 0, chunkSize)

	flush := func() error {
		if len(updates) == 0 {
			return nil
		}
		if err := store.PutGrants(ctx, updates...); err != nil {
			return err
		}
		updates = updates[:0]
		return nil
	}

	for _, edge := range delta.Removed {
		// Entitlement is only used for filtering by entitlement_id; ID is sufficient.
		ent := v2.Entitlement_builder{Id: edge.DstEntitlementID}.Build()
		pageToken := ""
		for {
			resp, err := store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
				Entitlement:              ent,
				PageToken:                pageToken,
				PrincipalResourceTypeIds: edge.PrincipalResourceTypeIDs,
			}.Build())
			if err != nil {
				return err
			}

			for _, g := range resp.GetList() {
				srcs := g.GetSources().GetSources()
				if len(srcs) == 0 {
					continue
				}
				if _, ok := srcs[edge.SrcEntitlementID]; !ok {
					continue
				}

				delete(srcs, edge.SrcEntitlementID)

				// The expander adds a "self-source" (destination entitlement ID) to mark
				// that a grant was originally direct. When all expansion sources are removed,
				// we should also remove the self-source so the grant matches a fresh full
				// expansion (which would have no sources for a direct grant).
				selfSourceID := g.GetEntitlement().GetId()
				if len(srcs) == 1 {
					delete(srcs, selfSourceID)
				}

				if len(srcs) == 0 {
					annos := annotations.Annotations(g.GetAnnotations())
					if annos.Contains(&v2.GrantImmutable{}) {
						if err := store.DeleteGrant(ctx, g.GetId()); err != nil {
							return err
						}
						continue
					}
					// Direct grant: clear sources entirely.
					g.SetSources(nil)
				} else {
					g.SetSources(v2.GrantSources_builder{Sources: srcs}.Build())
				}

				updates = append(updates, g)
				if len(updates) >= chunkSize {
					if err := flush(); err != nil {
						return err
					}
				}
			}

			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
	}

	if err := flush(); err != nil {
		return fmt.Errorf("invalidate removed edges: %w", err)
	}
	return nil
}
