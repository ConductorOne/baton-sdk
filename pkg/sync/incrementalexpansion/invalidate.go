package incrementalexpansion

import (
	"context"
	"fmt"
	"strings"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// InvalidateRemovedEdges removes only the specific source keys implied by removed edges.
//
// For each removed edge srcE->dstE (source entitlement to destination entitlement):
// - list grants G for entitlement dstE (filtered by edge principal resource types)
// - remove sources[srcE] from those grants
// - if a grant G becomes sourceless and is GrantImmutable, delete it
// - otherwise, persist the updated sources map.
func InvalidateRemovedEdges(ctx context.Context, store connectorstore.InternalWriter, targetSyncID string, delta *EdgeDelta) error {
	if delta == nil || len(delta.Removed) == 0 {
		return nil
	}

	type groupKey struct {
		dstEntitlementID string
		// Use a stable join so we can group by principal type filters.
		principalTypes string
	}

	// Group removed edges so we only scan/write each destination entitlement once per filter.
	removedByGroup := make(map[groupKey]map[string]struct{}, len(delta.Removed))
	for _, edge := range delta.Removed {
		k := groupKey{
			dstEntitlementID: edge.DstEntitlementID,
			principalTypes:   strings.Join(edge.PrincipalResourceTypeIDs, "\x1f"),
		}
		m := removedByGroup[k]
		if m == nil {
			m = make(map[string]struct{}, 4)
			removedByGroup[k] = m
		}
		m[edge.SrcEntitlementID] = struct{}{}
	}

	// Batch updates to reduce write overhead.
	const chunkSize = 10000
	updates := make([]*v2.Grant, 0, chunkSize)

	flush := func() error {
		if len(updates) == 0 {
			return nil
		}
		if err := store.UpsertGrants(ctx, connectorstore.GrantUpsertOptions{
			Mode:   connectorstore.GrantUpsertModeReplace,
			SyncID: targetSyncID,
		}, updates...); err != nil {
			return err
		}
		updates = updates[:0]
		return nil
	}

	for k, srcIDs := range removedByGroup {
		// Entitlement is only used for filtering by entitlement_id; ID is sufficient.
		ent := v2.Entitlement_builder{Id: k.dstEntitlementID}.Build()
		reqAnnos := annotations.New(c1zpb.SyncDetails_builder{Id: targetSyncID}.Build())
		var principalTypes []string
		if k.principalTypes != "" {
			principalTypes = strings.Split(k.principalTypes, "\x1f")
		}
		pageToken := ""
		for {
			resp, err := store.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
				Entitlement:              ent,
				PageToken:                pageToken,
				PrincipalResourceTypeIds: principalTypes,
				Annotations:              reqAnnos,
			}.Build())
			if err != nil {
				return err
			}

			for _, g := range resp.GetList() {
				srcs := g.GetSources().GetSources()
				if len(srcs) == 0 {
					continue
				}
				removedAny := false
				for srcID := range srcIDs {
					if _, ok := srcs[srcID]; !ok {
						continue
					}
					delete(srcs, srcID)
					removedAny = true
				}
				if !removedAny {
					continue
				}

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
						if err := store.DeleteGrantInternal(ctx, connectorstore.GrantDeleteOptions{
							SyncID: targetSyncID,
						}, g.GetId()); err != nil {
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
