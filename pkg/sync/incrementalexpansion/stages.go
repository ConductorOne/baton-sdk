package incrementalexpansion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	c1zpb "github.com/conductorone/baton-sdk/pb/c1/c1z/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

// affectedEntitlements computes the forward-closure of entitlements potentially impacted by an edge delta.
// Seeds include all src/dst entitlements that appear in added/removed edge sets.
//
// The closure is computed over the current edge set for targetSyncID (read from expandable-grant columns),
// following src -> dst direction.
func affectedEntitlements(ctx context.Context, store connectorstore.InternalWriter, targetSyncID string, delta *edgeDelta) (map[string]struct{}, error) {
	if delta == nil {
		return map[string]struct{}{}, nil
	}

	adj, err := buildAdjacency(ctx, store, targetSyncID)
	if err != nil {
		return nil, err
	}

	affected := make(map[string]struct{}, 256)
	queue := make([]string, 0, 256)

	seed := func(entID string) {
		if entID == "" {
			return
		}
		if _, ok := affected[entID]; ok {
			return
		}
		affected[entID] = struct{}{}
		queue = append(queue, entID)
	}

	for _, e := range delta.added {
		seed(e.srcEntitlementID)
		seed(e.dstEntitlementID)
	}
	for _, e := range delta.removed {
		seed(e.srcEntitlementID)
		seed(e.dstEntitlementID)
	}

	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		for _, v := range adj[u] {
			if _, ok := affected[v]; ok {
				continue
			}
			affected[v] = struct{}{}
			queue = append(queue, v)
		}
	}

	return affected, nil
}

func buildAdjacency(ctx context.Context, store connectorstore.InternalWriter, syncID string) (map[string][]string, error) {
	adj := make(map[string][]string, 1024)
	pageToken := ""
	for {
		resp, err := store.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
			Mode:           connectorstore.GrantListModeExpansion,
			SyncID:         syncID,
			PageToken:      pageToken,
			ExpandableOnly: true,
		})

		if err != nil {
			return nil, fmt.Errorf("build adjacency: %w", err)
		}

		for _, row := range resp.Rows {
			def := row.Expansion
			if def == nil {
				continue
			}
			dst := def.TargetEntitlementID
			for _, src := range def.SourceEntitlementIDs {
				if src == "" || dst == "" {
					continue
				}
				adj[src] = append(adj[src], dst)
			}
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return adj, nil
}

// invalidateChangedSourceEntitlements invalidates propagated sources for any entitlement whose grant-set changed.
// It removes only the specific source key (the entitlement ID) from downstream grants along outgoing edges.
func invalidateChangedSourceEntitlements(ctx context.Context, store connectorstore.InternalWriter, targetSyncID string, changedSources map[string]struct{}) error {
	if len(changedSources) == 0 {
		return nil
	}

	outgoing := make(map[string]edge)
	pageToken := ""
	for {
		resp, err := store.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
			Mode:           connectorstore.GrantListModeExpansion,
			SyncID:         targetSyncID,
			PageToken:      pageToken,
			ExpandableOnly: true,
		})
		if err != nil {
			return err
		}
		for _, row := range resp.Rows {
			def := row.Expansion
			if def == nil {
				continue
			}
			for _, src := range def.SourceEntitlementIDs {
				if _, ok := changedSources[src]; !ok {
					continue
				}
				e := edge{
					srcEntitlementID:         src,
					dstEntitlementID:         def.TargetEntitlementID,
					shallow:                  def.Shallow,
					principalResourceTypeIDs: def.ResourceTypeIDs,
				}
				outgoing[e.key()] = e
			}
		}
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	if len(outgoing) == 0 {
		return nil
	}

	return invalidateRemovedEdges(ctx, store, targetSyncID, &edgeDelta{removed: outgoing})
}

// invalidateRemovedEdges removes only the specific source keys implied by removed edges.
//
// For each removed edge srcE->dstE (source entitlement to destination entitlement):
// - list grants G for entitlement dstE (filtered by edge principal resource types)
// - remove sources[srcE] from those grants
// - if a grant G becomes sourceless and is GrantImmutable, delete it
// - otherwise, persist the updated sources map.
func invalidateRemovedEdges(ctx context.Context, store connectorstore.InternalWriter, targetSyncID string, delta *edgeDelta) error {
	if delta == nil || len(delta.removed) == 0 {
		return nil
	}

	type groupKey struct {
		dstEntitlementID string
		principalTypes   string
	}

	removedByGroup := make(map[groupKey]map[string]struct{}, len(delta.removed))
	for _, e := range delta.removed {
		k := groupKey{
			dstEntitlementID: e.dstEntitlementID,
			principalTypes:   strings.Join(e.principalResourceTypeIDs, "\x1f"),
		}
		m := removedByGroup[k]
		if m == nil {
			m = make(map[string]struct{}, 4)
			removedByGroup[k] = m
		}
		m[e.srcEntitlementID] = struct{}{}
	}

	const chunkSize = 10000
	updates := make([]*v2.Grant, 0, chunkSize)

	flush := func() error {
		if len(updates) == 0 {
			return nil
		}
		if err := store.UpsertGrants(ctx, connectorstore.GrantUpsertOptions{
			Mode:   connectorstore.GrantUpsertModePreserveExpansion,
			SyncID: targetSyncID,
		}, updates...); err != nil {
			return err
		}
		updates = updates[:0]
		return nil
	}

	for k, srcIDs := range removedByGroup {
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

// markNeedsExpansionForAffectedEdges sets needs_expansion=1 for expandable grants whose edges are
// in/leading into the affected subgraph.
//
// With grant-column storage, we conservatively mark an expandable grant dirty if:
// - its destination entitlement is affected, OR
// - any of its source entitlement IDs is affected.
func markNeedsExpansionForAffectedEdges(ctx context.Context, store connectorstore.InternalWriter, targetSyncID string, affected map[string]struct{}) error {
	if len(affected) == 0 {
		return nil
	}

	pageToken := ""
	toMark := make([]string, 0, 1024)

	for {
		resp, err := store.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
			Mode:           connectorstore.GrantListModeExpansion,
			SyncID:         targetSyncID,
			PageToken:      pageToken,
			ExpandableOnly: true,
		})
		if err != nil {
			return err
		}

		for _, def := range resp.Rows {
			_, dstAffected := affected[def.Expansion.TargetEntitlementID]
			srcAffected := false
			if !dstAffected {
				for _, src := range def.Expansion.SourceEntitlementIDs {
					if _, ok := affected[src]; ok {
						srcAffected = true
						break
					}
				}
			}
			if dstAffected || srcAffected {
				toMark = append(toMark, def.Expansion.GrantExternalID)
			}
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}

	const chunk = 5000
	for i := 0; i < len(toMark); i += chunk {
		j := i + chunk
		if j > len(toMark) {
			j = len(toMark)
		}
		if err := store.SetNeedsExpansionForGrants(ctx, targetSyncID, toMark[i:j], true); err != nil {
			return fmt.Errorf("mark needs_expansion: %w", err)
		}
	}
	return nil
}

// expandDirtySubgraph loads only expandable edges marked needs_expansion=1 for syncID,
// runs the standard expander, then clears needs_expansion.
//
// NOTE: This expands only edges present in the loaded subgraph. The caller is responsible for
// marking the correct edge-defining grants as needs_expansion based on the affected subgraph.
func expandDirtySubgraph(ctx context.Context, c1f *dotc1z.C1File, syncID string) error {
	if err := c1f.SetSupportsDiff(ctx, syncID); err != nil {
		return err
	}

	graph := expand.NewEntitlementGraph(ctx)
	reqAnnos := annotations.New(c1zpb.SyncDetails_builder{Id: syncID}.Build())

	pageToken := ""
	for {
		resp, err := c1f.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
			Mode:               connectorstore.GrantListModeExpansionNeedsOnly,
			SyncID:             syncID,
			PageToken:          pageToken,
			NeedsExpansionOnly: true,
		})
		if err != nil {
			return err
		}

		for _, row := range resp.Rows {
			def := row.Expansion
			if def == nil {
				continue
			}
			principalID := v2.ResourceId_builder{
				ResourceType: def.PrincipalResourceTypeID,
				Resource:     def.PrincipalResourceID,
			}.Build()

			for _, srcEntitlementID := range def.SourceEntitlementIDs {
				srcEntitlement, err := c1f.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
					EntitlementId: srcEntitlementID,
					Annotations:   reqAnnos,
				}.Build())
				if err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						continue
					}
					return fmt.Errorf("error fetching source entitlement %q: %w", srcEntitlementID, err)
				}

				sourceEntitlementResourceID := srcEntitlement.GetEntitlement().GetResource().GetId()
				if sourceEntitlementResourceID == nil {
					return fmt.Errorf("source entitlement resource id was nil")
				}
				if principalID.GetResourceType() != sourceEntitlementResourceID.GetResourceType() ||
					principalID.GetResource() != sourceEntitlementResourceID.GetResource() {
					return fmt.Errorf("source entitlement resource id did not match grant principal id")
				}

				graph.AddEntitlementID(def.TargetEntitlementID)
				graph.AddEntitlementID(srcEntitlementID)
				if err := graph.AddEdge(ctx, srcEntitlementID, def.TargetEntitlementID, def.Shallow, def.ResourceTypeIDs); err != nil {
					return fmt.Errorf("error adding edge to graph: %w", err)
				}
			}
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	graph.Loaded = true

	if err := graph.FixCycles(ctx); err != nil {
		return err
	}

	expander := expand.NewExpander(c1f, graph).WithSyncID(syncID)
	if err := expander.Run(ctx); err != nil {
		return err
	}

	return c1f.ClearNeedsExpansionForSync(ctx, syncID)
}
