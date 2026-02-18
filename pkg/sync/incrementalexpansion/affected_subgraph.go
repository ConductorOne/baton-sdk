package incrementalexpansion

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// AffectedEntitlements computes the forward-closure of entitlements potentially impacted by an edge delta.
// Seeds include all src/dst entitlements that appear in Added/Removed edge sets.
//
// The closure is computed over the current edge set for targetSyncID (read from expandable-grant columns),
// following src -> dst direction.
func AffectedEntitlements(ctx context.Context, store connectorstore.InternalWriter, targetSyncID string, delta *EdgeDelta) (map[string]struct{}, error) {
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

	for _, e := range delta.Added {
		seed(e.SrcEntitlementID)
		seed(e.DstEntitlementID)
	}
	for _, e := range delta.Removed {
		seed(e.SrcEntitlementID)
		seed(e.DstEntitlementID)
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
