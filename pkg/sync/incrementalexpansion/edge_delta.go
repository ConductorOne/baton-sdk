package incrementalexpansion

import (
	"context"
	"fmt"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// Edge represents an expansion edge from a source entitlement to a descendant entitlement,
// with expansion constraints.
type Edge struct {
	SrcEntitlementID string
	DstEntitlementID string
	Shallow          bool
	// PrincipalResourceTypeIDs is the filter applied when listing source grants to propagate.
	PrincipalResourceTypeIDs []string
}

func (e Edge) Key() string {
	// Use an unlikely separator to avoid accidental collisions.
	sep := "\x1f"
	shallow := "0"
	if e.Shallow {
		shallow = "1"
	}
	return strings.Join([]string{
		e.SrcEntitlementID,
		e.DstEntitlementID,
		shallow,
		strings.Join(e.PrincipalResourceTypeIDs, sep),
	}, sep)
}

type EdgeDelta struct {
	Added   map[string]Edge
	Removed map[string]Edge
}

// EdgeDeltaFromDiffSyncs computes edge additions/removals from a paired diff sync:
// - upsertsSyncID contains NEW versions (adds + modifications)
// - deletionsSyncID contains OLD versions (deletes + OLD side of modifications)
//
// This function assumes diff generation inserts OLD versions of modified grants into the deletions sync.
func EdgeDeltaFromDiffSyncs(ctx context.Context, store connectorstore.InternalWriter, upsertsSyncID, deletionsSyncID string) (*EdgeDelta, error) {
	added, err := edgeSetFromSync(ctx, store, upsertsSyncID)
	if err != nil {
		return nil, fmt.Errorf("edge delta: failed reading upserts sync %s: %w", upsertsSyncID, err)
	}
	removed, err := edgeSetFromSync(ctx, store, deletionsSyncID)
	if err != nil {
		return nil, fmt.Errorf("edge delta: failed reading deletions sync %s: %w", deletionsSyncID, err)
	}
	return &EdgeDelta{Added: added, Removed: removed}, nil
}

func edgeSetFromSync(ctx context.Context, store connectorstore.InternalWriter, syncID string) (map[string]Edge, error) {
	out := make(map[string]Edge)
	pageToken := ""
	for {
		resp, err := store.ListGrantsInternal(ctx, connectorstore.GrantListOptions{
			Mode:           connectorstore.GrantListModeExpansion,
			SyncID:         syncID,
			PageToken:      pageToken,
			ExpandableOnly: true,
		})

		if err != nil {
			return nil, err
		}
		for _, row := range resp.Rows {
			def := row.Expansion
			if def == nil {
				continue
			}
			for _, src := range def.SourceEntitlementIDs {
				e := Edge{
					SrcEntitlementID:         src,
					DstEntitlementID:         def.TargetEntitlementID,
					Shallow:                  def.Shallow,
					PrincipalResourceTypeIDs: def.ResourceTypeIDs,
				}
				out[e.Key()] = e
			}
		}
		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return out, nil
}
