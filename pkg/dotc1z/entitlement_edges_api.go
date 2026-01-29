package dotc1z

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/doug-martin/goqu/v9"
)

// UpsertEntitlementEdgesForGrant replaces all edges contributed by a given source grant (for the given sync)
// and then inserts the provided edges.
func (c *C1File) UpsertEntitlementEdgesForGrant(ctx context.Context, syncID string, sourceGrantExternalID string, edges []EntitlementEdgeInput) error {
	if c.readOnly {
		return ErrReadOnly
	}
	if err := c.validateDb(ctx); err != nil {
		return err
	}
	if syncID == "" {
		return fmt.Errorf("syncID is required")
	}
	if sourceGrantExternalID == "" {
		return fmt.Errorf("sourceGrantExternalID is required")
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Delete all edges for this grant/sync first (handles shrink and simplifies updates).
	_, err = tx.ExecContext(
		ctx,
		//nolint:gosec // table name is constant from descriptor
		fmt.Sprintf("delete from %s where sync_id = ? and source_grant_external_id = ?", entitlementEdges.Name()),
		syncID,
		sourceGrantExternalID,
	)
	if err != nil {
		return err
	}

	if len(edges) == 0 {
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		c.dbUpdated = true
		return nil
	}

	discoveredAt := time.Now().Format("2006-01-02 15:04:05.999999999")
	rows := make([]goqu.Record, 0, len(edges))
	for _, e := range edges {
		key, rtJSON, err := makeEdgeKey(e.SrcEntitlementID, e.DstEntitlementID, e.Shallow, e.ResourceTypeIDs)
		if err != nil {
			return err
		}

		shallowInt := 0
		if e.Shallow {
			shallowInt = 1
		}

		rows = append(rows, goqu.Record{
			"src_entitlement_id":       e.SrcEntitlementID,
			"dst_entitlement_id":       e.DstEntitlementID,
			"shallow":                  shallowInt,
			"resource_type_ids_json":   rtJSON,
			"edge_key":                 key,
			"source_grant_external_id": sourceGrantExternalID,
			"sync_id":                  syncID,
			"discovered_at":            discoveredAt,
		})
	}

	// Insert (or replace) edges for this sync.
	q := c.db.Insert(entitlementEdges.Name()).
		Prepared(true).
		Rows(rows).
		OnConflict(goqu.DoUpdate("sync_id, edge_key", goqu.Record{
			"src_entitlement_id":       goqu.I("EXCLUDED.src_entitlement_id"),
			"dst_entitlement_id":       goqu.I("EXCLUDED.dst_entitlement_id"),
			"shallow":                  goqu.I("EXCLUDED.shallow"),
			"resource_type_ids_json":   goqu.I("EXCLUDED.resource_type_ids_json"),
			"source_grant_external_id": goqu.I("EXCLUDED.source_grant_external_id"),
			"discovered_at":            goqu.I("EXCLUDED.discovered_at"),
		}))

	query, args, err := q.ToSQL()
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	c.dbUpdated = true
	return nil
}

// DeleteEntitlementEdgesForGrant removes all edges contributed by a source grant in a sync.
func (c *C1File) DeleteEntitlementEdgesForGrant(ctx context.Context, syncID string, sourceGrantExternalID string) error {
	if c.readOnly {
		return ErrReadOnly
	}
	if err := c.validateDb(ctx); err != nil {
		return err
	}
	if syncID == "" {
		return fmt.Errorf("syncID is required")
	}
	if sourceGrantExternalID == "" {
		return fmt.Errorf("sourceGrantExternalID is required")
	}

	//nolint:gosec // table name is constant from descriptor
	_, err := c.db.ExecContext(ctx, fmt.Sprintf("delete from %s where sync_id = ? and source_grant_external_id = ?", entitlementEdges.Name()), syncID, sourceGrantExternalID)
	if err != nil {
		return err
	}
	c.dbUpdated = true
	return nil
}

// DeleteEntitlementEdgesForEntitlement removes all edges incident to the given entitlement in a sync.
func (c *C1File) DeleteEntitlementEdgesForEntitlement(ctx context.Context, syncID string, entitlementID string) error {
	if c.readOnly {
		return ErrReadOnly
	}
	if err := c.validateDb(ctx); err != nil {
		return err
	}
	if syncID == "" {
		return fmt.Errorf("syncID is required")
	}
	if entitlementID == "" {
		return fmt.Errorf("entitlementID is required")
	}

	//nolint:gosec // table name is constant from descriptor
	_, err := c.db.ExecContext(ctx, fmt.Sprintf("delete from %s where sync_id = ? and (src_entitlement_id = ? or dst_entitlement_id = ?)", entitlementEdges.Name()), syncID, entitlementID, entitlementID)
	if err != nil {
		return err
	}
	c.dbUpdated = true
	return nil
}

// EntitlementEdgeInput is the in-memory representation of a single edge to persist.
type EntitlementEdgeInput struct {
	SrcEntitlementID string
	DstEntitlementID string
	Shallow          bool
	ResourceTypeIDs  []string
}

func edgesFromGrantExpansionAnnotation(grant *v2.Grant, anno *v2.GrantExpansionEdges) []EntitlementEdgeInput {
	if grant == nil || anno == nil {
		return nil
	}
	dst := grant.GetEntitlement().GetId()
	if dst == "" {
		return nil
	}
	rtids := slices.Clone(anno.GetResourceTypeIds())
	slices.Sort(rtids)
	rtids = slices.Compact(rtids)

	ret := make([]EntitlementEdgeInput, 0, len(anno.GetEntitlementIds()))
	for _, src := range anno.GetEntitlementIds() {
		if src == "" {
			continue
		}
		ret = append(ret, EntitlementEdgeInput{
			SrcEntitlementID: src,
			DstEntitlementID: dst,
			Shallow:          anno.GetShallow(),
			ResourceTypeIDs:  rtids,
		})
	}
	return ret
}

// EntitlementEdgeRow is a decoded edge row from v1_entitlement_edges.
type EntitlementEdgeRow struct {
	SrcEntitlementID string
	DstEntitlementID string
	Shallow          bool
	ResourceTypeIDs  []string
	SourceGrantID    string
}

// ListEntitlementEdgesForSources returns all edges for the given src entitlement IDs within a sync.
func (c *C1File) ListEntitlementEdgesForSources(ctx context.Context, syncID string, srcEntitlementIDs []string) ([]EntitlementEdgeRow, error) {
	if err := c.validateDb(ctx); err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, fmt.Errorf("syncID is required")
	}
	if len(srcEntitlementIDs) == 0 {
		return nil, nil
	}

	q := c.db.From(entitlementEdges.Name()).Prepared(true)
	q = q.Select("src_entitlement_id", "dst_entitlement_id", "shallow", "resource_type_ids_json", "source_grant_external_id")
	q = q.Where(goqu.C("sync_id").Eq(syncID))
	q = q.Where(goqu.C("src_entitlement_id").In(srcEntitlementIDs))

	sqlQ, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}
	rows, err := c.db.QueryContext(ctx, sqlQ, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ret := make([]EntitlementEdgeRow, 0)
	for rows.Next() {
		var src, dst, rtJSON, sourceGrant string
		var shallowInt int
		if err := rows.Scan(&src, &dst, &shallowInt, &rtJSON, &sourceGrant); err != nil {
			return nil, err
		}
		var rtids []string
		if rtJSON != "" {
			_ = json.Unmarshal([]byte(rtJSON), &rtids)
		}
		ret = append(ret, EntitlementEdgeRow{
			SrcEntitlementID: src,
			DstEntitlementID: dst,
			Shallow:          shallowInt != 0,
			ResourceTypeIDs:  rtids,
			SourceGrantID:    sourceGrant,
		})
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return ret, nil
}

// EntitlementDescendantsClosure returns the set of entitlements reachable from the given seeds
// following expansion edges (src -> dst) within a sync.
func (c *C1File) EntitlementDescendantsClosure(ctx context.Context, syncID string, seedEntitlementIDs []string) ([]string, error) {
	if err := c.validateDb(ctx); err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, fmt.Errorf("syncID is required")
	}
	if len(seedEntitlementIDs) == 0 {
		return nil, nil
	}

	seen := make(map[string]struct{}, len(seedEntitlementIDs))
	frontier := make([]string, 0, len(seedEntitlementIDs))
	for _, s := range seedEntitlementIDs {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		frontier = append(frontier, s)
	}

	for len(frontier) > 0 {
		q := c.db.From(entitlementEdges.Name()).Prepared(true)
		q = q.Select("dst_entitlement_id")
		q = q.Where(goqu.C("sync_id").Eq(syncID))
		q = q.Where(goqu.C("src_entitlement_id").In(frontier))

		sqlQ, args, err := q.ToSQL()
		if err != nil {
			return nil, err
		}
		rows, err := c.db.QueryContext(ctx, sqlQ, args...)
		if err != nil {
			return nil, err
		}

		next := make([]string, 0)
		for rows.Next() {
			var dst string
			if err := rows.Scan(&dst); err != nil {
				_ = rows.Close()
				return nil, err
			}
			if dst == "" {
				continue
			}
			if _, ok := seen[dst]; ok {
				continue
			}
			seen[dst] = struct{}{}
			next = append(next, dst)
		}
		_ = rows.Close()
		if rows.Err() != nil {
			return nil, rows.Err()
		}
		frontier = next
	}

	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	return out, nil
}
