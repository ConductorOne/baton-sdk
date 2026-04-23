package dotc1z

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/doug-martin/goqu/v9"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// resourceKey uniquely identifies a resource for hydration lookups.
type resourceKey struct {
	ResourceTypeID string
	ResourceID     string
}

// grantJoinKeys is the per-row data needed to hydrate a slim grant —
// i.e. the three grants-table columns that are stripped from the
// serialized data blob when WithV2GrantsWriter is on.
type grantJoinKeys struct {
	EntitlementID           string
	PrincipalResourceTypeID string
	PrincipalResourceID     string
}

// hydrateGrants reconstitutes Grant.Entitlement and/or Grant.Principal
// on any grants that have nil values for those fields (slim-blob writes).
// Grants with both fields already populated are left unchanged.
//
// grantKeys must be index-aligned with grants: each entry holds the
// (entitlement_id, principal_resource_type_id, principal_resource_id)
// columns for the corresponding row. Non-slim entries in grantKeys are
// ignored.
//
// Missing joins are not errors: the grant is returned with the nil field
// intact and a dotc1z.grant_hydrate_miss counter increment (tagged
// "entitlement" or "principal"). Orphan grants are a real present-day
// scenario — pre-slim readers succeeded on them because the fields were
// embedded, so a hard-erroring reader would regress every caller.
func hydrateGrants(
	ctx context.Context,
	c *C1File,
	syncID string,
	grants []*v2.Grant,
	grantKeys []grantJoinKeys,
) error {
	if len(grants) == 0 {
		return nil
	}
	if len(grantKeys) != len(grants) {
		return fmt.Errorf("hydrateGrants: len(grantKeys)=%d does not match len(grants)=%d", len(grantKeys), len(grants))
	}

	needEnt := make(map[string]struct{}, len(grants))
	needRes := make(map[resourceKey]struct{}, len(grants))
	slimIndices := make([]int, 0, len(grants))

	for i, g := range grants {
		entNil := g.GetEntitlement() == nil
		princNil := g.GetPrincipal() == nil
		if !entNil && !princNil {
			continue
		}
		slimIndices = append(slimIndices, i)
		k := grantKeys[i]
		if entNil && k.EntitlementID != "" {
			needEnt[k.EntitlementID] = struct{}{}
		}
		// Both columns must be populated to form a valid resource lookup key.
		// Writer invariants guarantee this (baseGrantRecord sets both from
		// the same ResourceId), so a partial key is an orphan signal.
		if princNil && k.PrincipalResourceID != "" && k.PrincipalResourceTypeID != "" {
			needRes[resourceKey{k.PrincipalResourceTypeID, k.PrincipalResourceID}] = struct{}{}
		}
	}
	if len(slimIndices) == 0 {
		return nil
	}

	entMap, err := batchFetchEntitlements(ctx, c, syncID, needEnt)
	if err != nil {
		return fmt.Errorf("hydrateGrants: batch fetch entitlements: %w", err)
	}
	resMap, err := batchFetchResources(ctx, c, syncID, needRes)
	if err != nil {
		return fmt.Errorf("hydrateGrants: batch fetch resources: %w", err)
	}

	for _, i := range slimIndices {
		g := grants[i]
		k := grantKeys[i]
		if g.GetEntitlement() == nil {
			if ent, ok := entMap[k.EntitlementID]; ok {
				g.SetEntitlement(ent)
			}
		}
		if g.GetPrincipal() == nil {
			if res, ok := resMap[resourceKey{k.PrincipalResourceTypeID, k.PrincipalResourceID}]; ok {
				g.SetPrincipal(res)
			}
		}
		// If either side is still nil after the lookup, count it as a miss.
		if g.GetEntitlement() == nil || g.GetPrincipal() == nil {
			recordHydrateMiss(ctx, g)
		}
	}
	return nil
}

// batchFetchEntitlements returns a map external_id -> Entitlement for
// the given entitlement IDs, scoped by sync_id. Missing IDs are simply
// absent from the map. An empty input returns an empty map.
func batchFetchEntitlements(ctx context.Context, c *C1File, syncID string, ids map[string]struct{}) (map[string]*v2.Entitlement, error) {
	out := make(map[string]*v2.Entitlement, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	args := make([]any, 0, len(ids))
	for id := range ids {
		args = append(args, id)
	}

	q := c.db.From(entitlements.Name()).Prepared(true).
		Select("external_id", "data").
		Where(goqu.C("external_id").In(args...))
	if syncID != "" {
		q = q.Where(goqu.C("sync_id").Eq(syncID))
	}

	query, qargs, err := q.ToSQL()
	if err != nil {
		return nil, err
	}
	rows, err := c.db.QueryContext(ctx, query, qargs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	unmarshal := proto.UnmarshalOptions{Merge: true, DiscardUnknown: true}
	for rows.Next() {
		var (
			externalID string
			data       []byte
		)
		if err := rows.Scan(&externalID, &data); err != nil {
			return nil, err
		}
		ent := &v2.Entitlement{}
		if err := unmarshal.Unmarshal(data, ent); err != nil {
			return nil, fmt.Errorf("batchFetchEntitlements: unmarshal %q: %w", externalID, err)
		}
		out[externalID] = ent
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// batchFetchResources returns a map (resource_type_id, resource_id) ->
// Resource for the given keys, scoped by sync_id.
//
// NOTE: the resources table stores external_id as the composite
// "<resource_type>:<resource_id>" (see putResourcesInternal). We build
// composite keys for the IN clause and recover the raw resource_id from
// the unmarshaled Resource's proto fields — that's the identity the
// caller passed in as resourceKey.ResourceID.
func batchFetchResources(ctx context.Context, c *C1File, syncID string, keys map[resourceKey]struct{}) (map[resourceKey]*v2.Resource, error) {
	out := make(map[resourceKey]*v2.Resource, len(keys))
	if len(keys) == 0 {
		return out, nil
	}

	// Group composite IDs by resource_type_id so we can emit one
	// (resource_type_id=? AND external_id IN (...)) clause per type.
	byType := make(map[string][]any, 4)
	for k := range keys {
		byType[k.ResourceTypeID] = append(byType[k.ResourceTypeID], k.ResourceTypeID+":"+k.ResourceID)
	}

	// Build: (resource_type_id = ? AND external_id IN (...)) OR (...) OR ...
	var ors []goqu.Expression
	for rtID, ids := range byType {
		ors = append(ors,
			goqu.And(
				goqu.C("resource_type_id").Eq(rtID),
				goqu.C("external_id").In(ids...),
			),
		)
	}
	q := c.db.From(resources.Name()).Prepared(true).
		Select("data").
		Where(goqu.Or(ors...))
	if syncID != "" {
		q = q.Where(goqu.C("sync_id").Eq(syncID))
	}

	query, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	unmarshal := proto.UnmarshalOptions{Merge: true, DiscardUnknown: true}
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		res := &v2.Resource{}
		if err := unmarshal.Unmarshal(data, res); err != nil {
			return nil, fmt.Errorf("batchFetchResources: unmarshal: %w", err)
		}
		id := res.GetId()
		out[resourceKey{ResourceTypeID: id.GetResourceType(), ResourceID: id.GetResource()}] = res
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// hydrateGrantsGroupedBySync hydrates slim grants where each row's
// sync_id is independently known (via the main SELECT that produced the
// rows). Grants are grouped by sync_id so hydration joins are always
// scoped to the originating sync — essential when the list query went
// unscoped because the caller's sync_id resolution cascaded to empty.
//
// grants / keys / syncIDs must be index-aligned and the same length.
// Common case is a single sync_id across the page (scoped list query);
// the per-sync loop then runs exactly once.
func hydrateGrantsGroupedBySync(
	ctx context.Context,
	c *C1File,
	grantList []*v2.Grant,
	keys []grantJoinKeys,
	syncIDs []string,
) error {
	if len(grantList) != len(keys) || len(grantList) != len(syncIDs) {
		return fmt.Errorf("hydrateGrantsGroupedBySync: len mismatch grants=%d keys=%d syncIDs=%d", len(grantList), len(keys), len(syncIDs))
	}

	// Fast path: all rows share one sync_id.
	if len(grantList) > 0 {
		first := syncIDs[0]
		sameSync := true
		for i := 1; i < len(syncIDs); i++ {
			if syncIDs[i] != first {
				sameSync = false
				break
			}
		}
		if sameSync {
			return hydrateGrants(ctx, c, first, grantList, keys)
		}
	}

	// Mixed page: group by sync_id and hydrate each group.
	bySync := make(map[string][]int, 2)
	for i, sid := range syncIDs {
		bySync[sid] = append(bySync[sid], i)
	}
	for sid, indices := range bySync {
		subGrants := make([]*v2.Grant, len(indices))
		subKeys := make([]grantJoinKeys, len(indices))
		for j, idx := range indices {
			subGrants[j] = grantList[idx]
			subKeys[j] = keys[idx]
		}
		if err := hydrateGrants(ctx, c, sid, subGrants, subKeys); err != nil {
			return err
		}
	}
	return nil
}

// hydrateSingleGrant hydrates one slim grant by fetching its join keys
// from v1_grants and running batch joins. The sync_id scopes both the
// join-key fetch and the entitlement / resource lookups — callers must
// pass the same sync_id that getConnectorObject used to retrieve the
// grant.
//
// Orphan contract mirrors the batch path (hydrateGrants): when the row
// cannot be resolved (empty sync_id or the grant row vanished between
// getConnectorObject and this call — possible under a concurrent sync
// completion race), the grant is returned with its nil fields intact
// and the grant_hydrate_miss counter is bumped per side. Never errors
// on misses — a hard-erroring reader would regress any caller that
// tolerates orphan grants today.
func hydrateSingleGrant(ctx context.Context, c *C1File, syncID string, g *v2.Grant) error {
	if syncID == "" {
		recordHydrateMiss(ctx, g)
		return nil
	}

	row := c.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT entitlement_id, principal_resource_type_id, principal_resource_id
		 FROM %s WHERE external_id = ? AND sync_id = ?`, grants.Name(),
	), g.GetId(), syncID)
	var k grantJoinKeys
	if err := row.Scan(&k.EntitlementID, &k.PrincipalResourceTypeID, &k.PrincipalResourceID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			recordHydrateMiss(ctx, g)
			return nil
		}
		return fmt.Errorf("hydrateSingleGrant: fetch join keys for %q: %w", g.GetId(), err)
	}
	return hydrateGrants(ctx, c, syncID, []*v2.Grant{g}, []grantJoinKeys{k})
}

// recordHydrateMiss bumps the grant_hydrate_miss counter for each nil
// side on g. Consolidates the empty-syncID / ErrNoRows / batch-miss
// paths that all share the "grant is effectively orphan" contract.
func recordHydrateMiss(ctx context.Context, g *v2.Grant) {
	if g.GetEntitlement() == nil {
		grantHydrateMissCounter.Add(ctx, 1, otelmetric.WithAttributes(attribute.String("missing", "entitlement")))
	}
	if g.GetPrincipal() == nil {
		grantHydrateMissCounter.Add(ctx, 1, otelmetric.WithAttributes(attribute.String("missing", "principal")))
	}
}
