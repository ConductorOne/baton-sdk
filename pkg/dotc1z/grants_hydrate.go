package dotc1z

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// grantJoinKeys is the per-row identity data needed to construct stub
// Entitlement / Principal protos for a slim grant. Every field maps
// directly to a column on the grants row — no joins required.
//
// Stubs carry only identity (Id + nested Resource.Id). DisplayName,
// Annotations, Purpose, Slug, traits, and any other non-identity field
// on the original Entitlement / Resource are zero-value. This is the
// documented contract of the slim writer (see WithC1FV2GrantsWriter).
type grantJoinKeys struct {
	EntitlementID             string
	EntitlementResourceTypeID string
	EntitlementResourceID     string
	PrincipalResourceTypeID   string
	PrincipalResourceID       string
}

// hydrateGrants fills in Grant.Entitlement and Grant.Principal on any
// slim row (one with a nil Entitlement or Principal after unmarshal)
// by constructing minimal stubs from the row's own column data. Grants
// that already have both fields populated are left unchanged.
//
// Pure function. No DB access, no network, no errors. grantKeys must
// be index-aligned with grants.
func hydrateGrants(grants []*v2.Grant, grantKeys []grantJoinKeys) {
	for i, g := range grants {
		entNil := g.GetEntitlement() == nil
		princNil := g.GetPrincipal() == nil
		if !entNil && !princNil {
			continue
		}
		k := grantKeys[i]
		if entNil {
			g.SetEntitlement(stubEntitlement(k))
		}
		if princNil {
			g.SetPrincipal(stubPrincipal(k))
		}
	}
}

func stubEntitlement(k grantJoinKeys) *v2.Entitlement {
	return v2.Entitlement_builder{
		Id: k.EntitlementID,
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: k.EntitlementResourceTypeID,
				Resource:     k.EntitlementResourceID,
			}.Build(),
		}.Build(),
	}.Build()
}

func stubPrincipal(k grantJoinKeys) *v2.Resource {
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: k.PrincipalResourceTypeID,
			Resource:     k.PrincipalResourceID,
		}.Build(),
	}.Build()
}

// hydrateSingleGrant fills in Entitlement / Principal on a single slim
// grant returned by GetGrant. Unlike the list paths, GetGrant's caller
// (getConnectorObject) only retrieves the data blob, so we still need
// a small SQL round-trip to read the row's identity columns. Once we
// have them, stub construction is pure.
//
// Concurrent-write race contract: if the row vanished between
// getConnectorObject and this call (sql.ErrNoRows), or if syncID is
// empty (no resolvable sync context), we leave nil fields intact and
// return nil — same as a row whose columns are all empty strings.
// Callers downstream tolerate empty identity per existing behavior.
func hydrateSingleGrant(ctx context.Context, c *C1File, syncID string, g *v2.Grant) error {
	if syncID == "" {
		return nil
	}

	row := c.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT entitlement_id, resource_type_id, resource_id, principal_resource_type_id, principal_resource_id
		 FROM %s WHERE external_id = ? AND sync_id = ?`, grants.Name(),
	), g.GetId(), syncID)
	var k grantJoinKeys
	if err := row.Scan(
		&k.EntitlementID,
		&k.EntitlementResourceTypeID,
		&k.EntitlementResourceID,
		&k.PrincipalResourceTypeID,
		&k.PrincipalResourceID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("hydrateSingleGrant: fetch identity columns for %q: %w", g.GetId(), err)
	}
	hydrateGrants([]*v2.Grant{g}, []grantJoinKeys{k})
	return nil
}
