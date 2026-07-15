package pebble

// Resumed-replay regression tests: a crashed sync attempt can commit a
// replay page's OWN overlay rows (net-new identities stamped with the
// scope) after the replay copy lands and before the action finishes. The
// resumed action re-runs the same page, and the re-run's replay must
// converge — clear the earlier attempt's rows, re-copy the base, and pass
// its post-copy recount — instead of false-failing verifyReplayedScopeCount
// with ErrReplayIntegrity (which would burn the whole warm sync on a cold
// retry).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

// TestSourceCacheReplayResumeAfterOverlayCommit pins the grants shape:
// replay copy + committed overlay row with a net-new identity, then a
// re-run of the same replay.
func TestSourceCacheReplayResumeAfterOverlayCommit(t *testing.T) {
	ctx := context.Background()

	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	base1 := scGrant("member", "alice", false)
	base2 := scGrant("member", "bob", false)
	require.NoError(t, prev.PutGrants(sourcecache.WithScope(ctx, scopeA), base1, base2))

	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// First attempt: the replay copies the base...
	res1, err := cur.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Equal(t, int64(2), res1.Rows)
	require.Zero(t, res1.ResumedRowsCleared)

	// ...then the page's own overlay row commits under the same scope —
	// a NET-NEW identity the previous file does not contain...
	overlay := scGrant("member", "carol", false)
	require.NoError(t, cur.PutGrants(sourcecache.WithScope(ctx, scopeA), overlay))

	// ...plus a row another scope legitimately owns, and a stale scopeA
	// index entry pointing at it: the re-run's pre-clear must keep the
	// row and drop only the orphaned entry.
	otherScopeRow := scGrant("owner", "dave", false)
	require.NoError(t, cur.PutGrants(sourcecache.WithScope(ctx, scopeB), otherScopeRow))
	otherRec, err := cur.PebbleEngine().GetGrantRecord(ctx, otherScopeRow.GetId())
	require.NoError(t, err)
	staleIdx := replayTestGrantScopeIndexKey(t, scopeA, otherRec)
	require.NoError(t, cur.PebbleEngine().DB().Set(staleIdx, nil, nil))

	// The sync crashes before the action finishes; the resumed action
	// re-runs the same page. The replay must converge, not false-fail
	// its post-copy recount against the overlay row.
	res2, err := cur.PebbleEngine().ReplaySourceCacheGrants(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err, "resumed replay over committed overlay rows must not false-fail")
	require.Equal(t, int64(2), res2.Rows)
	require.Equal(t, int64(3), res2.ResumedRowsCleared,
		"the crashed attempt's two base copies plus its overlay row")

	// The other scope's row survived the pre-clear.
	_, err = cur.PebbleEngine().GetGrantRecord(ctx, otherScopeRow.GetId())
	require.NoError(t, err, "a row stamped with another scope must survive the pre-clear")

	// The resumed page re-delivers its rows after the replay.
	require.NoError(t, cur.PutGrants(sourcecache.WithScope(ctx, scopeA), overlay))

	// Converged state: base + overlay + the other scope's row, once each.
	for _, g := range []*v2.Grant{base1, base2, overlay, otherScopeRow} {
		_, err := cur.PebbleEngine().GetGrantRecord(ctx, g.GetId())
		require.NoError(t, err)
	}
	var seen int
	require.NoError(t, cur.PebbleEngine().IterateGrants(ctx, func(*v3.GrantRecord) bool {
		seen++
		return true
	}))
	require.Equal(t, 4, seen)

	// The scope index is exact again: a replay FROM this store (as the
	// next sync's previous) finds exactly the scope's three rows.
	next := newAdapter(t)
	_, err = next.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	res3, err := next.PebbleEngine().ReplaySourceCacheGrants(ctx, cur.PebbleEngine(), scopeA)
	require.NoError(t, err)
	require.Equal(t, int64(3), res3.Rows)
	require.Zero(t, res3.StaleSkipped)
}

// TestSourceCacheReplayResumeAfterOverlayCommitResourcesAndEntitlements
// covers the same crash shape for the other two row kinds, including the
// entitlement bare-id lookup invalidation for pre-clear deletes.
func TestSourceCacheReplayResumeAfterOverlayCommitResourcesAndEntitlements(t *testing.T) {
	ctx := context.Background()

	prev := newAdapter(t)
	_, err := prev.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	baseRes := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		DisplayName: "Group One",
	}.Build()
	baseEntID := "group:g1:member"
	baseEnt := v2.Entitlement_builder{Id: baseEntID, Resource: baseRes, DisplayName: "member"}.Build()
	scoped := sourcecache.WithScope(ctx, scopeA)
	require.NoError(t, prev.PutResources(scoped, baseRes))
	require.NoError(t, prev.PutEntitlements(scoped, baseEnt))

	cur := newAdapter(t)
	_, err = cur.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	// First attempt: replay copies, then overlay rows commit.
	_, err = cur.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err)
	_, err = cur.PebbleEngine().ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err)

	overlayRes := v2.Resource_builder{
		Id:          v2.ResourceId_builder{ResourceType: "group", Resource: "g2"}.Build(),
		DisplayName: "Group Two",
	}.Build()
	overlayEntID := "group:g2:member"
	overlayEnt := v2.Entitlement_builder{Id: overlayEntID, Resource: overlayRes, DisplayName: "member"}.Build()
	curScoped := sourcecache.WithScope(ctx, scopeA)
	require.NoError(t, cur.PutResources(curScoped, overlayRes))
	require.NoError(t, cur.PutEntitlements(curScoped, overlayEnt))

	// Resumed re-run: both kinds converge instead of false-failing.
	rres, err := cur.PebbleEngine().ReplaySourceCacheResources(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err, "resumed resources replay must not false-fail")
	require.Equal(t, int64(1), rres.Rows)
	require.Equal(t, int64(2), rres.ResumedRowsCleared)

	eres, err := cur.PebbleEngine().ReplaySourceCacheEntitlements(ctx, prev.PebbleEngine(), scopeA)
	require.NoError(t, err, "resumed entitlements replay must not false-fail")
	require.Equal(t, int64(1), eres.Rows)
	require.Equal(t, int64(2), eres.ResumedRowsCleared)

	// The pre-clear's deletes invalidated the bare-id lookup: the overlay
	// entitlement is gone until the resumed page re-delivers it.
	_, err = cur.PebbleEngine().GetEntitlementRecord(ctx, overlayEntID)
	require.Error(t, err, "cleared overlay entitlement must not resolve through a stale bare-id map")

	// The resumed page re-delivers its rows; state converges.
	require.NoError(t, cur.PutResources(curScoped, overlayRes))
	require.NoError(t, cur.PutEntitlements(curScoped, overlayEnt))

	gotRes, err := cur.PebbleEngine().GetResourceRecord(ctx, "group", "g2")
	require.NoError(t, err)
	require.Equal(t, "Group Two", gotRes.GetDisplayName())
	gotEnt, err := cur.PebbleEngine().GetEntitlementRecord(ctx, overlayEntID)
	require.NoError(t, err)
	require.Equal(t, scopeA, gotEnt.GetSourceScopeKey())
}
