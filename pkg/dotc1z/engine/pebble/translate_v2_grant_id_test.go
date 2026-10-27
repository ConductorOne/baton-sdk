package pebble

import (
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
)

func grantIDTestGrant(id, entRT, entRID, entID, prt, pid string) *v2.Grant {
	return v2.Grant_builder{
		Id: id,
		Entitlement: v2.Entitlement_builder{
			Id: entID,
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{ResourceType: entRT, Resource: entRID}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: prt, Resource: pid}.Build(),
		}.Build(),
	}.Build()
}

// grantIDTestCase builds a grant whose Id is grant.NewGrantID of its own
// entitlement and principal, unless id overrides it.
type grantIDTestCase struct {
	name                 string
	id                   string
	entRT, entRID, entID string
	prt, pid             string
}

func (tc grantIDTestCase) grant(useDerivedID bool) *v2.Grant {
	g := grantIDTestGrant("", tc.entRT, tc.entRID, tc.entID, tc.prt, tc.pid)
	switch {
	case tc.id != "":
		g.SetId(tc.id)
	case useDerivedID:
		g.SetId(grant.NewGrantID(g.GetPrincipal(), g.GetEntitlement()))
	}
	return g
}

func grantIDTestCases() []grantIDTestCase {
	return []grantIDTestCase{
		{name: "entitlement id with rt:rid prefix", entRT: "app", entRID: "github", entID: "app:github:member", prt: "user", pid: "alice"},
		{name: "entitlement id without prefix", entRT: "app", entRID: "github", entID: "member", prt: "user", pid: "alice"},
		{name: "colons in resource and principal ids", entRT: "app", entRID: "git:hub", entID: "app:git:hub:member", prt: "us:er", pid: "a:b:c"},
		{name: "unicode", entRT: "приложение", entRID: "гитхаб", entID: "приложение:гитхаб:участник", prt: "usér", pid: "ид-42"},
		{name: "connector custom id", id: "0b6f3a52-5d1e-4c1b-9a0e-6f1f2a3b4c5d", entRT: "app", entRID: "github", entID: "app:github:member", prt: "user", pid: "alice"},
		{name: "nul and escape bytes", entRT: "a\x00pp", entRID: "g\x01h", entID: "e\x00n\x01t", prt: "u\x01", pid: "\x00"},
	}
}

func TestNewGrantIDFromRecordMatchesV2GrantID(t *testing.T) {
	for _, tc := range grantIDTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			g := tc.grant(true)
			require.NotEmpty(t, g.GetId())
			require.Equal(t, g.GetId(), grant.NewGrantIDFromRecord(V2GrantToV3("sync", g)))
		})
	}
}

func TestV3GrantToV2RoundTripKeepsID(t *testing.T) {
	for _, tc := range grantIDTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			g := tc.grant(true)
			back := V3GrantToV2(V2GrantToV3("sync", g))
			require.Equal(t, g.GetId(), back.GetId())
		})
	}
}

// An empty v2 Id stores as an empty external_id, so the id read back is
// the canonical join of entitlement id and principal.
func TestNewGrantIDFromRecordEmptyIDFallsBackToCanonicalJoin(t *testing.T) {
	for _, tc := range grantIDTestCases() {
		if tc.id != "" {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			g := tc.grant(false)
			require.Empty(t, g.GetId())
			rec := V2GrantToV3("sync", g)
			want := grant.NewGrantID(g.GetPrincipal(), g.GetEntitlement())
			require.Equal(t, want, grant.NewGrantIDFromRecord(rec))
			require.Equal(t, want, V3GrantToV2(rec).GetId())
		})
	}
}

func TestNewGrantIDFromRecordMissingRefs(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*v2.Grant)
	}{
		{name: "nil entitlement", mutate: func(g *v2.Grant) { g.ClearEntitlement() }},
		{name: "nil principal", mutate: func(g *v2.Grant) { g.ClearPrincipal() }},
		{name: "missing ent resource type", mutate: func(g *v2.Grant) { g.GetEntitlement().GetResource().GetId().SetResourceType("") }},
		{name: "missing ent resource id", mutate: func(g *v2.Grant) { g.GetEntitlement().GetResource().GetId().SetResource("") }},
		{name: "missing ent id", mutate: func(g *v2.Grant) { g.GetEntitlement().SetId("") }},
		{name: "missing principal type", mutate: func(g *v2.Grant) { g.GetPrincipal().GetId().SetResourceType("") }},
		{name: "missing principal id", mutate: func(g *v2.Grant) { g.GetPrincipal().GetId().SetResource("") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := grantIDTestGrant("", "app", "github", "app:github:member", "user", "alice")
			tc.mutate(g)
			rec := V2GrantToV3("sync", g)
			require.Equal(t, "", grant.NewGrantIDFromRecord(rec))
			require.Equal(t, "", V3GrantToV2(rec).GetId())

			g.SetId("custom")
			require.Equal(t, "custom", grant.NewGrantIDFromRecord(V2GrantToV3("sync", g)), "external id wins over missing refs")
		})
	}
	require.Equal(t, "", grant.NewGrantIDFromRecord(nil))
}

func FuzzNewGrantIDFromRecord(f *testing.F) {
	f.Add("app", "github", "member", "user", "alice", "")
	f.Add("app", "git:hub", "app:git:hub:member", "us:er", "a:b", "custom")
	f.Add("a\x00", "\x01", "e\x00", "u\x01", "\x00", "")
	f.Fuzz(func(t *testing.T, entRT, entRID, entID, prt, pid, id string) {
		if entRT == "" || entRID == "" || entID == "" || prt == "" || pid == "" {
			t.Skip()
		}
		g := grantIDTestGrant(id, entRT, entRID, entID, prt, pid)
		rec := V2GrantToV3("sync", g)
		want := id
		if id == "" {
			want = entID + ":" + prt + ":" + pid
		}
		require.Equal(t, want, grant.NewGrantIDFromRecord(rec))
		require.Equal(t, want, V3GrantToV2(rec).GetId())
	})
}
