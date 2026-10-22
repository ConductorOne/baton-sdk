package pebble

import (
	"context"
	"math/rand"
	"strings"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

func withExpandable(t testing.TB, g *v2.Grant, entitlementIDs ...string) {
	t.Helper()
	a, err := anypb.New(v2.GrantExpandable_builder{EntitlementIds: entitlementIDs, Shallow: true}.Build())
	require.NoError(t, err)
	g.SetAnnotations(append(g.GetAnnotations(), a))
}

func requireRecordHashMatchesV2(t testing.TB, g *v2.Grant) {
	t.Helper()
	want, err := GrantContentHash(g)
	require.NoError(t, err)
	got, err := GrantRecordContentHash(V2GrantToV3("", g))
	require.NoError(t, err)
	require.Equal(t, want, got, "GrantRecordContentHash vs GrantContentHash")
}

func TestGrantRecordContentHashMatchesGrantContentHash(t *testing.T) {
	cases := []struct {
		name                 string
		entRT, entRID, entID string
		prt, pid             string
		immutable            bool
		expandable           bool
		srcs                 []testSrcFact
	}{
		{name: "plain opaque ent id", entRT: "app", entRID: "github", entID: "ent-1", prt: "user", pid: "user-42"},
		{name: "stripped ent id", entRT: "app", entRID: "github", entID: "app:github:member", prt: "user", pid: "user-42"},
		{name: "sources", entRT: "app", entRID: "github", entID: "ent-1", prt: "user", pid: "user-42", srcs: []testSrcFact{{"c-src", true}, {"a-src", false}, {"b-src", true}}},
		{name: "immutable, no sources", entRT: "app", entRID: "github", entID: "ent-1", prt: "user", pid: "user-42", immutable: true},
		{name: "immutable with sources", entRT: "app", entRID: "github", entID: "ent-1", prt: "user", pid: "user-42", immutable: true, srcs: []testSrcFact{{"c-src", false}, {"a-src", true}}},
		{name: "expandable", entRT: "app", entRID: "github", entID: "ent-1", prt: "user", pid: "user-42", expandable: true},
		{name: "expandable and immutable with sources", entRT: "app", entRID: "github", entID: "ent-1", prt: "user", pid: "user-42",
			immutable: true, expandable: true, srcs: []testSrcFact{{"s", true}}},
		{name: "embedded NUL", entRT: "a\x00pp", entRID: "git\x00hub", entID: "ent\x00x", prt: "us\x00er", pid: "id\x00", srcs: []testSrcFact{{"s\x00rc", true}, {"\x00", false}}},
		{name: "escape byte", entRT: "a\x01pp", entRID: "hub", entID: "ent\x01x", prt: "us\x01er", pid: "\x01id", srcs: []testSrcFact{{"\x01", false}, {"\x00", true}}},
		{name: "colons", entRT: "a:pp", entRID: "git:hub", entID: "a:pp:git:hub:m:x", prt: "u:r", pid: "::", srcs: []testSrcFact{{"x:y:z", true}}},
		{name: "unicode", entRT: "приложение", entRID: "гитхаб", entID: "entitlé", prt: "usér", pid: "ид-42"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := grantV2WithSourceFacts(tc.entRT, tc.entRID, tc.entID, tc.prt, tc.pid, tc.immutable, tc.srcs...)
			if tc.expandable {
				withExpandable(t, g, "app:github:other")
			}
			requireRecordHashMatchesV2(t, g)
		})
	}
}

func TestGrantRecordContentHashMissingIdentity(t *testing.T) {
	full := func() *v2.Grant { return grantV2WithSources("app", "github", "ent-1", "user", "user-42") }
	cases := []struct {
		name   string
		mutate func(*v2.Grant)
	}{
		{name: "nil grant"},
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
			var g *v2.Grant
			if tc.mutate != nil {
				g = full()
				tc.mutate(g)
			}
			_, v2Err := GrantContentHash(g)
			require.Error(t, v2Err)
			_, v3Err := GrantRecordContentHash(V2GrantToV3("", g))
			require.Error(t, v3Err)

			var acc GrantDigestAccumulator
			require.Error(t, acc.AddRecord(V2GrantToV3("", g)))
			require.Equal(t, int64(0), acc.Root().Count)
		})
	}
	_, err := GrantRecordContentHash(nil)
	require.Error(t, err)
}

func accumulatorTestGrants(t *testing.T) []*v2.Grant {
	t.Helper()
	imm := grantV2WithSourceFacts("app", "github", "ent-a", "user", "carol", true, testSrcFact{"s1", true})
	exp := grantV2WithSources("app", "github", "ent-b", "group", "eng", "s2")
	withExpandable(t, exp, "app:github:ent-a")
	return []*v2.Grant{
		grantV2WithSources("app", "github", "ent-a", "user", "alice"),
		grantV2WithSources("app", "github", "ent-a", "user", "bob", "src-2", "src-1"),
		grantV2WithSources("app", "other", "app:other:ent-a", "user", "alice"),
		grantV2WithSources("a\x00", "\x01", "e\x00", "u\x01", "\x00", "s\x00"),
		imm,
		exp,
	}
}

func TestGrantDigestAccumulatorAddRecordMatchesAdd(t *testing.T) {
	grants := accumulatorTestGrants(t)

	var viaAdd, viaRecord GrantDigestAccumulator
	for _, g := range grants {
		require.NoError(t, viaAdd.Add(g))
		require.NoError(t, viaRecord.AddRecord(V2GrantToV3("", g)))
	}
	require.Equal(t, viaAdd.Root(), viaRecord.Root())
	require.Equal(t, int64(len(grants)), viaRecord.Root().Count)
}

func TestGrantDigestAccumulatorAddRecordOrderIndependent(t *testing.T) {
	grants := accumulatorTestGrants(t)
	records := make([]*v3.GrantRecord, len(grants))
	for i, g := range grants {
		records[i] = V2GrantToV3("", g)
	}
	var want GrantDigestAccumulator
	for _, r := range records {
		require.NoError(t, want.AddRecord(r))
	}
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic seeded test data.
	for range 20 {
		rng.Shuffle(len(records), func(i, j int) { records[i], records[j] = records[j], records[i] })
		var got GrantDigestAccumulator
		for _, r := range records {
			require.NoError(t, got.AddRecord(r))
		}
		require.Equal(t, want.Root(), got.Root())
	}
}

func TestGrantDigestAccumulatorAddRecordErrorLeavesUnchanged(t *testing.T) {
	var acc GrantDigestAccumulator
	for _, g := range accumulatorTestGrants(t) {
		require.NoError(t, acc.AddRecord(V2GrantToV3("", g)))
	}
	before := acc.Root()

	bad := V2GrantToV3("", grantV2WithSources("app", "github", "ent-1", "user", "u"))
	bad.GetPrincipal().SetResourceId("")
	require.Error(t, acc.AddRecord(bad))
	require.Error(t, acc.AddRecord(nil))
	require.Equal(t, before, acc.Root())
}

// TestGrantRecordContentHashIgnoresExpandable pins that the
// GrantExpandable annotation (moved into GrantRecord.expansion by
// V2GrantToV3) does not enter the content hash.
func TestGrantRecordContentHashIgnoresExpandable(t *testing.T) {
	plain := grantV2WithSourceFacts("app", "github", "ent-1", "user", "u", true, testSrcFact{"s", true})
	expandable := grantV2WithSourceFacts("app", "github", "ent-1", "user", "u", true, testSrcFact{"s", true})
	withExpandable(t, expandable, "app:github:a", "app:github:b")

	recPlain, recExp := V2GrantToV3("", plain), V2GrantToV3("", expandable)
	require.Nil(t, recPlain.GetExpansion())
	require.NotNil(t, recExp.GetExpansion())

	hPlain, err := GrantRecordContentHash(recPlain)
	require.NoError(t, err)
	hExp, err := GrantRecordContentHash(recExp)
	require.NoError(t, err)
	require.Equal(t, hPlain, hExp)
}

// FuzzGrantRecordContentHashMatchesV2 is the differential oracle for the
// v3 hash path: for any v2 grant with a hashable identity,
// GrantRecordContentHash(V2GrantToV3(g)) must equal GrantContentHash(g),
// and adding a GrantExpandable annotation must not change either.
func FuzzGrantRecordContentHashMatchesV2(f *testing.F) {
	f.Add("app", "github", "member", "user", "alice", "grant-id", false, false, "", uint32(0), false)
	f.Add("a\x00pp", "g\x01h", "e\x00\x01", "u:r", "\x00:\x01", "", true, true, "s1,s\x00,\x01,,x:y", uint32(5), true)
	f.Fuzz(func(t *testing.T, entRT, entRID, entID, prt, pid, gid string, immutable, expandable bool, srcKeys string, directBits uint32, nilSources bool) {
		if entRT == "" || entRID == "" || entID == "" || prt == "" || pid == "" {
			t.Skip()
		}
		g := grantIDTestGrant(gid, entRT, entRID, entID, prt, pid)
		if srcKeys != "" {
			m := map[string]*v2.GrantSources_GrantSource{}
			for i, k := range strings.Split(srcKeys, ",") {
				if nilSources && i%3 == 0 {
					m[k] = nil
					continue
				}
				m[k] = v2.GrantSources_GrantSource_builder{IsDirect: directBits&(1<<(uint(i)%32)) != 0}.Build()
			}
			g.SetSources(v2.GrantSources_builder{Sources: m}.Build())
		}
		if immutable {
			annos := annotations.Annotations(g.GetAnnotations())
			annos.Update(&v2.GrantImmutable{})
			g.SetAnnotations(annos)
		}

		want, err := GrantContentHash(g)
		require.NoError(t, err)
		got, err := GrantRecordContentHash(V2GrantToV3("", g))
		require.NoError(t, err)
		require.Equal(t, want, got)

		if expandable {
			withExpandable(t, g, "app:github:x", "e\x00:\x01")
			gotExp, err := GrantRecordContentHash(V2GrantToV3("", g))
			require.NoError(t, err)
			require.Equal(t, want, gotExp, "GrantExpandable must not change the record hash")
			v2Exp, err := GrantContentHash(g)
			require.NoError(t, err)
			require.Equal(t, want, v2Exp, "GrantExpandable must not change the v2 hash")
		}
	})
}

// TestGrantDigestAccumulatorAddRecordMatchesSealedRoots is
// TestGrantDigestAccumulatorMatchesSealedRoots with the fold taken over
// the stored records read back through IterateGrants.
func TestGrantDigestAccumulatorAddRecordMatchesSealedRoots(t *testing.T) {
	ctx := context.Background()
	e, _ := newTestEngine(t)
	require.NoError(t, e.bindCurrentSync(ksuid.New().String()))

	entGrants := map[string][]*v2.Grant{
		"ent-a": {
			grantV2WithSources("app", "github", canonicalTestEntID("ent-a"), "user", "alice"),
			grantV2WithSources("app", "github", canonicalTestEntID("ent-a"), "user", "bob", "src-2", "src-1"),
			grantV2WithSourceFacts("app", "github", canonicalTestEntID("ent-a"), "group", "eng", true, testSrcFact{"src-1", true}),
		},
		"ent-b": {
			grantV2WithSources("app", "github", canonicalTestEntID("ent-b"), "user", "alice", "src-1"),
		},
		"ent-zero": {},
	}
	for entID, grants := range entGrants {
		putEnt(t, e, ctx, entID)
		for _, g := range grants {
			require.NoError(t, e.PutGrantRecords(ctx, V2GrantToV3("", g)))
		}
	}
	sealGrantDigests(t, e)

	perEnt := map[string]*GrantDigestAccumulator{}
	var global GrantDigestAccumulator
	require.NoError(t, e.IterateGrants(ctx, func(r *v3.GrantRecord) bool {
		key := r.GetEntitlement().GetEntitlementId()
		if perEnt[key] == nil {
			perEnt[key] = &GrantDigestAccumulator{}
		}
		require.NoError(t, perEnt[key].AddRecord(r))
		require.NoError(t, global.AddRecord(r))
		return true
	}))

	for entID, grants := range entGrants {
		want, ok, err := e.GetEntitlementDigestRoot(ctx, testEntIdentity(entID))
		require.NoError(t, err)
		require.True(t, ok, "digest root present for %s", entID)
		acc := perEnt[canonicalTestEntID(entID)]
		if len(grants) == 0 {
			require.Nil(t, acc)
			require.Equal(t, int64(0), want.Count)
			continue
		}
		require.NotNil(t, acc)
		require.Equal(t, want.Hash, acc.Root().Hash, "entitlement %s digest", entID)
		require.Equal(t, want.Count, acc.Root().Count, "entitlement %s count", entID)
	}

	want, ok, err := e.GetGrantDigestGlobalRoot(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want.Hash, global.Root().Hash, "global digest")
	require.Equal(t, want.Count, global.Root().Count, "global count")
}
