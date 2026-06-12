package expand

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// buildFanoutC1Z writes a REAL disk-backed c1z for the high-out-degree scenario:
// one hot source entitlement (group:everyone:member) with `members` grants, and
// `dests` destination entitlements each assigned to group-everyone (expandable
// from the source). Expanding it produces members*dests grants on disk.
// Returns the base file path and its syncID.
func buildFanoutC1Z(b *testing.B, ctx context.Context, members, dests int) (string, string) {
	b.Helper()
	tmp, err := os.CreateTemp("", "fanout-base-*.c1z")
	if err != nil {
		b.Fatal(err)
	}
	_ = tmp.Close()
	path := tmp.Name()

	c1f, err := dotc1z.NewC1ZFile(ctx, path)
	if err != nil {
		b.Fatal(err)
	}
	syncID, err := c1f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		b.Fatal(err)
	}

	res := func(rt, id string) *v2.Resource {
		return v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: rt, Resource: id}.Build()}.Build()
	}

	must := func(err error) {
		if err != nil {
			b.Fatal(err)
		}
	}

	must(c1f.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build(),
		v2.ResourceType_builder{Id: "role"}.Build(),
	))

	const srcEntID = "group:everyone:member"
	groupRes := res("group", "everyone")
	srcEnt := v2.Entitlement_builder{Id: srcEntID, Resource: groupRes}.Build()
	must(c1f.PutResources(ctx, groupRes))

	// Destination role resources + entitlements.
	roleResources := make([]*v2.Resource, dests)
	destEnts := make([]*v2.Entitlement, dests)
	for d := 0; d < dests; d++ {
		rr := res("role", "r"+strconv.Itoa(d))
		roleResources[d] = rr
		destEnts[d] = v2.Entitlement_builder{Id: "role:r" + strconv.Itoa(d) + ":assigned", Resource: rr}.Build()
	}
	putInChunks(b, dests, 500, func(lo, hi int) { must(c1f.PutResources(ctx, roleResources[lo:hi]...)) })
	must(c1f.PutEntitlements(ctx, srcEnt))
	putInChunks(b, dests, 500, func(lo, hi int) { must(c1f.PutEntitlements(ctx, destEnts[lo:hi]...)) })

	// Source member grants (plain): `members` distinct users hold group:everyone:member.
	userResources := make([]*v2.Resource, members)
	sourceGrants := make([]*v2.Grant, members)
	for i := 0; i < members; i++ {
		ur := res("user", "u"+strconv.Itoa(i))
		userResources[i] = ur
		sourceGrants[i] = v2.Grant_builder{Id: "g-src-" + strconv.Itoa(i), Entitlement: srcEnt, Principal: ur}.Build()
	}
	putInChunks(b, members, 1000, func(lo, hi int) { must(c1f.PutResources(ctx, userResources[lo:hi]...)) })
	putInChunks(b, members, 1000, func(lo, hi int) { must(c1f.PutGrants(ctx, sourceGrants[lo:hi]...)) })

	// Expandable grants: each role assigned to group-everyone, expandable from the source entitlement.
	expGrants := make([]*v2.Grant, dests)
	for d := 0; d < dests; d++ {
		g := v2.Grant_builder{Id: "g-exp-" + strconv.Itoa(d), Entitlement: destEnts[d], Principal: groupRes}.Build()
		annos := annotations.Annotations(g.GetAnnotations())
		annos.Update(v2.GrantExpandable_builder{EntitlementIds: []string{srcEntID}}.Build())
		g.SetAnnotations(annos)
		expGrants[d] = g
	}
	putInChunks(b, dests, 500, func(lo, hi int) { must(c1f.PutGrants(ctx, expGrants[lo:hi]...)) })

	must(c1f.EndSync(ctx))
	must(c1f.Close(ctx))
	return path, syncID
}

func putInChunks(b *testing.B, n, chunk int, fn func(lo, hi int)) {
	b.Helper()
	for lo := 0; lo < n; lo += chunk {
		hi := lo + chunk
		if hi > n {
			hi = n
		}
		fn(lo, hi)
	}
}

// loadFanoutGraph builds the entitlement graph from the store's pending-expansion
// metadata (self-contained copy of the loader so it does not depend on stashed test files).
func loadFanoutGraph(b *testing.B, ctx context.Context, store *dotc1z.C1File) *EntitlementGraph {
	b.Helper()
	graph := NewEntitlementGraph(ctx)
	for def, err := range store.Grants().PendingExpansion(ctx) {
		if errors.Is(err, sql.ErrNoRows) {
			graph.Loaded = true
			return graph
		}
		if err != nil {
			b.Fatal(err)
		}
		for _, srcID := range def.Annotation.GetEntitlementIds() {
			srcEnt, err := store.GetEntitlement(ctx, reader_v2.EntitlementsReaderServiceGetEntitlementRequest_builder{
				EntitlementId: srcID,
			}.Build())
			if err != nil {
				continue
			}
			graph.AddEntitlementID(def.TargetEntitlementID)
			graph.AddEntitlementID(srcEnt.GetEntitlement().GetId())
			_ = graph.AddEdge(ctx, srcEnt.GetEntitlement().GetId(), def.TargetEntitlementID,
				def.Annotation.GetShallow(), def.Annotation.GetResourceTypeIds())
		}
	}
	graph.Loaded = true
	return graph
}

// 10k members x 1000 dests = 10,000,000 expanded grants, written to a real c1z on disk.
func BenchmarkExpandFanout10MDisk(b *testing.B) {
	ctx := context.Background()
	const members, dests = 10000, 1000

	basePath, syncID := buildFanoutC1Z(b, ctx, members, dests)
	defer os.Remove(basePath)
	baseData, err := os.ReadFile(basePath)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmp, err := os.CreateTemp("", "fanout-run-*.c1z")
		if err != nil {
			b.Fatal(err)
		}
		_ = tmp.Close()
		runPath := tmp.Name()
		// runPath is from os.CreateTemp().Name(), not external input — benchmark-only.
		if err := os.WriteFile(runPath, baseData, 0600); err != nil { //nolint:gosec // G703: path is a test-created temp file, not tainted
			b.Fatal(err)
		}
		c1f, err := dotc1z.NewC1ZFile(ctx, runPath)
		if err != nil {
			b.Fatal(err)
		}
		if err := c1f.SetSyncID(ctx, syncID); err != nil {
			b.Fatal(err)
		}
		graph := loadFanoutGraph(b, ctx, c1f)
		b.StartTimer()

		expander := NewExpander(c1f, graph)
		err = expander.Run(ctx)

		b.StopTimer()
		if err != nil {
			b.Fatalf("expand: %v", err)
		}
		_ = c1f.Close(ctx)
		_ = os.Remove(runPath)
		b.StartTimer()
	}
}
