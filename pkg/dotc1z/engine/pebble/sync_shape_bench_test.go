package pebble

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// baton-demo realistic Sync() shape — approximate counts taken
// from baton-demo's pkg/client data fixtures. The shape covers a
// medium-sized organisation: several thousand users, hundreds of
// groups, dozens of roles, each emitting realistic grant fan-out
// onto users + groups.
//
// This is NOT the literal baton-demo Sync — that requires the
// full pkg/sync.Syncer machinery plus a C1ZStore adapter on top
// of the Pebble engine (the engine currently exposes only
// connectorstore.Writer; the v3 sub-store interfaces — Grants(),
// SyncMeta(), FileOps() — are a separate piece of work tracked
// in tracker.md). Until that adapter lands, this bench exercises
// the same record shapes and per-call sizes the baton-demo
// connector emits to the writer, which is the per-engine signal
// we care about.
const (
	demoUsers            = 5_000
	demoGroups           = 200
	demoRoles            = 25
	demoGrantsPerUser    = 8 // role grants + group memberships
	demoGroupMemberships = 12
)

// BenchmarkSyncShape_BatonDemo runs the bench against both
// SQLite (the production default) and Pebble (the new engine).
// Selectable via the SYNC_BENCH_ENGINE env var so CI can opt in
// to one engine; defaults to running both back to back.
//
// Usage:
//
//	SYNC_BENCH_ENGINE=both go test -tags=baton_lambda_support,batonsdkv2 \
//	    -bench=BenchmarkSyncShape_BatonDemo -benchtime=1x \
//	    ./pkg/dotc1z/engine/pebble
func BenchmarkSyncShape_BatonDemo(b *testing.B) {
	engines := []dotc1z.Engine{dotc1z.EngineSQLite, dotc1z.EnginePebble}
	if env := os.Getenv("SYNC_BENCH_ENGINE"); env != "" && env != "both" {
		engines = []dotc1z.Engine{dotc1z.Engine(env)}
	}
	if err := Register(); err != nil {
		b.Fatalf("Register: %v", err)
	}
	for _, eng := range engines {
		b.Run(string(eng), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				runBatonDemoSyncShape(b, eng)
			}
		})
	}
}

func runBatonDemoSyncShape(b *testing.B, engine dotc1z.Engine) {
	b.Helper()
	ctx := context.Background()
	path := fmt.Sprintf("%s/%s.c1z", b.TempDir(), engine)
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(engine))
	if err != nil {
		b.Fatalf("NewStore(%s): %v", engine, err)
	}
	defer func() {
		_ = store.Close(ctx)
	}()
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		b.Fatalf("StartNewSync: %v", err)
	}
	_ = syncID

	start := time.Now()

	// 1. Resource types: user, group, role.
	if err := store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build(),
		v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build(),
		v2.ResourceType_builder{Id: "role", DisplayName: "Role"}.Build(),
	); err != nil {
		b.Fatalf("PutResourceTypes: %v", err)
	}

	// 2. Users.
	users := make([]*v2.Resource, 0, demoUsers)
	for i := 0; i < demoUsers; i++ {
		users = append(users, v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "user",
				Resource:     "u-" + strconv.Itoa(i),
			}.Build(),
			DisplayName: "User " + strconv.Itoa(i),
		}.Build())
	}
	if err := store.PutResources(ctx, users...); err != nil {
		b.Fatalf("PutResources users: %v", err)
	}

	// 3. Groups.
	groups := make([]*v2.Resource, 0, demoGroups)
	for i := 0; i < demoGroups; i++ {
		groups = append(groups, v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "group",
				Resource:     "g-" + strconv.Itoa(i),
			}.Build(),
			DisplayName: "Group " + strconv.Itoa(i),
		}.Build())
	}
	if err := store.PutResources(ctx, groups...); err != nil {
		b.Fatalf("PutResources groups: %v", err)
	}

	// 4. Roles.
	roles := make([]*v2.Resource, 0, demoRoles)
	for i := 0; i < demoRoles; i++ {
		roles = append(roles, v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: "role",
				Resource:     "r-" + strconv.Itoa(i),
			}.Build(),
			DisplayName: "Role " + strconv.Itoa(i),
		}.Build())
	}
	if err := store.PutResources(ctx, roles...); err != nil {
		b.Fatalf("PutResources roles: %v", err)
	}

	// 5. Entitlements: each role has a "member" entitlement, each
	//    group has a "member" entitlement.
	ents := make([]*v2.Entitlement, 0, demoRoles+demoGroups)
	for i := 0; i < demoRoles; i++ {
		ents = append(ents, v2.Entitlement_builder{
			Id:       "role-" + strconv.Itoa(i) + ":member",
			Resource: roles[i],
			Slug:     "member",
		}.Build())
	}
	for i := 0; i < demoGroups; i++ {
		ents = append(ents, v2.Entitlement_builder{
			Id:       "group-" + strconv.Itoa(i) + ":member",
			Resource: groups[i],
			Slug:     "member",
		}.Build())
	}
	if err := store.PutEntitlements(ctx, ents...); err != nil {
		b.Fatalf("PutEntitlements: %v", err)
	}

	// 6. Grants. Each user gets demoGrantsPerUser role grants and
	//    each user is a member of demoGroupMemberships groups.
	grants := make([]*v2.Grant, 0, demoUsers*(demoGrantsPerUser+demoGroupMemberships))
	for i := 0; i < demoUsers; i++ {
		uid := "u-" + strconv.Itoa(i)
		for g := 0; g < demoGrantsPerUser; g++ {
			roleIdx := (i + g) % demoRoles
			grants = append(grants,
				makeGrantV2("u-"+strconv.Itoa(i)+":role-"+strconv.Itoa(roleIdx),
					"role", "r-"+strconv.Itoa(roleIdx), "role-"+strconv.Itoa(roleIdx)+":member",
					"user", uid))
		}
		for g := 0; g < demoGroupMemberships; g++ {
			gIdx := (i*3 + g) % demoGroups
			grants = append(grants,
				makeGrantV2("u-"+strconv.Itoa(i)+":group-"+strconv.Itoa(gIdx),
					"group", "g-"+strconv.Itoa(gIdx), "group-"+strconv.Itoa(gIdx)+":member",
					"user", uid))
		}
	}
	if err := store.PutGrants(ctx, grants...); err != nil {
		b.Fatalf("PutGrants: %v", err)
	}

	if err := store.EndSync(ctx); err != nil {
		b.Fatalf("EndSync: %v", err)
	}
	if err := store.Close(ctx); err != nil {
		b.Fatalf("Close: %v", err)
	}
	elapsed := time.Since(start)

	fi, statErr := os.Stat(path) //nolint:gosec // path is from b.TempDir() — bench-controlled, no taint.
	var size int64
	if statErr == nil {
		size = fi.Size()
	}
	b.ReportMetric(float64(elapsed.Milliseconds()), "ms/sync")
	b.ReportMetric(float64(size)/(1<<20), "MB/c1z")
	b.ReportMetric(float64(len(grants)), "grants")
}

func makeGrantV2(id, entRT, entRes, entID, principalRT, principalID string) *v2.Grant {
	return v2.Grant_builder{
		Id: id,
		Entitlement: v2.Entitlement_builder{
			Id: entID,
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: entRT,
					Resource:     entRes,
				}.Build(),
			}.Build(),
		}.Build(),
		Principal: v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: principalRT,
				Resource:     principalID,
			}.Build(),
		}.Build(),
		Annotations: annotations.Annotations(nil),
	}.Build()
}
