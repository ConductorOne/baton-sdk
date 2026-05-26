package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/logging"
)

// BenchmarkFullSync_BatonDemoShape drives pkg/sync.NewSyncer
// end-to-end against a mockConnector seeded at baton-demo scale —
// the same data dimensions the public baton-demo connector emits —
// through both the SQLite engine (WithC1ZPath) and the Pebble
// engine (WithConnectorStore against a Pebble-backed dotc1z.C1ZStore).
//
// This is the FULL Sync() pipeline, not just the writer surface:
// ResourceTypes → Resources → Entitlements → Grants iteration
// through the connectorbuilder, the syncer's action-loop
// orchestration, grant expansion, and final c1z save.
//
// Reports ms/sync, MB/c1z, and grants/op so engine deltas are
// directly comparable. Run:
//
//	go test -tags=baton_lambda_support,batonsdkv2 \
//	    -bench=BenchmarkFullSync_BatonDemoShape -benchtime=1x \
//	    -run='^$' ./pkg/sync/
func BenchmarkFullSync_BatonDemoShape(b *testing.B) {
	// Scale dimensions chosen to span baton-demo's data profile.
	// "large" is the closest approximation to a real production
	// tenant: 10k users, 500 groups, ~25 memberships/group → 12.5k
	// grants. Adjust via SYNC_BENCH_SCALE env var to skip large for
	// quick iteration on a laptop.
	scales := []struct {
		name             string
		users            int
		groups           int
		membershipsRange int // memberships per group
	}{
		{"small", 200, 20, 10},
		{"medium", 2_000, 100, 25},
		{"large", 10_000, 500, 25},
		// xlarge approximates a "real production tenant" scale —
		// 50k groups × 200 memberships ≈ 10M grants. Skipped by
		// default (set SYNC_BENCH_SCALE=xlarge to opt in). Expect
		// SQLite to take minutes; Pebble should land in the
		// low-tens-of-seconds range.
		{"xlarge", 50_000, 50_000, 200},
	}
	if env := os.Getenv("SYNC_BENCH_SCALE"); env != "" {
		filtered := scales[:0]
		for _, s := range scales {
			if s.name == env {
				filtered = append(filtered, s)
			}
		}
		scales = filtered
	} else {
		// Default excludes xlarge — it produces ~10M grants and
		// takes several minutes on SQLite. Opt in via
		// SYNC_BENCH_SCALE=xlarge.
		filtered := scales[:0]
		for _, s := range scales {
			if s.name != "xlarge" {
				filtered = append(filtered, s)
			}
		}
		scales = filtered
	}
	for _, sc := range scales {
		b.Run(sc.name, func(b *testing.B) {
			for _, eng := range []string{"sqlite", "pebble"} {
				b.Run(eng, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						runOneFullSync(b, eng, sc.users, sc.groups, sc.membershipsRange)
					}
				})
			}
		})
	}
}

func runOneFullSync(b *testing.B, engine string, nUsers, nGroups, membershipsPerGroup int) {
	b.Helper()
	ctx := b.Context()
	ctx, err := logging.Init(ctx)
	if err != nil {
		b.Fatalf("logging.Init: %v", err)
	}
	tmpDir := b.TempDir()

	// Build the connector graph: nGroups groups, nUsers users,
	// each group sampling membershipsPerGroup users — produces
	// nGroups * membershipsPerGroup grants. With small=200, scale
	// settings give 20×10=200 grants; medium gives 100×25=2500.
	mc := newMockConnector()
	mc.rtDB = append(mc.rtDB, groupResourceType, userResourceType)

	groupRes := make([]*v2.Resource, nGroups)
	groupEnt := make([]*v2.Entitlement, nGroups)
	for i := 0; i < nGroups; i++ {
		g, e, err := mc.AddGroup(ctx, "g-"+strconv.Itoa(i))
		if err != nil {
			b.Fatalf("AddGroup: %v", err)
		}
		groupRes[i] = g
		groupEnt[i] = e
	}
	users := make([]*v2.Resource, nUsers)
	for i := 0; i < nUsers; i++ {
		u, err := mc.AddUser(ctx, "u-"+strconv.Itoa(i))
		if err != nil {
			b.Fatalf("AddUser: %v", err)
		}
		users[i] = u
	}
	// Wire memberships deterministically. We pass no expandEnts —
	// these are flat user-in-group grants, not chained
	// subgroup-in-group expansions (which require the source
	// entitlement's resource to match the grant's principal).
	for gi := 0; gi < nGroups; gi++ {
		for u := 0; u < membershipsPerGroup; u++ {
			user := users[(gi*membershipsPerGroup+u)%nUsers]
			mc.AddGroupMember(ctx, groupRes[gi], user)
		}
	}
	_ = groupEnt // referenced in the expansion variant; left for future use

	c1zPath := filepath.Join(tmpDir, engine+".c1z")

	start := time.Now()

	var opts []SyncOpt
	switch engine {
	case "sqlite":
		opts = []SyncOpt{WithC1ZPath(c1zPath), WithTmpDir(tmpDir)}
	case "pebble":
		if err := pebble.Register(); err != nil {
			b.Fatalf("pebble.Register: %v", err)
		}
		store, err := dotc1z.NewStore(ctx, c1zPath,
			dotc1z.WithEngine(dotc1z.EnginePebble),
			dotc1z.WithTmpDir(tmpDir),
		)
		if err != nil {
			b.Fatalf("NewStore pebble: %v", err)
		}
		c1zStore, ok := store.(dotc1z.C1ZStore)
		if !ok {
			b.Fatalf("pebble store does not satisfy dotc1z.C1ZStore")
		}
		opts = []SyncOpt{WithConnectorStore(c1zStore), WithTmpDir(tmpDir)}
	default:
		b.Fatalf("unknown engine %q", engine)
	}

	syncer, err := NewSyncer(ctx, mc, opts...)
	if err != nil {
		b.Fatalf("NewSyncer: %v", err)
	}
	if err := syncer.Sync(ctx); err != nil {
		b.Fatalf("Sync: %v", err)
	}
	if err := syncer.Close(ctx); err != nil {
		b.Fatalf("syncer.Close: %v", err)
	}
	elapsed := time.Since(start)

	fi, statErr := os.Stat(c1zPath) //nolint:gosec // path is from b.TempDir() — bench-controlled.
	var size int64
	if statErr == nil {
		size = fi.Size()
	}
	b.ReportMetric(float64(elapsed.Milliseconds()), "ms/sync")
	b.ReportMetric(float64(size)/(1<<20), "MB/c1z")
}
