package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"fmt"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// BenchmarkProcessGrantsWithExternalPrincipals pins the cost curve of
// processGrantsWithExternalPrincipals's ExternalResourceMatchAll fan-out --
// the exact loop #1046 rewrote from O(grants x principals) to O(grants +
// principals), and that this PR's mid-scan flushing makes read its own
// writes back (G scanned becomes G+N processed, per the cost note on that
// loop). Reports ms/sync and grants/op so a future regression in either the
// matching cost or the re-read amplification shows up as a measurable
// slowdown, on both storage engines.
//
//	go test -run='^$' -bench=BenchmarkProcessGrantsWithExternalPrincipals ./pkg/sync/
func BenchmarkProcessGrantsWithExternalPrincipals(b *testing.B) {
	for _, principalCount := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("principals-%d", principalCount), func(b *testing.B) {
			for _, engine := range []c1zstore.Engine{c1zstore.EngineSQLite, c1zstore.EnginePebble} {
				b.Run(string(engine), func(b *testing.B) {
					runExternalMatchAllBenchmark(b, engine, principalCount)
				})
			}
		})
	}
}

func runExternalMatchAllBenchmark(b *testing.B, engine c1zstore.Engine, principalCount int) {
	b.Helper()
	ctx := b.Context()
	tmpDir := b.TempDir()

	// Built once and reused read-only across iterations -- only the
	// internal side (the actual matching work) is what's being measured.
	externalMc := newMockConnector()
	externalMc.rtDB = append(externalMc.rtDB, userResourceType, groupResourceType)
	for i := range principalCount {
		if _, err := externalMc.AddUserProfile(ctx, fmt.Sprintf("ext_user_%d", i), map[string]any{}); err != nil {
			b.Fatalf("AddUserProfile: %v", err)
		}
	}
	externalC1zpath := filepath.Join(tmpDir, "external.c1z")
	externalSyncer, err := NewSyncer(ctx, externalMc, WithC1ZPath(externalC1zpath), WithTmpDir(tmpDir))
	if err != nil {
		b.Fatalf("NewSyncer(external): %v", err)
	}
	if err := externalSyncer.Sync(ctx); err != nil {
		b.Fatalf("Sync(external): %v", err)
	}
	if err := externalSyncer.Close(ctx); err != nil {
		b.Fatalf("Close(external): %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		internalMc := newMockConnector()
		internalMc.rtDB = append(internalMc.rtDB, userResourceType, groupResourceType)
		internalGroup, _, err := internalMc.AddGroup(ctx, "internal_group")
		if err != nil {
			b.Fatalf("AddGroup: %v", err)
		}
		internalMc.grantDB[internalGroup.GetId().GetResource()] = []*v2.Grant{
			gt.NewGrant(
				internalGroup, "member",
				v2.ResourceId_builder{ResourceType: userResourceType.GetId(), Resource: "placeholder"}.Build(),
				gt.WithAnnotation(v2.ExternalResourceMatchAll_builder{ResourceType: v2.ResourceType_TRAIT_USER}.Build()),
			),
		}
		internalC1zpath := filepath.Join(tmpDir, fmt.Sprintf("internal-%d.c1z", i))
		var opts []SyncOpt
		switch engine {
		case c1zstore.EngineSQLite:
			opts = []SyncOpt{WithC1ZPath(internalC1zpath), WithTmpDir(tmpDir), WithExternalResourceC1ZPath(externalC1zpath)}
		case c1zstore.EnginePebble:
			store, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(tmpDir))
			if err != nil {
				b.Fatalf("NewStore pebble: %v", err)
			}
			opts = []SyncOpt{WithConnectorStore(store), WithTmpDir(tmpDir), WithExternalResourceC1ZPath(externalC1zpath)}
		default:
			b.Fatalf("unknown engine %q", engine)
		}
		internalSyncer, err := NewSyncer(ctx, internalMc, opts...)
		if err != nil {
			b.Fatalf("NewSyncer(internal): %v", err)
		}
		b.StartTimer()

		if err := internalSyncer.Sync(ctx); err != nil {
			b.Fatalf("Sync(internal): %v", err)
		}
		if err := internalSyncer.Close(ctx); err != nil {
			b.Fatalf("Close(internal): %v", err)
		}
	}
	b.ReportMetric(float64(principalCount), "grants/op")
}
