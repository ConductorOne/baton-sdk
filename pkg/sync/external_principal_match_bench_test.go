package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// countingStore wraps a c1zstore.Store to tally every grant handed to
// PutGrants across a benchmark's b.N iterations -- a real measurement of
// the scan/flush loop's work, unlike principalCount (a loop constant known
// before the benchmark runs): it moves if the loop writes more or fewer
// grants than its own cost model predicts.
type countingStore struct {
	c1zstore.Store
	grantsWritten *int64
}

func (c *countingStore) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	atomic.AddInt64(c.grantsWritten, int64(len(grants)))
	return c.Store.PutGrants(ctx, grants...)
}

// DeleteGrantByRefs forwards like failAfterNPutGrants's (see its comment):
// embedding the c1zstore.Store interface doesn't promote this optional
// method, so without an explicit passthrough the syncer's delete loop would
// always fall back to the id-based path even on Pebble.
func (c *countingStore) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	deleter, ok := c.Store.(grantByRefsDeleter)
	if !ok {
		return c.DeleteGrant(ctx, grant.GetId())
	}
	return deleter.DeleteGrantByRefs(ctx, grant)
}

// BenchmarkProcessGrantsWithExternalPrincipals pins the cost curve of the
// ExternalResourceMatchAll fan-out -- the exact loop #1046 rewrote from
// O(grants x principals) to O(grants + principals). Each iteration matches
// one placeholder against principalCount external users, so the loop's own
// cost model predicts exactly principalCount+1 grants written (1 native
// placeholder + one replacement per principal); countingStore measures the
// actual total and the benchmark fails outright on drift -- a structural
// gate, not just a timing number that quietly regresses. Standard
// `go test -bench` output covers ns/op, B/op, and allocs/op; this adds
// grants-written/op for the write-volume side, on both engines.
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

	var grantsWritten int64

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
		rawStore, err := dotc1z.NewStore(ctx, internalC1zpath, dotc1z.WithEngine(engine), dotc1z.WithTmpDir(tmpDir))
		if err != nil {
			b.Fatalf("NewStore %s: %v", engine, err)
		}
		countedStore := &countingStore{Store: rawStore, grantsWritten: &grantsWritten}
		opts := []SyncOpt{WithConnectorStore(countedStore), WithTmpDir(tmpDir), WithExternalResourceC1ZPath(externalC1zpath)}
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

	wantGrantsWritten := int64(b.N) * int64(principalCount+1)
	gotGrantsWritten := atomic.LoadInt64(&grantsWritten)
	if gotGrantsWritten != wantGrantsWritten {
		b.Fatalf("grants written = %d, want %d (principalCount+1 per iteration: 1 native placeholder "+
			"grant + one resolved replacement per matched principal); a mismatch means the matching/flush "+
			"loop did more or less work than its own cost model predicts, not just that it ran slower",
			gotGrantsWritten, wantGrantsWritten)
	}
	b.ReportMetric(float64(gotGrantsWritten)/float64(b.N), "grants-written/op")
}
