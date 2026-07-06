package pebble

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/segmentio/ksuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	enginepkg "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
)

func BenchmarkMergeFilesIntoKWay(b *testing.B) {
	ctx := context.Background()
	const sourceCount = 50
	const grantsPerSource = 2500

	fixtureDir := b.TempDir()
	sources := make([]SourceFile, 0, sourceCount)
	for sourceIdx := 0; sourceIdx < sourceCount; sourceIdx++ {
		path := filepath.Join(fixtureDir, fmt.Sprintf("source-%03d.c1z", sourceIdx))
		w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(fixtureDir))
		if err != nil {
			b.Fatal(err)
		}
		store := w
		src, ok := enginepkg.AsEngine(w)
		if !ok {
			b.Fatalf("store is not pebble: %T", w)
		}
		syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		if err != nil {
			b.Fatal(err)
		}
		if err := src.PutResourceTypeRecords(ctx,
			v3.ResourceTypeRecord_builder{ExternalId: "user", DiscoveredAt: timestamppb.Now()}.Build(),
			v3.ResourceTypeRecord_builder{ExternalId: "group", DiscoveredAt: timestamppb.Now()}.Build(),
		); err != nil {
			b.Fatal(err)
		}
		group := v3.ResourceRecord_builder{
			ResourceTypeId: "group", ResourceId: "engineering", DiscoveredAt: timestamppb.Now(),
		}.Build()
		if err := src.PutResourceRecord(ctx, group); err != nil {
			b.Fatal(err)
		}
		ent := v3.EntitlementRecord_builder{
			ExternalId:   "member",
			Resource:     v3.ResourceRef_builder{ResourceTypeId: "group", ResourceId: "engineering"}.Build(),
			DiscoveredAt: timestamppb.Now(),
		}.Build()
		if err := src.PutEntitlementRecord(ctx, ent); err != nil {
			b.Fatal(err)
		}
		batch := make([]*v3.GrantRecord, 0, grantsPerSource)
		for grantIdx := 0; grantIdx < grantsPerSource; grantIdx++ {
			// 20% overlap across source syncs, matching the exploratory
			// benchmark shape and exercising newer-wins replacement.
			grantID := fmt.Sprintf("source-%03d-grant-%05d", sourceIdx, grantIdx)
			if grantIdx < grantsPerSource/5 {
				grantID = fmt.Sprintf("shared-grant-%05d", grantIdx)
			}
			batch = append(batch, v3.GrantRecord_builder{
				ExternalId:   grantID,
				Entitlement:  v3.EntitlementRef_builder{ResourceTypeId: "group", ResourceId: "engineering", EntitlementId: "member"}.Build(),
				Principal:    v3.PrincipalRef_builder{ResourceTypeId: "user", ResourceId: fmt.Sprintf("user-%05d", grantIdx)}.Build(),
				DiscoveredAt: timestamppb.New(time.Unix(int64(1000+sourceIdx), 0).UTC()),
			}.Build())
		}
		if err := src.PutGrantRecords(ctx, batch...); err != nil {
			b.Fatal(err)
		}
		if err := store.EndSync(ctx); err != nil {
			b.Fatal(err)
		}
		if err := store.Close(ctx); err != nil {
			b.Fatal(err)
		}
		sources = append(sources, SourceFile{Path: path, SyncID: syncID})
	}

	b.ReportAllocs()
	b.ReportMetric(sourceCount, "sources/op")
	b.ReportMetric(grantsPerSource, "grants_per_source")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dest, _ := benchNewEngine(b, fmt.Sprintf("dest-%d", i))
		destSyncID := ksuid.New().String()
		tmpDir := filepath.Join(fixtureDir, fmt.Sprintf("tmp-%d", i))
		if err := os.MkdirAll(tmpDir, 0o755); err != nil {
			b.Fatal(err)
		}
		if _, err := MergeFilesInto(ctx, dest, sources, destSyncID, tmpDir); err != nil {
			b.Fatal(err)
		}
		if err := dest.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
