package pebble_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// TestManifestSyncRunProjection locks in the envelope's sync-run
// projection: after a sync is written and the store saved, the v3
// manifest header alone (no payload unpack) must carry the sync run
// and its stats sidecar. This is what lets compaction source selection
// and overlay bucket planning skip unpacking sources entirely.
func TestManifestSyncRunProjection(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "projection.c1z")
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	syncID, err := w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	if err != nil {
		t.Fatal(err)
	}
	userRT := v2.ResourceType_builder{Id: "user", DisplayName: "User"}.Build()
	groupRT := v2.ResourceType_builder{Id: "group", DisplayName: "Group"}.Build()
	if err := w.PutResourceTypes(ctx, userRT, groupRT); err != nil {
		t.Fatal(err)
	}
	user := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u1"}.Build()}.Build()
	group := v2.Resource_builder{Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build()}.Build()
	if err := w.PutResources(ctx, user, group); err != nil {
		t.Fatal(err)
	}
	ent := v2.Entitlement_builder{Id: "ent-1", Resource: group}.Build()
	if err := w.PutEntitlements(ctx, ent); err != nil {
		t.Fatal(err)
	}
	grant := v2.Grant_builder{Id: "grant-1", Entitlement: ent, Principal: user}.Build()
	if err := w.PutGrants(ctx, grant); err != nil {
		t.Fatal(err)
	}
	if err := w.EndSync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	m, err := formatv3.ReadManifestHeader(f)
	if err != nil {
		t.Fatalf("ReadManifestHeader: %v", err)
	}
	runs := m.GetSyncRuns()
	if len(runs) != 1 {
		t.Fatalf("manifest sync_runs = %d, want 1", len(runs))
	}
	run := runs[0]
	if run.GetSyncId() != syncID {
		t.Fatalf("summary sync_id = %q, want %q", run.GetSyncId(), syncID)
	}
	if run.GetEndedAt() == nil {
		t.Fatal("summary ended_at is nil for a finished sync")
	}
	stats := run.GetStats()
	if stats == nil {
		t.Fatal("summary stats is nil; expected stats sidecar projection")
	}
	if got, want := stats.GetResourceTypes(), int64(2); got != want {
		t.Fatalf("stats resource_types = %d, want %d", got, want)
	}
	if got, want := stats.GetResources(), int64(2); got != want {
		t.Fatalf("stats resources = %d, want %d", got, want)
	}
	if got, want := stats.GetEntitlements(), int64(1); got != want {
		t.Fatalf("stats entitlements = %d, want %d", got, want)
	}
	if got, want := stats.GetGrants(), int64(1); got != want {
		t.Fatalf("stats grants = %d, want %d", got, want)
	}

	// The engine-dispatch header path must surface the same projection.
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	env, err := formatv3.ReadEnvelopeHeader(f)
	if err != nil {
		t.Fatalf("ReadEnvelopeHeader: %v", err)
	}
	defer env.Close()
	if got := len(env.Manifest.GetSyncRuns()); got != 1 {
		t.Fatalf("ReadEnvelopeHeader sync_runs = %d, want 1", got)
	}
}
