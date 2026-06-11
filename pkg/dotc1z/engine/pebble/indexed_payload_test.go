package pebble_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

// TestIndexedPayloadStoreLifecycle covers the indexed encoding through
// the full store path: write a sync with
// WithPayloadEncoding(IndexedZstd), reopen the file WITHOUT specifying
// an encoding (the store must adopt indexed from the file), run a
// second sync, save again, and read everything back. The second save
// goes through the splice path since the first sync's SSTs are
// unchanged hard-links in the checkpoint.
func TestIndexedPayloadStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "indexed.c1z")

	runSync := func(rtID string, opts ...dotc1z.C1ZOption) string {
		t.Helper()
		opts = append(opts, dotc1z.WithTmpDir(t.TempDir()))
		w, err := dotc1z.NewStore(ctx, path, opts...)
		if err != nil {
			t.Fatal(err)
		}
		syncID, err := w.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
		if err != nil {
			t.Fatal(err)
		}
		if err := w.PutResourceTypes(ctx, v2.ResourceType_builder{Id: rtID}.Build()); err != nil {
			t.Fatal(err)
		}
		res := v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: rtID, Resource: rtID + "-1"}.Build(),
		}.Build()
		if err := w.PutResources(ctx, res); err != nil {
			t.Fatal(err)
		}
		if err := w.EndSync(ctx); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(ctx); err != nil {
			t.Fatal(err)
		}
		return syncID
	}

	first := runSync("user", dotc1z.WithEngine(dotc1z.EnginePebble), dotc1z.WithPayloadEncoding(dotc1z.PayloadEncodingIndexedZstd))

	// The file on disk must carry the indexed encoding.
	requireFileEncoding := func() {
		t.Helper()
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		m, err := formatv3.ReadManifestHeader(f)
		if err != nil {
			t.Fatal(err)
		}
		if got := m.GetPayloadEncoding().String(); got != "PAYLOAD_ENCODING_INDEXED_ZSTD" {
			t.Fatalf("file payload encoding = %s, want PAYLOAD_ENCODING_INDEXED_ZSTD", got)
		}
	}
	requireFileEncoding()

	// Reopen with NO explicit encoding: the store must adopt the
	// file's indexed encoding so the rewrite stays indexed (and the
	// splice path stays viable for future rewrites).
	second := runSync("group")
	requireFileEncoding()

	// Both syncs readable after the spliced rewrite.
	w, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	eng, ok := pebble.AsEngine(w)
	if !ok {
		t.Fatalf("not pebble: %T", w)
	}
	for _, syncID := range []string{first, second} {
		if _, err := eng.GetSyncRunRecord(ctx, syncID); err != nil {
			t.Fatalf("sync %s missing after indexed round trips: %v", syncID, err)
		}
	}
	if _, err := eng.GetResourceRecord(ctx, first, "user", "user-1"); err != nil {
		t.Fatalf("first sync's resource missing after spliced rewrite: %v", err)
	}
	if _, err := eng.GetResourceRecord(ctx, second, "group", "group-1"); err != nil {
		t.Fatalf("second sync's resource missing: %v", err)
	}
}
