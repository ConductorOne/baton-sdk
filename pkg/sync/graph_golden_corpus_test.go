package sync //nolint:revive,nolintlint // package name kept for compatibility

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
)

// TestGraphSidecarGoldenCorpus is the non-aging, semantic form of the seven
// artifact corpus: each row generates one sealed c1z from its stated premise,
// closes it, reopens it read-only, and proves inspection never changes bytes.
// The old-binary physical artifact cells live in baton-compat-harness.
func TestGraphSidecarGoldenCorpus(t *testing.T) {
	type corpusCase struct {
		name      string
		mutate    func(*testing.T, []byte) []byte
		delete    bool
		wantGraph bool
		wantError bool
	}
	tests := []corpusCase{
		{name: "old artifact without graph"},
		{name: "valid current graph", mutate: func(_ *testing.T, data []byte) []byte { return data }, wantGraph: true},
		{name: "foreign sync binding", mutate: mutateGraphEnvelope("sync_id", "foreign-sync")},
		{name: "unknown future version", mutate: mutateGraphEnvelope("format_version", float64(999))},
		{name: "truncated graph", mutate: func(_ *testing.T, _ []byte) []byte { return []byte("{truncated") }, wantError: true},
		{name: "graph grant digest mismatch", mutate: mutateGraphDigest},
		{name: "rollback invalidated graph", mutate: func(_ *testing.T, data []byte) []byte { return data }, delete: true},
	}
	require.Len(t, tests, 7, "corpus size is a coverage guard")

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "corpus.c1z")
			store, err := dotc1z.NewStore(ctx, path,
				dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
			require.NoError(t, err)
			syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, store.EndSync(ctx))

			if tc.mutate != nil {
				digestReader, ok := store.(c1zstore.GrantGenerationDigestReader)
				require.True(t, ok)
				digest, found, digestErr := digestReader.GrantGenerationDigest(ctx)
				require.NoError(t, digestErr)
				require.True(t, found)
				graph := expand.NewEntitlementGraph(ctx)
				graph.AddEntitlementID("ent-a")
				graph.MarkExpansionComplete()
				graph.Loaded = true
				graph.HasNoCycles = true
				data, marshalErr := expand.MarshalGraphBlobWithGrantDigest(syncID, graph, digest)
				require.NoError(t, marshalErr)
				graphStore, ok := store.(EntitlementGraphStore)
				require.True(t, ok)
				require.NoError(t, graphStore.PutEntitlementGraphBlob(ctx, tc.mutate(t, data)))
				if tc.delete {
					require.NoError(t, graphStore.DeleteEntitlementGraphBlob(ctx))
				}
			}
			require.NoError(t, store.Close(ctx))

			before, err := os.ReadFile(path)
			require.NoError(t, err)
			reader, err := dotc1z.NewStore(ctx, path,
				dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
			require.NoError(t, err)
			graph, graphErr := GraphFromStore(ctx, reader, syncID)
			if tc.wantError {
				require.Error(t, graphErr)
			} else {
				require.NoError(t, graphErr)
			}
			require.Equal(t, tc.wantGraph, graph != nil)
			require.NoError(t, reader.Close(ctx))
			after, err := os.ReadFile(path)
			require.NoError(t, err)
			require.True(t, bytes.Equal(before, after), "read-only corpus inspection mutated the artifact")
		})
	}
}

func TestGraphSidecarCloneAndCopyIsolation(t *testing.T) {
	for _, operation := range []struct {
		name string
		copy func(context.Context, c1zstore.Store, string, string) error
	}{
		{name: "clone", copy: func(ctx context.Context, source c1zstore.Store, path, syncID string) error {
			return source.FileOps().CloneSync(ctx, path, syncID)
		}},
		{name: "copy isolate", copy: func(ctx context.Context, source c1zstore.Store, path, syncID string) error {
			return source.FileOps().CopyIsolateSync(ctx, path, syncID)
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			sourcePath := filepath.Join(dir, "source.c1z")
			source, err := dotc1z.NewStore(ctx, sourcePath,
				dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(t.TempDir()))
			require.NoError(t, err)
			syncID, err := source.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
			require.NoError(t, err)
			require.NoError(t, source.EndSync(ctx))

			digestReader := source.(c1zstore.GrantGenerationDigestReader)
			digest, found, err := digestReader.GrantGenerationDigest(ctx)
			require.NoError(t, err)
			require.True(t, found)
			graph := expand.NewEntitlementGraph(ctx)
			graph.AddEntitlementID("ent-a")
			graph.MarkExpansionComplete()
			graph.Loaded = true
			graph.HasNoCycles = true
			blob, err := expand.MarshalGraphBlobWithGrantDigest(syncID, graph, digest)
			require.NoError(t, err)
			require.NoError(t, source.(EntitlementGraphStore).PutEntitlementGraphBlob(ctx, blob))

			outPath := filepath.Join(dir, "copy.c1z")
			require.NoError(t, operation.copy(ctx, source, outPath, syncID))
			require.NoError(t, source.Close(ctx))

			clone, err := dotc1z.NewStore(ctx, outPath,
				dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(t.TempDir()))
			require.NoError(t, err)
			clonedGraph, err := GraphFromStore(ctx, clone, syncID)
			require.NoError(t, err)
			require.NotNil(t, clonedGraph, "an exact clone must preserve its valid graph")
			require.NoError(t, clonedGraph.ValidateCompleted())
			require.NoError(t, clone.Close(ctx))
		})
	}
}

func mutateGraphEnvelope(key string, value any) func(*testing.T, []byte) []byte {
	return func(t *testing.T, data []byte) []byte {
		t.Helper()
		var envelope map[string]any
		require.NoError(t, json.Unmarshal(data, &envelope))
		envelope[key] = value
		out, err := json.Marshal(envelope)
		require.NoError(t, err)
		return out
	}
}

func mutateGraphDigest(t *testing.T, data []byte) []byte {
	t.Helper()
	var envelope map[string]any
	require.NoError(t, json.Unmarshal(data, &envelope))
	digest, ok := envelope["grant_digest"].(map[string]any)
	require.True(t, ok)
	digest["Count"] = digest["Count"].(float64) + 1
	out, err := json.Marshal(envelope)
	require.NoError(t, err)
	return out
}
