//go:build compatharness

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/conductorone/baton-sdk/pkg/synccompactor"
)

func init() {
	graphCompatHandler = runGraphCompatMode
}

func runGraphCompatMode(ctx context.Context, mode, c1zPath, outPath string) (compatResult, error) {
	switch mode {
	case "graph-seed":
		return graphCompatSeed(ctx, c1zPath)
	case "graph-inspect":
		return graphCompatInspect(ctx, c1zPath)
	case "graph-compact":
		return graphCompatCompact(ctx, c1zPath, outPath, true)
	case "graph-compact-full":
		return graphCompatCompact(ctx, c1zPath, outPath, false)
	case "graph-old-compact":
		return graphCompatCompact(ctx, c1zPath, outPath, false)
	case "graph-corrupt":
		return graphCompatMutate(ctx, c1zPath, "corrupt")
	case "graph-unknown-version":
		return graphCompatMutate(ctx, c1zPath, "unknown-version")
	case "graph-foreign-sync":
		return graphCompatMutate(ctx, c1zPath, "foreign-sync")
	default:
		return compatResult{}, fmt.Errorf("unknown graph compatibility mode %q", mode)
	}
}

func graphCompatMutate(ctx context.Context, path, mutation string) (compatResult, error) {
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(os.TempDir()))
	if err != nil {
		return compatResult{}, err
	}
	graphStore, ok := store.(sdksync.EntitlementGraphStore)
	if !ok {
		_ = store.Close(ctx)
		return compatResult{}, fmt.Errorf("candidate store lacks graph sidecar capability")
	}
	data, err := graphStore.GetEntitlementGraphBlob(ctx)
	if err != nil {
		_ = store.Close(ctx)
		return compatResult{}, err
	}
	switch mutation {
	case "corrupt":
		data = []byte("{truncated")
	case "unknown-version", "foreign-sync":
		var envelope map[string]any
		if err := json.Unmarshal(data, &envelope); err != nil {
			_ = store.Close(ctx)
			return compatResult{}, err
		}
		if mutation == "unknown-version" {
			envelope["format_version"] = float64(999)
		} else {
			envelope["sync_id"] = "foreign-sync"
		}
		data, err = json.Marshal(envelope)
		if err != nil {
			_ = store.Close(ctx)
			return compatResult{}, err
		}
	}
	if err := graphStore.PutEntitlementGraphBlob(ctx, data); err != nil {
		_ = store.Close(ctx)
		return compatResult{}, err
	}
	if err := store.Close(ctx); err != nil {
		return compatResult{}, err
	}
	return graphCompatInspect(ctx, path)
}

func graphCompatSeed(ctx context.Context, path string) (compatResult, error) {
	connector, err := newCompatConnector()
	if err != nil {
		return compatResult{}, err
	}
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(os.TempDir()))
	if err != nil {
		return compatResult{}, err
	}
	syncer, err := sdksync.NewSyncer(ctx, connector,
		sdksync.WithConnectorStore(store),
		sdksync.WithTmpDir(os.TempDir()),
		sdksync.WithWorkerCount(2),
	)
	if err != nil {
		return compatResult{}, err
	}
	if err := syncer.Sync(ctx); err != nil {
		return compatResult{}, err
	}
	if err := syncer.Close(ctx); err != nil {
		return compatResult{}, err
	}

	// Attach the candidate graph envelope to a real sealed artifact. The graph
	// is deliberately edge-free: every stored grant is direct, so this is the
	// exact completed graph for the fixture rather than a synthetic mismatch.
	store, err = dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(os.TempDir()))
	if err != nil {
		return compatResult{}, err
	}
	run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	if err != nil {
		_ = store.Close(ctx)
		return compatResult{}, err
	}
	if run == nil {
		_ = store.Close(ctx)
		return compatResult{}, fmt.Errorf("candidate artifact has no finished sync")
	}
	graph := expand.NewEntitlementGraph(ctx)
	for _, entitlement := range connector.entsByRes {
		graph.AddEntitlementID(entitlement.GetId())
	}
	graph.MarkExpansionComplete()
	graph.Loaded = true
	graph.HasNoCycles = true
	digestReader, ok := store.(c1zstore.GrantGenerationDigestReader)
	if !ok {
		_ = store.Close(ctx)
		return compatResult{}, fmt.Errorf("candidate store lacks grant generation digest")
	}
	digest, found, err := digestReader.GrantGenerationDigest(ctx)
	if err != nil || !found {
		_ = store.Close(ctx)
		return compatResult{}, fmt.Errorf("candidate grant digest unavailable: found=%t err=%w", found, err)
	}
	data, err := expand.MarshalGraphBlobWithGrantDigest(run.ID, graph, digest)
	if err != nil {
		_ = store.Close(ctx)
		return compatResult{}, err
	}
	graphStore, ok := store.(sdksync.EntitlementGraphStore)
	if !ok {
		_ = store.Close(ctx)
		return compatResult{}, fmt.Errorf("candidate store lacks graph sidecar capability")
	}
	if err := graphStore.PutEntitlementGraphBlob(ctx, data); err != nil {
		_ = store.Close(ctx)
		return compatResult{}, err
	}
	if err := store.Close(ctx); err != nil {
		return compatResult{}, err
	}
	return graphCompatInspect(ctx, path)
}

func graphCompatInspect(ctx context.Context, path string) (compatResult, error) {
	result := compatResult{Mode: "graph-inspect", ArtifactPath: path}
	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithTmpDir(os.TempDir()))
	if err != nil {
		return result, err
	}
	run, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	if err != nil {
		_ = store.Close(ctx)
		return result, err
	}
	if run == nil {
		_ = store.Close(ctx)
		return result, fmt.Errorf("inspected artifact has no finished sync")
	}
	if graphStore, ok := store.(sdksync.EntitlementGraphStore); ok {
		data, graphErr := graphStore.GetEntitlementGraphBlob(ctx)
		if graphErr != nil {
			_ = store.Close(ctx)
			return result, graphErr
		}
		result.GraphPresent = len(data) > 0
	}
	graph, err := sdksync.GraphFromStore(ctx, store, run.ID)
	if err != nil {
		result.GraphErr = err.Error()
		err = nil
	}
	result.GraphReusable = graph != nil
	result.Resources, result.Ents, result.Grants, err = summarizeRows(ctx, store, &result)
	if closeErr := store.Close(ctx); err == nil {
		err = closeErr
	}
	return result, err
}

func graphCompatCompact(ctx context.Context, inputPath, outPath string, incremental bool) (compatResult, error) {
	if outPath == "" {
		return compatResult{}, fmt.Errorf("graph compact mode requires -out")
	}
	input, err := dotc1z.NewStore(ctx, inputPath,
		dotc1z.WithReadOnly(true), dotc1z.WithTmpDir(os.TempDir()))
	if err != nil {
		return compatResult{}, err
	}
	run, err := input.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	if closeErr := input.Close(ctx); err == nil {
		err = closeErr
	}
	if err != nil {
		return compatResult{}, err
	}
	if run == nil {
		return compatResult{}, fmt.Errorf("compaction input has no finished sync")
	}

	var logBytes bytes.Buffer
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&logBytes), zap.InfoLevel)
	compactCtx := ctxzap.ToContext(ctx, zap.New(core))
	opts := []synccompactor.Option{
		synccompactor.WithTmpDir(os.TempDir()),
		synccompactor.WithEngine(c1zstore.EnginePebble),
	}
	if incremental {
		opts = append(opts, synccompactor.WithIncrementalExpansion())
	}
	outputDir := filepath.Dir(outPath)
	empty, err := graphCompatEmptyPartial(ctx, outputDir)
	if err != nil {
		return compatResult{}, err
	}
	compactor, cleanup, err := synccompactor.NewCompactor(compactCtx, outputDir,
		[]*synccompactor.CompactableSync{{FilePath: inputPath, SyncID: run.ID}, empty}, opts...)
	if err != nil {
		return compatResult{}, err
	}
	defer cleanup()
	out, err := compactor.Compact(compactCtx)
	if err != nil {
		return compatResult{}, err
	}
	if out.FilePath != outPath {
		_ = os.Remove(outPath)
		if err := os.Rename(out.FilePath, outPath); err != nil {
			return compatResult{}, err
		}
	}
	result, err := graphCompatInspect(ctx, outPath)
	if err != nil {
		return compatResult{}, err
	}
	result.Mode = "graph-compact"
	result.ArtifactPath = outPath
	for _, line := range bytes.Split(bytes.TrimSpace(logBytes.Bytes()), []byte{'\n'}) {
		var entry map[string]any
		if json.Unmarshal(line, &entry) != nil || entry["msg"] != "incremental grant expansion outcome" {
			continue
		}
		result.IncrementalRan = entry["incremental_expansion_outcome"] == "succeeded"
		if value, ok := entry["incremental_expansion_outcome"].(string); ok {
			result.IncrementalOutcome = value
		}
		if value, ok := entry["incremental_expansion_reason"].(string); ok {
			result.IncrementalReason = value
		}
		if value, ok := entry["error"].(string); ok {
			result.IncrementalError = value
		}
	}
	return result, nil
}
