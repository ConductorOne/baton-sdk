package expand

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

const (
	defaultPebbleDenseParents    = 16
	defaultPebbleDenseDests      = 16
	defaultPebbleDensePrincipals = 256
)

func densePebbleBenchParam(name string, fallback int) int {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func densePebbleBenchAlgos() map[string]bool {
	v := os.Getenv("BATON_BENCH_DENSE_ALGOS")
	if v == "" {
		return nil
	}
	out := make(map[string]bool)
	for _, part := range strings.Split(v, ",") {
		name := strings.TrimSpace(part)
		if name != "" {
			out[name] = true
		}
	}
	return out
}

func BenchmarkTopologicalMergeDensePebble(b *testing.B) {
	parents := densePebbleBenchParam("BATON_BENCH_DENSE_PARENTS", defaultPebbleDenseParents)
	dests := densePebbleBenchParam("BATON_BENCH_DENSE_DESTS", defaultPebbleDenseDests)
	principals := densePebbleBenchParam("BATON_BENCH_DENSE_PRINCIPALS", defaultPebbleDensePrincipals)
	algos := densePebbleBenchAlgos()

	ctx := context.Background()
	seedPath := fmt.Sprintf("%s/dense.pebble.c1z", b.TempDir())
	graph, syncID := seedDensePebbleC1Z(b, ctx, seedPath, parents, dests, principals)

	b.Logf("dense pebble shape: parents=%d dests=%d principals=%d edges=%d", parents, dests, principals, len(graph.Edges))
	b.ReportMetric(float64(parents), "parents")
	b.ReportMetric(float64(dests), "dests")
	b.ReportMetric(float64(principals), "principals")

	for _, tc := range []struct {
		name string
		run  func(context.Context, ExpanderStore, *EntitlementGraph) (float64, error)
	}{
		{
			name: "current",
			run: func(ctx context.Context, store ExpanderStore, graph *EntitlementGraph) (float64, error) {
				return 0, NewExpander(store, graph).Run(ctx)
			},
		},
		{
			name: "topological_streaming",
			run: func(ctx context.Context, store ExpanderStore, graph *EntitlementGraph) (float64, error) {
				return 0, NewExpander(store, graph).RunTopologicalMergeStreaming(ctx)
			},
		},
		{
			name: "topological_projection",
			run: func(ctx context.Context, store ExpanderStore, graph *EntitlementGraph) (float64, error) {
				return 0, NewExpander(store, graph).RunTopologicalMergeProjection(ctx)
			},
		},
		{
			name: "shuffle_pebble_temp",
			run: func(ctx context.Context, store ExpanderStore, graph *EntitlementGraph) (float64, error) {
				temp, err := runPebbleShuffleMergeWithStore(ctx, store, graph)
				return float64(temp), err
			},
		},
		{
			name: "shuffle_sst_temp",
			run: func(ctx context.Context, store ExpanderStore, graph *EntitlementGraph) (float64, error) {
				temp, err := runSSTShuffleMergeWithStore(ctx, store, graph)
				return float64(temp), err
			},
		},
		{
			name: "shuffle_sst_spill_temp",
			run: func(ctx context.Context, store ExpanderStore, graph *EntitlementGraph) (float64, error) {
				temp, err := runSpillSSTShuffleMergeWithStore(ctx, store, graph)
				return float64(temp), err
			},
		},
	} {
		if algos != nil && !algos[tc.name] {
			continue
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tmpPath := fmt.Sprintf("%s/%s-%d.c1z", b.TempDir(), tc.name, i)
				copyFileForBenchmark(b, seedPath, tmpPath)

				store, err := dotc1z.NewStore(ctx, tmpPath)
				require.NoError(b, err)
				_, started, err := store.StartOrResumeSync(ctx, connectorstore.SyncTypeFull, syncID)
				require.NoError(b, err)
				require.False(b, started)

				graphCopy := copyGraph(graph)
				b.StartTimer()
				tempContribs, err := tc.run(ctx, benchmarkExpanderStore{store: store}, graphCopy)
				if err != nil {
					b.StopTimer()
					_ = store.Close(ctx)
					b.Fatalf("run %s: %v", tc.name, err)
				}
				if err := store.EndSync(ctx); err != nil {
					b.StopTimer()
					_ = store.Close(ctx)
					b.Fatalf("EndSync: %v", err)
				}
				b.StopTimer()
				b.ReportMetric(tempContribs, "temp_contribs/op")
				require.NoError(b, store.Close(ctx))
				require.NoError(b, os.Remove(tmpPath))
			}
		})
	}
}

func seedDensePebbleC1Z(
	b *testing.B,
	ctx context.Context,
	path string,
	parents int,
	dests int,
	principals int,
) (*EntitlementGraph, string) {
	b.Helper()

	store, err := dotc1z.NewStore(ctx, path, dotc1z.WithEngine(dotc1z.EnginePebble))
	require.NoError(b, err)
	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(b, err)

	require.NoError(b, store.PutResourceTypes(ctx,
		v2.ResourceType_builder{Id: "group"}.Build(),
		v2.ResourceType_builder{Id: "user"}.Build(),
	))

	group := v2.Resource_builder{
		Id: v2.ResourceId_builder{ResourceType: "group", Resource: "dense"}.Build(),
	}.Build()
	require.NoError(b, store.PutResources(ctx, group))

	users := make([]*v2.Resource, 0, principals)
	for i := 0; i < principals; i++ {
		users = append(users, v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "user", Resource: "u:" + strconv.Itoa(i)}.Build(),
		}.Build())
	}
	for start := 0; start < len(users); start += 5000 {
		end := start + 5000
		if end > len(users) {
			end = len(users)
		}
		require.NoError(b, store.PutResources(ctx, users[start:end]...))
	}

	graph := NewEntitlementGraph(ctx)
	parentEnts := make([]*v2.Entitlement, 0, parents)
	destEnts := make([]*v2.Entitlement, 0, dests)
	entitlements := make([]*v2.Entitlement, 0, parents+dests)
	for p := 0; p < parents; p++ {
		ent := v2.Entitlement_builder{Id: "ent:parent:" + strconv.Itoa(p), Resource: group}.Build()
		parentEnts = append(parentEnts, ent)
		entitlements = append(entitlements, ent)
		graph.AddEntitlementID(ent.GetId())
	}
	for d := 0; d < dests; d++ {
		ent := v2.Entitlement_builder{Id: "ent:dest:" + strconv.Itoa(d), Resource: group}.Build()
		destEnts = append(destEnts, ent)
		entitlements = append(entitlements, ent)
		graph.AddEntitlementID(ent.GetId())
	}
	require.NoError(b, store.PutEntitlements(ctx, entitlements...))

	grants := make([]*v2.Grant, 0, parents*principals)
	for p, ent := range parentEnts {
		for i, user := range users {
			grants = append(grants, v2.Grant_builder{
				Id:          fmt.Sprintf("grant:p:%d:u:%d", p, i),
				Entitlement: ent,
				Principal:   user,
			}.Build())
		}
	}
	for start := 0; start < len(grants); start += 5000 {
		end := start + 5000
		if end > len(grants) {
			end = len(grants)
		}
		require.NoError(b, store.PutGrants(ctx, grants[start:end]...))
	}

	for _, parent := range parentEnts {
		for _, dest := range destEnts {
			require.NoError(b, graph.AddEdge(ctx, parent.GetId(), dest.GetId(), false, []string{"user"}))
		}
	}
	graph.HasNoCycles = true

	require.NoError(b, store.EndSync(ctx))
	require.NoError(b, store.Close(ctx))
	return graph, syncID
}

func copyFileForBenchmark(b *testing.B, src, dst string) {
	b.Helper()
	data, err := os.ReadFile(src) // #nosec G304 -- benchmark-controlled seed path
	require.NoError(b, err)
	require.NoError(b, os.WriteFile(dst, data, 0600)) // #nosec G306,G703 -- benchmark temp file
}

func runPebbleShuffleMergeWithStore(ctx context.Context, store ExpanderStore, graph *EntitlementGraph) (int, error) {
	tempDir, err := os.MkdirTemp("", "baton-expand-shuffle-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(tempDir)

	tempDB, err := pebble.Open(tempDir, &pebble.Options{})
	if err != nil {
		return 0, err
	}
	defer tempDB.Close()

	expander := NewExpander(store, graph)
	order, err := topologicalNodeOrder(graph)
	if err != nil {
		return 0, err
	}

	entitlements := make(map[string]*v2.Entitlement, len(graph.EntitlementsToNodes))
	for _, entID := range graph.GetEntitlements() {
		ent, ok, err := expander.fetchEntitlement(ctx, entID)
		if err != nil {
			return 0, err
		}
		if ok {
			entitlements[entID] = ent
		}
	}

	tempContribs := 0
	batch := tempDB.NewBatch()
	batchCount := 0
	commitBatch := func() error {
		if batchCount == 0 {
			return nil
		}
		if err := batch.Commit(pebble.NoSync); err != nil {
			return err
		}
		if err := batch.Close(); err != nil {
			return err
		}
		batch = tempDB.NewBatch()
		batchCount = 0
		return nil
	}
	defer batch.Close()

	for _, nodeID := range order {
		node := graph.Nodes[nodeID]
		if batchCount > 0 {
			if err := commitBatch(); err != nil {
				return 0, err
			}
			if err := tempDB.Flush(); err != nil {
				return 0, err
			}
		}
		destIDs := append([]string(nil), node.EntitlementIDs...)
		sort.Strings(destIDs)
		for _, destID := range destIDs {
			destEntitlement := entitlements[destID]
			if destEntitlement == nil {
				continue
			}
			contribs, err := readPebbleShuffleContributions(tempDB, destID)
			if err != nil {
				return 0, err
			}
			dirty, err := expander.mergeDestinationFromContributionMap(ctx, destEntitlement, contribs)
			if err != nil {
				return 0, err
			}
			if len(dirty) > 0 {
				if err := store.StoreExpandedGrants(ctx, dirty...); err != nil {
					return 0, err
				}
			}
		}

		sourceIDs := append([]string(nil), node.EntitlementIDs...)
		sort.Strings(sourceIDs)
		outgoing := outgoingEdgesSorted(graph, nodeID)
		for _, sourceID := range sourceIDs {
			sourceEntitlement := entitlements[sourceID]
			if sourceEntitlement == nil {
				continue
			}
			sourceGroups, err := expander.loadGrantGroups(ctx, sourceEntitlement)
			if err != nil {
				return 0, err
			}
			for _, outgoingEdge := range outgoing {
				destNode := graph.Nodes[outgoingEdge.destNodeID]
				outDestIDs := append([]string(nil), destNode.EntitlementIDs...)
				sort.Strings(outDestIDs)
				for _, destID := range outDestIDs {
					for key, grants := range sourceGroups {
						for _, grant := range grants {
							if !grantContributesOverEdge(grant, sourceID, outgoingEdge.edge) {
								continue
							}
							k := pebbleShuffleKey(destID, key, sourceID)
							v := []byte{0}
							if isGrantDirectOnEntitlement(grant, sourceID) {
								v[0] = 1
							}
							if err := batch.Set(k, v, nil); err != nil {
								return 0, err
							}
							batchCount++
							tempContribs++
							if batchCount >= 10_000 {
								if err := commitBatch(); err != nil {
									return 0, err
								}
							}
						}
					}
				}
			}
		}
	}
	if err := commitBatch(); err != nil {
		return 0, err
	}
	if err := tempDB.Flush(); err != nil {
		return 0, err
	}
	return tempContribs, nil
}

type shuffleKV struct {
	key []byte
	val []byte
}

func runSSTShuffleMergeWithStore(ctx context.Context, store ExpanderStore, graph *EntitlementGraph) (int, error) {
	tempDir, err := os.MkdirTemp("", "baton-expand-shuffle-sst-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(tempDir)

	tempDBDir := filepath.Join(tempDir, "db")
	tempDB, err := pebble.Open(tempDBDir, &pebble.Options{})
	if err != nil {
		return 0, err
	}
	defer tempDB.Close()

	expander := NewExpander(store, graph)
	order, err := topologicalNodeOrder(graph)
	if err != nil {
		return 0, err
	}

	entitlements := make(map[string]*v2.Entitlement, len(graph.EntitlementsToNodes))
	for _, entID := range graph.GetEntitlements() {
		ent, ok, err := expander.fetchEntitlement(ctx, entID)
		if err != nil {
			return 0, err
		}
		if ok {
			entitlements[entID] = ent
		}
	}

	kvs := make([]shuffleKV, 0)
	for _, nodeID := range order {
		node := graph.Nodes[nodeID]
		sourceIDs := append([]string(nil), node.EntitlementIDs...)
		sort.Strings(sourceIDs)
		outgoing := outgoingEdgesSorted(graph, nodeID)
		for _, sourceID := range sourceIDs {
			sourceEntitlement := entitlements[sourceID]
			if sourceEntitlement == nil {
				continue
			}
			sourceGroups, err := expander.loadGrantGroups(ctx, sourceEntitlement)
			if err != nil {
				return 0, err
			}
			for _, outgoingEdge := range outgoing {
				destNode := graph.Nodes[outgoingEdge.destNodeID]
				outDestIDs := append([]string(nil), destNode.EntitlementIDs...)
				sort.Strings(outDestIDs)
				for _, destID := range outDestIDs {
					for key, grants := range sourceGroups {
						for _, grant := range grants {
							if !grantContributesOverEdge(grant, sourceID, outgoingEdge.edge) {
								continue
							}
							val := []byte{0}
							if isGrantDirectOnEntitlement(grant, sourceID) {
								val[0] = 1
							}
							kvs = append(kvs, shuffleKV{
								key: pebbleShuffleKey(destID, key, sourceID),
								val: val,
							})
						}
					}
				}
			}
		}
	}

	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].key, kvs[j].key) < 0
	})
	sstPath := filepath.Join(tempDir, "shuffle.sst")
	if err := writeShuffleSST(sstPath, kvs); err != nil {
		return 0, err
	}
	if len(kvs) > 0 {
		if err := tempDB.Ingest(ctx, []string{sstPath}); err != nil {
			return 0, err
		}
	}

	for _, nodeID := range order {
		node := graph.Nodes[nodeID]
		destIDs := append([]string(nil), node.EntitlementIDs...)
		sort.Strings(destIDs)
		for _, destID := range destIDs {
			destEntitlement := entitlements[destID]
			if destEntitlement == nil {
				continue
			}
			contribs, err := readPebbleShuffleContributions(tempDB, destID)
			if err != nil {
				return 0, err
			}
			dirty, err := expander.mergeDestinationFromContributionMap(ctx, destEntitlement, contribs)
			if err != nil {
				return 0, err
			}
			if len(dirty) > 0 {
				if err := store.StoreExpandedGrants(ctx, dirty...); err != nil {
					return 0, err
				}
			}
		}
	}
	return len(kvs), nil
}

func writeShuffleSST(path string, kvs []shuffleKV) error {
	file, err := vfs.Default.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(file), sstable.WriterOptions{
		BlockSize: 32 << 10,
	})
	success := false
	defer func() {
		_ = w.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()
	var last []byte
	for _, kv := range kvs {
		if len(last) > 0 && bytes.Compare(kv.key, last) <= 0 {
			// Duplicate contribution keys are semantically idempotent for this
			// benchmark shape; the key includes source so duplicates here would
			// indicate duplicate grants from the source stream. Skip exact
			// duplicates to satisfy SST strict ordering.
			if bytes.Equal(kv.key, last) {
				continue
			}
			return fmt.Errorf("shuffle SST keys out of order")
		}
		if err := w.Set(kv.key, kv.val); err != nil {
			return err
		}
		last = append(last[:0], kv.key...)
	}
	if err := w.Close(); err != nil {
		return err
	}
	success = true
	return nil
}

func runSpillSSTShuffleMergeWithStore(ctx context.Context, store ExpanderStore, graph *EntitlementGraph) (int, error) {
	tempDir, err := os.MkdirTemp("", "baton-expand-shuffle-spill-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(tempDir)

	tempDBDir := filepath.Join(tempDir, "db")
	tempDB, err := pebble.Open(tempDBDir, &pebble.Options{})
	if err != nil {
		return 0, err
	}
	defer tempDB.Close()

	expander := NewExpander(store, graph)
	order, err := topologicalNodeOrder(graph)
	if err != nil {
		return 0, err
	}

	entitlements := make(map[string]*v2.Entitlement, len(graph.EntitlementsToNodes))
	for _, entID := range graph.GetEntitlements() {
		ent, ok, err := expander.fetchEntitlement(ctx, entID)
		if err != nil {
			return 0, err
		}
		if ok {
			entitlements[entID] = ent
		}
	}

	sorter := newShuffleSpillSorter(tempDir, densePebbleBenchParam("BATON_BENCH_SHUFFLE_CHUNK", 250_000))
	tempContribs := 0
	for _, nodeID := range order {
		node := graph.Nodes[nodeID]
		sourceIDs := append([]string(nil), node.EntitlementIDs...)
		sort.Strings(sourceIDs)
		outgoing := outgoingEdgesSorted(graph, nodeID)
		for _, sourceID := range sourceIDs {
			sourceEntitlement := entitlements[sourceID]
			if sourceEntitlement == nil {
				continue
			}
			sourceGroups, err := expander.loadGrantGroups(ctx, sourceEntitlement)
			if err != nil {
				return 0, err
			}
			for _, outgoingEdge := range outgoing {
				destNode := graph.Nodes[outgoingEdge.destNodeID]
				outDestIDs := append([]string(nil), destNode.EntitlementIDs...)
				sort.Strings(outDestIDs)
				for _, destID := range outDestIDs {
					for key, grants := range sourceGroups {
						for _, grant := range grants {
							if !grantContributesOverEdge(grant, sourceID, outgoingEdge.edge) {
								continue
							}
							val := []byte{0}
							if isGrantDirectOnEntitlement(grant, sourceID) {
								val[0] = 1
							}
							if err := sorter.add(pebbleShuffleKey(destID, key, sourceID), val); err != nil {
								return 0, err
							}
							tempContribs++
						}
					}
				}
			}
		}
	}

	sstPath := filepath.Join(tempDir, "shuffle-spill.sst")
	if err := sorter.finishToSST(sstPath); err != nil {
		return 0, err
	}
	if tempContribs > 0 {
		if err := tempDB.Ingest(ctx, []string{sstPath}); err != nil {
			return 0, err
		}
	}

	for _, nodeID := range order {
		node := graph.Nodes[nodeID]
		destIDs := append([]string(nil), node.EntitlementIDs...)
		sort.Strings(destIDs)
		for _, destID := range destIDs {
			destEntitlement := entitlements[destID]
			if destEntitlement == nil {
				continue
			}
			contribs, err := readPebbleShuffleContributions(tempDB, destID)
			if err != nil {
				return 0, err
			}
			dirty, err := expander.mergeDestinationFromContributionMap(ctx, destEntitlement, contribs)
			if err != nil {
				return 0, err
			}
			if len(dirty) > 0 {
				if err := store.StoreExpandedGrants(ctx, dirty...); err != nil {
					return 0, err
				}
			}
		}
	}
	return tempContribs, nil
}

type shuffleSpillSorter struct {
	dir      string
	chunkMax int
	kvs      []shuffleKV
	chunks   []string
}

func newShuffleSpillSorter(dir string, chunkMax int) *shuffleSpillSorter {
	if chunkMax <= 0 {
		chunkMax = 250_000
	}
	return &shuffleSpillSorter{
		dir:      dir,
		chunkMax: chunkMax,
		kvs:      make([]shuffleKV, 0, chunkMax),
	}
}

func (s *shuffleSpillSorter) add(key, val []byte) error {
	s.kvs = append(s.kvs, shuffleKV{
		key: append([]byte(nil), key...),
		val: append([]byte(nil), val...),
	})
	if len(s.kvs) < s.chunkMax {
		return nil
	}
	return s.flushChunk()
}

func (s *shuffleSpillSorter) flushChunk() error {
	if len(s.kvs) == 0 {
		return nil
	}
	sort.Slice(s.kvs, func(i, j int) bool {
		return bytes.Compare(s.kvs[i].key, s.kvs[j].key) < 0
	})
	path := filepath.Join(s.dir, fmt.Sprintf("shuffle-chunk-%06d.bin", len(s.chunks)))
	if err := writeShuffleChunk(path, s.kvs); err != nil {
		return err
	}
	s.chunks = append(s.chunks, path)
	s.kvs = s.kvs[:0]
	return nil
}

func (s *shuffleSpillSorter) finishToSST(sstPath string) error {
	if err := s.flushChunk(); err != nil {
		return err
	}
	return mergeShuffleChunksToSST(sstPath, s.chunks)
}

func writeShuffleChunk(path string, kvs []shuffleKV) error {
	f, err := os.Create(path) // #nosec G304 -- benchmark temp path
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()
	var lenBuf [4]byte
	var last []byte
	var pending shuffleKV
	hasPending := false
	flushPending := func() error {
		if !hasPending {
			return nil
		}
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(pending.key))) // #nosec G115 -- benchmark keys are bounded
		if _, err := w.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := w.Write(pending.key); err != nil {
			return err
		}
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(pending.val))) // #nosec G115 -- benchmark values are bounded
		if _, err := w.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := w.Write(pending.val); err != nil {
			return err
		}
		return nil
	}
	for _, kv := range kvs {
		if len(last) > 0 && bytes.Equal(kv.key, last) {
			if len(kv.val) > 0 && kv.val[0] == 1 {
				pending.val[0] = 1
			}
			continue
		}
		if err := flushPending(); err != nil {
			return err
		}
		pending = shuffleKV{
			key: append(pending.key[:0], kv.key...),
			val: append(pending.val[:0], kv.val...),
		}
		hasPending = true
		last = append(last[:0], kv.key...)
	}
	if err := flushPending(); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	success = true
	return nil
}

type shuffleChunkReader struct {
	f *os.File
	r *bufio.Reader
}

func newShuffleChunkReader(path string) (*shuffleChunkReader, error) {
	f, err := os.Open(path) // #nosec G304 -- benchmark temp path
	if err != nil {
		return nil, err
	}
	return &shuffleChunkReader{f: f, r: bufio.NewReaderSize(f, 1<<20)}, nil
}

func (r *shuffleChunkReader) close() {
	_ = r.f.Close()
}

func (r *shuffleChunkReader) next() (shuffleKV, bool, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r.r, lenBuf[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return shuffleKV{}, false, nil
		}
		return shuffleKV{}, false, err
	}
	key := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(r.r, key); err != nil {
		return shuffleKV{}, false, err
	}
	if _, err := io.ReadFull(r.r, lenBuf[:]); err != nil {
		return shuffleKV{}, false, err
	}
	val := make([]byte, binary.BigEndian.Uint32(lenBuf[:]))
	if _, err := io.ReadFull(r.r, val); err != nil {
		return shuffleKV{}, false, err
	}
	return shuffleKV{key: key, val: val}, true, nil
}

type shuffleChunkHeapItem struct {
	idx int
	kv  shuffleKV
}

type shuffleChunkHeap []shuffleChunkHeapItem

func (h shuffleChunkHeap) Len() int { return len(h) }
func (h shuffleChunkHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].kv.key, h[j].kv.key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].idx < h[j].idx
}
func (h shuffleChunkHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *shuffleChunkHeap) Push(x any)   { *h = append(*h, x.(shuffleChunkHeapItem)) }
func (h *shuffleChunkHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func mergeShuffleChunksToSST(sstPath string, chunks []string) error {
	readers := make([]*shuffleChunkReader, 0, len(chunks))
	defer func() {
		for _, r := range readers {
			r.close()
		}
	}()
	h := make(shuffleChunkHeap, 0, len(chunks))
	for i, path := range chunks {
		r, err := newShuffleChunkReader(path)
		if err != nil {
			return err
		}
		readers = append(readers, r)
		kv, ok, err := r.next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(&h, shuffleChunkHeapItem{idx: i, kv: kv})
		}
	}

	file, err := vfs.Default.Create(sstPath, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(file), sstable.WriterOptions{
		BlockSize: 32 << 10,
	})
	success := false
	defer func() {
		_ = w.Close()
		if !success {
			_ = os.Remove(sstPath)
		}
	}()
	var last []byte
	var pending shuffleKV
	hasPending := false
	flushPending := func() error {
		if !hasPending {
			return nil
		}
		if len(last) > 0 && bytes.Compare(pending.key, last) <= 0 {
			return fmt.Errorf("shuffle spill SST keys out of order")
		}
		if err := w.Set(pending.key, pending.val); err != nil {
			return err
		}
		last = append(last[:0], pending.key...)
		return nil
	}
	for h.Len() > 0 {
		item := heap.Pop(&h).(shuffleChunkHeapItem)
		if hasPending && bytes.Equal(item.kv.key, pending.key) {
			if len(item.kv.val) > 0 && item.kv.val[0] == 1 {
				pending.val[0] = 1
			}
		} else {
			if err := flushPending(); err != nil {
				return err
			}
			pending = shuffleKV{
				key: append(pending.key[:0], item.kv.key...),
				val: append(pending.val[:0], item.kv.val...),
			}
			hasPending = true
		}
		next, ok, err := readers[item.idx].next()
		if err != nil {
			return err
		}
		if ok {
			heap.Push(&h, shuffleChunkHeapItem{idx: item.idx, kv: next})
		}
	}
	if err := flushPending(); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	success = true
	return nil
}

func pebbleShuffleKey(destID string, principal topoPrincipalKey, sourceID string) []byte {
	buf := make([]byte, 0, len(destID)+len(principal.resourceType)+len(principal.resource)+len(sourceID)+4)
	buf = append(buf, destID...)
	buf = append(buf, 0)
	buf = append(buf, principal.resourceType...)
	buf = append(buf, 0)
	buf = append(buf, principal.resource...)
	buf = append(buf, 0)
	buf = append(buf, sourceID...)
	return buf
}

func pebbleShufflePrefix(destID string) []byte {
	buf := make([]byte, 0, len(destID)+1)
	buf = append(buf, destID...)
	buf = append(buf, 0)
	return buf
}

func upperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func readPebbleShuffleContributions(tempDB *pebble.DB, destID string) (map[topoPrincipalKey]*topoContribution, error) {
	prefix := pebbleShufflePrefix(destID)
	iter, err := tempDB.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	out := make(map[topoPrincipalKey]*topoContribution)
	for iter.First(); iter.Valid(); iter.Next() {
		rt, resource, sourceID, ok := decodePebbleShuffleKey(iter.Key(), prefix)
		if !ok {
			continue
		}
		key := topoPrincipalKey{resourceType: rt, resource: resource}
		contrib := out[key]
		if contrib == nil {
			contrib = &topoContribution{
				principal: v2.Resource_builder{
					Id: v2.ResourceId_builder{ResourceType: rt, Resource: resource}.Build(),
				}.Build(),
			}
			out[key] = contrib
		}
		isDirect := len(iter.Value()) > 0 && iter.Value()[0] == 1
		contrib.add(sourceID, isDirect, contrib.principal)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

func decodePebbleShuffleKey(key, prefix []byte) (string, string, string, bool) {
	if len(key) <= len(prefix) {
		return "", "", "", false
	}
	tail := key[len(prefix):]
	parts := make([][]byte, 0, 3)
	start := 0
	for i, b := range tail {
		if b != 0 {
			continue
		}
		parts = append(parts, tail[start:i])
		start = i + 1
		if len(parts) == 2 {
			break
		}
	}
	if len(parts) != 2 || start > len(tail) {
		return "", "", "", false
	}
	source := tail[start:]
	if len(source) == 0 {
		return "", "", "", false
	}
	return string(parts[0]), string(parts[1]), string(source), true
}
