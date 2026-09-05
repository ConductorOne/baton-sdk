package pebble

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
)

// ErrBulkImportOutOfOrder is returned when a key arrives at an ordered
// SST writer without sorting strictly after its predecessor. Callers can
// only trigger it through AddResourceTypes, the one add method with a
// sorted-arrival contract; resources, entitlements, and grants re-sort
// internally, and their duplicate keys surface at Finish as corrupt
// input (ErrBulkImportDuplicateKey) instead. The import cannot continue
// past it and must be Abort()ed.
var ErrBulkImportOutOfOrder = errors.New("bulk sync import: keys are not strictly increasing")

// ErrBulkImportDuplicateKey is returned when a spill merge finds two
// records with the same key — from BulkSyncImport.Finish, and from the
// engine's other spill merges (the deferred index and digest builds at
// EndSync, the synth-grant layer, the open-time id-index migration).
// Duplicates are impossible for well-formed sources (every producer
// requires global uniqueness), so hitting this means corrupt input,
// not a caller ordering bug.
var ErrBulkImportDuplicateKey = errors.New("bulk sync import: duplicate key in spill merge")

// bulkSpillKeyChunkBytes sizes the shared spill arena pool
// (getSpillArena) and the deferred build's translate-batch arenas. The
// engine's single-producer spill sorters cut chunks at
// deferredIndexSpillChunkBytes with explicit freelists (see that
// constant for the sizing trade-off); the bulk import derives its chunk
// size from bulkImportSortBudgetBytes instead, because its sorter count
// scales with the caller's scan fan-out.
const bulkSpillKeyChunkBytes = 8 << 20

// bulkImportSortBudgetBytes bounds the anonymous memory the bulk import's
// spill arenas can hold at once. Chunk size is derived from it, not
// hard-coded: chunkBytes = budget / liveArenas, where liveArenas counts
// every arena that can be committed simultaneously — one per sorter
// (resources, entitlements, the parent index, and per grant shard one
// primary plus one per index family) plus one per background sort slot.
// Adding an index family or raising the shard count therefore shrinks
// the chunks instead of silently raising peak RSS.
//
// Chunk size sets Finish's merge fan-in: every chunk stays open behind a
// bulkSpillBufferSize reader for the whole merge, so the derived value is
// clamped to [bulkImportMinChunkBytes, deferredIndexSpillChunkBytes] —
// the floor caps the run count at half what 8MiB chunks produce (a
// whale-sized ~17GB grant family is ~1,100 runs at the floor versus
// ~2,200 at 8MiB; grants.go records the synth-layer incident), and the
// ceiling is the size the single-producer builds already validated.
//
// Worked examples with three grant index families and 4 sort slots: the
// sanitizer (1 shard, 11 arenas) derives ~93MiB; the converter at 4 lanes
// (23 arenas) ~44MiB; at 8 lanes (39 arenas) ~26MiB. The freelist
// recycles arenas within the same budget, so idle arenas never add to it.
const bulkImportSortBudgetBytes = 1 << 30

// bulkImportMinChunkBytes is the floor on the derived bulk-import chunk
// size; see bulkImportSortBudgetBytes.
const bulkImportMinChunkBytes = 16 << 20

// bulkSpillBufferSize is the bufio size for spill-chunk IO.
const bulkSpillBufferSize = 1 << 20

// grantIndexFamilies are the index-discriminator bytes AddGrants can
// emit (see grantIndexKeys). The by_entitlement_principal_hash index
// is deliberately absent: it and the grant digests are derived from
// the primaries by the fused deferred pass at seal time
// (BuildDeferredGrantIndexes), never written inline.
var grantIndexFamilies = []byte{
	idxGrantByPrincipal,
	idxGrantByNeedsExpansion,
	idxGrantBySourceScope,
}

// bulkSSTWriter builds one SST file for a single disjoint key bucket.
// Keys must arrive in strictly increasing order (enforced). Mirrors the
// compactor's sstBuilder: the table format is pinned to SDKPebbleFormat
// so the SST is ingestible without a format upgrade, and 32 KiB blocks
// trade a little space for less per-block framing CPU on write-once
// sequentially-read tables.
type bulkSSTWriter struct {
	name  string
	path  string
	w     *sstable.Writer
	last  []byte
	count int
}

// newBulkSSTWriter creates an SST staging file on fs. fs must be the
// engine FS (Engine.fs()): the finished SST is handed to the pebble.DB's
// Ingest/IngestAndExcise, which resolves the path through the DB's own
// filesystem — creating it anywhere else breaks WithVFS engines.
func newBulkSSTWriter(fs vfs.FS, dir, name string) (*bulkSSTWriter, error) {
	path := filepath.Join(dir, name+".sst")
	file, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, fmt.Errorf("bulk sync import: create %q: %w", path, err)
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(file), sstable.WriterOptions{
		TableFormat: SDKPebbleFormat.MaxTableFormat(),
		BlockSize:   32 << 10,
	})
	return &bulkSSTWriter{name: name, path: path, w: w}, nil
}

func (w *bulkSSTWriter) add(key, val []byte) error {
	if len(w.last) > 0 && bytes.Compare(key, w.last) <= 0 {
		return fmt.Errorf("%w: bucket %s", ErrBulkImportOutOfOrder, w.name)
	}
	if err := w.w.Set(key, val); err != nil {
		return fmt.Errorf("bulk sync import: set %s: %w", w.name, err)
	}
	w.last = append(w.last[:0], key...)
	w.count++
	return nil
}

// finish closes the sstable writer (flushing the table footer). Safe to
// call more than once. The underlying vfs.File is closed by the writer.
func (w *bulkSSTWriter) finish() error {
	if w.w == nil {
		return nil
	}
	err := w.w.Close()
	w.w = nil
	if err != nil {
		return fmt.Errorf("bulk sync import: close %s: %w", w.name, err)
	}
	return nil
}

// BulkSyncImport is the trusted one-shot import fast path: instead of
// committing records through the memtable/WAL, it builds sorted SSTs on
// disk and ingests them all in a single pebble Ingest at Finish. Each key
// is written once into its final table — no WAL append, no L0 flush, no
// background compaction debt.
//
// Resource types stream straight into one SST and must arrive in strictly
// increasing encoded-key order (SQLite BINARY collation order on the key's
// tuple columns — the tuple key codec is order-preserving; violations fail
// with ErrBulkImportOutOfOrder). AddResourceTypes is single-threaded.
//
// Every other bucket is re-sorted on the way in and imposes no ordering
// requirement on its caller. Resources are keyed (resource_type_id,
// resource_id); entitlements and grants are keyed by structural identity,
// whose tuple order does NOT match a converter's external-id scan order.
// All three go through spill sorters (resources and entitlements
// single-threaded on the parent; grants scale across goroutines, each
// scanning goroutine taking its own shard via NewGrantShard so the grant
// hot path acquires no shared lock at all). The secondary index families
// are derived internally from the translated records — the same nil-guards
// and key shapes as the engine's canonical writeXxxIndexes paths — and are
// key-only spill-sorted. Spill chunks sort and flush to disk in the
// background while the scans keep streaming; Finish k-way merges each
// family's sorted runs (across all shards) into one SST per family, in
// parallel, and ingests everything in a single call.
//
// SQLite's UNIQUE(external_id, sync_id) guarantees each row appears once.
// Grant rows with distinct external ids but identical refs fold to one
// structural identity; Finish field-merges such duplicates and warns.
// Entitlement identities embed the raw external id and cannot fold. The
// destination sync must be freshly started (MarkFreshSync via
// StartNewSync) and empty; nothing else may write to the engine between
// Start and Finish. Used by C1File.ToPebble for sqlite→pebble conversion.
type BulkSyncImport struct {
	e      *Engine
	syncID string
	dir    string
	done   bool

	resourceTypes *bulkSSTWriter

	// resources and entitlements go through spill sorters, not ordered SST
	// writers, so neither imposes an ordering precondition on its caller.
	// For entitlements the mismatch is unconditional: the structural key
	// never sorts the way an external-id scan does (tuple separators sort
	// below printable bytes, and the flag component reorders stripped vs
	// opaque ids), so the stream must be re-sorted before it can become an
	// SST.
	//
	// For resources, keyed (resource_type_id, resource_id), it depends on
	// the producer. A converter scanning SQLite with ORDER BY on that tuple
	// already arrives sorted and paid nothing for an ordered writer, but a
	// producer that rewrites resource ids — the c1z sanitizer HMACs them —
	// emits an order unrelated to the destination key. Sorting here keeps
	// every producer on one path rather than making sortedness a
	// precondition each one has to re-establish, and it holds when a single
	// resource type is too large to sort in memory.
	//
	// Spill-sorting stages each family transiently at ~2x (sorted runs
	// plus the SST merged from them; the runs' ranges overlap uniformly,
	// so release-on-exhaustion frees little before the merge tail). The
	// ordered writer resources used before staged ~1x but pushed
	// sortedness onto every producer.
	resources    *spillSorter
	entitlements *spillSorter

	// sortSem bounds concurrently running background chunk sorts
	// across all sorters and shards.
	sortSem chan struct{}

	// Every sorter in the import — resources, entitlements, the parent
	// index, and each shard's grant and index sorters — cuts chunks at
	// chunkBytes and recycles arenas through this import-wide freelist
	// (see newSorter). chunkBytes is derived from
	// bulkImportSortBudgetBytes and the caller's expected shard count so
	// the arenas' aggregate stays under the budget: sorts in flight are
	// capped by sortSem, and idle arenas are capped by the freelist, so
	// the live set is exactly the arena count the derivation counted.
	// (A fresh arena's pages also commit only as it fills, so small
	// imports stay well under the budget; recycled arenas are fully
	// committed, which is why the budget counts them all.)
	chunkBytes int
	arenaFree  *spillArenaFreeList

	// Parent-level sorter for the (single-threaded) resource index keys.
	idxResourceByParent *spillSorter

	// mu guards shard registration/aggregation. The grant hot path
	// never takes it.
	mu       sync.Mutex
	shards   []*BulkGrantShard
	shardSeq int

	resourcesByRT    map[string]int64
	entitlementsByRT map[string]int64

	// Duplicate-fold accounting, written by Finish's grants merge goroutine
	// and read by ComputedStats afterwards. Legacy files can hold grant rows
	// with distinct external ids but identical refs — one structural
	// identity; those fold to a single row at merge time, and the streaming
	// per-RT counters above must be corrected to match. Entitlements cannot
	// fold: their identity embeds the raw external id, which SQLite's
	// UNIQUE(external_id, sync_id) makes unique.
	grantDupRowsMerged  int64
	grantDupRowsByEntRT map[string]int64

	// Ref-less legacy rows cannot be represented in the structured keyspace
	// at all; they are dropped with a warning, matching the in-place
	// id-index migration's policy (a single malformed row in a legacy
	// SQLite file must not fail the whole conversion). Written by
	// AddEntitlements (single-threaded) and by shards under their own
	// accounting, aggregated at Finish.
	entitlementsSkippedMissingRefs atomic.Int64
	grantsSkippedMissingRefs       atomic.Int64
}

// StartBulkSyncImport opens a bulk import targeting syncID, which must be
// the engine's current FRESH sync (see BulkSyncImport contract). Working
// files are staged in a fresh directory under tmpDir ("" = system temp
// dir) and removed by Finish/Abort.
//
// expectedShards is the number of grant shards the caller intends to open
// via NewGrantShard (values < 1 are treated as 1). It is a sizing hint,
// not a limit: the import derives its spill chunk size from it so the
// arenas' aggregate stays within bulkImportSortBudgetBytes. Opening more
// shards than declared still works but raises peak memory proportionally.
func (e *Engine) StartBulkSyncImport(ctx context.Context, syncID string, tmpDir string, expectedShards int) (*BulkSyncImport, error) {
	if !e.IsFreshSync() {
		return nil, errors.New("StartBulkSyncImport: sync is not fresh")
	}
	idBytes, err := codec.EncodeSyncID(syncID)
	if err != nil {
		return nil, fmt.Errorf("StartBulkSyncImport: %w", err)
	}
	if cur := e.currentSyncBytes(); !bytes.Equal(cur, idBytes) {
		return nil, fmt.Errorf("StartBulkSyncImport: sync %s is not the engine's current sync", syncID)
	}
	dir, err := e.prepareStagingDir(tmpDir, "pebble-bulk-import-")
	if err != nil {
		return nil, fmt.Errorf("StartBulkSyncImport: mkdir temp: %w", err)
	}
	// Background chunk-sort fan-out: enough to keep sorts off the scan
	// goroutines' critical path, with an absolute cap so a large shared
	// host doesn't see a conversion grab dozens of cores (each slot also
	// pins one chunk arena, so this bounds sort memory too).
	sorters := min(4, max(2, runtime.GOMAXPROCS(0)/2))
	b := &BulkSyncImport{
		e:                   e,
		syncID:              syncID,
		dir:                 dir,
		sortSem:             make(chan struct{}, sorters),
		chunkBytes:          bulkImportChunkBytes(expectedShards, sorters),
		resourcesByRT:       map[string]int64{},
		entitlementsByRT:    map[string]int64{},
		grantDupRowsByEntRT: map[string]int64{},
	}
	b.arenaFree = newSpillArenaFreeList(b.chunkBytes, sorters+2)
	sw, err := newBulkSSTWriter(e.fs(), dir, "resource-types")
	if err != nil {
		b.Abort()
		return nil, err
	}
	b.resourceTypes = sw
	b.resources = b.newSorter("resources")
	b.entitlements = b.newSorter("entitlements")
	b.idxResourceByParent = b.newSorter(fmt.Sprintf("index-%02x-p", idxResourceByParent))
	return b, nil
}

// bulkImportChunkBytes derives the spill chunk size that keeps the
// import's simultaneously-live arenas within bulkImportSortBudgetBytes.
// The live set is one arena per sorter — the three lane-independent
// sorters plus, per grant shard, one primary and one per index family —
// plus one per background sort slot (a sorter that just cut a chunk
// fills a fresh arena while the cut one sorts). See
// bulkImportSortBudgetBytes for the clamp rationale.
func bulkImportChunkBytes(expectedShards, sortSlots int) int {
	expectedShards = max(1, expectedShards)
	liveArenas := 3 + (1+len(grantIndexFamilies))*expectedShards + sortSlots
	return min(deferredIndexSpillChunkBytes, max(bulkImportMinChunkBytes, bulkImportSortBudgetBytes/liveArenas))
}

// newSorter creates a spill sorter wired to the import's shared sort
// semaphore and arena freelist (see arenaFree for the sizing rationale).
func (b *BulkSyncImport) newSorter(name string) *spillSorter {
	s := newSpillSorter(b.dir, name, b.sortSem, b.chunkBytes)
	s.free = b.arenaFree
	return s
}

// BulkGrantShard is one goroutine's private view of the grant import:
// a primary-record spill sorter and per-family index sorters. Create
// one per scanning goroutine via NewGrantShard; AddGrants on a shard
// takes no shared locks. Close the shard when its scan completes.
//
// Grant primary keys are structural identities, which do not sort in the
// converter's external-id scan order, so both the primary rows and the
// index keys go through spill sorters; Finish k-way merges the runs of
// all shards per family. Arrival order therefore does not matter for
// correctness. Distinct legacy external ids that fold to one structural
// identity are merged at Finish with a warning.
type BulkGrantShard struct {
	b      *BulkSyncImport
	grants *spillSorter
	idx    map[byte]*spillSorter
	entRT  map[string]int64
	closed bool
	err    error
	// valBuf is the reusable marshal scratch for primary record values.
	// Safe because both the sstable writer and the spill sorters copy
	// bytes out before add() returns.
	valBuf []byte
}

// NewGrantShard registers a new grant import shard. Safe to call
// concurrently.
func (b *BulkSyncImport) NewGrantShard() (*BulkGrantShard, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.shardSeq
	b.shardSeq++
	s := &BulkGrantShard{
		b:      b,
		grants: b.newSorter(fmt.Sprintf("grants-s%03d", id)),
		idx:    map[byte]*spillSorter{},
		entRT:  map[string]int64{},
	}
	for _, idx := range grantIndexFamilies {
		s.idx[idx] = b.newSorter(fmt.Sprintf("index-%02x-s%03d", idx, id))
	}
	b.shards = append(b.shards, s)
	return s, nil
}

// AddGrants translates and appends grants to this shard, in ascending
// external-id order (see BulkGrantShard). Translation, the v3 marshal,
// and key encoding run in the caller's goroutine; all writers are
// shard-private, so concurrent shards never contend.
func (s *BulkGrantShard) AddGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.AddGrantsWithDiscoveredAt(ctx, grants, nil)
}

// AddGrantsWithDiscoveredAt is AddGrants with an optional per-record
// discovered_at, aligned by index with grants (nil slice or nil entries
// fall back to now). Conversions use it to preserve the source record's
// original discovery time: compaction merges pick winners by newest
// discovered_at, so re-stamping conversion wall-clock time would make
// converted records override genuinely newer data.
func (s *BulkGrantShard) AddGrantsWithDiscoveredAt(ctx context.Context, grants []*v2.Grant, discoveredAt []*timestamppb.Timestamp) error {
	if s.closed {
		return errors.New("bulk sync import: AddGrants on closed shard")
	}
	// Serial translate: the caller's lanes are the parallelism; the
	// fan-out inside translateGrants would only oversubscribe shared
	// hosts (lanes × translate shards goroutine bursts).
	records := translateGrantsSerial(s.b.syncID, grants, discoveredAt, timestamppb.Now())
	for _, r := range records {
		if r == nil {
			continue
		}
		val, err := marshalRecordAppend(s.valBuf[:0], r)
		if err != nil {
			return s.fail(err)
		}
		s.valBuf = val
		id, err := grantIdentityFromRecord(r)
		if err != nil {
			// Missing entitlement/principal refs: unrepresentable in the
			// structured keyspace. Drop with a warning (counted, logged at
			// Finish) — the in-place migration applies the same policy.
			s.b.grantsSkippedMissingRefs.Add(1)
			continue
		}
		if err := s.grants.add(encodeGrantIdentityKey(id), val); err != nil {
			return s.fail(err)
		}
		s.entRT[r.GetEntitlement().GetResourceTypeId()]++
		for _, k := range grantIndexKeys(r) {
			if len(k) < 3 || k[1] != typeIndex {
				return s.fail(fmt.Errorf("bulk sync import: malformed index key %x", k))
			}
			w := s.idx[k[2]]
			if w == nil {
				return s.fail(fmt.Errorf("bulk sync import: unknown index family %#02x", k[2]))
			}
			if err := w.add(k, nil); err != nil {
				return s.fail(err)
			}
		}
	}
	return nil
}

func (s *BulkGrantShard) fail(err error) error {
	if s.err == nil {
		s.err = err
	}
	return err
}

// Close finalizes the shard's primary SST and flushes its index tail
// chunks into the background sorters. Must be called once per shard
// before Finish; AddGrants is invalid afterwards.
func (s *BulkGrantShard) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.grants.cutAndDispatch()
	for _, w := range s.idx {
		w.cutAndDispatch()
	}
}

// discoveredAtOrNow returns the i-th per-record discovered_at when the
// caller supplied one, and now otherwise. The slice is aligned by index
// with the caller's input records; a short or nil slice falls back to
// now.
func discoveredAtOrNow(discoveredAt []*timestamppb.Timestamp, i int, now *timestamppb.Timestamp) *timestamppb.Timestamp {
	if i < len(discoveredAt) && discoveredAt[i] != nil {
		return discoveredAt[i]
	}
	return now
}

// AddResourceTypes translates and appends resource types, which must
// arrive sorted by external id.
func (b *BulkSyncImport) AddResourceTypes(ctx context.Context, rts ...*v2.ResourceType) error {
	return b.AddResourceTypesWithDiscoveredAt(ctx, rts, nil)
}

// AddResourceTypesWithDiscoveredAt is AddResourceTypes with an optional
// per-record discovered_at aligned by index with rts; nil entries fall
// back to now. See AddGrantsWithDiscoveredAt for why conversions must
// preserve source discovery times.
func (b *BulkSyncImport) AddResourceTypesWithDiscoveredAt(ctx context.Context, rts []*v2.ResourceType, discoveredAt []*timestamppb.Timestamp) error {
	now := timestamppb.Now()
	for i, rt := range rts {
		if rt == nil {
			continue
		}
		rec := V2ResourceTypeToV3(b.syncID, rt)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(discoveredAtOrNow(discoveredAt, i, now))
		}
		val, err := marshalRecord(rec)
		if err != nil {
			return err
		}
		if err := b.resourceTypes.add(encodeResourceTypeKey(rec.GetExternalId()), val); err != nil {
			return err
		}
	}
	return nil
}

// AddResources translates and appends resources. Rows are keyed by
// (resource_type_id, resource_id) and re-sorted through a spill sorter, so
// arrival order does not matter; a duplicate key is corrupt input and
// surfaces at Finish from the spill merge rather than here. by_parent
// index keys are derived and spilled with the same parent guard as
// writeResourceIndexes.
func (b *BulkSyncImport) AddResources(ctx context.Context, resources ...*v2.Resource) error {
	return b.AddResourcesWithDiscoveredAt(ctx, resources, nil)
}

// AddResourcesWithDiscoveredAt is AddResources with an optional
// per-record discovered_at aligned by index with resources; nil entries
// fall back to now. See AddGrantsWithDiscoveredAt for why conversions
// must preserve source discovery times.
func (b *BulkSyncImport) AddResourcesWithDiscoveredAt(ctx context.Context, resources []*v2.Resource, discoveredAt []*timestamppb.Timestamp) error {
	now := timestamppb.Now()
	for i, r := range resources {
		if r == nil {
			continue
		}
		rec := V2ResourceToV3(b.syncID, r)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(discoveredAtOrNow(discoveredAt, i, now))
		}
		val, err := marshalRecord(rec)
		if err != nil {
			return err
		}
		if err := b.resources.add(encodeResourceKey(rec.GetResourceTypeId(), rec.GetResourceId()), val); err != nil {
			return err
		}
		b.resourcesByRT[rec.GetResourceTypeId()]++
		if parent := rec.GetParent(); parent != nil && parent.GetResourceId() != "" {
			k := rawdb.EncodeResourceByParentIndexKey(
				parent.GetResourceTypeId(), parent.GetResourceId(),
				rec.GetResourceTypeId(), rec.GetResourceId(),
			)
			if err := b.idxResourceByParent.add(k, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// AddEntitlements translates and appends entitlements. Rows are keyed by
// structural identity and re-sorted through a spill sorter, so arrival
// order does not matter (the converter's external-id scan order does NOT
// match identity-tuple order). Identity embeds the raw external id, so
// duplicates are impossible for well-formed sources and fail the merge.
func (b *BulkSyncImport) AddEntitlements(ctx context.Context, ents ...*v2.Entitlement) error {
	return b.AddEntitlementsWithDiscoveredAt(ctx, ents, nil)
}

// AddEntitlementsWithDiscoveredAt is AddEntitlements with an optional
// per-record discovered_at aligned by index with ents; nil entries fall
// back to now. See AddGrantsWithDiscoveredAt for why conversions must
// preserve source discovery times.
func (b *BulkSyncImport) AddEntitlementsWithDiscoveredAt(ctx context.Context, ents []*v2.Entitlement, discoveredAt []*timestamppb.Timestamp) error {
	now := timestamppb.Now()
	for i, e := range ents {
		if e == nil {
			continue
		}
		rec := V2EntitlementToV3(b.syncID, e)
		if rec == nil {
			continue
		}
		if rec.GetDiscoveredAt() == nil {
			rec.SetDiscoveredAt(discoveredAtOrNow(discoveredAt, i, now))
		}
		val, err := marshalRecord(rec)
		if err != nil {
			return err
		}
		id, err := entitlementIdentityFromRecord(rec)
		if err != nil {
			// Missing resource ref: unrepresentable in the structured
			// keyspace. Drop with a warning (counted, logged at Finish) —
			// the in-place migration applies the same policy.
			b.entitlementsSkippedMissingRefs.Add(1)
			continue
		}
		if err := b.entitlements.add(encodeEntitlementIdentityKey(id), val); err != nil {
			return err
		}
		b.entitlementsByRT[rec.GetResource().GetResourceTypeId()]++
	}
	return nil
}

// ComputedStats returns a SyncStatsRecord built from the counts the
// import accumulated while streaming (primary record counts, resources
// by resource type, entitlements by resource type, grants by entitlement
// resource type). Valid after all grant shards are Closed; call it after
// Finish so the counts reflect any duplicate-identity grant rows that
// folded at merge time. Assets do not flow through the importer — the
// caller sets that count itself before stashing the record via
// Engine.StashComputedSyncStats.
func (b *BulkSyncImport) ComputedStats() *v3.SyncStatsRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	var grants int64
	grantsByEntRT := map[string]int64{}
	for _, s := range b.shards {
		grants += s.grants.count
		for rt, n := range s.entRT {
			grantsByEntRT[rt] += n
		}
	}
	grants -= b.grantDupRowsMerged
	for rt, n := range b.grantDupRowsByEntRT {
		grantsByEntRT[rt] -= n
	}
	rec := &v3.SyncStatsRecord{
		SyncId:                          b.syncID,
		ResourceTypes:                   int64(b.resourceTypes.count),
		Resources:                       b.resources.count,
		Entitlements:                    b.entitlements.count,
		Grants:                          grants,
		ResourcesByResourceType:         b.resourcesByRT,
		EntitlementsByResourceType:      b.entitlementsByRT,
		GrantsByEntitlementResourceType: grantsByEntRT,
	}
	return rec
}

// Finish finalizes the import: every spill sorter drains its background
// sorts, each index family k-way merges its sorted runs (combined
// across all shards) into one SST in parallel, the ordered primary SSTs
// (including each shard's grant SST) are collected, and everything is
// ingested in one pebble Ingest call (the key ranges are pairwise
// disjoint, so a single call is legal and costs one manifest rotation).
// The staging directory is removed on the way out regardless of outcome.
func (b *BulkSyncImport) Finish(ctx context.Context) error {
	if b.done {
		return errors.New("bulk sync import: already finished or aborted")
	}
	b.done = true
	// Full teardown, not a bare RemoveAll: Finish's error paths can return
	// with background chunk sorts still writing into the staging dir
	// (nothing finalized the sorters yet), and removing the dir while a
	// sort races its os.Create can strand the dir on disk. Abort is a
	// no-op once done is set, so this defer must do the waiting itself.
	// On success everything is already finished/finalized and teardown
	// reduces to the RemoveAll.
	defer b.teardown()
	start := time.Now()

	paths := make([]string, 0, 4+len(grantIndexFamilies))
	if err := b.resourceTypes.finish(); err != nil {
		return err
	}
	if b.resourceTypes.count > 0 {
		paths = append(paths, b.resourceTypes.path)
	}

	b.mu.Lock()
	shards := b.shards
	b.mu.Unlock()
	for _, s := range shards {
		if !s.closed {
			return errors.New("bulk sync import: Finish called with an unclosed grant shard")
		}
		if s.err != nil {
			return s.err
		}
	}

	// Build the merge units: each combines the sorted runs of one key
	// family across every sorter that produced them. Units with a resolve
	// function tolerate duplicate keys by merging their values (legacy
	// files can hold grant rows with distinct external ids but identical
	// refs); units without one treat duplicates as corruption.
	type mergeUnit struct {
		name    string
		sorters []*spillSorter
		resolve func(key []byte, values [][]byte) ([]byte, error)
	}
	units := []mergeUnit{
		{name: "grants", resolve: b.resolveDuplicateGrants},
		{name: "resources", sorters: []*spillSorter{b.resources}},
		{name: "entitlements", sorters: []*spillSorter{b.entitlements}},
		{name: fmt.Sprintf("index-%02x", idxResourceByParent), sorters: []*spillSorter{b.idxResourceByParent}},
	}
	for _, s := range shards {
		units[0].sorters = append(units[0].sorters, s.grants)
	}
	for _, idx := range grantIndexFamilies {
		u := mergeUnit{
			name: fmt.Sprintf("index-%02x", idx),
			// Grant index keys are emitted per input row, so folded
			// duplicate grants emit byte-identical (value-less) index keys;
			// keep the first and drop the rest.
			resolve: func(_ []byte, values [][]byte) ([]byte, error) { return values[0], nil },
		}
		for _, s := range shards {
			u.sorters = append(u.sorters, s.idx[idx])
		}
		units = append(units, u)
	}

	results := make([]struct {
		path string
		err  error
	}, len(units))
	var wg sync.WaitGroup
	for i, u := range units {
		wg.Add(1)
		go func(slot int, u mergeUnit) {
			defer wg.Done()
			var chunks []string
			for _, s := range u.sorters {
				c, err := s.finalize()
				if err != nil {
					results[slot].err = err
					return
				}
				chunks = append(chunks, c...)
			}
			if len(chunks) == 0 {
				return
			}
			sstPath := filepath.Join(b.dir, u.name+".sst")
			var err error
			if u.resolve != nil {
				_, err = mergeSpillChunksToSSTResolvingDuplicates(ctx, b.e.fs(), sstPath, u.name, chunks, u.resolve)
			} else {
				err = mergeSortedSpillChunksToSST(ctx, b.e.fs(), sstPath, u.name, chunks)
			}
			if err != nil {
				results[slot].err = err
				return
			}
			results[slot].path = sstPath
		}(i, u)
	}
	wg.Wait()
	for _, r := range results {
		if r.err != nil {
			return r.err
		}
		if r.path != "" {
			paths = append(paths, r.path)
		}
	}
	if b.grantDupRowsMerged > 0 {
		ctxzap.Extract(ctx).Warn("bulk sync import: merged legacy grant rows that share one structural identity",
			zap.Int64("rows_merged_away", b.grantDupRowsMerged),
		)
	}
	if n := b.entitlementsSkippedMissingRefs.Load(); n > 0 {
		ctxzap.Extract(ctx).Warn("bulk sync import: dropped legacy entitlements with no resource ref; they cannot be keyed in the structured layout",
			zap.Int64("dropped", n),
		)
	}
	if n := b.grantsSkippedMissingRefs.Load(); n > 0 {
		ctxzap.Extract(ctx).Warn("bulk sync import: dropped legacy grants with missing entitlement/principal refs; they cannot be keyed in the structured layout",
			zap.Int64("dropped", n),
		)
	}
	mergesDone := time.Now()

	if len(paths) == 0 {
		return nil
	}
	err := b.e.withWrite(func() error {
		if err := b.e.db.IngestSSTs(ctx, paths); err != nil {
			return fmt.Errorf("bulk sync import: ingest: %w", err)
		}
		b.e.noteEntitlementKeyspaceWrite()
		// The data keyspaces are no longer provably empty: subsequent
		// Put*Records calls in this sync must take their read-before-write
		// paths so overwrites of imported identities clean up index entries.
		_ = b.e.takeFreshGrantsEmpty()
		_ = b.e.takeFreshEntitlementsEmpty()
		_ = b.e.takeFreshResourcesEmpty()
		// Same contract for the source-scope gate: grantIndexKeys emits
		// by_source_scope entries for stamped records (the v2 translators
		// never stamp today, but the writer is wired for it), and ingest
		// bypasses the typed ops that would arm the gate. Re-derive it
		// from the actual family state — exact under the write barrier —
		// so a scoped import can never leave the unsound
		// false-with-entries state behind.
		if err := b.e.db.ProbeSourceScopeMayExist(); err != nil {
			return fmt.Errorf("bulk sync import: probe source-scope state: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	ctxzap.Extract(ctx).Debug("bulk sync import: finish timings",
		zap.Duration("spill_merge", mergesDone.Sub(start)),
		zap.Duration("ingest", time.Since(mergesDone)),
		zap.Int("ssts", len(paths)),
	)
	return nil
}

// resolveDuplicateGrants merges the values of grant rows whose distinct
// legacy external ids fold to one structural identity, using the same
// policy as the in-place id-index migration, and records the fold so
// ComputedStats can correct its streaming per-row counters.
func (b *BulkSyncImport) resolveDuplicateGrants(key []byte, values [][]byte) ([]byte, error) {
	merged, err := mergeDuplicateGrantValues(values)
	if err != nil {
		return nil, fmt.Errorf("bulk sync import: merge duplicate grant key %x: %w", key, err)
	}
	var rec v3.GrantRecord
	if err := unmarshalRecord(merged, &rec); err != nil {
		return nil, err
	}
	b.grantDupRowsMerged += int64(len(values) - 1)
	b.grantDupRowsByEntRT[rec.GetEntitlement().GetResourceTypeId()] += int64(len(values) - 1)
	return merged, nil
}

// Abort closes and removes all staged files without ingesting. Safe to
// call after Finish (no-op) or on a partially-failed import.
func (b *BulkSyncImport) Abort() {
	if b.done {
		return
	}
	b.done = true
	b.teardown()
}

// teardown closes the ordered SST writer, waits out every spill
// sorter's in-flight background chunk sorts, and then removes the
// staging directory. The waits must precede the RemoveAll: a chunk
// sort racing the removal can re-create a file mid-walk and strand the
// directory. Idempotent against an already-finished writer and
// already-finalized sorters, so Finish can run it unconditionally.
func (b *BulkSyncImport) teardown() {
	if b.resourceTypes != nil {
		_ = b.resourceTypes.finish()
	}
	b.mu.Lock()
	shards := b.shards
	b.mu.Unlock()
	for _, s := range shards {
		s.closed = true
		s.grants.abort()
		for _, w := range s.idx {
			w.abort()
		}
	}
	for _, w := range []*spillSorter{b.resources, b.entitlements, b.idxResourceByParent} {
		if w != nil {
			w.abort()
		}
	}
	b.e.removeStagingDir(b.dir)
}

// --- spill sorter: unordered (key[,value]) → background chunk sort →
//     k-way merge into one SST ---
//
// add() appends entries to an in-memory arena; when the arena reaches
// its chunk threshold it is handed to a background goroutine (bounded
// by the import-wide sortSem) that sorts it by key and writes a sorted
// run file, overlapping sort+IO with the caller's scan. finalize()
// flushes the tail and waits; the caller then merges the runs.

// kvView is an (offset, end, end) view into a sort arena: the key at
// [keyOff:keyEnd] is immediately followed by its value at [keyEnd:valEnd].
// Views over one contiguous buffer avoid a heap copy per entry and give
// the GC a pointer-free chunk to skip.
type kvView struct {
	keyOff int
	keyEnd int
	valEnd int
}

type spillSorter struct {
	name       string
	dir        string
	sem        chan struct{}
	chunkBytes int
	// free, when set, recycles chunk arenas through an explicit bounded
	// freelist instead of the sync.Pool. Long single-producer builds (the
	// synth-grant layer, the deferred index) allocate tens of GB per run
	// through the pool because GC cycles clear it faster than arenas
	// return; a plain channel survives GC and caps live arenas at the
	// sort concurrency anyway (cutAndDispatch blocks on the sem).
	free *spillArenaFreeList

	arena []byte
	views []kvView
	count int64

	wg       sync.WaitGroup
	chunkMu  sync.Mutex
	chunks   []string
	firstErr error
}

func newSpillSorter(dir, name string, sem chan struct{}, chunkBytes int) *spillSorter {
	return &spillSorter{name: name, dir: dir, sem: sem, chunkBytes: chunkBytes}
}

// spillArenaFreeList is a bounded, GC-proof recycler for chunk arenas of one
// size, shared by the sorters of one build (segments of a layer session, the
// deferred index sorter). Capacity should cover the maximum arenas live at
// once: one being filled by the producer plus one per concurrent sort slot.
type spillArenaFreeList struct {
	size int
	ch   chan []byte
}

func newSpillArenaFreeList(size, slots int) *spillArenaFreeList {
	return &spillArenaFreeList{size: size, ch: make(chan []byte, slots)}
}

func (f *spillArenaFreeList) get() []byte {
	select {
	case b := <-f.ch:
		return b[:0]
	default:
		return make([]byte, 0, f.size)
	}
}

func (f *spillArenaFreeList) put(b []byte) {
	if cap(b) < f.size {
		return
	}
	select {
	case f.ch <- b[:0]:
	default:
	}
}

// spillArenaPool / spillViewsPool recycle the per-chunk key/value arena and its
// kvView slice across chunks. Without recycling, every cut allocates a fresh
// arena that grows from zero by append-doubling and is then GC'd; deferred index
// builds can otherwise churn hundreds of GB in short-lived buffers.
var spillArenaPool = sync.Pool{New: func() any { b := make([]byte, 0, bulkSpillKeyChunkBytes); return &b }}
var spillViewsPool = sync.Pool{New: func() any { v := make([]kvView, 0, 1<<16); return &v }}

func getSpillArena() []byte {
	bp := spillArenaPool.Get().(*[]byte)
	return (*bp)[:0]
}

func putSpillArena(b []byte) {
	if cap(b) < bulkSpillKeyChunkBytes {
		return
	}
	b = b[:0]
	spillArenaPool.Put(&b)
}

func getSpillViews() []kvView {
	vp := spillViewsPool.Get().(*[]kvView)
	return (*vp)[:0]
}

func putSpillViews(v []kvView) {
	v = v[:0]
	spillViewsPool.Put(&v)
}

// add appends one entry. val may be nil (key-only spills). NOT
// goroutine-safe — each sorter has exactly one producer. May block on
// the sort semaphore when the arena fills (backpressure on this
// producer only).
func (s *spillSorter) add(key, val []byte) error {
	if len(key) > math.MaxUint32 || len(val) > math.MaxUint32 {
		return fmt.Errorf("bulk sync import: spill entry too large (%d/%d bytes)", len(key), len(val))
	}
	if err := s.takeErr(); err != nil {
		return err
	}
	// Cut BEFORE an entry would overflow the arena's capacity, not after.
	// Appending past cap makes Go reallocate the arena at ~1.25x with a
	// full copy — a 128MiB memcpy per fresh arena — and the freelist then
	// keeps the oversized copy, so every recycled arena would carry that
	// growth for the rest of the build. Cutting early keeps arenas at
	// exactly their allocated size and makes the budget arithmetic in
	// bulkImportSortBudgetBytes true. (An arena holding nothing yet is
	// left to grow: a single entry larger than a chunk is legal.)
	if len(s.views) > 0 && len(s.arena)+len(key)+len(val) > cap(s.arena) {
		s.cutAndDispatch()
	}
	if s.arena == nil {
		if s.free != nil {
			s.arena = s.free.get()
		} else {
			s.arena = getSpillArena()
			if cap(s.arena) < s.chunkBytes {
				// Pooled arenas are sized for bulkSpillKeyChunkBytes; a sorter
				// with a larger chunk size (deferredIndexSpillChunkBytes)
				// allocates its full arena up front instead of append-doubling
				// through it, and returns the small one to the pool.
				putSpillArena(s.arena)
				s.arena = make([]byte, 0, s.chunkBytes)
			}
		}
		s.views = getSpillViews()
	}
	keyOff := len(s.arena)
	s.arena = append(s.arena, key...)
	s.arena = append(s.arena, val...)
	s.views = append(s.views, kvView{keyOff: keyOff, keyEnd: keyOff + len(key), valEnd: keyOff + len(key) + len(val)})
	s.count++
	if len(s.arena) >= s.chunkBytes {
		s.cutAndDispatch()
	}
	return nil
}

// cutAndDispatch detaches the active arena and hands it to a background
// sorter, blocking while all sort slots are busy.
func (s *spillSorter) cutAndDispatch() {
	arena, views := s.arena, s.views
	s.arena, s.views = nil, nil
	if len(views) == 0 {
		return
	}
	s.wg.Add(1)
	s.sem <- struct{}{}
	go func() {
		defer func() {
			<-s.sem
			s.wg.Done()
		}()
		s.sortAndWriteChunk(arena, views)
	}()
}

func (s *spillSorter) sortAndWriteChunk(arena []byte, views []kvView) {
	// slices.SortFunc, not sort.Slice: typed swaps instead of the
	// reflection-based Swapper. Chunk sorts run hot (57M+ keys per whale
	// deferred index build) and the reflective swap was ~half the sort cost.
	slices.SortFunc(views, func(a, b kvView) int {
		return bytes.Compare(arena[a.keyOff:a.keyEnd], arena[b.keyOff:b.keyEnd])
	})
	s.chunkMu.Lock()
	chunkPath := filepath.Join(s.dir, fmt.Sprintf("%s-chunk-%04d.bin", s.name, len(s.chunks)))
	s.chunks = append(s.chunks, chunkPath)
	s.chunkMu.Unlock()
	if err := writeSortedSpillChunk(chunkPath, arena, views); err != nil {
		s.setErr(err)
	}
	if s.free != nil {
		s.free.put(arena)
	} else {
		putSpillArena(arena)
	}
	putSpillViews(views)
}

func (s *spillSorter) setErr(err error) {
	s.chunkMu.Lock()
	if s.firstErr == nil {
		s.firstErr = err
	}
	s.chunkMu.Unlock()
}

func (s *spillSorter) takeErr() error {
	s.chunkMu.Lock()
	defer s.chunkMu.Unlock()
	return s.firstErr
}

// abort waits out in-flight sorts so the staging dir can be removed.
func (s *spillSorter) abort() {
	s.arena, s.views = nil, nil
	s.wg.Wait()
}

// finalize flushes the tail chunk, waits for background sorts, and
// returns the sorted run paths.
func (s *spillSorter) finalize() ([]string, error) {
	s.cutAndDispatch()
	s.wg.Wait()
	if err := s.takeErr(); err != nil {
		return nil, err
	}
	return s.chunks, nil
}

func writeSortedSpillChunk(path string, arena []byte, views []kvView) error {
	f, err := os.Create(path) // #nosec G304 - staged under the import's MkdirTemp dir.
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(f, bulkSpillBufferSize)
	success := false
	defer func() {
		_ = f.Close()
		if !success {
			_ = os.Remove(path)
		}
	}()
	var lenBuf [4]byte
	for _, v := range views {
		// Lengths are bounded by add()'s MaxUint32 checks.
		binary.BigEndian.PutUint32(lenBuf[:], uint32(v.keyEnd-v.keyOff)) // #nosec G115
		if _, err := writer.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := writer.Write(arena[v.keyOff:v.keyEnd]); err != nil {
			return err
		}
		binary.BigEndian.PutUint32(lenBuf[:], uint32(v.valEnd-v.keyEnd)) // #nosec G115
		if _, err := writer.Write(lenBuf[:]); err != nil {
			return err
		}
		if _, err := writer.Write(arena[v.keyEnd:v.valEnd]); err != nil {
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	success = true
	return nil
}

type spillChunkItem struct {
	chunkIdx int
	key      []byte
	val      []byte
}

type spillChunkHeap []spillChunkItem

func (h spillChunkHeap) less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp == 0 {
		return h[i].chunkIdx < h[j].chunkIdx
	}
	return cmp < 0
}
func (h spillChunkHeap) swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *spillChunkHeap) push(item spillChunkItem) {
	*h = append(*h, item)
	for child := len(*h) - 1; child > 0; {
		parent := (child - 1) / 2
		if !h.less(child, parent) {
			break
		}
		h.swap(child, parent)
		child = parent
	}
}
func (h *spillChunkHeap) pop() spillChunkItem {
	old := *h
	item := old[0]
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	if len(*h) > 0 {
		(*h)[0] = last
		h.down(0)
	}
	return item
}
func (h *spillChunkHeap) down(parent int) {
	for {
		left := 2*parent + 1
		if left >= len(*h) {
			return
		}
		child := left
		if right := left + 1; right < len(*h) && h.less(right, left) {
			child = right
		}
		if !h.less(child, parent) {
			return
		}
		h.swap(parent, child)
		parent = child
	}
}

// readSpillEntry reads one length-prefixed (key, value) pair. Buffers
// are reused across calls per chunk.
func readSpillEntry(r io.Reader, key, val *[]byte, lenBuf *[4]byte) (bool, error) {
	readBlob := func(dst *[]byte) error {
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return err
		}
		n := int(binary.BigEndian.Uint32(lenBuf[:]))
		if cap(*dst) < n {
			*dst = make([]byte, n)
		} else {
			*dst = (*dst)[:n]
		}
		_, err := io.ReadFull(r, *dst)
		return err
	}
	if err := readBlob(key); err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, err
	}
	if err := readBlob(val); err != nil {
		return false, fmt.Errorf("bulk sync import: truncated spill entry: %w", err)
	}
	return true, nil
}

// spillChunkCursors owns the open chunk files for one merge pass and
// unlinks each chunk as soon as its last entry has been read. The chunks
// and the SST being written hold the same entries, so a merge that keeps
// every chunk until teardown needs staging space for both copies at once;
// releasing at exhaustion bounds the overlap to the chunks still in
// flight. All of the engine's k-way merges read their chunks through this
// type (advance is readSpillEntry's only caller), so the bound holds for
// every spill merge, not just the bulk import's.
//
// Unlinking is deliberately confined to the exhausted-chunk path, where
// the file is provably fully consumed. A merge that fails partway closes
// its remaining descriptors and leaves those files for the staging-dir
// teardown that already owns them.
type spillChunkCursors struct {
	paths   []string
	files   []*os.File
	bufs    []*bufio.Reader
	keyBufs [][]byte
	valBufs [][]byte
	lenBuf  [4]byte
}

func openSpillChunks(chunks []string) (*spillChunkCursors, error) {
	c := &spillChunkCursors{
		paths:   chunks,
		files:   make([]*os.File, len(chunks)),
		bufs:    make([]*bufio.Reader, len(chunks)),
		keyBufs: make([][]byte, len(chunks)),
		valBufs: make([][]byte, len(chunks)),
	}
	for i, chunk := range chunks {
		f, err := os.Open(chunk) // #nosec G304 - engine-private scratch, staged under the caller's MkdirTemp dir.
		if err != nil {
			c.closeAll()
			return nil, err
		}
		c.files[i] = f
		c.bufs[i] = bufio.NewReaderSize(f, bulkSpillBufferSize)
	}
	return c, nil
}

// advance reads the next entry of chunk i into that chunk's reusable
// buffers, reachable via key/val. It reports false once the chunk is
// exhausted, having already closed and unlinked it.
//
// Advancing an already-exhausted chunk keeps reporting false rather than
// faulting on the released reader. The merges re-push a chunk onto the
// heap only after a true, so none of them reach this today; the guard
// preserves the EOF idempotency of the inline readSpillEntry calls this
// replaced.
func (c *spillChunkCursors) advance(i int) (bool, error) {
	if c.bufs[i] == nil {
		return false, nil
	}
	ok, err := readSpillEntry(c.bufs[i], &c.keyBufs[i], &c.valBufs[i], &c.lenBuf)
	if err != nil {
		return false, err
	}
	if !ok {
		c.closeChunk(i)
		_ = os.Remove(c.paths[i])
	}
	return ok, nil
}

func (c *spillChunkCursors) key(i int) []byte { return c.keyBufs[i] }
func (c *spillChunkCursors) val(i int) []byte { return c.valBufs[i] }

func (c *spillChunkCursors) closeChunk(i int) {
	if c.files[i] == nil {
		return
	}
	_ = c.files[i].Close()
	c.files[i] = nil
	c.bufs[i] = nil
}

// closeAll releases descriptors without unlinking; see the type comment.
func (c *spillChunkCursors) closeAll() {
	for i := range c.files {
		c.closeChunk(i)
	}
}

// mergeSortedSpillChunksToSST heap-merges the sorted chunk files into a
// single SST. Duplicate keys are corruption (the importer requires
// globally unique tuples) and fail the merge. fs is the engine FS the
// SST is created on (chunk files are plain OS scratch — see spillSorter).
func mergeSortedSpillChunksToSST(ctx context.Context, fs vfs.FS, sstPath, name string, chunks []string) error {
	start := time.Now()
	l := ctxzap.Extract(ctx)
	cursors, err := openSpillChunks(chunks)
	if err != nil {
		return err
	}
	defer cursors.closeAll()
	h := &spillChunkHeap{}
	for i := range chunks {
		ok, err := cursors.advance(i)
		if err != nil {
			return err
		}
		if ok {
			h.push(spillChunkItem{chunkIdx: i, key: cursors.key(i), val: cursors.val(i)})
		}
	}

	writer, err := newBulkSSTWriter(fs, filepath.Dir(sstPath), name)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		_ = writer.finish()
		if !success {
			_ = fs.Remove(sstPath)
		}
	}()
	var last []byte
	var written int64
	lastLog := start
	for len(*h) > 0 {
		item := h.pop()
		if bytes.Equal(item.key, last) {
			return fmt.Errorf("%w: bucket %s key %x", ErrBulkImportDuplicateKey, name, item.key)
		}
		var v []byte
		if len(item.val) > 0 {
			v = item.val
		}
		if err := writer.add(item.key, v); err != nil {
			return err
		}
		written++
		// Throttle the per-entry bookkeeping: ctx.Err and time.Now on every
		// one of 57M+ merged entries were measurable in profiles.
		if written&0xFFFF == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if now := time.Now(); now.Sub(lastLog) >= 15*time.Second {
				l.Info("spill sorter: merging chunks",
					zap.String("name", name),
					zap.Int("chunks", len(chunks)),
					zap.Int("active_chunks", len(*h)+1),
					zap.Int64("entries_written", written),
					zap.Duration("elapsed", now.Sub(start)),
				)
				lastLog = now
			}
		}
		last = append(last[:0], item.key...)
		ok, err := cursors.advance(item.chunkIdx)
		if err != nil {
			return err
		}
		if ok {
			h.push(spillChunkItem{chunkIdx: item.chunkIdx, key: cursors.key(item.chunkIdx), val: cursors.val(item.chunkIdx)})
		}
	}
	if err := writer.finish(); err != nil {
		return err
	}
	l.Info("spill sorter: merge complete",
		zap.String("name", name),
		zap.Int("chunks", len(chunks)),
		zap.Int64("entries_written", written),
		zap.Duration("elapsed", time.Since(start)),
	)
	success = true
	return nil
}

// mergeSpillChunksToSSTResolvingDuplicates heap-merges sorted chunk files
// into a single SST like mergeSortedSpillChunksToSST, but tolerates duplicate
// keys: the values of each duplicate-key group are handed to resolve and the
// resolved value is written once. Legacy sources can hold rows with distinct
// external ids that fold to one structural identity, so buckets keyed by
// identity use this variant instead of treating duplicates as corruption.
//
// Unlike the strict variant, every entry is copied into reused group scratch
// before its chunk buffer is advanced (one extra value memcpy per entry, no
// per-entry allocation) — that is what makes the one-group lookahead safe
// against chunk-buffer reuse. Returns the number of duplicate groups
// resolved.
func mergeSpillChunksToSSTResolvingDuplicates(
	ctx context.Context,
	fs vfs.FS,
	sstPath, name string,
	chunks []string,
	resolve func(key []byte, values [][]byte) ([]byte, error),
) (int64, error) {
	start := time.Now()
	l := ctxzap.Extract(ctx)
	cursors, err := openSpillChunks(chunks)
	if err != nil {
		return 0, err
	}
	defer cursors.closeAll()
	h := &spillChunkHeap{}
	for i := range chunks {
		ok, err := cursors.advance(i)
		if err != nil {
			return 0, err
		}
		if ok {
			h.push(spillChunkItem{chunkIdx: i, key: cursors.key(i), val: cursors.val(i)})
		}
	}

	writer, err := newBulkSSTWriter(fs, filepath.Dir(sstPath), name)
	if err != nil {
		return 0, err
	}
	success := false
	defer func() {
		_ = writer.finish()
		if !success {
			_ = fs.Remove(sstPath)
		}
	}()

	type valSpan struct{ off, end int }
	var (
		haveCur  bool
		curKey   []byte // reused scratch: the pending group's key
		curArena []byte // reused scratch: the pending group's values
		curSpans []valSpan
	)
	var dupGroups, written, scanned int64
	flushCur := func() error {
		if !haveCur {
			return nil
		}
		var val []byte
		if len(curSpans) == 1 {
			val = curArena[curSpans[0].off:curSpans[0].end]
		} else {
			dupGroups++
			values := make([][]byte, len(curSpans))
			for i, sp := range curSpans {
				values[i] = curArena[sp.off:sp.end]
			}
			resolved, err := resolve(curKey, values)
			if err != nil {
				return err
			}
			val = resolved
		}
		if len(val) == 0 {
			val = nil
		}
		if err := writer.add(curKey, val); err != nil {
			return err
		}
		written++
		return nil
	}

	lastLog := start
	for len(*h) > 0 {
		item := h.pop()
		if haveCur && bytes.Equal(item.key, curKey) {
			off := len(curArena)
			curArena = append(curArena, item.val...)
			curSpans = append(curSpans, valSpan{off: off, end: len(curArena)})
		} else {
			if err := flushCur(); err != nil {
				return dupGroups, err
			}
			curKey = append(curKey[:0], item.key...)
			curArena = append(curArena[:0], item.val...)
			curSpans = append(curSpans[:0], valSpan{off: 0, end: len(curArena)})
			haveCur = true
		}
		scanned++
		// Same throttled bookkeeping as the strict merge.
		if scanned&0xFFFF == 0 {
			if err := ctx.Err(); err != nil {
				return dupGroups, err
			}
			if now := time.Now(); now.Sub(lastLog) >= 15*time.Second {
				l.Info("spill sorter: merging chunks",
					zap.String("name", name),
					zap.Int("chunks", len(chunks)),
					zap.Int("active_chunks", len(*h)+1),
					zap.Int64("entries_scanned", scanned),
					zap.Duration("elapsed", now.Sub(start)),
				)
				lastLog = now
			}
		}
		// Advance the popped chunk only AFTER the entry was consumed into
		// the group scratch: readSpillEntry overwrites the buffers item
		// aliases.
		ok, err := cursors.advance(item.chunkIdx)
		if err != nil {
			return dupGroups, err
		}
		if ok {
			h.push(spillChunkItem{chunkIdx: item.chunkIdx, key: cursors.key(item.chunkIdx), val: cursors.val(item.chunkIdx)})
		}
	}
	if err := flushCur(); err != nil {
		return dupGroups, err
	}
	if err := writer.finish(); err != nil {
		return dupGroups, err
	}
	l.Info("spill sorter: merge complete",
		zap.String("name", name),
		zap.Int("chunks", len(chunks)),
		zap.Int64("entries_written", written),
		zap.Int64("duplicate_groups_resolved", dupGroups),
		zap.Duration("elapsed", time.Since(start)),
	)
	success = true
	return dupGroups, nil
}
