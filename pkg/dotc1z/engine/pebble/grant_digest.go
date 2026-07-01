package pebble

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble/v2"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
)

// Grant instantiation of the digest core (digest.go): the per-
// entitlement grant digest, folded over the
// by_entitlement_principal_hash index.
//
//	partition    = entitlement_id
//	bucket hash  = principalBucketHash (identity of the principal)
//	content hash = grantContentHash (the membership edge)
//
// This answers "does this entitlement have exactly the same grants as
// some other sync/file?" with a single root read, and localizes any
// difference to principal-hash buckets so the diff driver loads only
// those grants (see adapter_diff.go).

// grantDigestSpec wires the grant hash index into the digest core. The
// index's key layout (encodeGrantByEntPrincHashIndexKey) satisfies the
// digestIndexSpec shape contract: partition prefix, then the raw 8-byte
// bucket hash, then the principal/external_id tail; the value is the
// grant content hash.
var grantDigestSpec = digestIndexSpec{
	indexID:         idxGrantByEntitlementPrincipalHash,
	partitionPrefix: encodeGrantByEntPrincHashEntPrefix,
}

// principalBucketHash is the bucket address for a principal: the top
// digestBucketHashLen bytes of the xxHash64 of (rt + "\x00" + id).
// Identity only — never the principal's full object — so the address is
// stable across syncs even when the principal's attributes change.
// Returns a fresh slice.
func principalBucketHash(rt, id string) []byte {
	h := xxhash.New()
	_, _ = h.WriteString(rt)
	_, _ = h.Write([]byte{0})
	_, _ = h.WriteString(id)
	// Keep the top digestBucketHashLen bytes of the big-endian hash —
	// the most significant bytes, which is where the bucket-selecting
	// bits live.
	var full [8]byte
	binary.BigEndian.PutUint64(full[:], h.Sum64())
	out := make([]byte, digestBucketHashLen)
	copy(out, full[:])
	return out
}

// grantContentHashFromParts is the canonical content hash of a grant —
// the value stored in the hash index and the unit the grant digest
// folds. sortedSourceKeys (the grant's source-entitlement ids) must
// already be sorted for order-independence; the source map VALUES
// (GrantSourceRecord) are not folded in v1 — only the set of source
// ids, which is the membership-composition signal.
//
// ABI: the field set below defines what "the same grant" means for the
// diff. It deliberately covers the membership EDGE (entitlement id,
// principal identity, external_id) plus the grant's source-entitlement
// set (expansion provenance), and deliberately EXCLUDES sync-relative
// and transient processing state — sync_id, discovered_at,
// needs_expansion, expansion, and annotations — none of which change
// "which principal holds which entitlement". This is a hand-rolled
// framing, NOT proto marshal: deterministic-proto output is not
// canonical across protobuf library versions, which would make two
// files written by different SDK builds hash identical grants
// differently. Changing this set requires an index-migration bump.
//
// The fields are serialized with the tuple codec (the same injective
// encoding the index keys use): every field is escaped and
// separator-delimited, so no field value — even one containing the raw
// separator or NUL bytes — can alias a different field split. A naive
// 0x00-joined concatenation is NOT injective (e.g. sources ["a","b"] vs
// ["a\x00b"]), which would deterministically collide distinct grants.
func grantContentHashFromParts(entID, principalRT, principalID, externalID string, sortedSourceKeys []string) []byte {
	fields := make([]string, 0, 4+len(sortedSourceKeys))
	fields = append(fields, entID, principalRT, principalID, externalID)
	fields = append(fields, sortedSourceKeys...)
	buf := codec.AppendTupleStrings(nil, fields...)
	out := make([]byte, hashLen)
	binary.BigEndian.PutUint64(out, xxhash.Sum64(buf))
	return out
}

// sealIndexSpillChunkBytes is the in-memory arena size of the seal-time
// index spill sorter: entries accumulate unsorted until the arena
// reaches this size, then the chunk sorts in the background and spills
// to a run file. Most connectors' whole index fits in one or two
// chunks; only very large syncs produce enough runs for a real
// multi-way merge. Bounds memory to O(chunk) regardless of grant count.
const sealIndexSpillChunkBytes = 32 << 20

// SealGrantHashIndexAndDigests builds the by_entitlement_principal_hash
// index and the per-entitlement grant digests, in that order. This is
// the ONLY writer of either keyspace: the grant write paths never
// maintain them inline (see the digest.go lifecycle contract). Called
// at seal time (Adapter.EndSync) after all grants — including expanded
// grants — are written.
//
// Phase 1 re-derives every index entry from the grant primaries (raw
// field scans, no proto unmarshal), tallying per-entitlement grant
// counts as it goes, and materializes the index as ONE ingested SST
// (see rebuildGrantHashIndex). Phase 2 folds the fresh index into
// digest nodes — also SST-ingested — with the tallied counts picking
// each partition's width without a second counting scan. Every
// entitlement gets a digest, including those with zero grants (a
// single count-0 root), so a reader can always distinguish "empty"
// from "never built". Entitlement ids seen only on grants (no
// entitlement record) get digests too.
//
// On error the caller must ensure no partial digest survives (drop the
// digest keyspace): a half-built digest that LOOKS present violates the
// present-means-exact contract.
func (e *Engine) SealGrantHashIndexAndDigests(ctx context.Context) error {
	counts, err := e.rebuildGrantHashIndex(ctx)
	if err != nil {
		return fmt.Errorf("seal grant hash index: %w", err)
	}
	return e.buildAllGrantDigests(ctx, counts)
}

// rebuildGrantHashIndex derives the by_entitlement_principal_hash index
// from the grant primary keyspace and returns the number of index
// entries written per entitlement id.
//
// The index never passes through the memtable/WAL: entries are
// spill-sorted and ingested as ONE SST over the excised index keyspace,
// so the swap is atomic and exactly replaces any prior index contents.
//
// The primaries iterate in external_id order, which is unordered with
// respect to the index's (entitlement, principal-hash) key order, so
// the rows need a sort before they can enter an SST. The bulk
// importer's spillSorter provides it: rows accumulate in an in-memory
// arena; each full arena sorts in the background (overlapping the
// scan) and spills to a sorted run file; the runs then k-way merge
// into the SST. Because the entitlement id LEADS the index key,
// per-entitlement grouping falls out of the global sort — an
// entitlement small enough to sit inside one arena chunk is sorted
// entirely in memory, and a 100k-grant entitlement simply spans
// several runs and reassembles, contiguously, during the merge.
func (e *Engine) rebuildGrantHashIndex(ctx context.Context) (map[string]int64, error) {
	return e.rebuildGrantHashIndexChunked(ctx, sealIndexSpillChunkBytes)
}

// rebuildGrantHashIndexChunked is rebuildGrantHashIndex with the spill
// arena size as a parameter, so tests can force the multi-run k-way
// merge path with a small data set.
func (e *Engine) rebuildGrantHashIndexChunked(ctx context.Context, chunkBytes int) (map[string]int64, error) {
	counts := make(map[string]int64)

	dir, err := os.MkdirTemp("", "pebble-hash-index-seal-")
	if err != nil {
		return nil, fmt.Errorf("temp dir: %w", err)
	}
	// On success pebble links the SST into its own store, so removing
	// the staging dir (runs + SST) is safe either way.
	defer func() { _ = os.RemoveAll(dir) }()

	// Two background sort slots: enough to overlap chunk sorting with
	// the scan without competing with it for cores.
	sorter := newSpillSorter(dir, "grant_hash_index", make(chan struct{}, 2), chunkBytes)
	// abort() waits out in-flight chunk sorts so RemoveAll can't race
	// them; harmless after a successful finalize.
	defer sorter.abort()

	err = e.withWrite(func() error {
		prefix := encodeGrantPrefix()
		iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upperBoundOf(prefix)})
		if err != nil {
			return err
		}
		defer iter.Close()

		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			ext, _, err := codec.DecodeTupleStringTo(nil, iter.Key(), len(prefix)+1)
			if err != nil {
				return fmt.Errorf("decode grant key %x: %w", iter.Key(), err)
			}
			val := iter.Value()
			_, _, entID, principalRT, principalID, _, err := scanGrantIndexFieldsRaw(val)
			if err != nil {
				return err
			}
			if entID == "" || principalRT == "" || principalID == "" {
				continue // no entitlement/principal → no place in the index
			}
			sourceKeys, err := scanGrantSourceKeysRaw(val)
			if err != nil {
				return err
			}
			sort.Strings(sourceKeys)
			externalID := string(ext)
			bh := principalBucketHash(principalRT, principalID)
			ch := grantContentHashFromParts(entID, principalRT, principalID, externalID, sourceKeys)
			hk := encodeGrantByEntPrincHashIndexKey(entID, bh, principalRT, principalID, externalID)
			if err := sorter.add(hk, ch); err != nil {
				return err
			}
			counts[entID]++
		}
		if err := iter.Error(); err != nil {
			return err
		}

		chunks, err := sorter.finalize()
		if err != nil {
			return err
		}
		if len(chunks) == 0 {
			// No grants at all — pebble rejects an empty SST, so just
			// clear any stale index rows directly.
			return e.db.DeleteRange(GrantByEntPrincHashLowerBound(), GrantByEntPrincHashUpperBound(), writeOpts(e.opts.durability))
		}
		const sstName = "grant_hash_index"
		sstPath := filepath.Join(dir, sstName+".sst")
		if err := mergeSortedSpillChunksToSST(ctx, sstPath, sstName, chunks); err != nil {
			return err
		}
		if _, err := e.db.IngestAndExcise(ctx, []string{sstPath}, nil, nil, pebble.KeyRange{
			Start: GrantByEntPrincHashLowerBound(),
			End:   GrantByEntPrincHashUpperBound(),
		}); err != nil {
			return fmt.Errorf("ingest: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return counts, nil
}

// buildAllGrantDigests builds the digest for every entitlement record
// plus every entitlement id that appears only on grants, using the
// per-entitlement counts tallied by rebuildGrantHashIndex to pick each
// partition's width.
//
// The nodes never pass through the memtable/WAL: every partition's
// root + leaves stream, in ascending key order, into ONE SST on disk,
// which is then IngestAndExcise'd over the whole digest keyspace.
// That makes the digest swap atomic (readers see the old digests or
// the new, never a mixture), replaces the per-partition DeleteRange
// tombstones with a single excise, and guarantees no stale node — even
// for a partition that no longer exists — survives a reseal.
//
// SST keys must be strictly increasing, which the key layout gives us
// for free: node keys order by the tuple-encoded partition (the tuple
// codec is order-preserving, so plain string sort of the entitlement
// ids matches encoded order), then by level — the root (level 0)
// precedes its leaves (level 1), and foldPartitionNodes yields leaves
// in ascending bucket order.
func (e *Engine) buildAllGrantDigests(ctx context.Context, counts map[string]int64) error {
	// Collect the digest partitions: every entitlement record, plus
	// grant-bearing entitlement ids with no record (orphans — rare,
	// but skipping them would leave grants no digest ever covers).
	seen := make(map[string]struct{}, len(counts))
	var ents []string
	collect := func(id string) {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			ents = append(ents, id)
		}
	}
	if err := e.IterateEntitlements(ctx, func(r *v3.EntitlementRecord) bool {
		collect(r.GetExternalId())
		return true
	}); err != nil {
		return fmt.Errorf("build grant digests: list entitlements: %w", err)
	}
	for id := range counts {
		collect(id)
	}
	sort.Strings(ents)

	dir, err := os.MkdirTemp("", "pebble-digest-seal-")
	if err != nil {
		return fmt.Errorf("build grant digests: temp dir: %w", err)
	}
	// On success pebble links the SST into its own store, so removing
	// the staging dir is safe either way.
	defer func() { _ = os.RemoveAll(dir) }()
	w, err := newBulkSSTWriter(dir, "grant-digests")
	if err != nil {
		return fmt.Errorf("build grant digests: %w", err)
	}
	defer func() { _ = w.finish() }()

	return e.withWrite(func() error {
		for _, ent := range ents {
			if err := ctx.Err(); err != nil {
				return err
			}
			rootVal, leaves, err := e.foldPartitionNodes(ctx, grantDigestSpec, ent, chooseDigestWidth(counts[ent]))
			if err != nil {
				return fmt.Errorf("build grant digests: entitlement %q: %w", ent, err)
			}
			rootKey := encodeDigestNodeKey(grantDigestSpec.indexID, ent, digestLevelRoot, nil)
			if err := w.add(rootKey, rootVal); err != nil {
				return fmt.Errorf("build grant digests: entitlement %q: %w", ent, err)
			}
			for i := range leaves {
				key := encodeDigestNodeKey(grantDigestSpec.indexID, ent, digestLevelLeaf, leaves[i].prefix[:])
				if err := w.add(key, leaves[i].val); err != nil {
					return fmt.Errorf("build grant digests: entitlement %q: %w", ent, err)
				}
			}
		}
		if err := w.finish(); err != nil {
			return fmt.Errorf("build grant digests: %w", err)
		}
		if w.count == 0 {
			// No partitions at all — pebble rejects an empty SST, so
			// just clear any stale nodes directly.
			return e.db.DeleteRange(DigestLowerBound(), DigestUpperBound(), writeOpts(e.opts.durability))
		}
		if _, err := e.db.IngestAndExcise(ctx, []string{w.path}, nil, nil, pebble.KeyRange{
			Start: DigestLowerBound(),
			End:   DigestUpperBound(),
		}); err != nil {
			return fmt.Errorf("build grant digests: ingest: %w", err)
		}
		return nil
	})
}

// GetEntitlementDigestRoot returns the stored grant-digest root for an
// entitlement. ok is false when no digest has been built for it (the
// caller can fall back to ComputeEntitlementBucketDigest, which derives
// the same digest from the index on demand).
func (e *Engine) GetEntitlementDigestRoot(ctx context.Context, syncID, entitlementID string) (DigestRoot, bool, error) {
	return e.getPartitionDigestRoot(grantDigestSpec, entitlementID)
}

// ComputeEntitlementBucketDigest folds the grant hash index over a
// single bucket of an entitlement (the zero bucket = the whole
// entitlement) — the authoritative on-demand counterpart of the stored
// digest nodes.
func (e *Engine) ComputeEntitlementBucketDigest(ctx context.Context, syncID, entitlementID string, bucket DigestBucket) ([]byte, int64, error) {
	return e.computeBucketDigest(ctx, grantDigestSpec, entitlementID, bucket)
}

// DirtyEntitlementBuckets compares this engine's entitlement against
// other's and returns the buckets whose grants differ — see
// dirtyPartitionBuckets for the comparison contract (zero bucket =
// whole entitlement; nil = identical).
func (e *Engine) DirtyEntitlementBuckets(ctx context.Context, syncID string, other *Engine, otherSyncID, entitlementID string) ([]DigestBucket, error) {
	return e.dirtyPartitionBuckets(ctx, grantDigestSpec, other, entitlementID)
}

// IterateGrantsByEntitlementBucket yields the grants in one
// principal-hash bucket of an entitlement (the zero bucket = the whole
// entitlement). This is the dirty-bucket loader: after a digest
// comparison flags a bucket, the caller materializes only those grants.
// Like the other index iterators it does a point Get per entry to fetch
// the primary; orphan index entries are skipped.
func (e *Engine) IterateGrantsByEntitlementBucket(ctx context.Context, syncID, entitlementID string, bucket DigestBucket, yield func(*v3.GrantRecord) bool) error {
	entPrefix := encodeGrantByEntPrincHashEntPrefix(entitlementID)
	lower, upper := grantDigestSpec.bucketBounds(entitlementID, bucket)
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, _, _, externalID, ok := decodeEntPrincHashTail(iter.Key(), entPrefix)
		if !ok {
			continue
		}
		val, closer, getErr := e.db.Get(encodeGrantKey(externalID))
		if getErr != nil {
			if errors.Is(getErr, pebble.ErrNotFound) {
				continue
			}
			return getErr
		}
		r := &v3.GrantRecord{}
		uErr := unmarshalRecord(val, r)
		closer.Close()
		if uErr != nil {
			return fmt.Errorf("IterateGrantsByEntitlementBucket: unmarshal: %w", uErr)
		}
		if !yield(r) {
			return nil
		}
	}
	return iter.Error()
}

// DropAllGrantDigests removes every stored digest node. Called when a
// seal-time build fails partway: a partially built digest that LOOKS
// present would violate the present-means-exact contract, whereas
// absent digests just make readers re-read the grants until the next
// successful seal recalculates them.
func (e *Engine) DropAllGrantDigests(ctx context.Context) error {
	return e.withWrite(func() error {
		return e.db.DeleteRange(DigestLowerBound(), DigestUpperBound(), writeOpts(e.opts.durability))
	})
}

// dropGrantDigestForEntitlement stages the removal of one entitlement's
// digest nodes (root included) into batch. Used by the record mutation
// paths that can run against a file holding built digests (today:
// DeleteGrantRecord, which is callable outside a sync): the partition's
// digest becomes "missing — recalculate" instead of silently stale.
func dropGrantDigestForEntitlement(batch *pebble.Batch, entitlementID string) error {
	return dropPartitionDigest(batch, grantDigestSpec, entitlementID)
}
