package pebble

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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

// grantContentHash is the canonical content hash of a grant — the value
// stored in the hash index and the unit the grant digest folds.
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
func grantContentHash(r *v3.GrantRecord) []byte {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()

	// Source-entitlement ids, sorted for order-independence. The map
	// values (GrantSourceRecord) are not folded in v1 — only the set of
	// source ids, which is the membership-composition signal.
	ids := make([]string, 0, len(r.GetSources()))
	for k := range r.GetSources() {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	return grantContentHashFromParts(ent.GetEntitlementId(), princ.GetResourceTypeId(), princ.GetResourceId(), r.GetExternalId(), ids)
}

// grantContentHashFromParts computes the content hash from pre-extracted
// fields. sortedSourceKeys must already be sorted for order-independence.
func grantContentHashFromParts(entID, principalRT, principalID, externalID string, sortedSourceKeys []string) []byte {
	fields := make([]string, 0, 4+len(sortedSourceKeys))
	fields = append(fields, entID, principalRT, principalID, externalID)
	fields = append(fields, sortedSourceKeys...)
	buf := codec.AppendTupleStrings(nil, fields...)
	out := make([]byte, hashLen)
	binary.BigEndian.PutUint64(out, xxhash.Sum64(buf))
	return out
}

// grantHashIndexKey returns the by_entitlement_principal_hash index key
// for r, or nil when the grant lacks the entitlement/principal needed to
// place it (mirrors the by_entitlement index's nil-guard).
func grantHashIndexKey(r *v3.GrantRecord) []byte {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	if ent == nil || princ == nil {
		return nil
	}
	bh := principalBucketHash(princ.GetResourceTypeId(), princ.GetResourceId())
	return encodeGrantByEntPrincHashIndexKey(
		ent.GetEntitlementId(), bh,
		princ.GetResourceTypeId(), princ.GetResourceId(), r.GetExternalId(),
	)
}

// BuildAllGrantDigests rebuilds the grant digest for every entitlement
// in syncID. Called at seal time (Adapter.EndSync) after all grants are
// written, and by the on-Open migration backfill. Every entitlement
// gets a digest — including those with zero grants, which store a
// single root node — so a reader can always distinguish "empty" from
// "never built".
func (e *Engine) BuildAllGrantDigests(ctx context.Context, syncID string) error {
	// Collect entitlement ids first: the build writes into the
	// typeDigest keyspace while we'd otherwise be mid-iteration over
	// typeEntitlement. Different keyspaces, but snapshotting the ids
	// keeps the iterator and the writes cleanly separated.
	var ents []string
	if err := e.IterateEntitlements(ctx, func(r *v3.EntitlementRecord) bool {
		ents = append(ents, r.GetExternalId())
		return true
	}); err != nil {
		return fmt.Errorf("BuildAllGrantDigests: list entitlements: %w", err)
	}
	for _, ent := range ents {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.buildPartitionDigest(ctx, grantDigestSpec, ent); err != nil {
			return fmt.Errorf("BuildAllGrantDigests: entitlement %q: %w", ent, err)
		}
	}
	return nil
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

// newGrantDigestMutator returns a digestMutator bound to the grant
// digest; feed it with addGrant/removeGrant.
func newGrantDigestMutator(e *Engine) *digestMutator {
	return newDigestMutator(e, grantDigestSpec)
}

// freshRootAcc accumulates the per-entitlement root digest in memory
// during a fresh sync. Flushed to Pebble at EndFreshSync (or Close).
type freshRootAcc struct {
	count     int64
	xorDigest [hashLen]byte
}

// updateFreshRoot applies a signed delta (±1) to the in-memory root
// accumulator for entID. No-op when freshRoots is nil.
func (e *Engine) updateFreshRoot(entID string, contentHash []byte, sign int64) {
	if e.freshRoots == nil {
		return
	}
	acc := e.freshRoots[entID]
	if acc == nil {
		acc = &freshRootAcc{}
		e.freshRoots[entID] = acc
	}
	acc.count += sign
	xorInto(acc.xorDigest[:], contentHash)
}

// addGrantToFreshRoot records a grant insertion into its entitlement's
// in-memory root accumulator. No-op when freshRoots is nil.
func (e *Engine) addGrantToFreshRoot(r *v3.GrantRecord) {
	entID := r.GetEntitlement().GetEntitlementId()
	if entID == "" {
		return
	}
	e.updateFreshRoot(entID, grantContentHash(r), +1)
}

// removeGrantFromFreshRootRaw records a grant removal from its
// entitlement's in-memory root accumulator, reading fields from the
// raw wire bytes without a full proto unmarshal. No-op when freshRoots
// is nil.
func (e *Engine) removeGrantFromFreshRootRaw(externalID string, value []byte) error {
	if e.freshRoots == nil {
		return nil
	}
	_, _, entID, principalRT, principalID, _, err := scanGrantIndexFieldsRaw(value)
	if err != nil {
		return err
	}
	if entID == "" {
		return nil
	}
	sourceKeys, err := scanGrantSourceKeysRaw(value)
	if err != nil {
		return err
	}
	sort.Strings(sourceKeys)
	ch := grantContentHashFromParts(entID, principalRT, principalID, externalID, sourceKeys)
	e.updateFreshRoot(entID, ch, -1)
	return nil
}

// FlushFreshRoots writes all in-memory root accumulators to Pebble and
// clears the map. Must be called before BuildAllGrantDigests so the stored
// roots are available for the count-skip optimisation, and before the
// EndFreshSync flush so the nodes are hardened. Also called from Close as
// a safety net for early-exit paths. Must be called within writeMu.
func (e *Engine) FlushFreshRoots() error {
	return e.flushFreshRoots()
}

// PersistFreshRoots writes the current in-memory root accumulators to
// Pebble without clearing the map, so accumulation continues. Safe to
// call at any point during a fresh sync (e.g. CheckpointSync) to make
// the current root state durable without interrupting the sync.
func (e *Engine) PersistFreshRoots() error {
	return e.withWrite(func() error {
		return e.persistFreshRoots()
	})
}

func (e *Engine) persistFreshRoots() error {
	if len(e.freshRoots) == 0 {
		return nil
	}
	batch := e.db.NewBatch()
	defer batch.Close()
	for entID, acc := range e.freshRoots {
		key := encodeDigestNodeKey(idxGrantByEntitlementPrincipalHash, entID, digestLevelRoot, nil)
		if err := batch.Set(key, packDigestRoot(0, acc.count, acc.xorDigest[:]), nil); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.NoSync)
}

func (e *Engine) flushFreshRoots() error {
	if err := e.persistFreshRoots(); err != nil {
		return err
	}
	e.freshRoots = nil
	return nil
}

// newGrantDigestMutatorFresh is newGrantDigestMutator for the fresh-sync
// path: absent roots are initialized as zero rather than dropped, so the
// root node is written live as grants arrive.
func newGrantDigestMutatorFresh(e *Engine) *digestMutator {
	m := newDigestMutator(e, grantDigestSpec)
	m.createIfAbsent = true
	return m
}

// addGrant records r's insertion into its entitlement's grant digest.
func (m *digestMutator) addGrant(r *v3.GrantRecord) error {
	return m.grantDelta(r, m.addHash)
}

// removeGrant records r's removal from its entitlement's grant digest.
func (m *digestMutator) removeGrant(r *v3.GrantRecord) error {
	return m.grantDelta(r, m.removeHash)
}

// removeGrantRaw is removeGrant without a full proto unmarshal. It extracts
// only the fields needed for the digest (entitlement, principal, sources keys)
// directly from the raw wire bytes, avoiding heap allocation of the GrantRecord
// and all its nested messages on the hot path.
func (m *digestMutator) removeGrantRaw(externalID string, value []byte) error {
	_, _, entID, principalRT, principalID, _, err := scanGrantIndexFieldsRaw(value)
	if err != nil {
		return err
	}
	if entID == "" || principalRT == "" || principalID == "" {
		return nil // not in the hash index → not in the digest
	}
	sourceKeys, err := scanGrantSourceKeysRaw(value)
	if err != nil {
		return err
	}
	sort.Strings(sourceKeys)
	bh := principalBucketHash(principalRT, principalID)
	ch := grantContentHashFromParts(entID, principalRT, principalID, externalID, sourceKeys)
	return m.removeHash(entID, bh, ch)
}

func (m *digestMutator) grantDelta(r *v3.GrantRecord, apply func(partition string, bucketHash, contentHash []byte) error) error {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	if ent == nil || princ == nil {
		return nil // not in the hash index → not in the digest
	}
	bh := principalBucketHash(princ.GetResourceTypeId(), princ.GetResourceId())
	return apply(ent.GetEntitlementId(), bh, grantContentHash(r))
}
