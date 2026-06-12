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
	syncBounds: func(syncIDBytes []byte) ([]byte, []byte) {
		return GrantByEntPrincHashSyncLowerBound(syncIDBytes), GrantByEntPrincHashSyncUpperBound(syncIDBytes)
	},
}

// principalBucketHash is the bucket address for a principal: the 8-byte
// xxHash64 of (rt + "\x00" + id). Identity only — never the principal's
// full object — so the address is stable across syncs even when the
// principal's attributes change. Returns a fresh slice.
func principalBucketHash(rt, id string) []byte {
	h := xxhash.New()
	_, _ = h.WriteString(rt)
	_, _ = h.Write([]byte{0})
	_, _ = h.WriteString(id)
	out := make([]byte, digestBucketHashLen)
	binary.BigEndian.PutUint64(out, h.Sum64())
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
func grantContentHash(r *v3.GrantRecord) []byte {
	h := xxhash.New()
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	_, _ = h.WriteString(ent.GetEntitlementId())
	_, _ = h.Write([]byte{0})
	_, _ = h.WriteString(princ.GetResourceTypeId())
	_, _ = h.Write([]byte{0})
	_, _ = h.WriteString(princ.GetResourceId())
	_, _ = h.Write([]byte{0})
	_, _ = h.WriteString(r.GetExternalId())
	_, _ = h.Write([]byte{0})

	// Source-entitlement ids, sorted for order-independence. The map
	// values (GrantSourceRecord) are not folded in v1 — only the set of
	// source ids, which is the membership-composition signal.
	sources := r.GetSources()
	ids := make([]string, 0, len(sources))
	for k := range sources {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	for _, id := range ids {
		_, _ = h.WriteString(id)
		_, _ = h.Write([]byte{0})
	}
	out := make([]byte, hashLen)
	binary.BigEndian.PutUint64(out, h.Sum64())
	return out
}

// grantHashIndexKey returns the by_entitlement_principal_hash index key
// for r, or nil when the grant lacks the entitlement/principal needed to
// place it (mirrors the by_entitlement index's nil-guard).
func grantHashIndexKey(syncIDBytes []byte, r *v3.GrantRecord) []byte {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	if ent == nil || princ == nil {
		return nil
	}
	bh := principalBucketHash(princ.GetResourceTypeId(), princ.GetResourceId())
	return encodeGrantByEntPrincHashIndexKey(
		syncIDBytes, ent.GetEntitlementId(), bh,
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
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	// Collect entitlement ids first: the build writes into the
	// typeDigest keyspace while we'd otherwise be mid-iteration over
	// typeEntitlement. Different keyspaces, but snapshotting the ids
	// keeps the iterator and the writes cleanly separated.
	var ents []string
	if err := e.IterateEntitlementsBySync(ctx, syncID, func(r *v3.EntitlementRecord) bool {
		ents = append(ents, r.GetExternalId())
		return true
	}); err != nil {
		return fmt.Errorf("BuildAllGrantDigests: list entitlements: %w", err)
	}
	for _, ent := range ents {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := e.buildPartitionDigest(ctx, grantDigestSpec, idBytes, ent); err != nil {
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
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return DigestRoot{}, false, err
	}
	return e.getPartitionDigestRoot(grantDigestSpec, idBytes, entitlementID)
}

// ComputeEntitlementBucketDigest folds the grant hash index over a
// single bucket of an entitlement (the zero bucket = the whole
// entitlement) — the authoritative on-demand counterpart of the stored
// digest nodes.
func (e *Engine) ComputeEntitlementBucketDigest(ctx context.Context, syncID, entitlementID string, bucket DigestBucket) ([]byte, int64, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, 0, err
	}
	return e.computeBucketDigest(ctx, grantDigestSpec, idBytes, entitlementID, bucket)
}

// DirtyEntitlementBuckets compares this engine's entitlement against
// other's and returns the buckets whose grants differ — see
// dirtyPartitionBuckets for the comparison contract (zero bucket =
// whole entitlement; nil = identical).
func (e *Engine) DirtyEntitlementBuckets(ctx context.Context, syncID string, other *Engine, otherSyncID, entitlementID string) ([]DigestBucket, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, err
	}
	otherIDBytes, err := other.resolveSyncBytes(otherSyncID)
	if err != nil {
		return nil, err
	}
	return e.dirtyPartitionBuckets(ctx, grantDigestSpec, idBytes, other, otherIDBytes, entitlementID)
}

// IterateGrantsByEntitlementBucket yields the grants in one
// principal-hash bucket of an entitlement (the zero bucket = the whole
// entitlement). This is the dirty-bucket loader: after a digest
// comparison flags a bucket, the caller materializes only those grants.
// Like the other index iterators it does a point Get per entry to fetch
// the primary; orphan index entries are skipped.
func (e *Engine) IterateGrantsByEntitlementBucket(ctx context.Context, syncID, entitlementID string, bucket DigestBucket, yield func(*v3.GrantRecord) bool) error {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return err
	}
	entPrefix := encodeGrantByEntPrincHashEntPrefix(idBytes, entitlementID)
	lower, upper := grantDigestSpec.bucketBounds(idBytes, entitlementID, bucket)
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
		val, closer, getErr := e.db.Get(encodeGrantKey(idBytes, externalID))
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

// addGrant records r's insertion into its entitlement's grant digest.
func (m *digestMutator) addGrant(idBytes []byte, r *v3.GrantRecord) error {
	return m.grantDelta(idBytes, r, m.addHash)
}

// removeGrant records r's removal from its entitlement's grant digest.
func (m *digestMutator) removeGrant(idBytes []byte, r *v3.GrantRecord) error {
	return m.grantDelta(idBytes, r, m.removeHash)
}

func (m *digestMutator) grantDelta(idBytes []byte, r *v3.GrantRecord, apply func(idBytes []byte, partition string, bucketHash, contentHash []byte) error) error {
	ent := r.GetEntitlement()
	princ := r.GetPrincipal()
	if ent == nil || princ == nil {
		return nil // not in the hash index → not in the digest
	}
	bh := principalBucketHash(princ.GetResourceTypeId(), princ.GetResourceId())
	return apply(idBytes, ent.GetEntitlementId(), bh, grantContentHash(r))
}
