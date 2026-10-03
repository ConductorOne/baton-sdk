package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"go.uber.org/zap"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/codec"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble/internal/rawdb"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

// validateImportedGrantDigestStateLocked verifies the grant-digest
// keyspace of an imported artifact against the grant primaries,
// read-only, before any reader is served. Every digest keyspace byte of
// an imported .c1z is attacker-authored, so self-consistency of those
// bytes proves nothing: a forged root plus a global root equal to its
// fold would seal grants the engine never hashed.
//
// It checks only state the engine would trust: present digest nodes
// under the current ABI stamp, with no interrupted-build marker. Older,
// unstamped, or malformed-stamp state is already untrusted
// (verifyGrantDigestABI drops it or gates it stale), so it is never
// rejected here. Absent digest state is the cold case and accepts.
//
// What is verified:
//   - per-partition roots recomputed from the grant primary records
//     (count + XOR fold of grant content hashes — the same computation
//     recomputeGrantDigestGlobalRootLocked folds) must match the
//     stored roots exactly, including orphan-grant partitions and
//     zero-grant entitlements' {count:0} roots;
//   - no phantom roots (partitions with no grants and no entitlement
//     record) and no missing grant-bearing roots;
//   - the global root must equal the fold of the correct roots;
//   - the by_entitlement_principal_hash index rows the read paths
//     consume must match the same recomputation;
//   - every root value must decode (unpackDigestRoot).
//
// An honest empty store (zero grants, zero entitlements) with the
// canonical {count:0, zero-hash} global root accepts.
//
// Cost: O(grant primaries + index/digest nodes), no sort, no spill, no
// proto decode; see BenchmarkImportValidation. EndSync's repair fast
// path stays a single point-Get because this runs at open.
func (e *Engine) validateImportedGrantDigestStateLocked(ctx context.Context) error {
	if !e.db.GrantDigestsPresent() || e.grantDigestStateUntrusted() {
		return nil
	}

	// Pass 1: recompute per-partition {count, XOR} folds from the grant
	// primary records. Keys iterate in partition-contiguous order.
	type recomputed struct {
		count int64
		xor   uint64
	}
	computed := make(map[string]*recomputed)
	anyGrants := false
	giter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: GrantLowerBound(),
		UpperBound: GrantUpperBound(),
	})
	if err != nil {
		return err
	}
	var (
		srcKeys      []grantSourceFact
		tupleScratch []byte
		lastRawPart  []byte
		cur          *recomputed
	)
	for giter.First(); giter.Valid(); giter.Next() {
		if err := ctx.Err(); err != nil {
			_ = giter.Close()
			return err
		}
		sep4, ok := rawdb.SplitGrantPrimaryKey(giter.Key())
		if !ok {
			// Malformed primary: the seal-time build drops these from
			// digests and logs; validation mirrors that (they cannot
			// be represented in any honest digest of this keyspace).
			continue
		}
		anyGrants = true
		// Grants iterate partition-contiguously, so the map key is built
		// once per partition. The digest keyspace's partition region is
		// the raw partition re-encoded through AppendTupleStrings
		// (rawdb.DigestPartitionPrefix): escape the raw splice so map keys
		// compare equal to the root node's partition bytes.
		rawPart := giter.Key()[grantPrimaryKeyPrefixLen:sep4]
		if cur == nil || !bytes.Equal(rawPart, lastRawPart) {
			lastRawPart = append(lastRawPart[:0], rawPart...)
			part := string(codec.AppendTupleStrings(nil, string(rawPart)))
			cur = computed[part]
			if cur == nil {
				cur = &recomputed{}
				computed[part] = cur
			}
		}
		isImmutable, srcs, ferr := scanGrantContentFactsRawBytes(giter.Value(), srcKeys[:0])
		if ferr != nil {
			_ = giter.Close()
			return fmt.Errorf("pebble: imported grant record failed content-fact scan: %w", ferr)
		}
		srcKeys = srcs
		if len(srcs) > 1 {
			srcs = sortGrantSourceFacts(srcs)
			srcKeys = srcs
		}
		ch64, tuple := grantContentHash64(tupleScratch, giter.Key()[grantPrimaryKeyPrefixLen:], isImmutable, srcs)
		tupleScratch = tuple
		cur.count++
		cur.xor ^= ch64
	}
	if err := giter.Error(); err != nil {
		_ = giter.Close()
		return err
	}
	if err := giter.Close(); err != nil {
		return err
	}

	// Zero-grant entitlement partitions: an entitlement RECORD whose
	// partition has no grants legitimately holds a {count:0} root (see
	// findMissingGrantDigestPartitionsLocked — the entitlement primary
	// tail IS the digest partition, a raw splice, no decode).
	zeroGrantEnt := make(map[string]struct{})
	anyEntitlements := false
	eiter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: EntitlementLowerBound(),
		UpperBound: EntitlementUpperBound(),
	})
	if err != nil {
		return err
	}
	for eiter.First(); eiter.Valid(); eiter.Next() {
		if err := ctx.Err(); err != nil {
			_ = eiter.Close()
			return err
		}
		key := eiter.Key()
		if len(key) <= grantPrimaryKeyPrefixLen {
			continue
		}
		anyEntitlements = true
		part := string(codec.AppendTupleStrings(nil, string(key[grantPrimaryKeyPrefixLen:])))
		if _, has := computed[part]; !has {
			zeroGrantEnt[part] = struct{}{}
		}
	}
	if err := eiter.Error(); err != nil {
		_ = eiter.Close()
		return err
	}
	if err := eiter.Close(); err != nil {
		return err
	}

	// Honest empty store: zero grants and zero entitlements, with the
	// canonical {count:0, zero-hash} global root and no partition roots.
	if !anyGrants && !anyEntitlements {
		global, ok, gerr := e.readStoredGlobalRoot()
		if gerr != nil {
			return gerr
		}
		if !ok {
			return fmt.Errorf(
				"pebble: imported digest state advertises presence over an empty store but carries no whole-file global root")
		}
		var zero [hashLen]byte
		if global.Count == 0 && bytes.Equal(global.Hash, zero[:]) {
			return nil
		}
		return fmt.Errorf(
			"pebble: imported digest state advertises a nonempty global root (count %d) over an empty store", global.Count)
	}

	// Pass 2: every STORED per-partition root must match the
	// recomputation (or a zero-grant entitlement's {count:0, zero}
	// root); phantom roots are rejected; every grant-bearing partition
	// must be present.
	var foldXOR uint64
	var foldCount int64
	seen := make(map[string]struct{})
	riter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: DigestLowerBound(),
		UpperBound: DigestUpperBound(),
	})
	if err != nil {
		return err
	}
	for riter.First(); riter.Valid(); riter.Next() {
		if err := ctx.Err(); err != nil {
			_ = riter.Close()
			return err
		}
		if !isGrantDigestRootKey(riter.Key()) {
			continue
		}
		part, ok := isGrantDigestRootKeyPartition(riter.Key())
		if !ok {
			_ = riter.Close()
			return fmt.Errorf(
				"pebble: imported digest root key does not parse: %x", riter.Key())
		}
		_, count, digest, ok := unpackDigestRoot(riter.Value())
		if !ok {
			_ = riter.Close()
			return fmt.Errorf(
				"pebble: imported digest root for entitlement partition %x is malformed", part)
		}
		want, hasGrants := computed[string(part)]
		_, isZeroGrantEnt := zeroGrantEnt[string(part)]
		switch {
		case hasGrants:
			var wantHash [hashLen]byte
			binary.BigEndian.PutUint64(wantHash[:], want.xor)
			if count != want.count || !bytes.Equal(digest, wantHash[:]) {
				_ = riter.Close()
				return fmt.Errorf(
					"pebble: imported digest root for entitlement partition %x does not match its grant primaries (stored count %d, recomputed %d)",
					part, count, want.count)
			}
		case isZeroGrantEnt:
			var zero [hashLen]byte
			if count != 0 || !bytes.Equal(digest, zero[:]) {
				_ = riter.Close()
				return fmt.Errorf(
					"pebble: imported digest root for zero-grant entitlement partition %x is not the canonical {count:0} root (count %d)", part, count)
			}
		default:
			_ = riter.Close()
			return fmt.Errorf(
				"pebble: imported digest state has a phantom root for entitlement partition %x (no grants, no entitlement record)", part)
		}
		seen[string(part)] = struct{}{}
		foldXOR ^= binary.BigEndian.Uint64(digest)
		foldCount += count
	}
	if err := riter.Error(); err != nil {
		_ = riter.Close()
		return err
	}
	if err := riter.Close(); err != nil {
		return err
	}
	for part := range computed {
		if _, ok := seen[part]; !ok {
			return fmt.Errorf(
				"pebble: imported digest state is missing a root for grant-bearing entitlement partition %x", []byte(part))
		}
	}

	// Pass 3: the by_entitlement_principal_hash index rows the read
	// paths consume must carry the recomputed grant content hash. One
	// Get per index row's reconstructed primary — the same O(grants)
	// curve as the fold. A correct root over a wrong index still
	// misdirects readers, so the root check alone is not enough.
	iiter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: GrantByEntPrincHashLowerBound(),
		UpperBound: GrantByEntPrincHashUpperBound(),
	})
	if err != nil {
		return err
	}
	var (
		idxSrcKeys      []grantSourceFact
		idxTupleScratch []byte
		idxPrimScratch  []byte
	)
	for iiter.First(); iiter.Valid(); iiter.Next() {
		if err := ctx.Err(); err != nil {
			_ = iiter.Close()
			return err
		}
		primaryKey, ok := grantPrimaryKeyFromHashIndexKey(idxPrimScratch[:0], iiter.Key())
		idxPrimScratch = primaryKey
		if !ok {
			_ = iiter.Close()
			return fmt.Errorf(
				"pebble: imported hash-index row key does not parse as a grant entry: %x", iiter.Key())
		}
		val, closer, gerr := e.db.Get(primaryKey)
		if gerr != nil {
			_ = iiter.Close()
			if err := errorFromPebbleGet(gerr); err != nil {
				return err
			}
			return fmt.Errorf(
				"pebble: imported hash-index row references a grant primary that does not exist: %x", primaryKey)
		}
		isImmutable, srcs, ferr := scanGrantContentFactsRawBytes(val, idxSrcKeys[:0])
		idxSrcKeys = srcs
		if ferr != nil {
			closer.Close()
			_ = iiter.Close()
			return fmt.Errorf("pebble: imported grant record failed content-fact scan: %w", ferr)
		}
		if len(srcs) > 1 {
			srcs = sortGrantSourceFacts(srcs)
			idxSrcKeys = srcs
		}
		ch64, tuple := grantContentHash64(idxTupleScratch, primaryKey[grantPrimaryKeyPrefixLen:], isImmutable, srcs)
		idxTupleScratch = tuple
		closer.Close()
		var want [hashLen]byte
		binary.BigEndian.PutUint64(want[:], ch64)
		if len(iiter.Value()) != hashLen || !bytes.Equal(iiter.Value(), want[:]) {
			_ = iiter.Close()
			return fmt.Errorf(
				"pebble: imported hash-index row value does not match the recomputed grant content hash at %x", iiter.Key())
		}
	}
	if err := iiter.Error(); err != nil {
		_ = iiter.Close()
		return err
	}
	if err := iiter.Close(); err != nil {
		return err
	}

	// The global root must equal the fold of the CORRECT roots.
	global, ok, err := e.readStoredGlobalRoot()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf(
			"pebble: imported digest state advertises partition roots but no whole-file global root")
	}
	if global.Count != foldCount || !bytesEqualUint64BE(global.Hash, foldXOR) {
		return fmt.Errorf(
			"pebble: imported global grant digest root does not equal the fold of its verified partition roots (stored count %d, recomputed %d)",
			global.Count, foldCount)
	}

	ctxzap.Extract(ctx).Debug("pebble: imported grant digest state verified against grant primaries",
		zap.Int("partitions", len(computed)),
		zap.Int64("grants", foldCount))
	return nil
}

// readStoredGlobalRoot reads the whole-file grant digest root node and
// rejects a value that does not decode.
func (e *Engine) readStoredGlobalRoot() (DigestRoot, bool, error) {
	val, closer, err := e.db.Get(rawdb.GlobalGrantDigestNodeKey())
	if err != nil {
		if err := errorFromPebbleGet(err); err != nil {
			return DigestRoot{}, false, err
		}
		return DigestRoot{}, false, nil
	}
	defer closer.Close()
	count, digest, ok := unpackDigestLeaf(val)
	if !ok {
		return DigestRoot{}, false, fmt.Errorf(
			"pebble: imported whole-file grant digest root is malformed")
	}
	out := make([]byte, len(digest))
	copy(out, digest)
	return DigestRoot{Hash: out, Count: count}, true, nil
}

// bytesEqualUint64BE reports whether the 8-byte BE hash equals v.
func bytesEqualUint64BE(hash []byte, v uint64) bool {
	if len(hash) != hashLen {
		return false
	}
	return binary.BigEndian.Uint64(hash) == v
}

// errorFromPebbleGet converts a pebble Get error: ErrNotFound maps to
// (nil, false semantics upstream — callers treat a nil return as
// absent); anything else is returned as-is.
func errorFromPebbleGet(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	return err
}
