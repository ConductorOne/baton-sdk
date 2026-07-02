package pebble

import (
	"sort"
	"sync"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	batonGrant "github.com/conductorone/baton-sdk/pkg/types/grant"
)

// expandedGrantImmutableAnnotations is the shared one-element annotation list
// every synthesized grant record carries. Marshal only reads it, so one slice
// serves all records (the Any inside is already a package singleton).
var expandedGrantImmutableAnnotations = []*anypb.Any{expandedGrantImmutableAnnotationAny}

// fillSynthGrantRecord populates a REUSED GrantRecord for one synthesized
// grant. The hot write loops marshal ~50M records per whale expansion and the
// marshaler retains nothing, so one record struct per loop replaces one heap
// allocation per row.
//
// INVARIANT: every field the synthesized writers emit must be assigned here
// unconditionally — a conditionally-set field would leak the previous row's
// value through the reused struct. Sources is intentionally NOT set: the
// writers hand-encode it onto the wire after the base marshal
// (appendGrantSourcesWire); Expansion/NeedsExpansion stay zero for
// synthesized grants.
func fillSynthGrantRecord(r *v3.GrantRecord, rec *synthesizedGrantRecord, now *timestamppb.Timestamp) {
	r.SetExternalId(rec.externalID)
	r.SetEntitlement(rec.entitlement)
	r.SetPrincipal(rec.principal)
	r.SetAnnotations(expandedGrantImmutableAnnotations)
	r.SetDiscoveredAt(now)
}

// Wire tags for the hand-encoded GrantRecord.sources map field. A protobuf
// map field is encoded as a repeated entry message: key is entry field 1,
// value is entry field 2.
var (
	grantSourcesFieldTag = protowire.EncodeTag(9, protowire.BytesType)  // GrantRecord.sources
	grantSourceKeyTag    = protowire.EncodeTag(1, protowire.BytesType)  // entry key (string)
	grantSourceValTag    = protowire.EncodeTag(2, protowire.BytesType)  // entry value (GrantSourceRecord)
	grantSourceDirectTag = protowire.EncodeTag(4, protowire.VarintType) // GrantSourceRecord.is_direct
)

// appendGrantSourcesWire appends the GrantRecord.sources map field (field 9)
// to dst, byte-for-byte identical to what proto.MarshalOptions{Deterministic:
// true} produces for the equivalent map[string]*GrantSourceRecord — entries
// sorted by key, key and value fields both emitted unconditionally, and the
// GrantSourceRecord value carrying only is_direct (the synthesized-grant
// writers never set the other fields). Duplicate entitlement IDs collapse
// last-wins, matching the map construction the reflective path used.
//
// This exists because the reflective deterministic map marshal
// (appendMapDeterministic) allocates via reflect.MapKeys and sorts per record;
// at 50M+ synthesized grants per expansion that was a measurable share of both
// CPU and allocations. sources must be the highest-numbered populated field on
// the record (it is: 9) so appending after the base marshal preserves the
// canonical ascending field order. Byte equality with the reflective marshal
// is pinned by TestAppendGrantSourcesWireMatchesDeterministicProto; it matters
// because the codec equivalence harness asserts equal records produce equal
// bytes.
//
// scratch is a reusable buffer for the sorted copy of sources (the caller's
// slice is not mutated); the (possibly grown) scratch is returned for reuse.
func appendGrantSourcesWire(dst []byte, scratch batonGrant.Sources, sources batonGrant.Sources) ([]byte, batonGrant.Sources) {
	if len(sources) == 0 {
		return dst, scratch
	}
	scratch = append(scratch[:0], sources...)
	sort.SliceStable(scratch, func(i, j int) bool {
		return scratch[i].EntitlementID < scratch[j].EntitlementID
	})
	for i, src := range scratch {
		// Last occurrence of a duplicate key wins (stable sort keeps the
		// original order within equal keys).
		if i+1 < len(scratch) && scratch[i+1].EntitlementID == src.EntitlementID {
			continue
		}
		valLen := 0
		if src.IsDirect {
			valLen = 2 // is_direct tag byte + varint(1)
		}
		keyLen := len(src.EntitlementID)
		entryLen := 1 + protowire.SizeVarint(uint64(keyLen)) + keyLen +
			1 + protowire.SizeVarint(uint64(valLen)) + valLen
		dst = protowire.AppendVarint(dst, grantSourcesFieldTag)
		dst = protowire.AppendVarint(dst, uint64(entryLen)) // #nosec G115 -- entryLen is a small positive length.
		dst = protowire.AppendVarint(dst, grantSourceKeyTag)
		dst = protowire.AppendVarint(dst, uint64(keyLen))
		dst = append(dst, src.EntitlementID...)
		dst = protowire.AppendVarint(dst, grantSourceValTag)
		dst = protowire.AppendVarint(dst, uint64(valLen))
		if src.IsDirect {
			dst = protowire.AppendVarint(dst, grantSourceDirectTag)
			dst = protowire.AppendVarint(dst, 1)
		}
	}
	return dst, scratch
}

// synthKVArenaChunkBytes is the pooled chunk size for synthKVArena. 1MiB
// chunks hold ~2K typical synthesized-grant KV pairs each, so a 250K-row
// flush takes ~130 chunk allocations instead of 500K individual key/val
// copies.
const synthKVArenaChunkBytes = 1 << 20

var synthKVChunkPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, synthKVArenaChunkBytes)
		return &b
	},
}

// synthKVArena hands out stable byte storage for the in-memory synthesized
// grant sort, backed by pooled chunks. Slices returned by copyKV remain valid
// until release; release returns pooled chunks for reuse, so callers must
// drop every arena-backed slice first.
type synthKVArena struct {
	chunks []*[]byte
}

func (a *synthKVArena) alloc(n int) []byte {
	if len(a.chunks) > 0 {
		last := a.chunks[len(a.chunks)-1]
		if cap(*last)-len(*last) >= n {
			start := len(*last)
			*last = (*last)[:start+n]
			return (*last)[start : start+n : start+n]
		}
	}
	var cp *[]byte
	if n > synthKVArenaChunkBytes {
		// Oversized entry: dedicated chunk, not pooled on release.
		b := make([]byte, 0, n)
		cp = &b
	} else {
		cp = synthKVChunkPool.Get().(*[]byte)
	}
	a.chunks = append(a.chunks, cp)
	*cp = (*cp)[:n]
	return (*cp)[:n:n]
}

// copyKV copies key and val into one arena allocation and returns stable
// subslices for each.
func (a *synthKVArena) copyKV(key, val []byte) ([]byte, []byte) {
	buf := a.alloc(len(key) + len(val))
	copy(buf, key)
	copy(buf[len(key):], val)
	return buf[:len(key):len(key)], buf[len(key):]
}

// release returns standard-size chunks to the pool. Every slice previously
// returned by copyKV is invalid after release.
func (a *synthKVArena) release() {
	for _, cp := range a.chunks {
		if cap(*cp) == synthKVArenaChunkBytes {
			*cp = (*cp)[:0]
			synthKVChunkPool.Put(cp)
		}
	}
	a.chunks = nil
}

// synthEntriesPool recycles the per-flush entry slice for the in-memory
// synthesized-grant sort (250K entries per flush by default).
var synthEntriesPool = sync.Pool{
	New: func() any { return new([]synthesizedGrantKV) },
}

// synthRecordsPool recycles the per-flush []synthesizedGrantRecord the
// adapter builds for the engine's synthesized-grant write paths. Each whale
// flush is ~250K records (~40MB); without reuse that backing array was the
// single largest flat allocation in the expansion profile.
var synthRecordsPool = sync.Pool{
	New: func() any { return new([]synthesizedGrantRecord) },
}

func getSynthRecords() *[]synthesizedGrantRecord {
	ptr := synthRecordsPool.Get().(*[]synthesizedGrantRecord)
	*ptr = (*ptr)[:0]
	return ptr
}

// putSynthRecords zeroes the used prefix (releasing the proto/message
// pointers records hold) and returns the buffer to the pool. Callers must
// guarantee the engine did not retain the slice — all Put/ingest paths copy
// key/value bytes out before returning.
func putSynthRecords(ptr *[]synthesizedGrantRecord) {
	clear(*ptr)
	*ptr = (*ptr)[:0]
	synthRecordsPool.Put(ptr)
}
