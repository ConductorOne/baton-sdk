package pebble

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// DefaultPageSize matches the SQLite c1file's maxPageSize (10000).
// Callers passing 0 or a value > MaxPageSize get clamped to this.
const DefaultPageSize = 10000

// MaxPageSize caps a single List call's record count. Matches
// sql_helpers.maxPageSize for consistency with the SQLite engine —
// some clients (the syncer, c1 backend) assume both engines clamp
// identically.
const MaxPageSize = 10000

// clampPageSize returns the SQLite-compatible page size: 0 or
// >MaxPageSize → DefaultPageSize; anything else stays as-is.
func clampPageSize(requested uint32) int {
	if requested == 0 || requested > MaxPageSize {
		return DefaultPageSize
	}
	return int(requested)
}

// encodeCursor returns a base64-encoded opaque cursor for the given
// raw pebble key. The cursor is meant to be round-tripped by the
// caller verbatim — anything decodeable is valid; anything else
// returns ErrInvalidPageToken.
func encodeCursor(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(key)
}

// decodeCursor parses a base64-encoded cursor back to raw pebble
// key bytes. Returns nil on empty input (start of iteration). Any
// non-empty input that fails to decode returns ErrInvalidPageToken
// so the caller can distinguish a malformed token from a missing one.
func decodeCursor(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidPageToken, err)
	}
	return b, nil
}

// rangeAfter returns the half-open Pebble range [start, upper) where
// `start` is positioned to be strictly greater than `cursor`. If
// `cursor` is empty, returns the prefix itself.
//
// The trick: pebble.IterOptions.LowerBound is inclusive, so we append
// 0x00 to the cursor to make the iteration start at any key
// lexicographically greater than `cursor` — there cannot be a key
// equal to `cursor + 0x00` for our tuple-encoded keyspace because
// tuple-encoded strings always end at the separator (0x00) or
// end-of-prefix, never with a bare 0x00 byte at the tail.
func rangeAfter(prefix, cursor []byte) ([]byte, []byte) {
	upper := upperBoundOf(prefix)
	if len(cursor) == 0 {
		return prefix, upper
	}
	lower := make([]byte, 0, len(cursor)+1)
	lower = append(lower, cursor...)
	lower = append(lower, 0x00)
	return lower, upper
}

// iteratePrimaryPageWithKey iterates over a [prefix, upperBoundOf(prefix))
// range starting strictly after `cursor`, decodes up to limit records
// via unmarshal, and captures the key of the LAST RETURNED row to
// emit as the next-page cursor.
//
// Walks the iterator capturing each key+value into `out` until limit
// is hit. On the next iter step (limit+1), records hasMore=true and
// breaks. Feeding the returned cursor back results in the next page
// starting strictly after the last returned row.
func iteratePrimaryPageWithKey[T proto.Message](
	ctx context.Context,
	db *pebble.DB,
	prefix, cursor []byte,
	limit int,
	newT func() T,
) ([]T, string, error) {
	if limit <= 0 {
		limit = DefaultPageSize
	}
	lower, upper := rangeAfter(prefix, cursor)
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, "", fmt.Errorf("page iter: %w", err)
	}
	defer iter.Close()
	out := make([]T, 0, limit)
	var lastReturnedKey []byte
	hasMore := false
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		if len(out) == limit {
			hasMore = true
			break
		}
		v := newT()
		if err := proto.Unmarshal(iter.Value(), v); err != nil {
			return nil, "", fmt.Errorf("page unmarshal: %w", err)
		}
		lastReturnedKey = append(lastReturnedKey[:0], iter.Key()...)
		out = append(out, v)
	}
	if err := iter.Error(); err != nil {
		return nil, "", err
	}
	var nextCursor string
	if hasMore {
		nextCursor = encodeCursor(lastReturnedKey)
	}
	return out, nextCursor, nil
}

// === Paginated grant variants ===

// grantReadArena batches the OUTER v3.GrantRecord allocations done
// when hydrating a page of grants via proto.Unmarshal. Each iter step
// of the page-read loop allocates a fresh GrantRecord; for a 1 M
// paginated read in 10 k chunks that's 1 M outer-struct allocs +
// associated memclr work. Arena collapses these to 100 slice allocs
// (one per page).
//
// We do NOT pre-populate nested fields (Entitlement/Principal/
// DiscoveredAt). An earlier attempt to do so (paginate.go, run #50)
// only saved the OUTER GrantRecord allocations, not the nested ones,
// while wasting memory on unused pre-populated arena slots at smaller
// scales. proto.Unmarshal's nested-message reuse path didn't trigger
// on our pre-populated pointers — the protobuf runtime's actual
// behavior differed from the consumeMessageInfo source-level read.
// Leaving nested message allocation to the runtime.
type grantReadArena struct {
	grants []v3.GrantRecord
}

// PaginateGrantsBySync returns up to `limit` grants from the
// primary-key range, starting strictly after `cursor`. Returns the
// next cursor (empty if no more) plus the materialized records.
//
// The page's proto.Unmarshal work is parallelized via BATCHED dispatch
// to a worker pool. Main goroutine iterates Pebble (iter.Value()'s
// storage is invalidated by iter.Next, so iteration must be serial),
// copies wire bytes into a per-batch concatenated buffer, and
// dispatches the batch to a worker. Workers proto.Unmarshal each
// record in their batch into pre-allocated arena slots.
//
// Batched dispatch (vs the per-record dispatch attempt #54) avoids:
//
//   - 1 M individual `make([]byte, N)` allocs for value buffers
//     (one slab per batch instead)
//   - 1 M channel send + receive pairs (≈64 × fewer at batchSize=64)
//
// At the 1 M paginated read bench, proto.Unmarshal is ≈470 ms of the
// 974 ms wallclock. 4-way parallel decode targets ≈120 ms decode +
// ≈50 ms dispatch/copy overhead = ≈170 ms total.
func (e *Engine) PaginateGrantsBySync(
	ctx context.Context, syncID, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, "", err
	}
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	prefix := encodeGrantPrefix(idBytes)
	lower, upper := rangeAfter(prefix, cursorBytes)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, "", fmt.Errorf("page iter: %w", err)
	}
	defer iter.Close()

	// Pre-allocate arena slots up to limit. Workers index into these
	// slots directly; no append, no race.
	arena := &grantReadArena{grants: make([]v3.GrantRecord, limit)}

	const (
		pageUnmarshalWorkers = 4
		unmarshalBatchSize   = 64
	)

	// Per-batch buffer: one concatenated []byte for all the record
	// values in the batch, plus per-record end-offsets. Workers split
	// the buffer by offsets and unmarshal each slice into
	// arena.grants[startIdx + i].
	type unmarshalBatch struct {
		startIdx int    // first arena.grants index this batch covers
		count    int    // records in this batch
		valueBuf []byte // concatenated value bytes
		ends     []int  // ends[i] = absolute end offset of record i in valueBuf
	}
	jobs := make(chan *unmarshalBatch, pageUnmarshalWorkers*2)

	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	setErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	wg.Add(pageUnmarshalWorkers)
	for w := 0; w < pageUnmarshalWorkers; w++ {
		go func() {
			defer wg.Done()
			for b := range jobs {
				prev := 0
				for i := 0; i < b.count; i++ {
					end := b.ends[i]
					if err := proto.Unmarshal(b.valueBuf[prev:end], &arena.grants[b.startIdx+i]); err != nil {
						setErr(fmt.Errorf("page unmarshal: %w", err))
						return
					}
					prev = end
				}
			}
		}()
	}

	// flushBatch sends `cur` to workers and prepares a fresh batch.
	newBatch := func(startIdx int) *unmarshalBatch {
		return &unmarshalBatch{
			startIdx: startIdx,
			valueBuf: make([]byte, 0, unmarshalBatchSize*512), // ~512 B/record estimate
			ends:     make([]int, 0, unmarshalBatchSize),
		}
	}
	cur := newBatch(0)

	count := 0
	var lastReturnedKey []byte
	hasMore := false
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			close(jobs)
			wg.Wait()
			return nil, "", err
		}
		if count == limit {
			hasMore = true
			break
		}
		v := iter.Value()
		cur.valueBuf = append(cur.valueBuf, v...)
		cur.ends = append(cur.ends, len(cur.valueBuf))
		cur.count++
		lastReturnedKey = append(lastReturnedKey[:0], iter.Key()...)
		count++
		if cur.count == unmarshalBatchSize {
			jobs <- cur
			cur = newBatch(count)
		}
	}
	if cur.count > 0 {
		jobs <- cur
	}
	close(jobs)
	wg.Wait()
	if iterErr := iter.Error(); iterErr != nil {
		return nil, "", iterErr
	}
	if firstErr != nil {
		return nil, "", firstErr
	}

	out := make([]*v3.GrantRecord, count)
	for i := 0; i < count; i++ {
		out[i] = &arena.grants[i]
	}
	var nextCursor string
	if hasMore {
		nextCursor = encodeCursor(lastReturnedKey)
	}
	return out, nextCursor, nil
}

// PaginateGrantsByEntitlement uses the by_entitlement index. The
// index keys carry the principal-rt/principal-id/external-id tail;
// each match triggers a secondary primary-key Get to materialize the
// full grant. Cursor is the index key, not the primary key.
func (e *Engine) PaginateGrantsByEntitlement(
	ctx context.Context, syncID, entitlementID, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, "", err
	}
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	indexPrefix := encodeGrantByEntitlementPrefix(idBytes, entitlementID)
	lower, upper := rangeAfter(indexPrefix, cursorBytes)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, "", fmt.Errorf("page iter: %w", err)
	}
	defer iter.Close()
	out := make([]*v3.GrantRecord, 0, limit)
	var lastReturnedKey []byte
	hasMore := false
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		if len(out) == limit {
			hasMore = true
			break
		}
		externalID := lastTupleComponent(iter.Key(), indexPrefix)
		if externalID == "" {
			continue
		}
		val, closer, getErr := e.db.Get(encodeGrantKey(idBytes, externalID))
		if getErr != nil {
			if errors.Is(getErr, pebble.ErrNotFound) {
				// Orphan index entry — primary deleted before the
				// index. Reconcile via fsck; keep iterating.
				continue
			}
			return nil, "", fmt.Errorf("paginate: get primary: %w", getErr)
		}
		r := &v3.GrantRecord{}
		err = proto.Unmarshal(val, r)
		closer.Close()
		if err != nil {
			return nil, "", err
		}
		lastReturnedKey = append(lastReturnedKey[:0], iter.Key()...)
		out = append(out, r)
	}
	if err := iter.Error(); err != nil {
		return nil, "", err
	}
	var nextCursor string
	if hasMore {
		nextCursor = encodeCursor(lastReturnedKey)
	}
	return out, nextCursor, nil
}

// PaginateGrantsByPrincipal uses the by_principal index. Same shape
// as PaginateGrantsByEntitlement.
func (e *Engine) PaginateGrantsByPrincipal(
	ctx context.Context, syncID, principalRT, principalID, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, "", err
	}
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	indexPrefix := encodeGrantByPrincipalPrefix(idBytes, principalRT, principalID)
	lower, upper := rangeAfter(indexPrefix, cursorBytes)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, "", fmt.Errorf("page iter: %w", err)
	}
	defer iter.Close()
	out := make([]*v3.GrantRecord, 0, limit)
	var lastReturnedKey []byte
	hasMore := false
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		if len(out) == limit {
			hasMore = true
			break
		}
		externalID := lastTupleComponent(iter.Key(), indexPrefix)
		if externalID == "" {
			continue
		}
		val, closer, getErr := e.db.Get(encodeGrantKey(idBytes, externalID))
		if getErr != nil {
			if errors.Is(getErr, pebble.ErrNotFound) {
				// Orphan index entry — primary deleted before the
				// index. Reconcile via fsck; keep iterating.
				continue
			}
			return nil, "", fmt.Errorf("paginate: get primary: %w", getErr)
		}
		r := &v3.GrantRecord{}
		err = proto.Unmarshal(val, r)
		closer.Close()
		if err != nil {
			return nil, "", err
		}
		lastReturnedKey = append(lastReturnedKey[:0], iter.Key()...)
		out = append(out, r)
	}
	if err := iter.Error(); err != nil {
		return nil, "", err
	}
	var nextCursor string
	if hasMore {
		nextCursor = encodeCursor(lastReturnedKey)
	}
	return out, nextCursor, nil
}

// === Paginated resource / entitlement / resource-type variants ===

// PaginateResourcesBySync returns a page of resources in primary
// key order.
func (e *Engine) PaginateResourcesBySync(
	ctx context.Context, syncID, cursor string, limit int,
) ([]*v3.ResourceRecord, string, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, "", err
	}
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	prefix := encodeResourcePrefix(idBytes)
	return iteratePrimaryPageWithKey(ctx, e.db, prefix, cursorBytes, limit, func() *v3.ResourceRecord {
		return &v3.ResourceRecord{}
	})
}

// PaginateResourcesByParent uses the by_parent index.
func (e *Engine) PaginateResourcesByParent(
	ctx context.Context, syncID, parentRT, parentID, cursor string, limit int,
) ([]*v3.ResourceRecord, string, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, "", err
	}
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	indexPrefix := encodeResourceByParentPrefix(idBytes, parentRT, parentID)
	lower, upper := rangeAfter(indexPrefix, cursorBytes)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, "", fmt.Errorf("page iter: %w", err)
	}
	defer iter.Close()
	out := make([]*v3.ResourceRecord, 0, limit)
	var lastReturnedKey []byte
	hasMore := false
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		if len(out) == limit {
			hasMore = true
			break
		}
		childRT, childID, ok := decodeTwoTupleComponents(iter.Key(), indexPrefix)
		if !ok {
			continue
		}
		val, closer, getErr := e.db.Get(encodeResourceKey(idBytes, childRT, childID))
		if getErr != nil {
			if errors.Is(getErr, pebble.ErrNotFound) {
				continue
			}
			return nil, "", fmt.Errorf("paginate: get primary: %w", getErr)
		}
		r := &v3.ResourceRecord{}
		err = proto.Unmarshal(val, r)
		closer.Close()
		if err != nil {
			return nil, "", err
		}
		lastReturnedKey = append(lastReturnedKey[:0], iter.Key()...)
		out = append(out, r)
	}
	if err := iter.Error(); err != nil {
		return nil, "", err
	}
	var nextCursor string
	if hasMore {
		nextCursor = encodeCursor(lastReturnedKey)
	}
	return out, nextCursor, nil
}

// PaginateResourceTypesBySync returns a page of resource_types.
func (e *Engine) PaginateResourceTypesBySync(
	ctx context.Context, syncID, cursor string, limit int,
) ([]*v3.ResourceTypeRecord, string, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, "", err
	}
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	prefix := encodeResourceTypePrefix(idBytes)
	return iteratePrimaryPageWithKey(ctx, e.db, prefix, cursorBytes, limit, func() *v3.ResourceTypeRecord {
		return &v3.ResourceTypeRecord{}
	})
}

// PaginateEntitlementsBySync returns a page of entitlements in
// primary key order.
func (e *Engine) PaginateEntitlementsBySync(
	ctx context.Context, syncID, cursor string, limit int,
) ([]*v3.EntitlementRecord, string, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, "", err
	}
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	prefix := encodeEntitlementPrefix(idBytes)
	return iteratePrimaryPageWithKey(ctx, e.db, prefix, cursorBytes, limit, func() *v3.EntitlementRecord {
		return &v3.EntitlementRecord{}
	})
}

// PaginateEntitlementsByResource uses the by_resource index.
func (e *Engine) PaginateEntitlementsByResource(
	ctx context.Context, syncID, resourceTypeID, resourceID, cursor string, limit int,
) ([]*v3.EntitlementRecord, string, error) {
	idBytes, err := e.resolveSyncBytes(syncID)
	if err != nil {
		return nil, "", err
	}
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	indexPrefix := encodeEntitlementByResourcePrefix(idBytes, resourceTypeID, resourceID)
	lower, upper := rangeAfter(indexPrefix, cursorBytes)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, "", fmt.Errorf("page iter: %w", err)
	}
	defer iter.Close()
	out := make([]*v3.EntitlementRecord, 0, limit)
	var lastReturnedKey []byte
	hasMore := false
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		if len(out) == limit {
			hasMore = true
			break
		}
		externalID := lastTupleComponent(iter.Key(), indexPrefix)
		if externalID == "" {
			continue
		}
		val, closer, getErr := e.db.Get(encodeEntitlementKey(idBytes, externalID))
		if getErr != nil {
			if errors.Is(getErr, pebble.ErrNotFound) {
				continue
			}
			return nil, "", fmt.Errorf("paginate: get primary: %w", getErr)
		}
		r := &v3.EntitlementRecord{}
		err = proto.Unmarshal(val, r)
		closer.Close()
		if err != nil {
			return nil, "", err
		}
		lastReturnedKey = append(lastReturnedKey[:0], iter.Key()...)
		out = append(out, r)
	}
	if err := iter.Error(); err != nil {
		return nil, "", err
	}
	var nextCursor string
	if hasMore {
		nextCursor = encodeCursor(lastReturnedKey)
	}
	return out, nextCursor, nil
}
