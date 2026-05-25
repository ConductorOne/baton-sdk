package pebble

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

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

func newGrantReadArena(pageLimit int) *grantReadArena {
	return &grantReadArena{
		grants: make([]v3.GrantRecord, 0, pageLimit),
	}
}

func (a *grantReadArena) allocGrant() *v3.GrantRecord {
	a.grants = append(a.grants, v3.GrantRecord{})
	return &a.grants[len(a.grants)-1]
}

// PaginateGrantsBySync returns up to `limit` grants from the
// primary-key range, starting strictly after `cursor`. Returns the
// next cursor (empty if no more) plus the materialized records.
//
// Uses grantReadArena for the per-iter outer-struct allocations —
// 1 page = 1 arena slice rather than O(page-size) individual mallocs.
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
	arena := newGrantReadArena(limit)
	return iteratePrimaryPageWithKey(ctx, e.db, prefix, cursorBytes, limit, arena.allocGrant)
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
