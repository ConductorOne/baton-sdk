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
		if err := unmarshalRecord(iter.Value(), v); err != nil {
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

// PaginateGrantsBySync returns up to `limit` grants from the
// primary-key range, starting strictly after `cursor`. Returns the
// next cursor (empty if no more) plus the materialized records.
//
// Uses a per-call grantReadArena to pre-allocate the GrantRecord +
// nested EntitlementRef + PrincipalRef slots. The vtproto-generated
// UnmarshalVT detects the pre-set pointers and reuses the arena
// slots instead of heap-allocating a fresh nested struct per record,
// collapsing 2N nested allocs into 2 slice allocs per page. The
// arena lifetime ends with this function — callers receive pointers
// into the arena's backing arrays, so retention is intentional.
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
	arena := newGrantReadArena(limit)
	idx := 0
	prefix := encodeGrantPrefix(idBytes)
	records, next, err := iteratePrimaryPageWithKey(ctx, e.db, prefix, cursorBytes, limit, func() *v3.GrantRecord {
		slot := arena.nextSlot(idx)
		idx++
		return slot
	})
	if err != nil {
		return nil, "", err
	}
	for _, r := range records {
		arena.reconcileAbsentFields(r)
	}
	return records, next, nil
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
		err = unmarshalRecord(val, r)
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

// PaginateGrantPrincipalKeysByEntitlement scans the existing by_entitlement
// index and returns only principal identity keys for each matching grant. The
// key format is principal_resource_type + "\x00" + principal_resource_id,
// matching pkg/sync/expand's descendantGrantKey.
func (e *Engine) PaginateGrantPrincipalKeysByEntitlement(
	ctx context.Context, syncID, entitlementID, cursor string, limit int,
) ([]string, string, error) {
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
	out := make([]string, 0, limit)
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
		principalRT, principalID, ok := decodeTwoTupleComponents(iter.Key(), indexPrefix)
		if !ok {
			continue
		}
		lastReturnedKey = append(lastReturnedKey[:0], iter.Key()...)
		out = append(out, principalRT+"\x00"+principalID)
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

// PaginateGrantsByEntitlementPrincipal uses the by_entitlement index narrowed
// to the entitlement_id + principal tuple. This is the hot path for grant
// expansion, where callers repeatedly ask whether a single principal already
// has a grant on a descendant entitlement.
func (e *Engine) PaginateGrantsByEntitlementPrincipal(
	ctx context.Context, syncID, entitlementID, principalRT, principalID, cursor string, limit int,
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
	outCap := limit
	if outCap > 16 {
		outCap = 16
	}
	indexPrefix := encodeGrantByEntitlementPrincipalPrefix(idBytes, entitlementID, principalRT, principalID)
	lower, upper := rangeAfter(indexPrefix, cursorBytes)
	iter, err := e.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, "", fmt.Errorf("page iter: %w", err)
	}
	defer iter.Close()
	out := make([]*v3.GrantRecord, 0, outCap)
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
				continue
			}
			return nil, "", fmt.Errorf("paginate: get primary: %w", getErr)
		}
		r := &v3.GrantRecord{}
		err = unmarshalRecord(val, r)
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
		err = unmarshalRecord(val, r)
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

// PaginateGrantsByEntitlementResource walks the by_entitlement_resource
// index — all grants in `syncID` whose entitlement's resource is
// (entRT, entRID). Cursor is the index key.
//
// Drives Adapter.ListGrants and ListWithAnnotationsForResourcePage
// when req.Resource is set, matching SQLite's `listGrantsGeneric`
// filter on grants.resource_id / resource_type_id (the entitlement-
// side resource columns). The pre-existing Pebble path used
// PaginateGrantsByPrincipal here, which returned empty for the
// common "grants on this group" semantic.
func (e *Engine) PaginateGrantsByEntitlementResource(
	ctx context.Context, syncID, entRT, entRID, cursor string, limit int,
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
	indexPrefix := encodeGrantByEntitlementResourcePrefix(idBytes, entRT, entRID)
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
				continue
			}
			return nil, "", fmt.Errorf("paginate: get primary: %w", getErr)
		}
		r := &v3.GrantRecord{}
		err = unmarshalRecord(val, r)
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

// PaginateGrantsByPrincipalResourceType walks the by-principal-RT
// index. Cursor is the index key. Drives the new fast path for
// the Adapter's ListGrantsForResourceType.
func (e *Engine) PaginateGrantsByPrincipalResourceType(
	ctx context.Context, syncID, principalRT, cursor string, limit int,
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
	indexPrefix := encodeGrantByPrincipalResourceTypePrefix(idBytes, principalRT)
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
				continue
			}
			return nil, "", fmt.Errorf("paginate: get primary: %w", getErr)
		}
		r := &v3.GrantRecord{}
		err = unmarshalRecord(val, r)
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

// PaginateGrantsByNeedsExpansion returns a page of grants whose
// NeedsExpansion flag is set. Backs the GrantStore
// PendingExpansionPage path; the SQLite equivalent is a query
// guarded by the partial index `WHERE needs_expansion = 1`.
//
// Cursor is the needs_expansion index key.
func (e *Engine) PaginateGrantsByNeedsExpansion(
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
	indexPrefix := encodeGrantByNeedsExpansionPrefix(idBytes)
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
				// Orphan index entry; skip.
				continue
			}
			return nil, "", fmt.Errorf("paginate: get primary: %w", getErr)
		}
		r := &v3.GrantRecord{}
		err = unmarshalRecord(val, r)
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
		err = unmarshalRecord(val, r)
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
		err = unmarshalRecord(val, r)
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
