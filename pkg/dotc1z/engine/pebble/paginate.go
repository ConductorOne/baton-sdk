package pebble

import (
	"bytes"
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
// `cursor` is empty, returns the prefix itself. A cursor that does not
// carry the prefix is rejected with ErrInvalidPageToken: cursors are
// raw keys we minted for THIS keyspace, so a foreign prefix means a
// corrupted or hostile token — accepting one whose bytes sort below
// the prefix would start the scan in a different record type's
// keyspace and serve its values as this type's records.
//
// The trick: pebble.IterOptions.LowerBound is inclusive, so we append
// 0x00 to the cursor to make the iteration start at any key
// lexicographically greater than `cursor` — there cannot be a key
// equal to `cursor + 0x00` for our tuple-encoded keyspace because
// tuple-encoded strings always end at the separator (0x00) or
// end-of-prefix, never with a bare 0x00 byte at the tail.
func rangeAfter(prefix, cursor []byte) ([]byte, []byte, error) {
	upper := upperBoundOf(prefix)
	if len(cursor) == 0 {
		return prefix, upper, nil
	}
	if !bytes.HasPrefix(cursor, prefix) {
		return nil, nil, fmt.Errorf("%w: cursor does not belong to this keyspace", ErrInvalidPageToken)
	}
	lower := make([]byte, 0, len(cursor)+1)
	lower = append(lower, cursor...)
	lower = append(lower, 0x00)
	return lower, upper, nil
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
	lower, upper, rangeErr := rangeAfter(prefix, cursor)
	if rangeErr != nil {
		return nil, "", rangeErr
	}
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

func iterateGrantPrimaryPage(
	ctx context.Context,
	db *pebble.DB,
	prefix, cursor []byte,
	limit int,
) ([]*v3.GrantRecord, string, error) {
	if limit <= 0 {
		limit = DefaultPageSize
	}
	lower, upper, rangeErr := rangeAfter(prefix, cursor)
	if rangeErr != nil {
		return nil, "", rangeErr
	}
	iter, err := db.NewIter(&pebble.IterOptions{
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
		r := &v3.GrantRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
			return nil, "", fmt.Errorf("page unmarshal: %w", err)
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

func getGrantByIdentity(ctx context.Context, db *pebble.DB, id grantIdentity) (*v3.GrantRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	val, closer, err := db.Get(encodeGrantIdentityKey(id))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	r := &v3.GrantRecord{}
	if err := unmarshalRecord(val, r); err != nil {
		return nil, err
	}
	return r, nil
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
func (e *Engine) PaginateGrants(
	ctx context.Context, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	arena := newGrantReadArena(limit)
	idx := 0
	prefix := encodeGrantPrefix()
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

// PaginateGrantsByEntitlement scans the primary grant keyspace under the
// entitlement identity prefix. Cursor is the primary key. Callers resolve
// the identity from structured refs (or the bare-id lookup) — the engine
// never parses id strings here.
func (e *Engine) PaginateGrantsByEntitlement(
	ctx context.Context, entID entitlementIdentity, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	return iterateGrantPrimaryPage(ctx, e.db, encodeGrantPrimaryEntitlementPrefix(entID), cursorBytes, limit)
}

// PaginateGrantPrincipalKeysByEntitlement scans the primary grant keyspace under
// the entitlement identity prefix and returns only principal identity keys for
// each matching grant. The key format is principal_resource_type + "\x00" +
// principal_resource_id, matching pkg/sync/expand's descendantGrantKey.
func (e *Engine) PaginateGrantPrincipalKeysByEntitlement(
	ctx context.Context, entID entitlementIdentity, cursor string, limit int,
) ([]string, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	primaryPrefix := encodeGrantPrimaryEntitlementPrefix(entID)
	lower, upper, err := rangeAfter(primaryPrefix, cursorBytes)
	if err != nil {
		return nil, "", err
	}
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
		principalRT, principalID, ok := decodeTwoTupleComponents(iter.Key(), primaryPrefix)
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

// PaginateGrantsByEntitlementPrincipal uses the structured primary key for a
// point lookup by entitlement + principal. This is the hot path for grant
// expansion, where callers repeatedly ask whether a single principal already
// has a grant on a descendant entitlement.
func (e *Engine) PaginateGrantsByEntitlementPrincipal(
	ctx context.Context, entID entitlementIdentity, principalRT, principalID, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if len(cursorBytes) != 0 {
		return nil, "", nil
	}
	id := grantIdentity{
		entitlement:     entID,
		principalTypeID: principalRT,
		principalID:     principalID,
	}
	r, err := getGrantByIdentity(ctx, e.db, id)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, "", nil
		}
		return nil, "", err
	}
	return []*v3.GrantRecord{r}, "", nil
}

// PaginateGrantsByPrincipal uses the by_principal index. Same shape
// as PaginateGrantsByEntitlement.
func (e *Engine) PaginateGrantsByPrincipal(
	ctx context.Context, principalRT, principalID, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	indexPrefix := encodeGrantByPrincipalPrefix(principalRT, principalID)
	lower, upper, err := rangeAfter(indexPrefix, cursorBytes)
	if err != nil {
		return nil, "", err
	}
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
		components, ok := decodeTupleComponents(iter.Key(), indexPrefix, 4)
		if !ok {
			continue
		}
		id := grantIdentity{
			entitlement: entitlementIdentity{
				resourceTypeID: components[0],
				resourceID:     components[1],
				stripped:       components[2] == idFlagStripped,
				tail:           components[3],
			},
			principalTypeID: principalRT,
			principalID:     principalID,
		}
		r, getErr := getGrantByIdentity(ctx, e.db, id)
		if getErr != nil {
			if errors.Is(getErr, pebble.ErrNotFound) {
				continue
			}
			return nil, "", fmt.Errorf("paginate: get primary: %w", getErr)
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

// PaginateGrantsByEntitlementResource walks the primary grant keyspace for all
// grants whose entitlement's resource is (entRT, entRID). Cursor is the primary
// key.
//
// Drives Adapter.ListGrants and ListWithAnnotationsForResourcePage when
// req.Resource is set, matching SQLite's `listGrantsGeneric` filter on
// grants.resource_id / resource_type_id (the entitlement-side resource columns).
// The pre-existing Pebble path used PaginateGrantsByPrincipal here, which
// returned empty for the common "grants on this group" semantic.
func (e *Engine) PaginateGrantsByEntitlementResource(
	ctx context.Context, entRT, entRID, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	return iterateGrantPrimaryPage(ctx, e.db, encodeGrantPrimaryEntitlementResourcePrefix(entRT, entRID), cursorBytes, limit)
}

// PaginateGrantsByPrincipalResourceType walks the by-principal-RT
// index. Cursor is the index key. Drives the new fast path for
// the Adapter's ListGrantsForResourceType.
func (e *Engine) PaginateGrantsByPrincipalResourceType(
	ctx context.Context, principalRT, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	indexPrefix := encodeGrantByPrincipalResourceTypeIdentityPrefix(principalRT)
	lower, upper, err := rangeAfter(indexPrefix, cursorBytes)
	if err != nil {
		return nil, "", err
	}
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
		components, ok := decodeTupleComponents(iter.Key(), indexPrefix, 5)
		if !ok {
			continue
		}
		id := grantIdentity{
			entitlement: entitlementIdentity{
				resourceTypeID: components[1],
				resourceID:     components[2],
				stripped:       components[3] == idFlagStripped,
				tail:           components[4],
			},
			principalTypeID: principalRT,
			principalID:     components[0],
		}
		r, getErr := getGrantByIdentity(ctx, e.db, id)
		if getErr != nil {
			if errors.Is(getErr, pebble.ErrNotFound) {
				continue
			}
			return nil, "", fmt.Errorf("paginate: get primary: %w", getErr)
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
	ctx context.Context, cursor string, limit int,
) ([]*v3.GrantRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	indexPrefix := encodeGrantByNeedsExpansionPrefix()
	lower, upper, err := rangeAfter(indexPrefix, cursorBytes)
	if err != nil {
		return nil, "", err
	}
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
		components, ok := decodeTupleComponents(iter.Key(), indexPrefix, 6)
		if !ok {
			continue
		}
		id := grantIdentity{
			entitlement: entitlementIdentity{
				resourceTypeID: components[0],
				resourceID:     components[1],
				stripped:       components[2] == idFlagStripped,
				tail:           components[3],
			},
			principalTypeID: components[4],
			principalID:     components[5],
		}
		r, getErr := getGrantByIdentity(ctx, e.db, id)
		if getErr != nil {
			if errors.Is(getErr, pebble.ErrNotFound) {
				continue
			}
			return nil, "", fmt.Errorf("paginate: get primary: %w", getErr)
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
func (e *Engine) PaginateResources(
	ctx context.Context, cursor string, limit int,
) ([]*v3.ResourceRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	prefix := encodeResourcePrefix()
	return iteratePrimaryPageWithKey(ctx, e.db, prefix, cursorBytes, limit, func() *v3.ResourceRecord {
		return &v3.ResourceRecord{}
	})
}

// PaginateResourcesByParent uses the by_parent index.
func (e *Engine) PaginateResourcesByParent(
	ctx context.Context, parentRT, parentID, cursor string, limit int,
) ([]*v3.ResourceRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	indexPrefix := encodeResourceByParentPrefix(parentRT, parentID)
	lower, upper, err := rangeAfter(indexPrefix, cursorBytes)
	if err != nil {
		return nil, "", err
	}
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
		val, closer, getErr := e.db.Get(encodeResourceKey(childRT, childID))
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
func (e *Engine) PaginateResourceTypes(
	ctx context.Context, cursor string, limit int,
) ([]*v3.ResourceTypeRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	prefix := encodeResourceTypePrefix()
	return iteratePrimaryPageWithKey(ctx, e.db, prefix, cursorBytes, limit, func() *v3.ResourceTypeRecord {
		return &v3.ResourceTypeRecord{}
	})
}

// PaginateEntitlementsBySync returns a page of entitlements in
// primary key order.
func (e *Engine) PaginateEntitlements(
	ctx context.Context, cursor string, limit int,
) ([]*v3.EntitlementRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	prefix := encodeEntitlementPrefix()
	return iteratePrimaryPageWithKey(ctx, e.db, prefix, cursorBytes, limit, func() *v3.EntitlementRecord {
		return &v3.EntitlementRecord{}
	})
}

// PaginateEntitlementsByResource uses the entitlement primary key prefix.
func (e *Engine) PaginateEntitlementsByResource(
	ctx context.Context, resourceTypeID, resourceID, cursor string, limit int,
) ([]*v3.EntitlementRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	indexPrefix := encodeEntitlementPrimaryResourcePrefix(resourceTypeID, resourceID)
	lower, upper, err := rangeAfter(indexPrefix, cursorBytes)
	if err != nil {
		return nil, "", err
	}
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
		r := &v3.EntitlementRecord{}
		if err := unmarshalRecord(iter.Value(), r); err != nil {
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
