package pebble

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/cockroachdb/pebble/v2"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
)

// ListGrantsForEntitlements is the batched counterpart to
// ListGrantsForEntitlement (RFC §A4). It walks K entitlements in
// one RPC and returns a flat grant list ordered by visitation;
// callers group by Grant.Entitlement.Id.
//
// Cursor encoding (Option C):
//
//	varint(entitlement_index) || varint(intra_cursor_len) ||
//	intra_cursor_bytes || crc32(list_checksum)
//
// list_checksum is crc32 over the sorted entitlement IDs in the
// request — if the caller changes the entitlement list between
// pages we detect the mismatch and restart from the beginning
// rather than silently mis-paginate.
func (a *Adapter) ListGrantsForEntitlements(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementsResponse, error) {
	syncID, err := a.resolveActiveSyncForReader(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}
	if syncID == "" {
		return nil, ErrNoCurrentSync
	}
	ents := req.GetEntitlements()
	if len(ents) == 0 {
		return reader_v2.GrantsReaderServiceListGrantsForEntitlementsResponse_builder{}.Build(), nil
	}
	limit := int(req.GetPageSize())
	if limit <= 0 {
		limit = DefaultPageSize
	}
	if limit > MaxPageSize {
		limit = MaxPageSize
	}

	listChecksum := entitlementListChecksum(ents)

	startIdx, startIntra, err := decodeBatchCursor(req.GetPageToken(), listChecksum)
	if err != nil {
		return nil, err
	}

	out := make([]*v2.Grant, 0, limit)
	var nextToken string

EntitlementLoop:
	for i := startIdx; i < len(ents); i++ {
		entID := ents[i].GetId()
		if entID == "" {
			continue
		}
		intraCursor := ""
		if i == startIdx {
			intraCursor = startIntra
		}
		for len(out) < limit {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			remaining := limit - len(out)
			records, next, err := a.engine.PaginateGrantsByEntitlement(ctx, syncID, entID, intraCursor, remaining)
			if err != nil {
				return nil, c1zstore.AdaptNotFound(err, pebble.ErrNotFound)
			}
			brokeEarly := false
			var lastIntra string
			for _, rec := range records {
				out = append(out, V3GrantToV2(rec))
				if len(out) == limit {
					p := rec.GetPrincipal()
					idBytes, encErr := a.engine.resolveSyncBytes(syncID)
					if encErr != nil {
						return nil, encErr
					}
					lastIntra = encodeCursor(encodeGrantByEntitlementIndexKey(
						idBytes,
						entID,
						p.GetResourceTypeId(), p.GetResourceId(),
						rec.GetExternalId(),
					))
					brokeEarly = true
					break
				}
			}
			if brokeEarly {
				nextToken = encodeBatchCursor(i, lastIntra, listChecksum)
				break EntitlementLoop
			}
			if next == "" || len(records) == 0 {
				break
			}
			intraCursor = next
		}
		if len(out) >= limit && nextToken == "" {
			// Filled exactly on the entitlement boundary; resume at
			// the next entitlement with no intra-cursor.
			if i+1 < len(ents) {
				nextToken = encodeBatchCursor(i+1, "", listChecksum)
			}
			break
		}
	}

	return reader_v2.GrantsReaderServiceListGrantsForEntitlementsResponse_builder{
		List:          out,
		NextPageToken: nextToken,
	}.Build(), nil
}

// entitlementListChecksum hashes the sorted entitlement ID set so
// it is stable across client-side reorderings. If the caller drops
// or adds an entitlement between pages the checksum changes and
// decodeBatchCursor rejects the cursor.
func entitlementListChecksum(ents []*v2.Entitlement) uint32 {
	ids := make([]string, 0, len(ents))
	for _, e := range ents {
		ids = append(ids, e.GetId())
	}
	sort.Strings(ids)
	h := crc32.NewIEEE()
	for _, id := range ids {
		_, _ = h.Write([]byte(id))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum32()
}

// encodeBatchCursor writes (entitlement_index, intra_cursor,
// list_checksum) into a base64 token. intra_cursor is the
// already-base64'd engine cursor; we re-encode it transparently.
func encodeBatchCursor(idx int, intra string, listChecksum uint32) string {
	buf := make([]byte, 0, 16+len(intra))
	var v [binary.MaxVarintLen64]byte
	if idx < 0 {
		idx = 0
	}
	n := binary.PutUvarint(v[:], uint64(idx)) // #nosec G115 -- idx is normalized non-negative; int values fit in uint64.
	buf = append(buf, v[:n]...)
	intraBytes := []byte(intra)
	n = binary.PutUvarint(v[:], uint64(len(intraBytes)))
	buf = append(buf, v[:n]...)
	buf = append(buf, intraBytes...)
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], listChecksum)
	buf = append(buf, sum[:]...)
	return base64.RawURLEncoding.EncodeToString(buf)
}

// decodeBatchCursor parses what encodeBatchCursor produced. If
// the embedded checksum doesn't match the current request's
// entitlement set, the cursor is treated as stale and the caller
// restarts from the beginning (returns 0, "", nil).
func decodeBatchCursor(token string, currentChecksum uint32) (int, string, error) {
	if token == "" {
		return 0, "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, "", fmt.Errorf("ListGrantsForEntitlements: bad cursor: %w", err)
	}
	idxU, n := binary.Uvarint(raw)
	if n <= 0 {
		return 0, "", errors.New("ListGrantsForEntitlements: cursor missing index")
	}
	raw = raw[n:]
	lenU, n := binary.Uvarint(raw)
	if n <= 0 {
		return 0, "", errors.New("ListGrantsForEntitlements: cursor missing intra length")
	}
	raw = raw[n:]
	if uint64(len(raw)) < lenU+4 {
		return 0, "", errors.New("ListGrantsForEntitlements: cursor truncated")
	}
	intra := string(raw[:lenU])
	raw = raw[lenU:]
	got := binary.BigEndian.Uint32(raw[:4])
	if got != currentChecksum {
		// Caller changed the entitlement set between pages. Don't
		// silently mis-paginate — restart from the beginning.
		return 0, "", nil
	}
	const maxInt = int(^uint(0) >> 1)
	if idxU > uint64(maxInt) {
		return 0, "", errors.New("ListGrantsForEntitlements: cursor index out of range")
	}
	return int(idxU), intra, nil
}
