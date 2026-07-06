package dotc1z

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"time"

	"github.com/doug-martin/goqu/v9"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

// ListGrantsForEntitlements batches K-entitlement grant fetches
// (RFC §A4). Single SQL query per page with entitlement_id IN
// (?, ?, ...) — uses the (entitlement_id) index on the grants
// table.
//
// Cursor encoding mirrors the Pebble adapter:
//
//	varint(entitlement_index) || varint(intra_cursor_len) ||
//	intra_cursor_bytes || crc32(sorted-ent-IDs)
//
// where intra_cursor here is the row-id of the last returned grant
// (encoded as a decimal string for compatibility with the row-id
// page-token used elsewhere in this package).
func (c *C1File) ListGrantsForEntitlements(
	ctx context.Context,
	req *reader_v2.GrantsReaderServiceListGrantsForEntitlementsRequest,
) (*reader_v2.GrantsReaderServiceListGrantsForEntitlementsResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListGrantsForEntitlements")
	var err error
	defer func() { uotel.EndSpanWithError(span, err) }()

	if err := c.validateDb(ctx); err != nil {
		return nil, err
	}

	ents := req.GetEntitlements()
	if len(ents) == 0 {
		return reader_v2.GrantsReaderServiceListGrantsForEntitlementsResponse_builder{}.Build(), nil
	}
	syncID, err := c.resolveSyncIDForRead(ctx, req.GetAnnotations())
	if err != nil {
		return nil, err
	}

	limit := int(req.GetPageSize())
	if limit <= 0 || limit > maxPageSize {
		limit = maxPageSize
	}

	checksum := entitlementListChecksum(ents)
	startIdx, startIntra, err := decodeBatchCursor(req.GetPageToken(), checksum)
	if err != nil {
		return nil, err
	}
	if startIdx >= len(ents) {
		return reader_v2.GrantsReaderServiceListGrantsForEntitlementsResponse_builder{}.Build(), nil
	}

	// Collect entitlement IDs to query in this RPC. We always query
	// IN (...) for the full set so SQLite can use the index for
	// every entitlement; the cursor is only used to filter which
	// rows we've already returned.
	entIDs := make([]string, 0, len(ents))
	for _, e := range ents {
		if id := e.GetId(); id != "" {
			entIDs = append(entIDs, id)
		}
	}
	if len(entIDs) == 0 {
		return reader_v2.GrantsReaderServiceListGrantsForEntitlementsResponse_builder{}.Build(), nil
	}

	// Page-token semantics for SQLite: we walk the grants table
	// ordered by (entitlement_id, id) — that matches the existing
	// (entitlement_id) index, so the cursor is (entitlement_id,
	// last_row_id). We encode entitlement_id as the index into the
	// caller's array and the intra-cursor as the row id.
	q := c.db.From(grants.Name()).Prepared(true).Select(
		"id",
		"data",
		"entitlement_id",
		"resource_type_id",
		"resource_id",
		"principal_resource_type_id",
		"principal_resource_id",
	).Where(goqu.C("entitlement_id").In(entIDs))

	if syncID != "" {
		q = q.Where(goqu.C("sync_id").Eq(syncID))
	}

	// Cursor filter: skip rows already returned in earlier pages.
	// Sorted by (entitlement_id, id) so the cursor is "for ent_id
	// = ents[startIdx], skip rows with id <= startIntra; for any
	// later entitlement_id, take all rows."
	if startIdx > 0 || startIntra != "" {
		startEntID := ents[startIdx].GetId()
		later := make([]string, 0, len(entIDs))
		for _, id := range entIDs {
			if id > startEntID {
				later = append(later, id)
			}
		}
		var startRowID int64
		if startIntra != "" {
			v, perr := strconv.ParseInt(startIntra, 10, 64)
			if perr != nil {
				return nil, fmt.Errorf("ListGrantsForEntitlements: bad intra cursor: %w", perr)
			}
			startRowID = v
		}
		intraSame := goqu.And(
			goqu.C("entitlement_id").Eq(startEntID),
			goqu.C("id").Gt(startRowID),
		)
		if len(later) > 0 {
			q = q.Where(goqu.Or(goqu.C("entitlement_id").In(later), intraSame))
		} else {
			q = q.Where(intraSame)
		}
	}

	if limit < 0 {
		limit = 0
	}
	q = q.Order(goqu.C("entitlement_id").Asc(), goqu.C("id").Asc()).Limit(uint(limit + 1)) // #nosec G115 -- limit is clamped to maxPageSize.

	query, args, err := q.ToSQL()
	if err != nil {
		return nil, err
	}

	queryStart := time.Now()
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if dur := time.Since(queryStart); dur > c.slowQueryThreshold {
		c.throttledWarnSlowQuery(ctx, query, dur)
	}

	unmarshal := proto.UnmarshalOptions{Merge: true, DiscardUnknown: true}

	out := make([]*v2.Grant, 0, limit)
	var (
		rowID        int64
		data         sql.RawBytes
		entIDRaw     sql.RawBytes
		rtIDRaw      sql.RawBytes
		ridRaw       sql.RawBytes
		princRTIDRaw sql.RawBytes
		princRIDRaw  sql.RawBytes
		lastRowID    int64
		lastEntID    string
		haveOverflow bool
	)
	for rows.Next() {
		if err := rows.Scan(&rowID, &data, &entIDRaw, &rtIDRaw, &ridRaw, &princRTIDRaw, &princRIDRaw); err != nil {
			return nil, err
		}
		entID := string(entIDRaw)
		if len(out) == limit {
			haveOverflow = true
			break
		}
		g := &v2.Grant{}
		if err := unmarshal.Unmarshal(data, g); err != nil {
			return nil, err
		}
		out = append(out, g)
		lastRowID = rowID
		lastEntID = entID
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var nextToken string
	if haveOverflow {
		idx := findEntitlementIndex(ents, lastEntID)
		if idx < 0 {
			idx = 0
		}
		nextToken = encodeBatchCursor(idx, strconv.FormatInt(lastRowID, 10), checksum)
	}

	return reader_v2.GrantsReaderServiceListGrantsForEntitlementsResponse_builder{
		List:          out,
		NextPageToken: nextToken,
	}.Build(), nil
}

func findEntitlementIndex(ents []*v2.Entitlement, id string) int {
	for i, e := range ents {
		if e.GetId() == id {
			return i
		}
	}
	return -1
}

// entitlementListChecksum hashes the entitlement ID list IN REQUEST
// ORDER. The cursor resumes positionally (ents[startIdx] provides the
// resume entitlement id), so a reordered same-set list must restart the
// scan — a sorted (order-insensitive) checksum would bless the stale
// token while startIdx selected a different entitlement, silently
// skipping and re-returning grants.
func entitlementListChecksum(ents []*v2.Entitlement) uint32 {
	h := crc32.NewIEEE()
	for _, e := range ents {
		_, _ = h.Write([]byte(e.GetId()))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum32()
}

func encodeBatchCursor(idx int, intra string, checksum uint32) string {
	if idx < 0 {
		idx = 0
	}
	buf := make([]byte, 0, 16+len(intra))
	var v [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(v[:], uint64(idx)) // #nosec G115 -- idx is normalized non-negative; int values fit in uint64.
	buf = append(buf, v[:n]...)
	intraBytes := []byte(intra)
	n = binary.PutUvarint(v[:], uint64(len(intraBytes)))
	buf = append(buf, v[:n]...)
	buf = append(buf, intraBytes...)
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], checksum)
	buf = append(buf, sum[:]...)
	return base64.RawURLEncoding.EncodeToString(buf)
}

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
		// Caller changed the entitlement set between pages —
		// restart from the beginning rather than mis-paginate.
		return 0, "", nil
	}
	const maxInt = int(^uint(0) >> 1)
	if idxU > uint64(maxInt) {
		return 0, "", errors.New("ListGrantsForEntitlements: cursor index out of range")
	}
	return int(idxU), intra, nil
}
