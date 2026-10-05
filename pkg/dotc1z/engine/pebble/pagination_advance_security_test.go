package pebble

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// TestSecurity_PaginationTokenMustStrictlyAdvance pins the page-token
// contract every exhaustive pager depends on: a next-page token must be
// strictly greater than every key already served, so `until token == ""`
// terminates. ListResources re-mints the token from the LAST RETURNED
// RECORD'S VALUE (cursorFor), not from the key the record was read under.
// In a file written by this SDK key and value agree and the substitution is
// invisible; in a hostile v3 .c1z they are independent bytes, and a row
// stored under key (user, "r002x") whose value names id "aaa" lands at a
// page boundary and mints a token sorting BELOW the row just served — the
// next page re-serves the same rows, and every exhaustive pager (the
// syncer's external-resource ingestion, baton resources/csv/diff, the
// explorer) loops forever, accumulating duplicates without bound until
// OOM. (Verified pre-fix: the hostile row at the boundary of a 3-row page
// re-serves the identical page; the token equals the previous token.)
//
// The token must be derived from (or verified against) the iterator key of
// the record actually emitted, never from untrusted value fields.
func TestSecurity_PaginationTokenMustStrictlyAdvance(t *testing.T) {
	ctx := t.Context()
	a := newAdapter(t)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	e := a.PebbleEngine()

	// Three honest rows plus one hostile row under key "r002x" — the last
	// slot of a 3-row first page — whose value names id "aaa" (sorts first).
	const limit = 3
	rb := e.db.NewRecordBatch()
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("r%03d", i)
		require.NoError(t, rb.StageResourcePut(
			encodeResourceKey("user", id),
			marshalResourceRecord(t, "user", id), nil, "", ""))
	}
	require.NoError(t, rb.StageResourcePut(
		encodeResourceKey("user", "r002x"),
		marshalResourceRecord(t, "user", "aaa"), nil, "", ""))
	require.NoError(t, rb.Commit(nil))
	require.NoError(t, rb.Close())

	req := v2.ResourcesServiceListResourcesRequest_builder{PageSize: limit}.Build()

	served := map[string]int{}
	pageCount := 0
	token := ""
	for {
		req.SetPageToken(token)
		resp, err := a.ListResources(ctx, req)
		require.NoError(t, err)
		pageCount++
		if pageCount > 10 {
			t.Fatalf("pagination did not terminate after %d pages — the token re-serves served rows", pageCount)
		}
		for _, r := range resp.GetList() {
			served[r.GetId().GetResource()]++
		}
		next := resp.GetNextPageToken()
		if next == "" {
			break
		}
		require.NotEqual(t, token, next,
			"a next-page token must strictly advance; re-serving the same token makes every exhaustive pager loop forever")
		token = next
	}

	// Every row served exactly once — no duplicates, no omission.
	for id, n := range served {
		require.Equal(t, 1, n, "resource %q served %d times", id, n)
	}
	require.Len(t, served, 4)
}

// marshalResourceRecord builds a marshaled v3.ResourceRecord naming
// (rt, id). The record's ids are the bytes the pager's cursorFor reads.
func marshalResourceRecord(t *testing.T, rt, id string) []byte {
	t.Helper()
	rec := v3.ResourceRecord_builder{
		ResourceTypeId: rt,
		ResourceId:     id,
	}.Build()
	val, err := marshalRecord(rec)
	require.NoError(t, err)
	return val
}