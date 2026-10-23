package pebble

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	reader_v3 "github.com/conductorone/baton-sdk/pb/c1/reader/v3"
	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
)

// principalTypePool holds types whose encoded forms are prefixes of each
// other or contain escape, NUL, and separator-adjacent bytes.
var principalTypePool = []string{"a", "ab", "a\x00", "a\x00b", "a\x01", "a\x01\x01", "a\x01\x02", "b", "\x00", "\x01", "user", "usera", "é"}

// grantsPrincipalTypesEntitlements covers stripped ids (resource prefix present)
// and opaque ids (it is not).
var grantsPrincipalTypesEntitlements = []string{"app:github:e1", "app:github:e2:x", "opaque", "app:github"}

type principalTypesFixture struct {
	e        *Engine
	grants   map[string][]*v3.GrantRecord // entitlement id -> grants in insertion order
	v3reader connectorstore.V3GrantReader
}

func entStub(entID string) *v2.Entitlement {
	return v2.Entitlement_builder{
		Id: entID,
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "app", Resource: "github"}.Build(),
		}.Build(),
	}.Build()
}

func newPrincipalTypesFixture(ctx context.Context, tb testing.TB, seed int64, perEntitlement int) *principalTypesFixture {
	tb.Helper()
	e, _ := newTestEngine(tb)
	a := NewAdapter(e)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(tb, err)
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic seeded test data.
	f := &principalTypesFixture{e: e, grants: map[string][]*v3.GrantRecord{}}
	var all []*v3.GrantRecord
	for _, entID := range grantsPrincipalTypesEntitlements {
		seen := map[string]bool{}
		n := rng.Intn(perEntitlement + 1)
		for i := 0; i < n; i++ {
			rt := principalTypePool[rng.Intn(len(principalTypePool))]
			pid := fmt.Sprintf("p%d", rng.Intn(perEntitlement))
			if rng.Intn(8) == 0 {
				pid = "a\x00" + pid
			}
			if seen[rt+"|"+pid] {
				continue
			}
			seen[rt+"|"+pid] = true
			rec := v3.GrantRecord_builder{
				ExternalId:   fmt.Sprintf("%s|%s|%s", entID, rt, pid),
				Entitlement:  v3.EntitlementRef_builder{ResourceTypeId: "app", ResourceId: "github", EntitlementId: entID}.Build(),
				Principal:    v3.PrincipalRef_builder{ResourceTypeId: rt, ResourceId: pid}.Build(),
				DiscoveredAt: timestamppb.New(time.Unix(1_700_000_000, 0)),
			}.Build()
			f.grants[entID] = append(f.grants[entID], rec)
			all = append(all, rec)
		}
	}
	require.NoError(tb, e.PutGrantRecords(ctx, all...))
	provider, ok := any(e).(connectorstore.V3GrantReaderProvider)
	require.True(tb, ok)
	f.v3reader = provider.V3GrantReader()
	return f
}

// oracleListGrantsForEntitlement is the pre-change implementation: scan the
// whole entitlement prefix and drop other principal types after unmarshal.
func oracleListGrantsForEntitlement(
	ctx context.Context, e *Engine, ent *v2.Entitlement, principalID *v2.ResourceId, rts []string, token string, limit int,
) ([]*v3.GrantRecord, string, error) {
	entIdentity, err := e.entitlementIdentityForRequest(ctx, ent)
	if err != nil {
		return nil, "", err
	}
	var keep func(*v3.GrantRecord) bool
	if len(rts) > 0 {
		set := map[string]struct{}{}
		for _, rt := range rts {
			set[rt] = struct{}{}
		}
		keep = func(r *v3.GrantRecord) bool {
			_, ok := set[r.GetPrincipal().GetResourceTypeId()]
			return ok
		}
	}
	if principalID == nil {
		return oraclePaginateGrantsByEntitlement(ctx, e, entIdentity, token, limit, keep)
	}
	records, next, err := e.PaginateGrantsByEntitlementPrincipal(ctx, entIdentity, principalID.GetResourceType(), principalID.GetResource(), token, limit)
	if err != nil {
		return nil, "", err
	}
	records = slices.DeleteFunc(records, func(r *v3.GrantRecord) bool {
		rp := r.GetPrincipal()
		return rp.GetResourceTypeId() != principalID.GetResourceType() || rp.GetResourceId() != principalID.GetResource() ||
			(keep != nil && !keep(r))
	})
	return records, next, nil
}

// oraclePaginateGrantsByEntitlement is iteratePrimaryPageWithKey with the keep
// func as it was before principal types were seeked.
func oraclePaginateGrantsByEntitlement(
	ctx context.Context, e *Engine, entID entitlementIdentity, cursor string, limit int, keep func(*v3.GrantRecord) bool,
) ([]*v3.GrantRecord, string, error) {
	cursorBytes, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = DefaultPageSize
	}
	lower, upper, err := rangeAfter(encodeGrantPrimaryEntitlementPrefix(entID), cursorBytes)
	if err != nil {
		return nil, "", err
	}
	iter, err := e.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, "", err
	}
	defer iter.Close()
	var out []*v3.GrantRecord
	var last []byte
	hasMore := false
	for iter.First(); iter.Valid(); iter.Next() {
		if len(out) == limit {
			hasMore = true
			break
		}
		v := &v3.GrantRecord{}
		if err := unmarshalRecord(iter.Value(), v); err != nil {
			return nil, "", err
		}
		if keep != nil && !keep(v) {
			continue
		}
		last = append(last[:0], iter.Key()...)
		out = append(out, v)
	}
	if err := iter.Error(); err != nil {
		return nil, "", err
	}
	if hasMore {
		return out, encodeCursor(last), nil
	}
	return out, "", nil
}

type principalTypesRequest struct {
	ent         *v2.Entitlement
	principalID *v2.ResourceId
	rts         []string
	limit       int
}

// listNew runs the request through the v2 or v3 reader method.
func (r principalTypesRequest) pageSize() uint32 {
	return uint32(r.limit) //nolint:gosec // test limits are small positive ints.
}

func (f *principalTypesFixture) listNew(ctx context.Context, useV3 bool, r principalTypesRequest, token string) ([]*v3.GrantRecord, string, error) {
	if useV3 {
		resp, err := f.v3reader.ListGrantsForEntitlement(ctx, reader_v3.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
			Entitlement: r.ent, PrincipalId: r.principalID, PrincipalResourceTypeIds: r.rts, PageSize: r.pageSize(), PageToken: token,
		}.Build())
		return resp.GetList(), resp.GetNextPageToken(), err
	}
	resp, err := f.e.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
		Entitlement: r.ent, PrincipalId: r.principalID, PrincipalResourceTypeIds: r.rts, PageSize: r.pageSize(), PageToken: token,
	}.Build())
	if err != nil {
		return nil, "", err
	}
	// The v2 reader downshifts records; compare by the grant id they carry.
	out := make([]*v3.GrantRecord, 0, len(resp.GetList()))
	for _, g := range resp.GetList() {
		out = append(out, v3.GrantRecord_builder{
			ExternalId: g.GetId(),
			Principal:  v3.PrincipalRef_builder{ResourceTypeId: g.GetPrincipal().GetId().GetResourceType(), ResourceId: g.GetPrincipal().GetId().GetResource()}.Build(),
		}.Build())
	}
	return out, resp.GetNextPageToken(), nil
}

func (f *principalTypesFixture) listOld(ctx context.Context, r principalTypesRequest, token string) ([]*v3.GrantRecord, string, error) {
	return oracleListGrantsForEntitlement(ctx, f.e, r.ent, r.principalID, r.rts, token, r.limit)
}

func grantKeys(rs []*v3.GrantRecord) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		p := r.GetPrincipal()
		out = append(out, p.GetResourceTypeId()+"\x00"+p.GetResourceId())
	}
	return out
}

// drain pages with next until the token is empty, returning each page's keys and token.
func drain(t testing.TB, list func(token string) ([]*v3.GrantRecord, string, error)) ([][]string, []string) {
	t.Helper()
	var pages [][]string
	var tokens []string
	token := ""
	for i := 0; i < 100_000; i++ {
		recs, next, err := list(token)
		require.NoError(t, err)
		pages = append(pages, grantKeys(recs))
		tokens = append(tokens, next)
		if next == "" {
			return pages, tokens
		}
		token = next
	}
	t.Fatal("pager did not terminate")
	return nil, nil
}

// requireSameListing compares the new and old listings of one request: the
// concatenated sequence, each page's records, and each non-empty new token.
// The old pager may add one trailing empty page when the rows after its last
// full page were all filtered out; the new one ends with an empty token.
func (f *principalTypesFixture) requireSameListing(ctx context.Context, t testing.TB, useV3 bool, r principalTypesRequest) {
	t.Helper()
	newPages, newTokens := drain(t, func(token string) ([]*v3.GrantRecord, string, error) { return f.listNew(ctx, useV3, r, token) })
	oldPages, oldTokens := drain(t, func(token string) ([]*v3.GrantRecord, string, error) { return f.listOld(ctx, r, token) })
	require.Equal(t, slices.Concat(oldPages...), slices.Concat(newPages...), "request %+v", r)
	for i := range newPages {
		require.Equal(t, oldPages[i], newPages[i], "page %d request %+v", i, r)
		if newTokens[i] != "" {
			require.Equal(t, oldTokens[i], newTokens[i], "token %d request %+v", i, r)
		}
	}
	require.LessOrEqual(t, len(oldPages)-len(newPages), 1)
	if len(oldPages) > len(newPages) {
		require.Empty(t, oldPages[len(oldPages)-1])
	}

	// A token from either pager resumes identically in the other.
	for i, tok := range newTokens {
		if tok == "" {
			continue
		}
		a, an, aerr := f.listOld(ctx, r, tok)
		b, bn, berr := f.listNew(ctx, useV3, r, tok)
		require.NoError(t, aerr)
		require.NoError(t, berr)
		require.Equal(t, grantKeys(a), grantKeys(b), "new token %d resumed by old vs new", i)
		if bn != "" {
			require.Equal(t, an, bn)
		}
	}
	for i, tok := range oldTokens {
		if tok == "" {
			continue
		}
		a, _, aerr := f.listOld(ctx, r, tok)
		b, _, berr := f.listNew(ctx, useV3, r, tok)
		require.NoError(t, aerr)
		require.NoError(t, berr)
		require.Equal(t, grantKeys(a), grantKeys(b), "old token %d resumed by old vs new", i)
	}
}

func (f *principalTypesFixture) randomRequest(rng *rand.Rand, entID string) principalTypesRequest {
	r := principalTypesRequest{ent: entStub(entID), limit: 1 + rng.Intn(9)}
	switch rng.Intn(5) {
	case 0: // empty
	case 1: // all types, shuffled
		r.rts = slices.Clone(principalTypePool)
		rng.Shuffle(len(r.rts), func(i, j int) { r.rts[i], r.rts[j] = r.rts[j], r.rts[i] })
	default:
		for n := 1 + rng.Intn(5); n > 0; n-- {
			r.rts = append(r.rts, principalTypePool[rng.Intn(len(principalTypePool))])
		}
		if rng.Intn(4) == 0 {
			r.rts = append(r.rts, "absent")
		}
	}
	if recs := f.grants[entID]; len(recs) > 0 && rng.Intn(4) == 0 {
		p := recs[rng.Intn(len(recs))].GetPrincipal()
		r.principalID = v2.ResourceId_builder{ResourceType: p.GetResourceTypeId(), Resource: p.GetResourceId()}.Build()
		if rng.Intn(3) == 0 {
			r.principalID = v2.ResourceId_builder{ResourceType: "absent", Resource: "none"}.Build()
		}
	}
	return r
}

func TestListGrantsForEntitlementPrincipalTypesDifferential(t *testing.T) {
	ctx := t.Context()
	for seed := int64(1); seed <= 4; seed++ {
		f := newPrincipalTypesFixture(ctx, t, seed, 30)
		rng := rand.New(rand.NewSource(seed * 7919)) //nolint:gosec // deterministic seeded test data.
		for _, entID := range append(slices.Clone(grantsPrincipalTypesEntitlements), "app:github:never") {
			for i := 0; i < 25; i++ {
				r := f.randomRequest(rng, entID)
				f.requireSameListing(ctx, t, false, r)
				f.requireSameListing(ctx, t, true, r)
			}
		}
	}
}

// Every page size from 1 to the entitlement's grant count, for each requested
// type list shape.
func TestListGrantsForEntitlementPrincipalTypesAllPageSizes(t *testing.T) {
	ctx := t.Context()
	f := newPrincipalTypesFixture(ctx, t, 99, 20)
	shapes := [][]string{
		nil,
		{"a"},
		{"ab", "a"},
		{"b", "a", "b", "a\x00"},
		{"a", "absent"},
		slices.Clone(principalTypePool),
		{"a\x00b", "a\x00", "a\x01", "a\x01\x01"},
	}
	for _, entID := range grantsPrincipalTypesEntitlements {
		for _, rts := range shapes {
			for limit := 1; limit <= len(f.grants[entID])+1; limit++ {
				r := principalTypesRequest{ent: entStub(entID), rts: rts, limit: limit}
				f.requireSameListing(ctx, t, false, r)
				f.requireSameListing(ctx, t, true, r)
			}
		}
	}
}

// Tokens outside the entitlement prefix are rejected before any read, as for
// an unfiltered listing.
func TestListGrantsForEntitlementPrincipalTypesRejectsForeignToken(t *testing.T) {
	ctx := t.Context()
	f := newPrincipalTypesFixture(ctx, t, 5, 10)
	other := encodeCursor(append(encodeGrantPrimaryEntitlementPrefix(entitlementIdentityFromParts("app", "github", "opaque")), "a\x00x"...))
	for _, tok := range []string{"!!!", other, encodeCursor([]byte{0xff})} {
		r := principalTypesRequest{ent: entStub("app:github:e1"), rts: []string{"a", "b"}, limit: 3}
		_, _, err := f.listNew(ctx, false, r, tok)
		require.ErrorIs(t, err, ErrInvalidPageToken, "token %q", tok)
		_, _, err = f.listNew(ctx, true, r, tok)
		require.ErrorIs(t, err, ErrInvalidPageToken, "token %q", tok)
		_, _, oldErr := f.listOld(ctx, r, tok)
		require.ErrorIs(t, oldErr, ErrInvalidPageToken)
	}
}

func FuzzListGrantsForEntitlementPrincipalTypes(f *testing.F) {
	ctx := context.Background()
	fx := newPrincipalTypesFixture(ctx, f, 3, 25)
	f.Add(int64(1), uint8(3), []byte{0, 1, 2}, false, uint8(0))
	f.Add(int64(2), uint8(1), []byte{12, 12, 200}, true, uint8(2))
	f.Add(int64(3), uint8(7), []byte{}, true, uint8(1))
	f.Fuzz(func(t *testing.T, seed int64, limit uint8, typeBytes []byte, useV3 bool, entIdx uint8) {
		rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic seeded test data.
		entID := grantsPrincipalTypesEntitlements[int(entIdx)%len(grantsPrincipalTypesEntitlements)]
		r := fx.randomRequest(rng, entID)
		r.limit = 1 + int(limit)%12
		if len(typeBytes) > 0 {
			r.rts = nil
			for _, b := range typeBytes {
				if int(b) == len(principalTypePool) {
					r.rts = append(r.rts, "absent")
					continue
				}
				r.rts = append(r.rts, principalTypePool[int(b)%len(principalTypePool)])
			}
		}
		fx.requireSameListing(ctx, t, useV3, r)
	})
}

func setupPrincipalTypesBench(b *testing.B, total, types int) (*Engine, *v2.Entitlement) {
	ctx := context.Background()
	e, _ := newTestEngine(b)
	a := NewAdapter(e)
	_, err := a.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(b, err)
	const entID = "app:github:bench"
	batch := make([]*v3.GrantRecord, 0, 5000)
	for i := 0; i < total; i++ {
		batch = append(batch, v3.GrantRecord_builder{
			ExternalId:   fmt.Sprintf("g%d", i),
			Entitlement:  v3.EntitlementRef_builder{ResourceTypeId: "app", ResourceId: "github", EntitlementId: entID}.Build(),
			Principal:    v3.PrincipalRef_builder{ResourceTypeId: fmt.Sprintf("t%d", i%types), ResourceId: fmt.Sprintf("p%07d", i)}.Build(),
			DiscoveredAt: timestamppb.New(time.Unix(1_700_000_000, 0)),
		}.Build())
		if len(batch) == cap(batch) || i == total-1 {
			require.NoError(b, e.PutGrantRecords(ctx, batch...))
			batch = batch[:0]
		}
	}
	return e, entStub(entID)
}

// BenchmarkListGrantsForEntitlementPrincipalTypes drains one principal type of
// ten from a 100k-grant entitlement: old scans and decodes all rows, new only
// the requested type's range.
func BenchmarkListGrantsForEntitlementPrincipalTypes(b *testing.B) {
	e, ent := setupPrincipalTypesBench(b, 100_000, 10)
	ctx := context.Background()
	rts := []string{"t7"}
	drainBench := func(b *testing.B, list func(token string) ([]*v3.GrantRecord, string, error)) {
		n := 0
		token := ""
		for {
			recs, next, err := list(token)
			if err != nil {
				b.Fatal(err)
			}
			n += len(recs)
			if next == "" {
				break
			}
			token = next
		}
		if n != 10_000 {
			b.Fatalf("got %d grants", n)
		}
	}
	b.Run("old", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			drainBench(b, func(token string) ([]*v3.GrantRecord, string, error) {
				return oracleListGrantsForEntitlement(ctx, e, ent, nil, rts, token, 1000)
			})
		}
	})
	b.Run("new", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			drainBench(b, func(token string) ([]*v3.GrantRecord, string, error) {
				resp, err := e.ListGrantsForEntitlement(ctx, reader_v2.GrantsReaderServiceListGrantsForEntitlementRequest_builder{
					Entitlement: ent, PrincipalResourceTypeIds: rts, PageSize: 1000, PageToken: token,
				}.Build())
				if err != nil {
					return nil, "", err
				}
				out := make([]*v3.GrantRecord, len(resp.GetList()))
				for j := range out {
					out[j] = &v3.GrantRecord{}
				}
				return out, resp.GetNextPageToken(), nil
			})
		}
	})
}
