package connectorbuilder

// Lookup continuation, builder side: a connector without a runner-supplied
// lookup gets a per-request ContinuationLookup when the request carries
// SourceCacheLookupOffer/Answers. Phase 1 (deferred lookups) must become an
// ask response with no failure recorded; phase 2 must serve answers to the
// unchanged connector code; a swallowed ErrLookupDeferred must fail loudly.

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
)

// continuationTestSyncer's Grants consults the source-cache lookup for one
// scope per page and serves either a replay annotation (hit) or a cold row
// + scope annotation (miss). Mode switches test the misuse paths.
type continuationTestSyncer struct {
	testResourceSyncerV2Simple

	scope   string
	swallow bool // ignore ErrLookupDeferred and return rows anyway
	batch   bool // resolve via LookupMany over three scopes

	lookupCalls int
}

func (s *continuationTestSyncer) Grants(ctx context.Context, r *v2.Resource, opts resource.SyncOpAttrs) ([]*v2.Grant, *resource.SyncOpResults, error) {
	s.lookupCalls++
	if s.batch {
		queries := []sourcecache.Query{
			{RowKind: sourcecache.RowKindGrants, ScopeHash: s.scope + "/1"},
			{RowKind: sourcecache.RowKindGrants, ScopeHash: s.scope + "/2"},
			{RowKind: sourcecache.RowKindGrants, ScopeHash: s.scope + "/3"},
		}
		answers, err := sourcecache.LookupMany(ctx, opts.SourceCache, queries)
		if err != nil {
			return nil, nil, fmt.Errorf("batch revalidation: %w", err)
		}
		ret := &resource.SyncOpResults{Annotations: annotations.Annotations{}}
		for _, a := range answers {
			if !a.Found || a.ETag != "current" {
				return nil, nil, fmt.Errorf("test wants all hits, got %+v", a)
			}
		}
		ret.Annotations.Update(v2.SourceCacheReplay_builder{ScopeHash: s.scope + "/1", Etag: "current"}.Build())
		return nil, ret, nil
	}

	entry, found, err := opts.SourceCache.LookupPreviousSourceCache(ctx, sourcecache.RowKindGrants, s.scope)
	if err != nil && !s.swallow {
		// Idiomatic wrapping — the builder must match via errors.Is.
		return nil, nil, fmt.Errorf("listing grants for %s: %w", r.GetId().GetResource(), err)
	}
	ret := &resource.SyncOpResults{Annotations: annotations.Annotations{}}
	if err == nil && found && entry.ETag == "current" {
		ret.Annotations.Update(v2.SourceCacheReplay_builder{ScopeHash: s.scope, Etag: entry.ETag}.Build())
		return nil, ret, nil
	}
	// Miss (or swallowed deferral): cold row.
	ret.Annotations.Update(v2.SourceCacheScope_builder{ScopeHash: s.scope, Etag: "current"}.Build())
	return []*v2.Grant{mkTestGrant(r)}, ret, nil
}

func mkTestGrant(r *v2.Resource) *v2.Grant {
	return v2.Grant_builder{
		Id: "grant-1",
		Entitlement: v2.Entitlement_builder{
			Id:       fmt.Sprintf("%s:%s:member", r.GetId().GetResourceType(), r.GetId().GetResource()),
			Resource: r,
		}.Build(),
		Principal: r,
	}.Build()
}

func newContinuationTestBuilder(t *testing.T, ts *continuationTestSyncer) *builder {
	t.Helper()
	cb := &testConnectorBuilderV2Full{resourceSyncers: []ResourceSyncerV2{ts}, hasActionManager: true}
	c, err := NewConnector(context.Background(), cb)
	require.NoError(t, err)
	b, ok := c.(*builder)
	require.True(t, ok)
	return b
}

func continuationGrantsRequest(annos annotations.Annotations) *v2.GrantsServiceListGrantsRequest {
	return v2.GrantsServiceListGrantsRequest_builder{
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		}.Build(),
		Annotations: annos,
	}.Build()
}

func TestContinuation_DeferThenAnswer(t *testing.T) {
	ctx := context.Background()
	ts := &continuationTestSyncer{testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"}, scope: "groups/g1/members"}
	b := newContinuationTestBuilder(t, ts)

	// Phase 1: offer only → the handler's deferred lookup becomes an ask.
	offer := annotations.New(&v2.SourceCacheLookupOffer{})
	resp, err := b.ListGrants(ctx, continuationGrantsRequest(offer))
	require.NoError(t, err, "a deferral is a protocol turn, not a failure")
	require.Empty(t, resp.GetList(), "ask response must carry no rows")
	require.Empty(t, resp.GetNextPageToken())

	ask := &v2.SourceCacheLookupAsk{}
	respAnnos := annotations.Annotations(resp.GetAnnotations())
	hasAsk, err := respAnnos.Pick(ask)
	require.NoError(t, err)
	require.True(t, hasAsk)
	require.Len(t, ask.GetQueries(), 1)
	require.Equal(t, "groups/g1/members", ask.GetQueries()[0].GetScopeHash())
	require.Equal(t, string(sourcecache.RowKindGrants), ask.GetQueries()[0].GetRowKind())

	// Phase 2: same request + answers → the SAME connector code replays.
	answers := annotations.New(&v2.SourceCacheLookupOffer{})
	answers.Update(sourcecache.AnswersProto([]sourcecache.Answer{
		{Query: sourcecache.Query{RowKind: sourcecache.RowKindGrants, ScopeHash: "groups/g1/members"}, Found: true, ETag: "current"},
	}))
	resp2, err := b.ListGrants(ctx, continuationGrantsRequest(answers))
	require.NoError(t, err)
	replay := &v2.SourceCacheReplay{}
	resp2Annos := annotations.Annotations(resp2.GetAnnotations())
	hasReplay, err := resp2Annos.Pick(replay)
	require.NoError(t, err)
	require.True(t, hasReplay, "phase 2 must reach the connector's replay branch")
	require.Empty(t, resp2.GetList())

	// A not-found answer is a MISS, not a deferral: cold fetch.
	miss := annotations.New(&v2.SourceCacheLookupOffer{})
	miss.Update(sourcecache.AnswersProto([]sourcecache.Answer{
		{Query: sourcecache.Query{RowKind: sourcecache.RowKindGrants, ScopeHash: "groups/g1/members"}, Found: false},
	}))
	resp3, err := b.ListGrants(ctx, continuationGrantsRequest(miss))
	require.NoError(t, err)
	require.Len(t, resp3.GetList(), 1, "not-found answer must fall through to cold fetch")
}

func TestContinuation_LookupManyBatchesOneAsk(t *testing.T) {
	ctx := context.Background()
	ts := &continuationTestSyncer{testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"}, scope: "chunk", batch: true}
	b := newContinuationTestBuilder(t, ts)

	resp, err := b.ListGrants(ctx, continuationGrantsRequest(annotations.New(&v2.SourceCacheLookupOffer{})))
	require.NoError(t, err)
	ask := &v2.SourceCacheLookupAsk{}
	respAnnos := annotations.Annotations(resp.GetAnnotations())
	hasAsk, err := respAnnos.Pick(ask)
	require.NoError(t, err)
	require.True(t, hasAsk)
	require.Len(t, ask.GetQueries(), 3, "LookupMany must collect the whole batch into ONE ask")

	var answers []sourcecache.Answer
	for _, q := range ask.GetQueries() {
		answers = append(answers, sourcecache.Answer{
			Query: sourcecache.Query{RowKind: sourcecache.RowKind(q.GetRowKind()), ScopeHash: q.GetScopeHash()},
			Found: true, ETag: "current",
		})
	}
	req := annotations.New(&v2.SourceCacheLookupOffer{})
	req.Update(sourcecache.AnswersProto(answers))
	resp2, err := b.ListGrants(ctx, continuationGrantsRequest(req))
	require.NoError(t, err)
	resp2Annos := annotations.Annotations(resp2.GetAnnotations())
	require.True(t, resp2Annos.Contains(&v2.SourceCacheReplay{}))
}

func TestContinuation_SwallowedDeferralFailsLoudly(t *testing.T) {
	ctx := context.Background()
	ts := &continuationTestSyncer{testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"}, scope: "s", swallow: true}
	b := newContinuationTestBuilder(t, ts)

	_, err := b.ListGrants(ctx, continuationGrantsRequest(annotations.New(&v2.SourceCacheLookupOffer{})))
	require.Error(t, err)
	require.Contains(t, err.Error(), "swallowed", "a swallowed ErrLookupDeferred must be caught in-process, not at the production bounce cap")
}

func TestContinuation_NoOfferMeansNoopLookup(t *testing.T) {
	ctx := context.Background()
	ts := &continuationTestSyncer{testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"}, scope: "s"}
	b := newContinuationTestBuilder(t, ts)

	// No offer, no answers: lookup misses (NoopLookup), connector fetches
	// cold, no ask, no error — exactly today's behavior.
	resp, err := b.ListGrants(ctx, continuationGrantsRequest(nil))
	require.NoError(t, err)
	require.Len(t, resp.GetList(), 1)
	respAnnos := annotations.Annotations(resp.GetAnnotations())
	require.False(t, respAnnos.Contains(&v2.SourceCacheLookupAsk{}))
}

func TestContinuation_DirectLookupWins(t *testing.T) {
	ctx := context.Background()
	ts := &continuationTestSyncer{testResourceSyncerV2Simple: testResourceSyncerV2Simple{resourceType: "group"}, scope: "s"}
	cb := &testConnectorBuilderV2Full{resourceSyncers: []ResourceSyncerV2{ts}, hasActionManager: true}
	c, err := NewConnector(context.Background(), cb, WithSourceCache(staticTestLookup{etag: "current"}))
	require.NoError(t, err)
	b, ok := c.(*builder)
	require.True(t, ok)

	// Offer present AND a direct lookup installed: the direct lookup wins,
	// the handler resolves in one phase, and no ask is emitted.
	resp, err := b.ListGrants(ctx, continuationGrantsRequest(annotations.New(&v2.SourceCacheLookupOffer{})))
	require.NoError(t, err)
	respAnnos := annotations.Annotations(resp.GetAnnotations())
	require.False(t, respAnnos.Contains(&v2.SourceCacheLookupAsk{}))
	require.True(t, respAnnos.Contains(&v2.SourceCacheReplay{}))
}

type staticTestLookup struct{ etag string }

func (s staticTestLookup) LookupPreviousSourceCache(context.Context, sourcecache.RowKind, string) (sourcecache.Entry, bool, error) {
	return sourcecache.Entry{ETag: s.etag}, true, nil
}
