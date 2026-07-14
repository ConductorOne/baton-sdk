package sourcecache

// Lookup continuation (ask/answer): the lookup transport for connector
// runtimes that cannot call back to the syncer mid-request (single-shot
// request/response tunnels, e.g. gRPC-over-Lambda). The connector's first
// execution of a page ("phase 1") records the scopes it needs and fails
// with ErrLookupDeferred; the SDK converts that into a
// SourceCacheLookupAsk response; the syncer resolves the queries against
// its local previous-sync store and re-invokes the same request with
// SourceCacheLookupAnswers attached ("phase 2"), where the same connector
// code gets real answers. See docs/tasks/source-cache-lambda-lookup.md.

import (
	"context"
	"errors"
	"fmt"
	"sync"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// ErrLookupDeferred is returned by a deferring Lookup (phase 1 of the
// ask/answer continuation) when the answer is not yet available. The SDK
// intercepts it and answers the RPC with a SourceCacheLookupAsk.
//
// Propagation contract for connectors: wrap with %w if you must, NEVER
// swallow. The SDK matches with errors.Is, so idiomatic wrapping
// (fmt.Errorf("listing members: %w", err)) is fine. A connector that
// logs-and-continues past this error re-asks for scopes it already
// "handled" and turns every warm sync into a hard failure at the bounce
// cap.
var ErrLookupDeferred = errors.New("source cache lookup deferred: answer arrives on re-invoke")

// Query identifies one scope to resolve against the previous sync.
type Query struct {
	RowKind   RowKind
	ScopeHash string
}

// Answer resolves one Query. Found=false means the previous sync has no
// entry for the scope: fetch fresh. (Distinct from a query that got no
// Answer at all, which means unresolved: ask again.)
type Answer struct {
	Query
	Found bool
	ETag  string
}

// BatchLookup is optionally implemented by Lookup implementations that can
// resolve many scopes in one round trip. Connectors should not type-assert
// for it directly; call LookupMany, which falls back to per-query lookups.
type BatchLookup interface {
	LookupPreviousSourceCacheMany(ctx context.Context, queries []Query) ([]Answer, error)
}

// LookupMany resolves a batch of queries through lookup, using one round
// trip when the implementation supports it (BatchLookup) and a loop of
// single lookups otherwise. This is the topology-uniform batch API:
// in-process and subprocess lookups loop over local/loopback point-reads;
// a deferring lookup collects the whole batch into ONE ask.
//
// The returned answers are exact and complete for the queried set — one
// Answer per Query, order preserved, explicit Found per entry. (A
// deferring lookup returns ErrLookupDeferred instead of answers; phase 2
// then answers the same calls. A found answer whose etag does not fit the
// transport size budget arrives as not-found — the scope goes cold, which
// is correct, just slower — never as a silent omission.)
func LookupMany(ctx context.Context, lookup Lookup, queries []Query) ([]Answer, error) {
	if bl, ok := lookup.(BatchLookup); ok {
		return bl.LookupPreviousSourceCacheMany(ctx, queries)
	}
	answers := make([]Answer, 0, len(queries))
	for _, q := range queries {
		entry, found, err := lookup.LookupPreviousSourceCache(ctx, q.RowKind, q.ScopeHash)
		if err != nil {
			return nil, err
		}
		a := Answer{Query: q, Found: found}
		if found {
			a.ETag = entry.ETag
		}
		answers = append(answers, a)
	}
	return answers, nil
}

// ContinuationLookup is the per-request Lookup installed for the
// ask/answer continuation. It serves lookups from the answers delivered on
// the request (phase 2) and defers everything else by recording the query
// and returning ErrLookupDeferred (phase 1, or a phase 2 that queries
// scopes it did not ask for in phase 1 — allowed but wasteful).
//
// It is constructed per RPC by the SDK; connectors only ever see it as
// SyncOpAttrs.SourceCache.
type ContinuationLookup struct {
	mu       sync.Mutex
	answers  map[Query]Answer
	asked    []Query
	askedSet map[Query]struct{}
}

var (
	_ Lookup      = (*ContinuationLookup)(nil)
	_ BatchLookup = (*ContinuationLookup)(nil)
)

// NewContinuationLookup builds a ContinuationLookup pre-loaded with the
// request's answers (nil/empty on phase 1).
func NewContinuationLookup(answers []Answer) *ContinuationLookup {
	m := make(map[Query]Answer, len(answers))
	for _, a := range answers {
		m[a.Query] = a
	}
	return &ContinuationLookup{
		answers:  m,
		askedSet: map[Query]struct{}{},
	}
}

func (c *ContinuationLookup) LookupPreviousSourceCache(_ context.Context, rowKind RowKind, scopeHash string) (Entry, bool, error) {
	q := Query{RowKind: rowKind, ScopeHash: scopeHash}
	c.mu.Lock()
	defer c.mu.Unlock()
	if a, ok := c.answers[q]; ok {
		if !a.Found {
			return Entry{}, false, nil
		}
		return Entry{ETag: a.ETag}, true, nil
	}
	c.recordLocked(q)
	return Entry{}, false, fmt.Errorf("lookup %s/%s: %w", rowKind, scopeHash, ErrLookupDeferred)
}

func (c *ContinuationLookup) LookupPreviousSourceCacheMany(_ context.Context, queries []Query) ([]Answer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	answers := make([]Answer, 0, len(queries))
	missing := 0
	for _, q := range queries {
		a, ok := c.answers[q]
		if !ok {
			c.recordLocked(q)
			missing++
			continue
		}
		answers = append(answers, a)
	}
	if missing > 0 {
		// Defer the whole batch: phase 2 re-runs the same call with the
		// full answer set (already-answered queries remain answered on the
		// re-invoked request).
		return nil, fmt.Errorf("batch lookup: %d of %d queries unresolved: %w", missing, len(queries), ErrLookupDeferred)
	}
	return answers, nil
}

func (c *ContinuationLookup) recordLocked(q Query) {
	if _, seen := c.askedSet[q]; seen {
		return
	}
	c.askedSet[q] = struct{}{}
	c.asked = append(c.asked, q)
}

// Asked returns the queries recorded by deferred lookups, deduplicated, in
// first-ask order. Empty means no lookup deferred and the handler's result
// stands.
func (c *ContinuationLookup) Asked() []Query {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Query, len(c.asked))
	copy(out, c.asked)
	return out
}

// --- proto conversions (shared by connectorbuilder and the syncer) ------

// AskProto builds the SourceCacheLookupAsk annotation for a set of
// deferred queries.
func AskProto(queries []Query) *v2.SourceCacheLookupAsk {
	qs := make([]*v2.SourceCacheLookupAsk_Query, 0, len(queries))
	for _, q := range queries {
		qs = append(qs, v2.SourceCacheLookupAsk_Query_builder{
			RowKind:   string(q.RowKind),
			ScopeHash: q.ScopeHash,
		}.Build())
	}
	return v2.SourceCacheLookupAsk_builder{Queries: qs}.Build()
}

// maxQueriesPerAsk mirrors SourceCacheLookupAsk.queries.max_items. Asks
// arrive in connector response annotations; annotations.Pick unmarshals the
// Any but does not invoke generated protobuf validation, so this boundary
// enforces the resource cap explicitly.
const maxQueriesPerAsk = 4096

// MaxLookupBouncesPerRequest bounds consecutive asks for one request
// (same page token; only the answers annotation differs between
// re-invokes). Owned here so the answer-set cap below stays coherent
// with the syncer's bounce loop, which enforces it: answers accumulate
// across bounces, so the most answers one request can legally carry is
// this many asks' worth of queries.
const MaxLookupBouncesPerRequest = 4

// QueriesFromProto extracts and validates the queries of an ask.
func QueriesFromProto(ask *v2.SourceCacheLookupAsk) ([]Query, error) {
	if len(ask.GetQueries()) == 0 {
		// Without this an empty ask would die on the syncer's no-progress
		// guard with a misleading "re-asked already-answered scopes"
		// message; name the actual bug.
		return nil, fmt.Errorf("source cache lookup ask: no queries (a connector must not ask without at least one unresolved scope)")
	}
	if len(ask.GetQueries()) > maxQueriesPerAsk {
		return nil, fmt.Errorf("source cache lookup ask: %d queries (max %d)", len(ask.GetQueries()), maxQueriesPerAsk)
	}
	out := make([]Query, 0, len(ask.GetQueries()))
	for _, q := range ask.GetQueries() {
		kind := RowKind(q.GetRowKind())
		if err := ValidateRowKind(kind); err != nil {
			return nil, err
		}
		if err := ValidateScopeHash(q.GetScopeHash()); err != nil {
			return nil, err
		}
		out = append(out, Query{RowKind: kind, ScopeHash: q.GetScopeHash()})
	}
	return out, nil
}

// AnswersProto builds the SourceCacheLookupAnswers annotation.
func AnswersProto(answers []Answer) *v2.SourceCacheLookupAnswers {
	as := make([]*v2.SourceCacheLookupAnswers_Answer, 0, len(answers))
	for _, a := range answers {
		as = append(as, v2.SourceCacheLookupAnswers_Answer_builder{
			RowKind:   string(a.RowKind),
			ScopeHash: a.ScopeHash,
			Found:     a.Found,
			Etag:      a.ETag,
		}.Build())
	}
	return v2.SourceCacheLookupAnswers_builder{Answers: as}.Build()
}

// Wire caps for answer sets. Answers come from the SDK (a trusted
// parent), but the same validation discipline as QueriesFromProto keeps a
// corrupted or hostile transport from smuggling malformed entries into
// lookup results.
//
// maxAnswersPerMessage is NOT one ask's max_items: answers accumulate
// across bounces (every re-invoke carries the union of all resolved
// queries), so the coherent bound is the most a compliant exchange can
// legally produce — MaxLookupBouncesPerRequest maximal asks. Capping at
// one ask's size would hard-fail a legal 4096-scope first ask followed by
// a single late scope on a later bounce.
const (
	maxAnswersPerMessage = maxQueriesPerAsk * MaxLookupBouncesPerRequest
	maxAnswerETagLen     = 65536
)

// AnswersFromProto extracts and validates the answers delivered on a
// request.
func AnswersFromProto(msg *v2.SourceCacheLookupAnswers) ([]Answer, error) {
	if len(msg.GetAnswers()) > maxAnswersPerMessage {
		return nil, fmt.Errorf("source cache lookup answers: %d entries (max %d)", len(msg.GetAnswers()), maxAnswersPerMessage)
	}
	out := make([]Answer, 0, len(msg.GetAnswers()))
	for _, a := range msg.GetAnswers() {
		kind := RowKind(a.GetRowKind())
		if err := ValidateRowKind(kind); err != nil {
			return nil, fmt.Errorf("source cache lookup answers: %w", err)
		}
		if err := ValidateScopeHash(a.GetScopeHash()); err != nil {
			return nil, fmt.Errorf("source cache lookup answers: %w", err)
		}
		if len(a.GetEtag()) > maxAnswerETagLen {
			return nil, fmt.Errorf("source cache lookup answers: etag for scope %q is %d bytes (max %d)", a.GetScopeHash(), len(a.GetEtag()), maxAnswerETagLen)
		}
		out = append(out, Answer{
			Query: Query{RowKind: kind, ScopeHash: a.GetScopeHash()},
			Found: a.GetFound(),
			ETag:  a.GetEtag(),
		})
	}
	return out, nil
}
