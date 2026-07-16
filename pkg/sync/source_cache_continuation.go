package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

// Syncer side of the source-cache lookup continuation (ask/answer). On
// single-shot transports (gRPC-over-Lambda) the connector cannot call the
// lookup service mid-request, so it answers a list RPC with a
// SourceCacheLookupAsk instead of rows; the syncer resolves the queries
// against its LOCAL previous-sync store — the same store replay copies
// from, so lookup and replay can never disagree — and re-invokes the same
// request with SourceCacheLookupAnswers attached. See
// docs/tasks/source-cache-lambda-lookup.md and the annotation contract in
// proto/c1/connector/v2/annotation_source_cache.proto.

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
)

const (
	// sourceCacheBounceCap bounds consecutive asks for the SAME request
	// (same page token; only the answers annotation differs between
	// re-invokes). A connector that keeps asking without progressing is
	// broken (most commonly: swallowing ErrLookupDeferred and re-asking
	// for scopes it already "handled"), and silence would be the
	// stale-data failure mode — so fail loudly.
	//
	// Deliberately NOT per action: a multi-page action that asks once per
	// page (e.g. a delta planner pre-resolving each planning page's chunk
	// scopes) bounces once per page with monotonic progress — every
	// NextPageToken advance is a new request and resets the counter.
	//
	// Shared with pkg/sourcecache so the connector-side answer-set cap
	// (bounces × ask size) stays coherent with this loop's enforcement.
	sourceCacheBounceCap = sourcecache.MaxLookupBouncesPerRequest

	// maxEnqueuePageTokensPerResponse bounds EnqueuePageTokens fan-out per response
	// (each spawned action persists in the checkpointed state token, so
	// unbounded fan-out bloats every checkpoint). Matches the proto
	// max_items validate rule on EnqueuePageTokens.page_tokens.
	maxEnqueuePageTokensPerResponse = 1024
	// maxEnqueuedPageTokenBytes matches the page-token request field and
	// the EnqueuePageTokens item validation. Enforced before creating Actions:
	// action tokens are checkpointed before they are sent through the
	// later request field's validator.
	maxEnqueuedPageTokenBytes = 1 << 20

	// maxEnqueuedPageTokenTotalBytes bounds the AGGREGATE token bytes of
	// one EnqueuePageTokens response. The per-item caps alone still admit
	// 1024 × 1MiB = 1GiB of tokens in a single legal response — all of it
	// persisted into every subsequent checkpoint token until the actions
	// drain. Real planner tokens are tiny (chunk ids, cursor strings);
	// 16MiB of aggregate budget is orders of magnitude above any sane
	// fan-out while keeping worst-case checkpoints bounded.
	maxEnqueuedPageTokenTotalBytes = 16 << 20

	// sourceCacheAnswerBudget caps the TOTAL answers payload attached to
	// one re-invoke — validator bytes AND per-answer identity bytes
	// (row_kind, scope_key, proto framing) — keeping the request under
	// single-shot transport payload limits (Lambda invokes cap at 6MB;
	// the dual-encoded frame at 5MiB). Identity bytes must be charged
	// too: at the proto caps (16384 accumulated answers × 256-byte scope
	// keys) identities alone reach ~5MiB with zero validator bytes, so a
	// validator-only budget could not actually bound the request.
	//
	// A found answer whose etag does not fit the remaining budget is
	// DEGRADED to a not-found answer: the connector fetches that scope
	// fresh (cold-correct, just slower). Leaving it absent-and-re-askable
	// would livelock — answers accumulate across bounces (the connector
	// re-executes from scratch each phase, so every re-invoke carries the
	// union), so the budget only shrinks and an etag that did not fit once
	// can never fit later; re-asking would burn straight into the bounce
	// cap and hard-fail a sync that cold-degrades correctly. Identity
	// bytes are NOT degradable (a not-found answer carries the same
	// identity, and an absent answer livelocks), so identity bytes alone
	// exceeding the budget is a hard failure — the connector asked for an
	// answer set no re-invoke could ever carry.
	sourceCacheAnswerBudget = 2 << 20

	// sourceCacheAnswerOverheadBytes approximates one answer's non-string
	// proto framing (field tags, length prefixes, the found bool, the Any
	// wrapping amortized across the message). Deliberately generous: the
	// budget exists to stay under transport limits, not to be exact.
	sourceCacheAnswerOverheadBytes = 16
)

// Continuation counters live in SourceCacheStats on the sync state (see
// source_cache_stats.go): token-persisted so a resumed sync keeps them,
// reported once in the source-cache summary line.

// listAttempt is one list-RPC attempt as observed by the continuation
// loop: enough of the response to detect and validate an ask.
type listAttempt struct {
	annos     annotations.Annotations
	rows      int
	nextToken string
}

// continuationCallMethod labels connector-call latency stats: re-invoked
// attempts (answer turns) get a ":cont" suffix so millisecond protocol
// turns don't blend into the base method's upstream-fetch distribution.
func continuationCallMethod(method string, attempt int) string {
	if attempt > 0 {
		return method + ":cont"
	}
	return method
}

// withSourceCacheContinuation drives the ask/answer loop around one list
// RPC. issue performs the RPC with extra request annotations (the lookup
// offer, plus accumulated answers on re-invokes) and reports the response
// surface; the loop returns once a response carries no ask — that final
// response is the one the caller processes. attempt is 0 for the first
// issue and increments per re-invoke, so callers can label protocol turns
// separately in connector-call latency stats (a re-invoke that replays is
// not comparable to a cold upstream fetch).
//
// The offer is attached only when the syncer can actually answer (warm
// previous-sync lookup). Old or cold syncers never send it, and a
// compliant connector never asks without it — that pairing is what makes
// version skew degrade to a cold sync instead of a misread response.
func (s *syncer) withSourceCacheContinuation(ctx context.Context, op string, issue func(extra annotations.Annotations, attempt int) (listAttempt, error)) error {
	l := ctxzap.Extract(ctx)

	warm := s.sourceCache.prev != nil
	extra := annotations.Annotations{}
	if warm {
		extra.Update(&v2.SourceCacheLookupOffer{})
	}

	// Answers accumulate across bounces: the connector re-executes from
	// scratch each phase, so every re-invoke must carry the union of all
	// resolved queries, in first-resolved order (deterministic requests).
	var ordered []sourcecache.Answer
	seen := map[sourcecache.Query]bool{}

	asked, found, notFound, truncated := 0, 0, 0, 0
	for bounce := 0; ; bounce++ {
		attempt, err := issue(extra, bounce)
		if err != nil {
			return err
		}

		ask := &v2.SourceCacheLookupAsk{}
		hasAsk, err := attempt.annos.Pick(ask)
		if err != nil {
			return fmt.Errorf("%s: error parsing source-cache lookup ask: %w", op, err)
		}
		if !hasAsk {
			if bounce > 0 {
				s.state.AddSourceCacheStats(SourceCacheStats{
					LookupBounces:          int64(bounce),
					LookupRequestsBounced:  1,
					LookupScopesAsked:      int64(asked),
					LookupAnsweredFound:    int64(found),
					LookupAnsweredNotFound: int64(notFound),
					LookupAnswersTruncated: int64(truncated),
					LookupBouncesByOp:      map[string]int64{op: int64(bounce)},
				})
			}
			return nil
		}

		// Ask legality. Failing loudly here is deliberate: every branch
		// is a connector bug that would otherwise surface as silently
		// wrong data or an unexplained stall.
		if !warm {
			return fmt.Errorf("%s: connector sent a source-cache lookup ask on a request that carried no offer (connector must gate asks on SourceCacheLookupOffer)", op)
		}
		if attempt.rows > 0 || attempt.nextToken != "" ||
			attempt.annos.Contains(&v2.SourceCacheRecord{}) || attempt.annos.Contains(&v2.SourceCacheReplay{}) ||
			attempt.annos.Contains(&v2.EnqueuePageTokens{}) {
			return fmt.Errorf("%s: source-cache lookup ask response must carry ONLY the ask: "+
				"no rows, no next page token, no scope/replay annotations, no spawned cursors "+
				"(spawn on the re-invoked request's real response instead)", op)
		}
		if bounce >= sourceCacheBounceCap {
			s.state.AddSourceCacheStats(SourceCacheStats{LookupCapFailures: 1})
			return fmt.Errorf("%s: source-cache lookup bounce cap (%d) exceeded for one request; "+
				"connector kept asking without progressing (%d scopes still unresolved) — "+
				"check for swallowed ErrLookupDeferred or nondeterministic scope computation",
				op, sourceCacheBounceCap, len(ask.GetQueries()))
		}

		queries, err := sourcecache.QueriesFromProto(ask)
		if err != nil {
			return fmt.Errorf("%s: invalid source-cache lookup ask: %w", op, err)
		}

		answerIdentityCost := func(q sourcecache.Query) int {
			return len(q.RowKind) + len(q.ScopeKey) + sourceCacheAnswerOverheadBytes
		}
		budget := sourceCacheAnswerBudget
		for _, a := range ordered {
			budget -= answerIdentityCost(a.Query) + len(a.CacheValidator)
		}
		newAsked, newFound, newNotFound, newTruncated := 0, 0, 0, 0
		for _, q := range queries {
			if seen[q] {
				continue
			}
			newAsked++
			// Identity bytes are spent for EVERY answered query — found,
			// not-found, and degraded alike (the answer must exist or the
			// connector livelocks re-asking it). If identities alone blow
			// the budget, no degradation can produce a carryable request.
			identityCost := answerIdentityCost(q)
			if identityCost > budget {
				s.state.AddSourceCacheStats(SourceCacheStats{LookupCapFailures: 1})
				return fmt.Errorf("%s: source-cache lookup answers exceed the %d-byte request budget on identity bytes alone "+
					"(%d answers accumulated); the connector's asks name more scope bytes than any re-invoke can carry",
					op, sourceCacheAnswerBudget, len(ordered))
			}
			budget -= identityCost
			entry, ok, err := s.sourceCache.lookup.Lookup(ctx, q.RowKind, q.ScopeKey)
			if err != nil {
				return fmt.Errorf("%s: resolving source-cache lookup ask: %w", op, err)
			}
			if !ok {
				seen[q] = true
				ordered = append(ordered, sourcecache.Answer{Query: q, Found: false})
				newNotFound++
				continue
			}
			if len(entry.CacheValidator) > budget {
				// Found, but the etag does not fit the remaining budget.
				// Degrade to an explicit not-found so the connector
				// fetches this scope fresh (cold-correct). See the
				// sourceCacheAnswerBudget comment: an absent answer could
				// never be resolved on a later bounce, only livelock into
				// the bounce cap.
				seen[q] = true
				ordered = append(ordered, sourcecache.Answer{Query: q, Found: false})
				newTruncated++
				continue
			}
			budget -= len(entry.CacheValidator)
			seen[q] = true
			ordered = append(ordered, sourcecache.Answer{Query: q, Found: true, CacheValidator: entry.CacheValidator})
			newFound++
		}
		asked += newAsked
		found += newFound
		notFound += newNotFound
		truncated += newTruncated

		if newAsked == 0 {
			// Every query was already answered on the request the
			// connector just saw; re-invoking cannot make progress.
			return fmt.Errorf("%s: connector re-asked only already-answered scopes (%d queries); connector lookup handling is broken", op, len(queries))
		}

		extra.Update(sourcecache.AnswersProto(ordered))

		l.Debug("source-cache lookup bounce",
			zap.String("op", op),
			zap.Int("bounce", bounce+1),
			zap.Int("asked", newAsked),
			zap.Int("found", newFound),
			zap.Int("not_found", newNotFound),
			zap.Int("degraded_to_cold_for_budget", newTruncated),
		)
	}
}
