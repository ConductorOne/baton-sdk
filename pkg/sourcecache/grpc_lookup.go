package sourcecache

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
)

// GRPCLookup is the connector-side Lookup implementation that talks to
// BatonSourceCacheService on the parent SDK.
//
// This is deliberately not routed through the session store: session data
// passes through the connector's local MemorySessionCache (otter), which
// would apply generic TTL/eviction policies to sync-scoped validator state.
// The dedicated service keeps the path uncached and the message shape
// explicit.
//
// The parent has exactly one active Lookup registered at a time (set per
// sync via SetSourceCache on the server), so the wire format carries no
// sync_id; routing is implicit.
//
// Per the Lookup contract, an RPC failure degrades to a miss rather than
// failing the connector's list call: a miss is always safe — the
// connector fetches fresh — while the error would ride back through the
// connector's handler wrappings, where its status code rarely survives to
// the syncer's retryer, hard-failing the sync on a transient loopback
// blip. Failures are logged at most once per minute with a running count
// (this object lives for the whole subprocess run, so a once-ever log
// would make a persistently broken parent path invisible after the first
// scope), and not at all when the caller's context is already done — a
// teardown-time Canceled is noise, and logging it would eat the rate
// window a real failure needs. Validation failures stay loud: a malformed
// row kind or scope key is a connector bug, and degrading it to a miss
// would let the connector silently cold-fetch forever with no signal.
type GRPCLookup struct {
	client v1.BatonSourceCacheServiceClient
	// failures counts degraded lookups for the log line; lastLogNano is
	// the unix-nano timestamp of the last emitted warning.
	failures    atomic.Int64
	lastLogNano atomic.Int64
}

// lookupFailureLogInterval rate-limits the degraded-to-miss warning.
const lookupFailureLogInterval = time.Minute

// NewGRPCLookup returns a Lookup backed by the given client. A nil client
// yields NoopLookup so callers can configure the client optionally without
// nil checks at every call site.
func NewGRPCLookup(client v1.BatonSourceCacheServiceClient) Lookup {
	if client == nil {
		return NoopLookup{}
	}
	return &GRPCLookup{client: client}
}

func (g *GRPCLookup) Lookup(ctx context.Context, rowKind RowKind, scopeKey string) (Entry, bool, error) {
	if err := ValidateRowKind(rowKind); err != nil {
		return Entry{}, false, err
	}
	if err := ValidateScopeKey(scopeKey); err != nil {
		return Entry{}, false, err
	}
	resp, err := g.client.Lookup(ctx, v1.LookupRequest_builder{
		RowKind:  string(rowKind),
		ScopeKey: scopeKey,
	}.Build())
	if err != nil {
		count := g.failures.Add(1)
		if ctx.Err() == nil {
			now := time.Now().UnixNano()
			last := g.lastLogNano.Load()
			if now-last >= int64(lookupFailureLogInterval) && g.lastLogNano.CompareAndSwap(last, now) {
				ctxzap.Extract(ctx).Warn("source cache rpc lookup failed; treating as miss",
					zap.Error(err),
					zap.Int64("failures_since_start", count))
			}
		}
		// Intentional nil error: a failed lookup degrades to a miss
		// (connector fetches fresh) rather than failing the connector call.
		return Entry{}, false, nil
	}
	if !resp.GetFound() {
		return Entry{}, false, nil
	}
	return Entry{CacheValidator: resp.GetCacheValidator()}, true, nil
}
