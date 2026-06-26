package c1zstore

import "context"

// Exchange is one captured upstream transport request/response (HTTP/SQL/LDAP),
// keyed by RequestID, persisted alongside the synced objects so an operator can
// resolve an emitted object's c1.connector.v2.RequestId annotations back to the
// exchanges that built it. Request/Response are stored as already-redacted JSON
// (the connector redacts at capture). See the per-object-request-provenance RFC.
type Exchange struct {
	RequestID     string
	SyncID        string
	TransportKind string // "http" | "sql" | "ldap"
	// RequestJSON / ResponseJSON are redacted, transport-agnostic JSON blobs.
	RequestJSON  []byte
	ResponseJSON []byte
	// Intent routing context (surface/entrypoint/resourceType), JSON-encoded.
	IntentJSON   []byte
	DiscoveredAt string
}

// ExchangeStore is the provenance sub-store. It is reached via the optional
// ExchangesProvider interface (NOT embedded in Store) so that engines and
// external implementations that predate provenance keep compiling unchanged —
// the syncer type-asserts for ExchangesProvider and skips provenance when a
// store does not implement it.
type ExchangeStore interface {
	// PutExchanges persists exchanges for the current sync (upsert by request id).
	PutExchanges(ctx context.Context, exchanges ...*Exchange) error
	// GetExchange returns the exchange for a request id, or (nil, nil) if absent.
	GetExchange(ctx context.Context, requestID string) (*Exchange, error)
	// ListExchangesForObject returns the exchanges for the given request ids
	// (those an object's RequestId annotations point at), skipping any absent.
	ListExchangesForObject(ctx context.Context, requestIDs []string) ([]*Exchange, error)
}

// ExchangesProvider is the optional capability a Store implementation advertises
// when it can persist provenance exchanges. Callers do:
//
//	if xp, ok := store.(c1zstore.ExchangesProvider); ok { xp.Exchanges()... }
//
// Absent → provenance is silently skipped; no behavior change, no break to
// external Store implementations.
type ExchangesProvider interface {
	Exchanges() ExchangeStore
}
