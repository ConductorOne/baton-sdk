package sourcecache

import "context"

type scopeContextKey struct{}

// WithScope returns a context carrying the source-cache scope key for rows
// written under it.
func WithScope(ctx context.Context, scopeKey string) context.Context {
	return context.WithValue(ctx, scopeContextKey{}, scopeKey)
}

// ScopeFromContext returns the scope key set by WithScope, or "".
func ScopeFromContext(ctx context.Context) string {
	scopeKey, _ := ctx.Value(scopeContextKey{}).(string)
	return scopeKey
}
