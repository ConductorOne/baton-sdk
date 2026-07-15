package sourcecache

import "context"

type scopeContextKey struct{}

// WithScope returns a context carrying the source-cache scope key for
// rows written under it. The syncer wraps a page's store writes in this
// context when the page carried a SourceCacheRecord annotation; the Pebble
// write path stamps the record's source_scope_key from it.
func WithScope(ctx context.Context, scopeKey string) context.Context {
	return context.WithValue(ctx, scopeContextKey{}, scopeKey)
}

// ScopeFromContext returns the scope key set by WithScope, or "" when
// the context carries none (the common, unstamped case).
func ScopeFromContext(ctx context.Context) string {
	s, _ := ctx.Value(scopeContextKey{}).(string)
	return s
}
