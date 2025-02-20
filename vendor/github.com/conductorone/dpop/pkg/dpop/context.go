package dpop

import (
	"context"
)

type contextKey string

const (
	claimsContextKey contextKey = "dpop-claims"
)

// WithClaims stores DPoP claims in the context
func WithClaims(ctx context.Context, claims *Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey, claims)
}

// ClaimsFromContext retrieves DPoP claims from the context
func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	claims, ok := ctx.Value(claimsContextKey).(*Claims)
	return claims, ok
}
