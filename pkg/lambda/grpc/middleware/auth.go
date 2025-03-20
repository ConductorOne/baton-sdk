package middleware

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type claimsContextKey struct{}

// ClaimsFromContext retrieves the JWT claims from the context.
func ClaimsFromContext(ctx context.Context) (map[string]interface{}, bool) {
	claims, ok := ctx.Value(claimsContextKey{}).(map[string]interface{})
	return claims, ok
}

// AuthInterceptor creates a new unary interceptor for JWT authentication.
func AuthInterceptor(validator *auth.Validator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Extract metadata from context.
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		// Get authorization header.
		authHeaders := md.Get("authorization")
		if len(authHeaders) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing authorization header")
		}

		// Extract bearer token.
		token, err := auth.ExtractBearerToken(authHeaders[0])
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "invalid authorization header")
		}

		// Validate token.
		claims, err := validator.ValidateToken(ctx, token)
		if err != nil {
			return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
		}

		// Add claims to context.
		newCtx := context.WithValue(ctx, claimsContextKey{}, claims)

		// Call the handler with the new context.
		return handler(newCtx, req)
	}
}

// WithAuth creates a new unary interceptor for JWT authentication.
func WithAuth(ctx context.Context, config auth.Config) (grpc.UnaryServerInterceptor, error) {
	validator, err := auth.NewValidator(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create JWT validator: %w", err)
	}

	return AuthInterceptor(validator), nil
}
