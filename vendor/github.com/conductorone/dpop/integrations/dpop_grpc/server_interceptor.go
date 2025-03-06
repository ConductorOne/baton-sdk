// Package dpop_grpc provides gRPC server interceptors with DPoP support
package dpop_grpc

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/conductorone/dpop/pkg/dpop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	// ErrMissingDPoPHeader is returned when the DPoP header is missing
	ErrMissingDPoPHeader = errors.New("missing DPoP header")

	// ErrInvalidDPoPHeader is returned when the DPoP header is invalid
	ErrInvalidDPoPHeader = errors.New("invalid DPoP header")

	// ErrInvalidAuthScheme is returned when the Authorization header has an invalid scheme
	ErrInvalidAuthScheme = errors.New("invalid authorization scheme")
)

const (
	// AuthorizationHeader is the standard HTTP header for authorization
	AuthorizationHeader = "authorization"
	// DPoPScheme is the scheme used for DPoP bound tokens
	DPoPScheme = "DPoP"
	// BearerScheme is the scheme used for Bearer tokens
	BearerScheme = "Bearer"
)

// serverOptions configures the behavior of the DPoP interceptor
type serverOptions struct {
	// validationOptions are the options for DPoP proof validation
	validationOptions []dpop.Option

	// nonceGenerator generates nonces for DPoP proofs
	nonceGenerator dpop.NonceGenerator

	// authority is the authority to use for DPoP proof validation
	authority string
}

// ServerOption configures how we set up the DPoP interceptor
type ServerOption func(*serverOptions)

// WithValidationOptions sets the validation options for DPoP proof validation
func WithValidationOptions(opts ...dpop.Option) ServerOption {
	return func(o *serverOptions) {
		o.validationOptions = opts
	}
}

func WithAuthority(authority string) ServerOption {
	return func(o *serverOptions) {
		o.authority = authority
	}
}

// WithNonceGenerator sets the nonce generator for DPoP proofs
func WithNonceGenerator(ng dpop.NonceGenerator) ServerOption {
	return func(o *serverOptions) {
		o.nonceGenerator = ng
	}
}

// defaultServerOptions returns the default options for the interceptor
func defaultServerOptions() *serverOptions {
	return &serverOptions{
		validationOptions: nil, // Will use defaults from dpop.NewValidator
	}
}

// ServerUnaryInterceptor creates a new unary server interceptor with DPoP support
func ServerUnaryInterceptor(opts ...ServerOption) grpc.UnaryServerInterceptor {
	options := defaultServerOptions()
	for _, opt := range opts {
		opt(options)
	}

	validator := dpop.NewValidator(options.validationOptions...)

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Generate and set a new nonce for the next request
		if options.nonceGenerator != nil {
			nonce, err := options.nonceGenerator(ctx)
			if err != nil {
				return nil, status.Error(codes.Unauthenticated, fmt.Sprintf("failed to generate nonce: %v", err))
			}
			header := metadata.Pairs(dpop.NonceHeaderName, nonce)
			grpc.SetHeader(ctx, header)
		}

		// Get the DPoP proof from the metadata
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		// Get DPoP proof and Authorization header
		proofs := md.Get(dpop.HeaderName)
		authHeaders := md.Get(AuthorizationHeader)

		// Parse Authorization header if present
		var authScheme, accessToken string
		if len(authHeaders) > 0 {
			parts := strings.SplitN(authHeaders[0], " ", 2)
			if len(parts) != 2 {
				return nil, status.Error(codes.Unauthenticated, "invalid authorization scheme")
			}
			authScheme = parts[0]
			accessToken = parts[1]
		}

		// If there's no DPoP proof and no DPoP Authorization header, proceed with the request
		// This allows the interceptor to be used where some endpoints don't require DPoP
		if len(proofs) == 0 && authScheme != DPoPScheme {
			return handler(ctx, req)
		}

		// Here, we know this request must be DPoP validated, so make sure we have a proof
		if len(proofs) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing DPoP header")
		}

		// If we have an Authorization header, it must be DPoP
		if len(authHeaders) > 0 && authScheme != DPoPScheme {
			return nil, status.Error(codes.Unauthenticated, "invalid authorization scheme")
		}

		// Prepare validation options
		var validationOpts []dpop.ValidationProofOption
		if authScheme == DPoPScheme {
			// If using DPoP scheme, the proof MUST be bound to the access token
			validationOpts = append(validationOpts, dpop.WithProofExpectedAccessToken(accessToken))
		}

		// Validate the proof
		fullURL := url.URL{
			Scheme: "https",
			Host:   options.authority,
			Path:   info.FullMethod,
		}
		claims, err := validator.ValidateProof(ctx, proofs[0], "POST", fullURL.String(), validationOpts...)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, fmt.Sprintf("invalid DPoP proof: %v", err))
		}

		// Store the validated claims in the context
		ctx = dpop.WithClaims(ctx, claims)

		// Call the handler
		return handler(ctx, req)
	}
}

// ServerStreamInterceptor creates a new stream server interceptor with DPoP support
func ServerStreamInterceptor(opts ...ServerOption) grpc.StreamServerInterceptor {
	options := defaultServerOptions()
	for _, opt := range opts {
		opt(options)
	}

	validator := dpop.NewValidator(options.validationOptions...)

	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()

		// Generate and set a new nonce for the next request
		if options.nonceGenerator != nil {
			nonce, err := options.nonceGenerator(ctx)
			if err != nil {
				return status.Error(codes.Unauthenticated, "failed to generate nonce")
			}
			header := metadata.Pairs(dpop.NonceHeaderName, nonce)
			grpc.SetHeader(ctx, header)
		}

		// Get the DPoP proof from the metadata
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return status.Error(codes.Unauthenticated, "missing metadata")
		}

		// Get DPoP proof and Authorization header
		proofs := md.Get(dpop.HeaderName)
		authHeaders := md.Get(AuthorizationHeader)

		// Parse Authorization header if present
		var authScheme, accessToken string
		if len(authHeaders) > 0 {
			parts := strings.SplitN(authHeaders[0], " ", 2)
			if len(parts) != 2 {
				return status.Error(codes.Unauthenticated, "invalid authorization scheme")
			}
			authScheme = parts[0]
			accessToken = parts[1]
		}

		// If there's no DPoP proof and no DPoP Authorization header, proceed with the request
		// This allows the interceptor to be used where some endpoints don't require DPoP
		if len(proofs) == 0 && authScheme != DPoPScheme {
			return handler(srv, ss)
		}

		// Here, we know this request must be DPoP validated, so make sure we have a proof
		if len(proofs) == 0 {
			return status.Error(codes.Unauthenticated, "missing DPoP header")
		}

		// If we have an Authorization header, it must be DPoP
		if len(authHeaders) > 0 && authScheme != DPoPScheme {
			return status.Error(codes.Unauthenticated, "invalid authorization scheme")
		}

		// Prepare validation options
		var validationOpts []dpop.ValidationProofOption
		if authScheme == DPoPScheme {
			// If using DPoP scheme, the proof MUST be bound to the access token
			validationOpts = append(validationOpts, dpop.WithProofExpectedAccessToken(accessToken))
		}

		// Validate the proof
		fullURL := url.URL{
			Scheme: "https",
			Host:   options.authority,
			Path:   info.FullMethod,
		}
		claims, err := validator.ValidateProof(ctx, proofs[0], "POST", fullURL.String(), validationOpts...)
		if err != nil {
			return status.Error(codes.Unauthenticated, fmt.Sprintf("invalid DPoP proof: %v", err))
		}

		// Store the validated claims in the context
		ctx = dpop.WithClaims(ctx, claims)

		// Wrap the server stream to use the new context
		wrappedStream := &wrappedServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}

		// Call the handler
		return handler(srv, wrappedStream)
	}
}

// wrappedServerStream wraps grpc.ServerStream to modify the context
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the wrapper's context
func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}
