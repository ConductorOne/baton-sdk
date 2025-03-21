// Package dpop_grpc provides gRPC server interceptors with DPoP support
package dpop_grpc

import (
	"context"
	"errors"
	"fmt"
	"net/url"

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

		proofs := md.Get(dpop.HeaderName)
		if len(proofs) == 0 {
			return nil, status.Error(codes.Unauthenticated, "missing DPoP header")
		}

		fullURL := url.URL{
			Scheme: "https",
			Host:   options.authority,
			Path:   info.FullMethod,
		}
		claims, err := validator.ValidateProof(ctx, proofs[0], "POST", fullURL.String())
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

		proofs := md.Get(dpop.HeaderName)
		if len(proofs) == 0 {
			return status.Error(codes.Unauthenticated, "missing DPoP header")
		}

		// Validate the proof
		fullURL := url.URL{
			Scheme: "https",
			Host:   options.authority,
			Path:   info.FullMethod,
		}
		claims, err := validator.ValidateProof(ctx, proofs[0], "POST", fullURL.String())
		if err != nil {
			return status.Error(codes.Unauthenticated, "invalid DPoP proof")
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
