// Package dpop_grpc provides gRPC client interceptors with DPoP support
package dpop_grpc

import (
	"context"
	"net/url"

	"github.com/conductorone/dpop/pkg/dpop"
	"github.com/go-jose/go-jose/v4"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	// AuthorizationHeaderName is the name of the Authorization header
	AuthorizationHeaderName = "authorization"
)

// getFullMethodURL constructs the full URL for a gRPC method using the connection target
func getFullMethodURL(cc *grpc.ClientConn, method string) (string, error) {
	// Get the target URI from the connection
	target := cc.Target()

	// Parse the target as a URL
	parsedURL, err := url.Parse(target)
	if err != nil {
		return "", err
	}

	outURL := url.URL{
		Scheme: "https",
		Host:   parsedURL.Host,
		Path:   method,
	}

	return outURL.String(), nil
}

// ClientUnaryInterceptor creates a new unary client interceptor with DPoP and OAuth2 support
func ClientUnaryInterceptor(key *jose.JSONWebKey, tokenSource oauth2.TokenSource, proofOpts []dpop.ProofOption) (grpc.UnaryClientInterceptor, error) {
	proofer, err := dpop.NewProofer(key)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
		// Get the full method URL
		fullURL, err := getFullMethodURL(cc, method)
		if err != nil {
			return err
		}

		token, err := tokenSource.Token()
		if err != nil {
			return err
		}

		// Add access token to proof options
		finalProofOpts := append([]dpop.ProofOption{}, proofOpts...)
		finalProofOpts = append(finalProofOpts, dpop.WithAccessToken(token.AccessToken))

		// Generate the proof
		dpopProof, err := proofer.CreateProof(ctx, "POST", fullURL, finalProofOpts...)
		if err != nil {
			return err
		}

		// Add both DPoP and Authorization headers to the outgoing context
		ctx = metadata.AppendToOutgoingContext(ctx,
			dpop.HeaderName, dpopProof,
			AuthorizationHeaderName, token.Type()+" "+token.AccessToken,
		)

		// Call the next handler
		return invoker(ctx, method, req, reply, cc, callOpts...)
	}, nil
}

// ClientStreamInterceptor creates a new stream client interceptor with DPoP and OAuth2 support
func ClientStreamInterceptor(key *jose.JSONWebKey, tokenSource oauth2.TokenSource, proofOpts []dpop.ProofOption) (grpc.StreamClientInterceptor, error) {
	proofer, err := dpop.NewProofer(key)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, callOpts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Get the full method URL
		fullURL, err := getFullMethodURL(cc, method)
		if err != nil {
			return nil, err
		}

		// Get the OAuth2 token
		token, err := tokenSource.Token()
		if err != nil {
			return nil, err
		}

		// Add access token to proof options
		finalProofOpts := append([]dpop.ProofOption{}, proofOpts...)
		finalProofOpts = append(finalProofOpts, dpop.WithAccessToken(token.AccessToken))

		// Generate the proof
		dpopProof, err := proofer.CreateProof(ctx, "POST", fullURL, finalProofOpts...)
		if err != nil {
			return nil, err
		}

		// Add both DPoP and Authorization headers to the outgoing context
		ctx = metadata.AppendToOutgoingContext(ctx,
			dpop.HeaderName, dpopProof,
			AuthorizationHeaderName, token.Type()+" "+token.AccessToken,
		)

		// Call the next handler
		return streamer(ctx, desc, cc, method, callOpts...)
	}, nil
}
