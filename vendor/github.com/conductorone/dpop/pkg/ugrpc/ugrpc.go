package ugrpc

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net/http"
	"net/url"
	"time"

	dpop "github.com/conductorone/dpop/pkg"
	"github.com/go-jose/go-jose/v4"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type SignerOptions struct {
	HttpClient *http.Client
}

func WithNewDPoPSigner(ctx context.Context, tokenURL *url.URL, clientID string, clientSecret *jose.JSONWebKey, claimsAdjuster dpop.ClaimsAdjuster, opts ...SignerOptions) (grpc.DialOption, error) {
	if len(opts) > 1 {
		return nil, fmt.Errorf("with-dpop: only one option is allowed")
	}
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf("with-dpop: failed to generate ed25519 key: %w", err)
	}

	dpopSigner, err := dpop.NewDPoPProofer(privateKey)
	if err != nil {
		return nil, fmt.Errorf("with-dpop: failed to create c1 lambda credential provider: %w", err)
	}
	httpClient := http.DefaultClient
	if len(opts) == 1 {
		o := opts[0]
		if o.HttpClient != nil {
			httpClient = o.HttpClient
		}
	}

	tokenSource, err := dpop.NewTokenSource(ctx, dpopSigner, tokenURL, clientID, clientSecret, claimsAdjuster, httpClient)
	if err != nil {
		return nil, fmt.Errorf("with-dpop: failed to create c1 token source: %w", err)
	}

	credProvider := &dpopPerRPCCredentials{
		tokenSource: tokenSource,
		dpopSigner:  dpopSigner,
	}

	return grpc.WithPerRPCCredentials(credProvider), nil
}

type dpopPerRPCCredentials struct {
	tokenSource oauth2.TokenSource
	dpopSigner  *dpop.DPoPProofer
	accessToken *oauth2.Token
}

func (c *dpopPerRPCCredentials) RequireTransportSecurity() bool {
	return true
}

func (c *dpopPerRPCCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	token := c.accessToken
	if c.accessToken == nil || c.accessToken.Expiry.Before(time.Now().Add(1*time.Minute)) {
		token, err := c.tokenSource.Token()
		if err != nil {
			return nil, err
		}
		c.accessToken = token
	}

	ri, _ := credentials.RequestInfoFromContext(ctx)
	err := credentials.CheckSecurityLevel(ri.AuthInfo, credentials.PrivacyAndIntegrity)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "connection is not secure enough to send credentials")
	}

	if len(uri) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "exactly one URI must be specified")
	}

	// FIXME(morgabra/kans): There must be a better way... maybe just set this directly?
	// uri here is just scheme://hostname/service, but we need to sign scheme://hostname/service/method
	parsedURI, err := url.Parse(uri[0])
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid URI: %s", err)
	}
	parsedURI.Path = ri.Method

	dpopProof, err := c.dpopSigner.Proof(http.MethodPost, parsedURI.String(), token.AccessToken, "")
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "failed to get dpop proof: %s", err)
	}

	return map[string]string{
		"authorization": token.Type() + " " + token.AccessToken,
		"dpop":          dpopProof,
	}, nil
}
