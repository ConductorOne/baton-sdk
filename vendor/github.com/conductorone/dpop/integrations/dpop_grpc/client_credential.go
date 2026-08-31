package dpop_grpc

import (
	"context"
	"errors"
	"net/url"

	"github.com/conductorone/dpop/integrations/dpop_oauth2"
	"github.com/conductorone/dpop/pkg/dpop"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// DPoPCredentials implements the credentials.PerRPCCredentials interface
// to provide DPoP proof generation and OAuth2 token injection for gRPC calls
type DPoPCredentials struct {
	proofer     *dpop.Proofer
	authority   string
	proofOpts   []dpop.ProofOption
	tokenSource oauth2.TokenSource
	requireTLS  bool
}

// NewDPoPCredentials creates a new DPoPCredentials instance
func NewDPoPCredentials(
	proofer *dpop.Proofer,
	tokenSource oauth2.TokenSource,
	authority string,
	proofOpts []dpop.ProofOption,
) (*DPoPCredentials, error) {
	return &DPoPCredentials{
		proofer:     proofer,
		authority:   authority,
		proofOpts:   proofOpts,
		tokenSource: tokenSource,
		requireTLS:  true,
	}, nil
}

var _ credentials.PerRPCCredentials = (*DPoPCredentials)(nil)

// GetRequestMetadata implements credentials.PerRPCCredentials.GetRequestMetadata
func (d *DPoPCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	reqInfo, ok := credentials.RequestInfoFromContext(ctx)
	if !ok {
		return nil, errors.New("dpop_grpc: DPoPCredentials: GetRequestMetadata: missing request info")
	}

	// Construct the full URL for the method
	fullURL := url.URL{
		Scheme: "https",
		Host:   d.authority,
		Path:   reqInfo.Method,
	}
	// Get the OAuth2 token
	token, err := d.tokenSource.Token()
	if err != nil {
		return nil, tokenStatusError(err)
	}

	// Add access token to proof options
	finalProofOpts := append([]dpop.ProofOption{}, d.proofOpts...)
	finalProofOpts = append(finalProofOpts, dpop.WithAccessToken(token.AccessToken))

	// Generate the proof
	dpopProof, err := d.proofer.CreateProof(ctx, "POST", fullURL.String(), finalProofOpts...)
	if err != nil {
		return nil, err
	}

	// Return both DPoP and Authorization headers
	return map[string]string{
		dpop.HeaderName:         dpopProof,
		AuthorizationHeaderName: token.Type() + " " + token.AccessToken,
	}, nil
}

// RequireTransportSecurity implements credentials.PerRPCCredentials.RequireTransportSecurity
func (d *DPoPCredentials) RequireTransportSecurity() bool {
	return d.requireTLS
}

// tokenStatusError maps a token source failure onto a gRPC status so the
// transient/definitive classification survives the per-RPC credentials
// boundary — grpc-go flattens any non-status credentials error to
// codes.Unauthenticated. Transient failures (5xx responses, transport errors,
// timeouts; see dpop_oauth2.IsTransient) become codes.Unavailable so callers'
// retry policies treat them as retryable. Definitive failures (e.g.
// invalid_client, a disabled credential) become codes.Unauthenticated and
// fail fast.
func tokenStatusError(err error) error {
	code := codes.Unauthenticated
	if dpop_oauth2.IsTransient(err) {
		code = codes.Unavailable
	}
	return status.Errorf(code, "dpop_grpc: failed to fetch token: %v", err)
}
