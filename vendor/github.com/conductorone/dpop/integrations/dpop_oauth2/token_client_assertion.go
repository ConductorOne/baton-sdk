package dpop_oauth2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/conductorone/dpop/pkg/dpop"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"golang.org/x/oauth2"
)

var (
	// ErrMissingRequiredField indicates a required field was not provided
	ErrMissingRequiredField = errors.New("dpop_oauth2: missing required field")

	// ErrInvalidToken indicates the token response was invalid
	ErrInvalidToken = errors.New("dpop_oauth2: invalid token response")

	// ErrNonceMissing indicates the server requested a nonce but didn't provide one
	ErrNonceMissing = errors.New("dpop_oauth2: server requested DPoP nonce but did not provide one")

	// ErrTokenRequestFailed indicates the token request failed
	ErrTokenRequestFailed = errors.New("dpop_oauth2: token request failed")

	// ErrProofCreationFailed indicates failure to create or sign DPoP proof
	ErrProofCreationFailed = errors.New("dpop_oauth2: failed to create or sign DPoP proof")
)

const (
	assertionType = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer-with-identity-attestation"
)

// TokenRequest encapsulates all the data needed for a token request
type TokenRequest struct {
	Claims    *jwt.Claims
	Body      url.Values
	Headers   http.Header
	Marshaler ClaimsMarshaler
}

// TokenRequestOption is a function that modifies a token request
type TokenRequestOption func(*TokenRequest) error

// ClaimsMarshaler is a function that marshals JWT claims
type ClaimsMarshaler func(*jwt.Claims) ([]byte, error)

// defaultMarshaler is the default claims marshaler using JSON
func defaultMarshaler(claims *jwt.Claims) ([]byte, error) {
	return json.Marshal(claims)
}

// WithCustomParameters returns an option that adds additional body parameters
func WithCustomParameters(fn func(url.Values) error) TokenRequestOption {
	return func(tr *TokenRequest) error {
		return fn(tr.Body)
	}
}

// WithCustomHeaders returns an option that adds additional headers
func WithCustomHeaders(fn func(http.Header) error) TokenRequestOption {
	return func(tr *TokenRequest) error {
		return fn(tr.Headers)
	}
}

// WithCustomMarshaler returns an option that sets a custom claims marshaler
func WithCustomMarshaler(m ClaimsMarshaler) TokenRequestOption {
	return func(tr *TokenRequest) error {
		tr.Marshaler = m
		return nil
	}
}

// TokenSourceOption configures the behavior of the token source
type TokenSourceOption func(*tokenSourceOptions)

type tokenSourceOptions struct {
	baseCtx        context.Context
	httpClient     *http.Client
	proofOptions   []dpop.ProofOption
	nonceStore     *NonceStore
	requestOptions []TokenRequestOption
}

// WithBaseContext sets a custom base context for the token source
func WithBaseContext(ctx context.Context) TokenSourceOption {
	return func(opts *tokenSourceOptions) {
		opts.baseCtx = ctx
	}
}

// WithHTTPClient sets a custom HTTP client
func WithHTTPClient(client *http.Client) TokenSourceOption {
	return func(opts *tokenSourceOptions) {
		opts.httpClient = client
	}
}

// WithProofOptions sets additional DPoP proof options
func WithProofOptions(options ...dpop.ProofOption) TokenSourceOption {
	return func(opts *tokenSourceOptions) {
		opts.proofOptions = append(opts.proofOptions, options...)
	}
}

// WithNonceStore sets a nonce store for DPoP proofs
func WithNonceStore(store *NonceStore) TokenSourceOption {
	return func(opts *tokenSourceOptions) {
		opts.nonceStore = store
	}
}

// WithRequestOption adds a TokenRequestOption to modify the token request
func WithRequestOption(opt TokenRequestOption) TokenSourceOption {
	return func(opts *tokenSourceOptions) {
		opts.requestOptions = append(opts.requestOptions, opt)
	}
}

func NewTokenSource(proofer *dpop.Proofer, tokenURL *url.URL, clientID string, clientSecret *jose.JSONWebKey, opts ...TokenSourceOption) (*tokenSource, error) {
	if proofer == nil {
		return nil, fmt.Errorf("%w: dpop-proofer", ErrMissingRequiredField)
	}

	if clientID == "" {
		return nil, fmt.Errorf("%w: client-id", ErrMissingRequiredField)
	}

	if clientSecret == nil {
		return nil, fmt.Errorf("%w: client-secret", ErrMissingRequiredField)
	}

	if tokenURL == nil {
		return nil, fmt.Errorf("%w: token-url", ErrMissingRequiredField)
	}

	options := &tokenSourceOptions{
		baseCtx:    context.Background(),
		httpClient: http.DefaultClient,
	}

	for _, opt := range opts {
		opt(options)
	}

	return &tokenSource{
		baseCtx:        options.baseCtx,
		clientID:       clientID,
		clientSecret:   clientSecret,
		tokenURL:       tokenURL,
		httpClient:     options.httpClient,
		proofer:        proofer,
		requestOptions: options.requestOptions,
		proofOptions:   options.proofOptions,
		nonceStore:     options.nonceStore,
	}, nil
}

type tokenSource struct {
	baseCtx        context.Context
	clientID       string
	clientSecret   *jose.JSONWebKey
	tokenURL       *url.URL
	httpClient     *http.Client
	proofer        *dpop.Proofer
	requestOptions []TokenRequestOption
	proofOptions   []dpop.ProofOption
	nonceStore     *NonceStore
}

func (c *tokenSource) Token() (*oauth2.Token, error) {
	ctx, done := context.WithTimeout(c.baseCtx, time.Second*30)
	defer done()
	return c.tryToken(ctx, true)
}

func (c *tokenSource) tryToken(ctx context.Context, firstAttempt bool) (*oauth2.Token, error) {
	jsigner, err := jose.NewSigner(
		jose.SigningKey{
			Algorithm: jose.EdDSA,
			Key:       c.clientSecret,
		},
		nil)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create signer: %v", ErrProofCreationFailed, err)
	}

	// Our token host may include a port, but the audience never expects a port
	aud := c.tokenURL.Hostname()
	now := time.Now()

	claims := &jwt.Claims{
		Issuer:    c.clientID,
		Subject:   c.clientID,
		Audience:  jwt.Audience{aud},
		Expiry:    jwt.NewNumericDate(now.Add(time.Minute * 2)),
		IssuedAt:  jwt.NewNumericDate(now),
		NotBefore: jwt.NewNumericDate(now.Add(-time.Minute * 1)),
	}

	tr := &TokenRequest{
		Claims: claims,
		Body: url.Values{
			"client_id":             []string{c.clientID},
			"grant_type":            []string{"client_credentials"},
			"client_assertion_type": []string{assertionType},
		},
		Headers:   http.Header{"Content-Type": []string{"application/x-www-form-urlencoded"}},
		Marshaler: defaultMarshaler,
	}

	for _, opt := range c.requestOptions {
		err = opt(tr)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to modify request: %v", ErrTokenRequestFailed, err)
		}
	}

	marshalledClaims, err := tr.Marshaler(claims)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to marshal claims: %v", ErrTokenRequestFailed, err)
	}

	method := http.MethodPost

	proofOpts := make([]dpop.ProofOption, 0, len(c.proofOptions)+2)
	proofOpts = append(proofOpts, c.proofOptions...)

	// Add nonce if available from store
	if c.nonceStore != nil {
		nonce := c.nonceStore.GetNonce()
		if nonce != "" {
			proofOpts = append(proofOpts, dpop.WithStaticNonce(nonce))
		}
	}

	dpopProof, err := c.proofer.CreateProof(ctx, method, c.tokenURL.String(), proofOpts...)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create proof: %v", ErrProofCreationFailed, err)
	}

	rv, err := jsigner.Sign(marshalledClaims)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to sign proof: %v", ErrProofCreationFailed, err)
	}

	s, err := rv.CompactSerialize()
	if err != nil {
		return nil, fmt.Errorf("%w: failed to serialize proof: %v", ErrProofCreationFailed, err)
	}

	tr.Body["client_assertion"] = []string{s}

	req, err := http.NewRequestWithContext(ctx, method, c.tokenURL.String(), strings.NewReader(tr.Body.Encode()))
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create request: %v", ErrTokenRequestFailed, err)
	}

	req.Header.Set(dpop.HeaderName, dpopProof)
	for k, v := range tr.Headers {
		req.Header[k] = v
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to execute request: %v", ErrTokenRequestFailed, err)
	}
	defer resp.Body.Close()

	// Check if we got a nonce challenge
	if resp.StatusCode == http.StatusBadRequest {
		var errorResp struct {
			Error            string `json:"error"`
			ErrorDescription string `json:"error_description"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errorResp); err != nil {
			return nil, fmt.Errorf("%w: failed to decode error response: %v", ErrTokenRequestFailed, err)
		}

		if errorResp.Error == "use_dpop_nonce" {
			// Get the new nonce from header
			nonce := resp.Header.Get(dpop.NonceHeaderName)
			if nonce == "" {
				return nil, ErrNonceMissing
			}

			// Store the nonce if we have a store
			if c.nonceStore != nil {
				c.nonceStore.SetNonce(nonce)
			}

			// Only retry once on first attempt
			if !firstAttempt {
				return nil, fmt.Errorf("%w: token request failed after retry: %s - %s", ErrTokenRequestFailed, errorResp.Error, errorResp.ErrorDescription)
			}

			// Try again with the new nonce
			return c.tryToken(ctx, false)
		}
		return nil, fmt.Errorf("%w: %s - %s", ErrTokenRequestFailed, errorResp.Error, errorResp.ErrorDescription)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: unexpected status code: %s", ErrTokenRequestFailed, resp.Status)
	}

	token := &oauth2.Token{}
	err = json.NewDecoder(resp.Body).Decode(token)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to decode token response: %v", ErrInvalidToken, err)
	}

	if token.AccessToken == "" {
		return nil, fmt.Errorf("%w: empty access token", ErrInvalidToken)
	}

	// Accept both DPoP and Bearer tokens
	// If we sent a DPoP proof but got a Bearer token, that means the AS doesn't support DPoP
	if !strings.EqualFold(token.TokenType, "DPoP") && !strings.EqualFold(token.TokenType, "Bearer") {
		return nil, fmt.Errorf("%w: invalid token type: %s", ErrInvalidToken, token.TokenType)
	}

	return token, nil
}
