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

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/google/uuid"
	"golang.org/x/oauth2"

	"github.com/conductorone/dpop/pkg/dpop"
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

	// ErrTokenRequestTransient classifies a token request failure as likely
	// transient: a 5xx or 429 response, a transport-level error, or a
	// timeout. Errors matching this sentinel always also match
	// ErrTokenRequestFailed; definitive OAuth protocol rejections (e.g.
	// invalid_client) match only ErrTokenRequestFailed. Use IsTransient to
	// test for it.
	ErrTokenRequestTransient = errors.New("dpop_oauth2: transient token request failure")

	// ErrProofCreationFailed indicates failure to create or sign DPoP proof
	ErrProofCreationFailed = errors.New("dpop_oauth2: failed to create or sign DPoP proof")
)

const (
	assertionType = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
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
	retry          RetryConfig
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

// WithRetryConfig overrides how transient token request failures are retried.
// See RetryConfig for field semantics; set MaxAttempts to 1 to disable
// retries entirely.
func WithRetryConfig(cfg RetryConfig) TokenSourceOption {
	return func(opts *tokenSourceOptions) {
		opts.retry = cfg
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
		retry:      DefaultRetryConfig(),
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
		retry:          options.retry.normalized(),
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
	retry          RetryConfig
}

func (c *tokenSource) Token() (*oauth2.Token, error) {
	ctx, done := context.WithTimeout(c.baseCtx, time.Second*30)
	defer done()

	// Transient failures (5xx/429, transport errors, timeouts) are retried
	// with capped exponential backoff + jitter. The retry re-enters tryToken,
	// so every attempt signs a fresh DPoP proof and client assertion — both
	// carry unique jtis, so an identical request is never replayed.
	// Definitive failures (OAuth protocol rejections) return immediately.
	//
	// A nonce learned from a use_dpop_nonce challenge is carried across
	// attempts so a bare consumer (no NonceStore) isn't re-challenged on
	// every retry.
	var lastErr error
	retryNonce := ""
	for attempt := 0; attempt < c.retry.MaxAttempts; attempt++ {
		if attempt > 0 {
			if !sleepBeforeRetry(ctx, c.retry, attempt) {
				// The context died mid-backoff. A deadline expiry (the 30s
				// Token() budget) is a timeout: surface the last transient
				// failure so callers can still classify it. A caller cancel
				// is not a timeout — strip the transient classification so
				// nothing retries abandoned work.
				if errors.Is(ctx.Err(), context.Canceled) {
					// context.Cause preserves a WithCancelCause cause in the
					// chain; for a plain cancel it is context.Canceled.
					return nil, fmt.Errorf("%w: %w during retry backoff (last error: %v)", ErrTokenRequestFailed, context.Cause(ctx), lastErr)
				}
				break
			}
		}

		token, nonce, err := c.tryToken(ctx, true, retryNonce)
		if err == nil {
			return token, nil
		}
		if nonce != "" {
			retryNonce = nonce
		}
		lastErr = err
		if !IsTransient(err) {
			return nil, err
		}
	}
	return nil, lastErr
}

// tryToken performs a single token request. retryNonce, when non-empty, is the
// nonce returned by a prior use_dpop_nonce challenge and is attached to this
// attempt's proof regardless of whether a NonceStore is configured. This is
// what makes a bare consumer (no NonceStore) nonce-aware: the challenge/retry
// is self-contained within a single Token() call.
//
// The second return value is the nonce in effect for this attempt (the
// carried retryNonce, a cached store nonce, or a newly challenged one), so
// the transient retry loop in Token() can carry it into the next attempt.
func (c *tokenSource) tryToken(ctx context.Context, firstAttempt bool, retryNonce string) (*oauth2.Token, string, error) {
	jsigner, err := jose.NewSigner(
		jose.SigningKey{
			Algorithm: jose.EdDSA,
			Key:       c.clientSecret,
		},
		nil)
	if err != nil {
		return nil, retryNonce, fmt.Errorf("%w: failed to create signer: %v", ErrProofCreationFailed, err)
	}

	// Our token host may include a port, but the audience never expects a port
	aud := c.tokenURL.Hostname()
	now := time.Now()

	claims := &jwt.Claims{
		// A unique jti makes every signed assertion distinct. Without it,
		// second-precision timestamps plus deterministic Ed25519 signatures
		// would make fast retries re-send a byte-identical assertion, which a
		// server enforcing RFC 7523 single-use may reject.
		ID:        uuid.New().String(),
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
			return nil, retryNonce, fmt.Errorf("%w: failed to modify request: %v", ErrTokenRequestFailed, err)
		}
	}

	marshalledClaims, err := tr.Marshaler(claims)
	if err != nil {
		return nil, retryNonce, fmt.Errorf("%w: failed to marshal claims: %v", ErrTokenRequestFailed, err)
	}

	method := http.MethodPost

	proofOpts := make([]dpop.ProofOption, 0, len(c.proofOptions)+2)
	proofOpts = append(proofOpts, c.proofOptions...)

	// Attach a nonce when available. Prefer the nonce from a use_dpop_nonce
	// challenge on this same Token() call (retryNonce); otherwise fall back to
	// a cached nonce from the configured store for cross-call reuse.
	nonce := retryNonce
	if nonce == "" && c.nonceStore != nil {
		nonce = c.nonceStore.GetNonce()
	}
	if nonce != "" {
		proofOpts = append(proofOpts, dpop.WithStaticNonce(nonce))
	}

	dpopProof, err := c.proofer.CreateProof(ctx, method, c.tokenURL.String(), proofOpts...)
	if err != nil {
		return nil, nonce, fmt.Errorf("%w: failed to create proof: %v", ErrProofCreationFailed, err)
	}

	rv, err := jsigner.Sign(marshalledClaims)
	if err != nil {
		return nil, nonce, fmt.Errorf("%w: failed to sign proof: %v", ErrProofCreationFailed, err)
	}

	s, err := rv.CompactSerialize()
	if err != nil {
		return nil, nonce, fmt.Errorf("%w: failed to serialize proof: %v", ErrProofCreationFailed, err)
	}

	tr.Body["client_assertion"] = []string{s}

	req, err := http.NewRequestWithContext(ctx, method, c.tokenURL.String(), strings.NewReader(tr.Body.Encode()))
	if err != nil {
		return nil, nonce, fmt.Errorf("%w: failed to create request: %v", ErrTokenRequestFailed, err)
	}

	req.Header.Set(dpop.HeaderName, dpopProof)
	for k, v := range tr.Headers {
		req.Header[k] = v
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// The transport error stays in the chain (%w) so callers can inspect
		// the underlying cause (context.Canceled, net errors, ...).
		reqErr := fmt.Errorf("%w: failed to execute request: %w", ErrTokenRequestFailed, err)
		// A canceled context means the caller abandoned the call — that is
		// not a transport failure, so don't classify it as retryable. Check
		// the context as well as the returned error: when the context was
		// canceled via context.WithCancelCause, Do returns the cause, which
		// need not match context.Canceled.
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return nil, nonce, reqErr
		}
		// Everything else that fails before an HTTP response (connection
		// resets, proxy errors, timeouts — including a deadline expiry, which
		// is exactly the timed-out token POST class) never reached the
		// authorization server's OAuth logic: it carries no verdict about the
		// credential, so it is safe to classify as retryable.
		return nil, nonce, markTransient(reqErr)
	}
	defer resp.Body.Close()

	// Check if we got a nonce challenge
	if resp.StatusCode == http.StatusBadRequest {
		var errorResp struct {
			Error            string `json:"error"`
			ErrorDescription string `json:"error_description"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&errorResp); err != nil {
			return nil, nonce, fmt.Errorf("%w: failed to decode error response: %v", ErrTokenRequestFailed, err)
		}

		if errorResp.Error == "use_dpop_nonce" {
			// Get the new nonce from header
			challengeNonce := resp.Header.Get(dpop.NonceHeaderName)
			if challengeNonce == "" {
				return nil, nonce, ErrNonceMissing
			}

			// Store the nonce for cross-call reuse if we have a store
			if c.nonceStore != nil {
				c.nonceStore.SetNonce(challengeNonce)
			}

			// Only retry once on first attempt
			if !firstAttempt {
				return nil, challengeNonce, fmt.Errorf("%w: token request failed after retry: %s - %s", ErrTokenRequestFailed, errorResp.Error, errorResp.ErrorDescription)
			}

			// Retry with the challenged nonce. Passing it explicitly means the
			// retry is nonce-aware even with no NonceStore configured.
			return c.tryToken(ctx, false, challengeNonce)
		}
		return nil, nonce, fmt.Errorf("%w: %s - %s", ErrTokenRequestFailed, errorResp.Error, errorResp.ErrorDescription)
	}

	if isRetryableStatus(resp.StatusCode) {
		return nil, nonce, markTransient(fmt.Errorf("%w: unexpected status code: %s", ErrTokenRequestFailed, resp.Status))
	}

	if resp.StatusCode != http.StatusOK {
		return nil, nonce, fmt.Errorf("%w: unexpected status code: %s", ErrTokenRequestFailed, resp.Status)
	}

	token := &oauth2.Token{}
	err = json.NewDecoder(resp.Body).Decode(token)
	if err != nil {
		return nil, nonce, fmt.Errorf("%w: failed to decode token response: %v", ErrInvalidToken, err)
	}

	if token.AccessToken == "" {
		return nil, nonce, fmt.Errorf("%w: empty access token", ErrInvalidToken)
	}

	if token.Expiry.IsZero() {
		token.Expiry = time.Now()
		if token.ExpiresIn > 0 {
			expiresIn := token.ExpiresIn - 10 // 10 seconds before the token expires
			if expiresIn < 0 {
				expiresIn = 0
			}
			token.Expiry = time.Now().Add(time.Duration(expiresIn) * time.Second)
		}
	}

	// Accept both DPoP and Bearer tokens
	// If we sent a DPoP proof but got a Bearer token, that means the AS doesn't support DPoP
	if !strings.EqualFold(token.TokenType, "DPoP") && !strings.EqualFold(token.TokenType, "Bearer") {
		return nil, nonce, fmt.Errorf("%w: invalid token type: %s", ErrInvalidToken, token.TokenType)
	}

	return token, nonce, nil
}
