package uhttp

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"golang.org/x/oauth2/jwt"
)

type AuthCredentials interface {
	GetClient(ctx context.Context, options ...Option) (*http.Client, error)
}

type NoAuth struct{}

var _ AuthCredentials = (*NoAuth)(nil)

func (n *NoAuth) GetClient(ctx context.Context, options ...Option) (*http.Client, error) {
	return getHttpClient(ctx, withoutTransientRetries(options)...)
}

type BearerAuth struct {
	Token string
}

var _ AuthCredentials = (*BearerAuth)(nil)

func NewBearerAuth(token string) *BearerAuth {
	return &BearerAuth{
		Token: token,
	}
}

func (b *BearerAuth) GetClient(ctx context.Context, options ...Option) (*http.Client, error) {
	httpClient, err := getHttpClient(ctx, withoutTransientRetries(options)...)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: b.Token},
	)
	httpClient = oauth2.NewClient(ctx, ts)

	return httpClient, nil
}

type BasicAuth struct {
	Username string

	Password string
}

var _ AuthCredentials = (*BasicAuth)(nil)

func NewBasicAuth(username, password string) *BasicAuth {
	return &BasicAuth{
		Username: username,
		Password: password,
	}
}

func (b *BasicAuth) GetClient(ctx context.Context, options ...Option) (*http.Client, error) {
	httpClient, err := getHttpClient(ctx, withoutTransientRetries(options)...)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
	auth := b.Username + ":" + b.Password
	token := base64.StdEncoding.EncodeToString([]byte(auth))
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token, TokenType: "basic"},
	)
	httpClient = oauth2.NewClient(ctx, ts)

	return httpClient, nil
}

type OAuth2ClientCredentials struct {
	cfg *clientcredentials.Config
}

var _ AuthCredentials = (*OAuth2ClientCredentials)(nil)

func NewOAuth2ClientCredentials(clientId, clientSecret string, tokenURL *url.URL, scopes []string) *OAuth2ClientCredentials {
	return &OAuth2ClientCredentials{
		cfg: &clientcredentials.Config{
			ClientID:     clientId,
			ClientSecret: clientSecret,
			TokenURL:     tokenURL.String(),
			Scopes:       scopes,
		},
	}
}

func (o *OAuth2ClientCredentials) GetClient(ctx context.Context, options ...Option) (*http.Client, error) {
	// Client-credentials grants are stateless; replaying one the origin
	// already processed is harmless, so the token client is ReplaySafe.
	clients, err := newOAuthClients(ctx, true, options...)
	if err != nil {
		return nil, err
	}
	ts := o.cfg.TokenSource(clients.tokenContext(ctx))
	return clients.apiClient(ts), nil
}

type CreateJWTConfig func(credentials []byte, scopes ...string) (*jwt.Config, error)

type OAuth2JWT struct {
	Credentials     []byte
	Scopes          []string
	CreateJWTConfig CreateJWTConfig
}

var _ AuthCredentials = (*OAuth2JWT)(nil)

func NewOAuth2JWT(credentials []byte, scopes []string, createfn CreateJWTConfig) *OAuth2JWT {
	return &OAuth2JWT{
		Credentials:     credentials,
		Scopes:          scopes,
		CreateJWTConfig: createfn,
	}
}

func (o *OAuth2JWT) GetClient(ctx context.Context, options ...Option) (*http.Client, error) {
	// JWT-bearer grants are stateless; replaying one the origin already
	// processed is harmless, so the token client is ReplaySafe.
	clients, err := newOAuthClients(ctx, true, options...)
	if err != nil {
		return nil, err
	}

	jwt, err := o.CreateJWTConfig(o.Credentials, o.Scopes...)
	if err != nil {
		return nil, fmt.Errorf("creating JWT config failed: %w", err)
	}

	ts := jwt.TokenSource(clients.tokenContext(ctx))
	return clients.apiClient(ts), nil
}

func getHttpClient(ctx context.Context, options ...Option) (*http.Client, error) {
	options = append(options, WithLogger(true, ctxzap.Extract(ctx)))

	httpClient, err := NewClient(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP client failed: %w", err)
	}

	return httpClient, nil
}

// oauthClients holds the two HTTP clients used by OAuth helpers. Token is
// used only for token-endpoint POSTs (WithTransientRetries). API is
// oauth2.Transport.Base and must never replay provisioning POSTs.
type oauthClients struct {
	api   *http.Client
	token *http.Client
}

// tokenContext is the only context value oauth2 needs: TokenSource looks up
// HTTPClient from it. The API client is not in context; it is Transport.Base.
func (c oauthClients) tokenContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, oauth2.HTTPClient, c.token)
}

// apiClient builds the caller-facing client. oauth2.NewClient is avoided
// because it reads its base transport from the context client, which would
// require a second WithValue; wiring Transport.Base explicitly keeps the
// api/token split visible. The ReuseTokenSource wrap matches NewClient's
// behavior and is a no-op for the Config.TokenSource values used here,
// which are already reuse-wrapped.
func (c oauthClients) apiClient(ts oauth2.TokenSource) *http.Client {
	return &http.Client{
		Timeout: c.api.Timeout,
		Transport: &oauth2.Transport{
			Base:   c.api.Transport,
			Source: oauth2.ReuseTokenSource(nil, ts),
		},
	}
}

func withoutTransientRetries(options []Option) []Option {
	out := make([]Option, 0, len(options))
	for _, opt := range options {
		if _, ok := opt.(transientRetriesOption); ok {
			continue
		}
		out = append(out, opt)
	}
	return out
}

func tokenRetryConfig(options []Option) TransientRetryConfig {
	var cfg TransientRetryConfig
	for _, opt := range options {
		if t, ok := opt.(transientRetriesOption); ok {
			cfg = t.cfg
		}
	}
	return cfg
}

// newOAuthClients returns an API client (never WithTransientRetries) and a
// token client (always WithTransientRetries). Caller-supplied
// WithTransientRetries tunes attempts and delays on the token client only;
// it is stripped from the API client so GetClient(ctx,
// WithTransientRetries(...)) cannot replay grants, revokes, or tickets.
//
// replaySafe is decided by the flow, not the caller: stateless grants
// (client credentials, JWT) may replay maybe-processed requests, rotating
// refresh grants must not.
func newOAuthClients(ctx context.Context, replaySafe bool, options ...Option) (oauthClients, error) {
	apiClient, err := getHttpClient(ctx, withoutTransientRetries(options)...)
	if err != nil {
		return oauthClients{}, err
	}
	cfg := tokenRetryConfig(options)
	cfg.ReplaySafe = replaySafe
	tokenOpts := append(withoutTransientRetries(options), WithTransientRetries(cfg))
	tokenClient, err := getHttpClient(ctx, tokenOpts...)
	if err != nil {
		return oauthClients{}, err
	}
	return oauthClients{api: apiClient, token: tokenClient}, nil
}

type OAuth2RefreshToken struct {
	cfg          *oauth2.Config
	accessToken  string
	refreshToken string
}

var _ AuthCredentials = (*OAuth2RefreshToken)(nil)

func NewOAuth2RefreshToken(clientID, clientSecret, redirectURI, tokenURL, accessToken, refreshToken string, scopes []string) *OAuth2RefreshToken {
	return &OAuth2RefreshToken{
		cfg: &oauth2.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			Scopes:       scopes,
			RedirectURL:  redirectURI,
			Endpoint: oauth2.Endpoint{
				TokenURL: tokenURL,
			},
		},
		accessToken:  accessToken,
		refreshToken: refreshToken,
	}
}

func (o *OAuth2RefreshToken) GetClient(ctx context.Context, options ...Option) (*http.Client, error) {
	// Refresh grants rotate provider state, so the token client is not
	// ReplaySafe: it retries 5xx the origin itself answered (never
	// 502/504) and dial-phase failures, but never replays a grant the
	// origin may have processed — that can trip refresh-token-reuse
	// detection and revoke the token family, forcing a manual re-auth.
	// Accepted residuals: a 500 sent after the origin committed a rotation
	// is still replayed, and with Endpoint.AuthStyle unset x/oauth2 itself
	// replays a failed token POST once to probe the other auth style until
	// a success caches the style. API traffic stays on clients.api, never
	// retried.
	clients, err := newOAuthClients(ctx, false, options...)
	if err != nil {
		return nil, err
	}

	token := &oauth2.Token{
		AccessToken:  o.accessToken,
		RefreshToken: o.refreshToken,
		TokenType:    "Bearer",
	}
	ts := o.cfg.TokenSource(clients.tokenContext(ctx), token)
	return clients.apiClient(ts), nil
}
