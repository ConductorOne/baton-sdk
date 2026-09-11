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
	return getHttpClient(ctx, options...)
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
	httpClient, err := getHttpClient(ctx, options...)
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
	httpClient, err := getHttpClient(ctx, options...)
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
	httpClient, err := getHttpClient(ctx, options...)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
	ts := o.cfg.TokenSource(ctx)
	httpClient = oauth2.NewClient(ctx, ts)

	return httpClient, nil
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

// reCreatingJWTSource is an oauth2.TokenSource that rebuilds the *jwt.Config via
// CreateJWTConfig on every token fetch. This lets callers inject per-request
// claims (e.g. a fresh jti nonce in PrivateClaims for RFC 7523 replay
// protection) that must change on each token acquisition. Wrapping this in an
// oauth2.ReuseTokenSource preserves normal token caching: CreateJWTConfig is
// only re-invoked when the cached token has expired, not on every HTTP request.
type reCreatingJWTSource struct {
	ctx         context.Context
	createfn    CreateJWTConfig
	credentials []byte
	scopes      []string
}

func (s reCreatingJWTSource) Token() (*oauth2.Token, error) {
	cfg, err := s.createfn(s.credentials, s.scopes...)
	if err != nil {
		return nil, fmt.Errorf("creating JWT config failed: %w", err)
	}
	return cfg.TokenSource(s.ctx).Token()
}

func (o *OAuth2JWT) GetClient(ctx context.Context, options ...Option) (*http.Client, error) {
	httpClient, err := getHttpClient(ctx, options...)
	if err != nil {
		return nil, err
	}

	ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
	ts := oauth2.ReuseTokenSource(nil, reCreatingJWTSource{
		ctx:         ctx,
		createfn:    o.CreateJWTConfig,
		credentials: o.Credentials,
		scopes:      o.Scopes,
	})
	httpClient = oauth2.NewClient(ctx, ts)

	return httpClient, nil
}

func getHttpClient(ctx context.Context, options ...Option) (*http.Client, error) {
	options = append(options, WithLogger(true, ctxzap.Extract(ctx)))

	httpClient, err := NewClient(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP client failed: %w", err)
	}

	return httpClient, nil
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
	httpClient, err := getHttpClient(ctx, options...)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)

	token := &oauth2.Token{
		AccessToken:  o.accessToken,
		RefreshToken: o.refreshToken,
		TokenType:    "Bearer",
	}
	httpClient = o.cfg.Client(ctx, token)

	return httpClient, nil
}
