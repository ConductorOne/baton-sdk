package helpers

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"golang.org/x/oauth2/jwt"
)

func SplitFullName(name string) (string, string) {
	names := strings.SplitN(name, " ", 2)
	var firstName, lastName string

	switch len(names) {
	case 1:
		firstName = names[0]
	case 2:
		firstName = names[0]
		lastName = names[1]
	}

	return firstName, lastName
}

func GetHTTPClient(ctx context.Context) (*http.Client, error) {
	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, nil))
	if err != nil {
		return nil, fmt.Errorf("creating http client failed: %w", err)
	}

	return httpClient, nil
}

type AuthenticationCredentials interface {
	GetClient(ctx context.Context) (*http.Client, error)
	Apply(req *http.Request)
}

type NoAuth struct{}

func (n *NoAuth) Apply(req *http.Request) {}

func (n *NoAuth) GetClient(ctx context.Context) (*http.Client, error) {
	return GetHTTPClient(ctx)
}

type BearerAuth struct {
	Token string
}

func NewBearerAuth(token string) *BearerAuth {
	return &BearerAuth{
		Token: token,
	}
}

func (b *BearerAuth) Apply(req *http.Request) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", b.Token))
}

func (b *BearerAuth) GetClient(ctx context.Context) (*http.Client, error) {
	return GetHTTPClient(ctx)
}

type BasicAuth struct {
	Username string
	Password string
}

func NewBasicAuth(username, password string) *BasicAuth {
	return &BasicAuth{
		Username: username,
		Password: password,
	}
}

func (b *BasicAuth) Apply(req *http.Request) {
	req.SetBasicAuth(b.Username, b.Password)
}

func (b *BasicAuth) GetClient(ctx context.Context) (*http.Client, error) {
	return GetHTTPClient(ctx)
}

type OAuth2ClientCredentials struct {
	cfg *clientcredentials.Config
}

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

func (o *OAuth2ClientCredentials) GetClient(ctx context.Context) (*http.Client, error) {
	ts := o.cfg.TokenSource(ctx)
	httpClient := oauth2.NewClient(ctx, ts)

	return httpClient, nil
}

func (o *OAuth2ClientCredentials) Apply(req *http.Request) {}

type CreateJWTConfig func(credentials []byte, scopes ...string) (*jwt.Config, error)

type OAuth2JWT struct {
	Credentials     []byte
	Scopes          []string
	CreateJWTConfig CreateJWTConfig
}

func NewOAuth2JWT(credentials []byte, scopes []string, createfn CreateJWTConfig) *OAuth2JWT {
	return &OAuth2JWT{
		Credentials:     credentials,
		Scopes:          scopes,
		CreateJWTConfig: createfn,
	}
}

func (o *OAuth2JWT) GetClient(ctx context.Context) (*http.Client, error) {
	httpClient, err := GetHTTPClient(ctx)
	if err != nil {
		return nil, err
	}

	jwt, err := o.CreateJWTConfig(
		o.Credentials,
		o.Scopes...,
	)
	if err != nil {
		return nil, fmt.Errorf("creating JWT config failed: %w", err)
	}

	ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)
	httpClient = &http.Client{
		Transport: &oauth2.Transport{
			Base:   httpClient.Transport,
			Source: jwt.TokenSource(ctx),
		},
	}

	return httpClient, nil
}

func (o *OAuth2JWT) Apply(req *http.Request) {}
