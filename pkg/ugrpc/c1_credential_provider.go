package ugrpc

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/json"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/pquerna/xjwt"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	assertionType = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
)

var (
	ErrInvalidClientID = errors.New("invalid client id")
)

type c1Token struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	Expiry      int    `json:"expires_in"`
}

type c1TokenSource struct {
	clientID     string
	clientSecret *jose.JSONWebKey
	tokenHost    string
	httpClient   *http.Client
}

func parseClientID(input string) (string, string, error) {
	// split the input into 2 parts by @
	items := strings.SplitN(input, "@", 2)
	if len(items) != 2 {
		return "", "", ErrInvalidClientID
	}
	clientName := items[0]

	// split the right part into 2 parts by /
	items = strings.SplitN(items[1], "/", 2)
	if len(items) != 2 {
		return "", "", ErrInvalidClientID
	}

	return clientName, items[0], nil
}

func (c *c1TokenSource) Token() (*oauth2.Token, error) {
	jsigner, err := jose.NewSigner(
		jose.SigningKey{
			Algorithm: jose.EdDSA,
			Key:       c.clientSecret,
		},
		&jose.SignerOptions{
			NonceSource: &xjwt.RandomNonce{Size: 16},
		})
	if err != nil {
		return nil, err
	}

	// Our token host may include a port, but the audience never expects a port
	aud := c.tokenHost
	host, _, err := net.SplitHostPort(aud)
	if err == nil {
		aud = host
	}
	now := time.Now()
	claims := &jwt.Claims{
		Issuer:    c.clientID,
		Subject:   c.clientID,
		Audience:  jwt.Audience{aud},
		Expiry:    jwt.NewNumericDate(now.Add(time.Minute * 2)),
		IssuedAt:  jwt.NewNumericDate(now),
		NotBefore: jwt.NewNumericDate(now.Add(-time.Minute * 2)),
	}
	b, err := json.Marshal(claims)
	if err != nil {
		return nil, err
	}

	rv, err := jsigner.Sign(b)
	if err != nil {
		return nil, err
	}

	s, err := rv.CompactSerialize()
	if err != nil {
		return nil, err
	}

	body := url.Values{
		"client_id":             []string{c.clientID},
		"grant_type":            []string{"client_credentials"},
		"client_assertion_type": []string{assertionType},
		"client_assertion":      []string{s},
	}

	tokenHost := c.tokenHost
	if envHost, ok := os.LookupEnv("BATON_C1_API_HOST"); ok {
		tokenHost = envHost
	}
	tokenUrl := url.URL{
		Scheme: "https",
		Host:   tokenHost,
		Path:   "auth/v1/token",
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, tokenUrl.String(), strings.NewReader(body.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, status.Errorf(codes.Unauthenticated, "failed to get token: %s", resp.Status)
	}

	c1t := &c1Token{}
	err = json.NewDecoder(resp.Body).Decode(c1t)
	if err != nil {
		return nil, err
	}

	if c1t.AccessToken == "" {
		return nil, status.Errorf(codes.Unauthenticated, "failed to get token: empty access token")
	}

	return &oauth2.Token{
		AccessToken: c1t.AccessToken,
		TokenType:   c1t.TokenType,
		Expiry:      time.Now().Add(time.Duration(c1t.Expiry) * time.Second),
	}, nil
}

func newC1TokenSource(ctx context.Context, clientID string, clientSecret string) (oauth2.TokenSource, string, string, error) {
	clientName, tokenHost, err := parseClientID(clientID)
	if err != nil {
		return nil, "", "", err
	}

	secret, err := crypto.ParseClientSecret([]byte(clientSecret), false)
	if err != nil {
		return nil, "", "", err
	}

	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, ctxzap.Extract(ctx)), uhttp.WithUserAgent("baton-c1-credential-provider"))
	if err != nil {
		return nil, "", "", err
	}
	return oauth2.ReuseTokenSource(nil, &c1TokenSource{
		clientID:     clientID,
		clientSecret: secret,
		tokenHost:    tokenHost,
		httpClient:   httpClient,
	}), clientName, tokenHost, nil
}

type c1CredentialProvider struct {
	tokenSource oauth2.TokenSource
}

func (c *c1CredentialProvider) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	token, err := c.tokenSource.Token()
	if err != nil {
		return nil, err
	}

	ri, _ := credentials.RequestInfoFromContext(ctx)
	err = credentials.CheckSecurityLevel(ri.AuthInfo, credentials.PrivacyAndIntegrity)
	if err != nil {
		return nil, errors.New("connection is not secure enough to send credentials")
	}

	return map[string]string{
		"authorization": token.Type() + " " + token.AccessToken,
	}, nil
}

func (c *c1CredentialProvider) RequireTransportSecurity() bool {
	return true
}

func NewC1CredentialProvider(ctx context.Context, clientID string, clientSecret string) (credentials.PerRPCCredentials, string, string, error) {
	tokenSource, clientName, tokenHost, err := newC1TokenSource(ctx, clientID, clientSecret)
	if err != nil {
		return nil, "", "", err
	}

	return &c1CredentialProvider{
		tokenSource: tokenSource,
	}, clientName, tokenHost, nil
}
