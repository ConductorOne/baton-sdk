package ugrpc

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pquerna/xjwt"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/credentials"
	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/json"
	"gopkg.in/square/go-jose.v2/jwt"
)

const (
	assertionType = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
)

var (
	ErrInvalidClientSecret = errors.New("invalid client secret")
	ErrInvalidClientID     = errors.New("invalid client id")
)

type c1TokenSource struct {
	clientID     string
	clientSecret *jose.JSONWebKey
	tokenHost    string
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

func parseSecret(input []byte) (*jose.JSONWebKey, error) {
	items := bytes.SplitN(input, []byte(":"), 4)
	if len(items) != 4 {
		return nil, ErrInvalidClientSecret
	}

	jwkData, err := base64.RawURLEncoding.DecodeString(string(items[3]))
	if err != nil {
		return nil, ErrInvalidClientSecret
	}

	npk := &jose.JSONWebKey{}
	err = npk.UnmarshalJSON(jwkData)
	if err != nil {
		return nil, ErrInvalidClientSecret
	}

	if npk.IsPublic() || !npk.Valid() {
		return nil, ErrInvalidClientSecret
	}

	_, ok := npk.Key.(ed25519.PrivateKey)
	if !ok {
		return nil, ErrInvalidClientSecret
	}

	return npk, nil
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
		Expiry:    jwt.NewNumericDate(now.Add(time.Second * 15)),
		IssuedAt:  jwt.NewNumericDate(now),
		NotBefore: jwt.NewNumericDate(now.Add(-time.Second * 15)),
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
	resp, err := http.PostForm(tokenUrl.String(), body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	token := &oauth2.Token{}
	err = json.NewDecoder(resp.Body).Decode(token)
	if err != nil {
		return nil, err
	}

	return token, nil
}

func newC1TokenSource(clientID string, clientSecret string) (oauth2.TokenSource, string, string, error) {
	clientName, tokenHost, err := parseClientID(clientID)
	if err != nil {
		return nil, "", "", err
	}

	secret, err := parseSecret([]byte(clientSecret))
	if err != nil {
		return nil, "", "", err
	}

	return oauth2.ReuseTokenSource(nil, &c1TokenSource{
		clientID:     clientID,
		clientSecret: secret,
		tokenHost:    tokenHost,
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

func NewC1CredentialProvider(clientID string, clientSecret string) (credentials.PerRPCCredentials, string, string, error) {
	tokenSource, clientName, tokenHost, err := newC1TokenSource(clientID, clientSecret)
	if err != nil {
		return nil, "", "", err
	}

	return &c1CredentialProvider{
		tokenSource: tokenSource,
	}, clientName, tokenHost, nil
}
