package ugrpc

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/pquerna/xjwt"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/json"
	"gopkg.in/square/go-jose.v2/jwt"

	"github.com/ductone/c1-lambda/pkg/uhttp"
)

const (
	assertionType = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer-with-identity-attestation"
)

var (
	ErrInvalidClientSecret = errors.New("invalid client secret")
	ErrInvalidClientID     = errors.New("invalid client id")

	v1SecretTokenIdentifier = []byte("v1")
)

type c1Token struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	Expiry      int    `json:"expires_in"`
}

type c1TokenSource struct {
	awsConfig    aws.Config
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

func parseSecret(input []byte) (*jose.JSONWebKey, error) {
	items := bytes.SplitN(input, []byte(":"), 4)
	if len(items) != 4 {
		return nil, ErrInvalidClientSecret
	}

	if !bytes.Equal(items[2], v1SecretTokenIdentifier) {
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

type claimsWithIDAtt struct {
	jwt.Claims
	IDAtt string `json:"id_att"` // Identity Attestation: Currently only supports v1.Sigv4SignedRequestSTSGetCallerIdentity
}

// getIdentityAttestation creates a signed sts GetCallerIdentity request and marshals it to a base64 encoded string
func (c *c1TokenSource) getIdentityAttestation() (string, error) {
	req, err := CreateSigv4STSGetCallerIdentityRequest(context.Background(), c.awsConfig)
	if err != nil {
		return "", fmt.Errorf("c1-credential-provider: failed to create sigv4 sts get caller identity request: %w", err)
	}

	anyReq, err := anypb.New(req)
	if err != nil {
		return "", fmt.Errorf("c1-credential-provider: failed to create sigv4 sts get caller identity request: %w", err)
	}

	b, err := proto.Marshal(anyReq)
	if err != nil {
		return "", fmt.Errorf("c1-credential-provider: failed to marshal sigv4 sts get caller identity request: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(b), nil
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
	idAtt, err := c.getIdentityAttestation()
	if err != nil {
		return nil, fmt.Errorf("c1-credential-provider: failed to get identity attestation: %w", err)
	}
	claims := &claimsWithIDAtt{
		Claims: jwt.Claims{
			Issuer:    c.clientID,
			Subject:   c.clientID,
			Audience:  jwt.Audience{aud},
			Expiry:    jwt.NewNumericDate(now.Add(time.Minute * 2)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now.Add(-time.Minute * 2)),
		},
		IDAtt: idAtt,
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
	if envHost, ok := os.LookupEnv("BATON_LAMBDA_TOKEN_HOST"); ok {
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

func newC1TokenSource(ctx context.Context, clientID string, clientSecret string, awsConfig aws.Config) (oauth2.TokenSource, string, string, error) {
	clientName, tokenHost, err := parseClientID(clientID)
	if err != nil {
		return nil, "", "", err
	}

	secret, err := parseSecret([]byte(clientSecret))
	if err != nil {
		return nil, "", "", err
	}

	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, ctxzap.Extract(ctx)), uhttp.WithUserAgent("baton-lambda-c1-credential-provider"))
	if err != nil {
		return nil, "", "", err
	}
	return oauth2.ReuseTokenSource(nil, &c1TokenSource{
		awsConfig:    awsConfig,
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

func NewC1LambdaCredentialProvider(ctx context.Context, clientID string, clientSecret string) (credentials.PerRPCCredentials, string, string, error) {
	cfg, err := LoadDefaultAWSConfig(ctx)
	if err != nil {
		return nil, "", "", fmt.Errorf("c1-credential-provider: failed to load AWS config: %w", err)
	}

	tokenSource, clientName, tokenHost, err := newC1TokenSource(ctx, clientID, clientSecret, cfg)
	if err != nil {
		return nil, "", "", err
	}

	return &c1CredentialProvider{
		tokenSource: tokenSource,
	}, clientName, tokenHost, nil
}
