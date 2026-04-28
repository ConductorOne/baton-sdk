package config

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/conductorone/dpop/pkg/dpop"

	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	dpop_grpc "github.com/conductorone/dpop/integrations/dpop_grpc"
	dpop_oauth "github.com/conductorone/dpop/integrations/dpop_oauth2"
	"golang.org/x/oauth2"
)

var (
	ErrInvalidClientID = errors.New("invalid client id")
)

const (
	lambdaTokenHostEnv         = "BATON_LAMBDA_TOKEN_HOST" //nolint:gosec // Environment variable name, not a credential value.
	lambdaConfigurationHostEnv = "BATON_LAMBDA_CONFIGURATION_HOST"
	lambdaCACertPathEnv        = "BATON_LAMBDA_CA_CERT_PATH"
)

// NewDPoPClient creates a gRPC client with DPoP authentication.
func NewDPoPClient(ctx context.Context, clientID string, clientSecret string) (grpc.ClientConnInterface, *jose.JSONWebKey, oauth2.TokenSource, error) {
	_, tokenHost, err := parseClientID(clientID)
	if err != nil {
		return nil, nil, nil, err
	}
	if envHost, ok := os.LookupEnv(lambdaTokenHostEnv); ok {
		tokenHost = envHost
	}
	configurationHost := tokenHost
	if envHost, ok := os.LookupEnv(lambdaConfigurationHostEnv); ok {
		configurationHost = envHost
	}

	tokenURL := &url.URL{
		Scheme: "https",
		Host:   tokenHost,
		Path:   "auth/v1/token",
	}

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new-dpop-client: failed to generate ed25519: %w", err)
	}

	jwk := &jose.JSONWebKey{
		Key:       priv,
		KeyID:     "key",
		Algorithm: string(jose.EdDSA),
		Use:       "sig",
	}

	clientSecretJWK, err := crypto.ParseClientSecret([]byte(clientSecret), false)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new-dpop-client: failed to unmarshal client secret: %w", err)
	}

	proofer, err := dpop.NewProofer(jwk)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new-dpop-client: failed to create proofer: %w", err)
	}

	tlsConfig, err := lambdaTLSConfig()
	if err != nil {
		return nil, nil, nil, err
	}

	idAttMarshaller, err := NewIdAttMarshaller(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new-dpop-client: failed to create claims adjuster: %w", err)
	}
	opts := []dpop_oauth.TokenSourceOption{
		dpop_oauth.WithRequestOption(dpop_oauth.WithCustomMarshaler(idAttMarshaller.Marshal)),
		dpop_oauth.WithHTTPClient(lambdaHTTPClient(tlsConfig)),
	}
	tokenSource, err := dpop_oauth.NewTokenSource(proofer, tokenURL, clientID, clientSecretJWK, opts...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new-dpop-client: failed to create token source: %w", err)
	}

	creds, err := dpop_grpc.NewDPoPCredentials(proofer, tokenSource, configurationHost, []dpop.ProofOption{
		dpop.WithValidityDuration(time.Minute * 5),
		dpop.WithProofNowFunc(time.Now),
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new-dpop-client: failed to create dpop credentials: %w", err)
	}

	transportCreds := credentials.NewTLS(tlsConfig)

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithUserAgent(fmt.Sprintf("baton-lambda/%s %s", clientID, sdk.Version)),
		grpc.WithPerRPCCredentials(creds),
	}

	client, err := grpc.NewClient(configurationHost, dialOpts...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("new-dpop-client: failed to create client: %w", err)
	}

	return client, jwk, tokenSource, nil
}

func lambdaTLSConfig() (*tls.Config, error) {
	systemCertPool, err := x509.SystemCertPool()
	if err != nil || systemCertPool == nil {
		return nil, fmt.Errorf("new-dpop-client: failed to load system cert pool: %w", err)
	}
	if certPath := strings.TrimSpace(os.Getenv(lambdaCACertPathEnv)); certPath != "" {
		pemBytes, err := os.ReadFile(certPath) //nolint:gosec // Operator-provided CA bundle path for lambda-hosted connector configuration.
		if err != nil {
			return nil, fmt.Errorf("new-dpop-client: failed to read %s: %w", lambdaCACertPathEnv, err)
		}
		if ok := systemCertPool.AppendCertsFromPEM(pemBytes); !ok {
			return nil, fmt.Errorf("new-dpop-client: failed to append CA certificates from %s", lambdaCACertPathEnv)
		}
	}
	return &tls.Config{
		RootCAs:    systemCertPool,
		MinVersion: tls.VersionTLS12,
	}, nil
}

func lambdaHTTPClient(tlsConfig *tls.Config) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig.Clone()
	return &http.Client{Transport: transport}
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
