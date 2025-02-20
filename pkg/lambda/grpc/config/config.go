package config

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb_connector_manager "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"

	"github.com/conductorone/dpop/pkg/dpop"

	dpop_grpc "github.com/conductorone/dpop/integrations/dpop_grpc"
	dpop_oauth "github.com/conductorone/dpop/integrations/dpop_oauth2"
)

var (
	ErrInvalidClientSecret  = errors.New("invalid client secret")
	ErrInvalidClientID      = errors.New("invalid client id")
	v1SecretTokenIdentifier = []byte("v1")
)

func GetConnectorConfigServiceClient(ctx context.Context, clientID string, clientSecret string) (pb_connector_manager.ConnectorConfigServiceClient, error) {
	clientName, tokenHost, err := parseClientID(clientID)
	if err != nil {
		return nil, err
	}

	if envHost, ok := os.LookupEnv("BATON_LAMBDA_TOKEN_HOST"); ok {
		tokenHost = envHost
	}

	tokenURL := &url.URL{
		Scheme: "https",
		Host:   tokenHost,
		Path:   "auth/v1/token",
	}

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return nil, fmt.Errorf("get-connector-service-client: failed to generate ed25519: %w", err)
	}

	jwk := &jose.JSONWebKey{
		Key:       priv,
		KeyID:     "key",
		Algorithm: string(jose.EdDSA),
		Use:       "sig",
	}

	clientSecretJWK, err := parseSecret([]byte(clientSecret))
	if err != nil {
		return nil, fmt.Errorf("get-connector-service-client: failed to unmarshal client secret: %w", err)
	}

	proofer, err := dpop.NewProofer(jwk)
	if err != nil {
		return nil, fmt.Errorf("get-connector-service-client: failed to create proofer: %w", err)
	}

	idAttMarshaller, err := NewIdAttMarshaller(ctx)
	if err != nil {
		return nil, fmt.Errorf("get-connector-service-client: failed to create claims adjuster: %w", err)
	}
	opts := dpop_oauth.WithRequestOption(dpop_oauth.WithCustomMarshaler(idAttMarshaller.Marshal))
	tokenSource, err := dpop_oauth.NewTokenSource(proofer, tokenURL, clientID, clientSecretJWK, opts)
	if err != nil {
		return nil, fmt.Errorf("get-connector-service-client: failed to create token source: %w", err)
	}

	creds, err := dpop_grpc.NewDPoPCredentials(proofer, tokenSource, tokenHost, []dpop.ProofOption{
		dpop.WithValidityDuration(time.Minute * 5),
		dpop.WithProofNowFunc(time.Now),
	})
	if err != nil {
		return nil, fmt.Errorf("get-connector-service-client: failed to create dpop credentials: %w", err)
	}

	systemCertPool, err := x509.SystemCertPool()
	if err != nil || systemCertPool == nil {
		return nil, fmt.Errorf("get-connector-service-client: failed to load system cert pool: %w", err)
	}
	transportCreds := credentials.NewTLS(&tls.Config{
		RootCAs:    systemCertPool,
		MinVersion: tls.VersionTLS12,
	})

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithUserAgent(fmt.Sprintf("%s baton-lambda/%s", clientName, "v0.0.1")),
		grpc.WithPerRPCCredentials(creds),
	}

	client, err := grpc.NewClient(tokenHost, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("connector-manager-client: failed to create client: %w", err)
	}

	return pb_connector_manager.NewConnectorConfigServiceClient(client), nil
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
