package session

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/types"
	dpop_grpc "github.com/conductorone/dpop/integrations/dpop_grpc"
	"github.com/conductorone/dpop/pkg/dpop"
	"github.com/go-jose/go-jose/v4"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// No longer needed since we're reusing existing credentials

const (
	// KeyPrefixDelimiter is the delimiter used to separate prefix from key in session cache keys
	KeyPrefixDelimiter = "::"
)

// GRPCSessionCache implements SessionCache interface using gRPC calls to BatonSessionService.
type GRPCSessionCache struct {
	client v1.BatonSessionServiceClient
}

// NewGRPCSessionClient creates a new gRPC session service client using existing DPoP credentials.
// It reuses an existing access token and DPoP key instead of performing a new authentication round.
// It reads the session service address from the BATON_SESSION_SERVICE_ADDR environment variable,
// defaulting to "localhost:50051" if not set.
func NewGRPCSessionClient(ctx context.Context, accessToken string, dpopKey *jose.JSONWebKey, opt ...types.SessionCacheConstructorOption) (v1.BatonSessionServiceClient, error) {
	// Apply constructor options
	for _, option := range opt {
		var err error
		ctx, err = option(ctx)
		if err != nil {
			return nil, err
		}
	}

	// Get the session service address from environment variable
	addr := os.Getenv("BATON_SESSION_SERVICE_ADDR")
	if addr == "" {
		addr = "localhost:50051"
	}

	// Validate the address format
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid session service address %q: %w", addr, err)
	}
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "50051"
	}
	addr = net.JoinHostPort(host, port)

	// Create DPoP proofer using the provided key
	proofer, err := dpop.NewProofer(dpopKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create dpop proofer: %w", err)
	}

	// Create a simple token source that returns the existing access token
	tokenSource := &staticTokenSource{accessToken: accessToken}

	// Create DPoP credentials using the existing token and key
	creds, err := dpop_grpc.NewDPoPCredentials(proofer, tokenSource, host, []dpop.ProofOption{
		dpop.WithValidityDuration(time.Minute * 5),
		dpop.WithProofNowFunc(time.Now),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create dpop credentials: %w", err)
	}

	// Create TLS transport credentials
	systemCertPool, err := x509.SystemCertPool()
	if err != nil || systemCertPool == nil {
		return nil, fmt.Errorf("failed to load system cert pool: %w", err)
	}
	transportCreds := credentials.NewTLS(&tls.Config{
		RootCAs:    systemCertPool,
		MinVersion: tls.VersionTLS12,
	})

	// Create dial options
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithUserAgent(fmt.Sprintf("baton-session/%s", sdk.Version)),
		grpc.WithPerRPCCredentials(creds),
		grpc.WithBlock(),
		grpc.WithTimeout(30 * time.Second),
	}

	// Create the gRPC connection
	conn, err := grpc.DialContext(ctx, addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to session service at %s: %w", addr, err)
	}

	// Create and return the client
	client := v1.NewBatonSessionServiceClient(conn)
	return client, nil
}

// staticTokenSource implements oauth2.TokenSource to return a static access token
type staticTokenSource struct {
	accessToken string
}

func (s *staticTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{
		AccessToken: s.accessToken,
		TokenType:   "DPoP",
	}, nil
}

// These functions are no longer needed since we're reusing existing credentials

// NewGRPCSessionCache creates a new gRPC session cache instance.
func NewGRPCSessionCache(ctx context.Context, client v1.BatonSessionServiceClient, opt ...types.SessionCacheConstructorOption) (types.SessionCache, error) {
	// Apply constructor options
	for _, option := range opt {
		var err error
		ctx, err = option(ctx)
		if err != nil {
			return nil, err
		}
	}

	return &GRPCSessionCache{
		client: client,
	}, nil
}

// Get retrieves a value from the cache by key.
func (g *GRPCSessionCache) Get(ctx context.Context, key string, opt ...types.SessionCacheOption) ([]byte, bool, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, false, err
	}

	if bag.Prefix != "" {
		key = bag.Prefix + KeyPrefixDelimiter + key
	}

	req := &v1.BatonServiceGetRequest{
		SyncId: bag.SyncID,
		Key:    key,
	}

	resp, err := g.client.Get(ctx, req)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get value from gRPC session cache: %w", err)
	}

	// If the response has no value, the key doesn't exist
	if len(resp.Value) == 0 {
		return nil, false, nil
	}

	return resp.Value, true, nil
}

// GetMany retrieves multiple values from the cache by keys.
func (g *GRPCSessionCache) GetMany(ctx context.Context, keys []string, opt ...types.SessionCacheOption) (map[string][]byte, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, err
	}

	// Apply prefix to keys if specified
	prefixedKeys := make([]string, len(keys))
	for i, key := range keys {
		if bag.Prefix != "" {
			prefixedKeys[i] = bag.Prefix + KeyPrefixDelimiter + key
		} else {
			prefixedKeys[i] = key
		}
	}

	req := &v1.BatonServiceGetManyRequest{
		SyncId: bag.SyncID,
		Keys:   prefixedKeys,
	}

	resp, err := g.client.GetMany(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get many values from gRPC session cache: %w", err)
	}

	// Convert the response back to the original keys (without prefix)
	result := make(map[string][]byte)
	for respKey, value := range resp.Values {
		// Remove prefix from response key to match original key
		originalKey := respKey
		if bag.Prefix != "" {
			prefixWithSeparator := bag.Prefix + KeyPrefixDelimiter
			if len(respKey) > len(prefixWithSeparator) && respKey[:len(prefixWithSeparator)] == prefixWithSeparator {
				originalKey = respKey[len(prefixWithSeparator):]
			}
		}
		result[originalKey] = value
	}

	return result, nil
}

// Set stores a value in the cache with the given key.
func (g *GRPCSessionCache) Set(ctx context.Context, key string, value []byte, opt ...types.SessionCacheOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	if bag.Prefix != "" {
		key = bag.Prefix + KeyPrefixDelimiter + key
	}

	req := &v1.BatonServiceSetRequest{
		SyncId: bag.SyncID,
		Key:    key,
		Value:  value,
	}

	_, err = g.client.Set(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to set value in gRPC session cache: %w", err)
	}

	return nil
}

// SetMany stores multiple values in the cache.
func (g *GRPCSessionCache) SetMany(ctx context.Context, values map[string][]byte, opt ...types.SessionCacheOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	// Apply prefix to keys if specified
	prefixedValues := make(map[string][]byte)
	for key, value := range values {
		if bag.Prefix != "" {
			prefixedValues[bag.Prefix+KeyPrefixDelimiter+key] = value
		} else {
			prefixedValues[key] = value
		}
	}

	req := &v1.BatonServiceSetManyRequest{
		SyncId: bag.SyncID,
		Values: prefixedValues,
	}

	_, err = g.client.SetMany(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to set many values in gRPC session cache: %w", err)
	}

	return nil
}

// Delete removes a value from the cache by key.
func (g *GRPCSessionCache) Delete(ctx context.Context, key string, opt ...types.SessionCacheOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	if bag.Prefix != "" {
		key = bag.Prefix + KeyPrefixDelimiter + key
	}

	req := &v1.BatonServiceDeleteRequest{
		SyncId: bag.SyncID,
		Key:    key,
	}

	_, err = g.client.Delete(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to delete value from gRPC session cache: %w", err)
	}

	return nil
}

// Clear removes all values from the cache.
func (g *GRPCSessionCache) Clear(ctx context.Context, opt ...types.SessionCacheOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	req := &v1.BatonServiceClearRequest{
		SyncId: bag.SyncID,
	}

	_, err = g.client.Clear(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to clear gRPC session cache: %w", err)
	}

	return nil
}

// GetAll returns all key-value pairs.
// Note: The gRPC service doesn't have a GetAll method, so we'll need to implement this
// by getting all keys first and then using GetMany. This is a limitation of the current
// gRPC service definition.
func (g *GRPCSessionCache) GetAll(ctx context.Context, opt ...types.SessionCacheOption) (map[string][]byte, error) {
	// Since the gRPC service doesn't provide a GetAll method, we'll return an empty map
	// with a note that this operation is not supported by the gRPC service.
	// In a real implementation, you might want to:
	// 1. Add a GetAll method to the gRPC service
	// 2. Or implement this by maintaining a list of keys locally
	// 3. Or return an error indicating this operation is not supported
	return map[string][]byte{}, fmt.Errorf("GetAll operation is not supported by the gRPC session service")
}

// Close performs any necessary cleanup when the cache is no longer needed.
func (g *GRPCSessionCache) Close() error {
	// No cleanup needed for gRPC client
	return nil
}
