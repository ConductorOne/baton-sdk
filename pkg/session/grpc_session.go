package session

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"iter"
	"net"
	"os"
	"strings"
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

type GRPCSessionCache struct {
	client v1.BatonSessionServiceClient
}

const maxKeysPerRequest = 200

func Chunk[T any](items []T, chunkSize int) iter.Seq[[]T] {
	return func(yield func([]T) bool) {
		for i := 0; i < len(items); i += chunkSize {
			end := i + chunkSize
			if end > len(items) {
				end = len(items)
			}
			if !yield(items[i:end]) {
				return
			}
		}
	}
}

// NewGRPCSessionClient creates a new gRPC session service client using existing DPoP credentials.
// It reuses an existing access token and DPoP key instead of performing a new authentication round.
// It reads the session service address from the BATON_SESSION_SERVICE_ADDR environment variable,
// defaulting to "localhost:50051" if not set.
func NewGRPCSessionClient(ctx context.Context, accessToken string, dpopKey *jose.JSONWebKey, opt ...types.SessionConstructorOption) (v1.BatonSessionServiceClient, error) {
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
	}

	// Create the gRPC connection
	conn, err := grpc.DialContext(ctx, addr, dialOpts...) //nolint:staticcheck // grpc.DialContext is deprecated but we are using it still.
	if err != nil {
		return nil, fmt.Errorf("failed to connect to session service at %s: %w", addr, err)
	}

	return v1.NewBatonSessionServiceClient(conn), nil
}

type staticTokenSource struct {
	accessToken string
}

func (s *staticTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{
		AccessToken: s.accessToken,
		TokenType:   "DPoP",
	}, nil
}

func NewGRPCSessionCache(ctx context.Context, client v1.BatonSessionServiceClient, opt ...types.SessionConstructorOption) (types.SessionStore, error) {
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
func (g *GRPCSessionCache) Get(ctx context.Context, key string, opt ...types.SessionOption) ([]byte, bool, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, false, err
	}

	if bag.Prefix != "" {
		key = bag.Prefix + KeyPrefixDelimiter + key
	}

	req := &v1.GetRequest{
		SyncId: bag.SyncID,
		Key:    key,
	}

	resp, err := g.client.Get(ctx, req)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get value from gRPC session cache: %w", err)
	}

	if resp == nil {
		return nil, false, nil
	}

	return resp.Value, true, nil
}

// GetMany retrieves multiple values from the cache by keys.
func (g *GRPCSessionCache) GetMany(ctx context.Context, keys []string, opt ...types.SessionOption) (map[string][]byte, error) {
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

	results := make(map[string][]byte)
	// TODO(kans): we may need to chunk if the values are too large for a single gRPC request.
	// The GetMany interface may be backed by gPRC, memory, etc, so we need to handle pagination at the client level.
	for keys := range Chunk(prefixedKeys, maxKeysPerRequest) {
		resp, err := g.client.GetMany(ctx, &v1.GetManyRequest{
			SyncId: bag.SyncID,
			Keys:   keys,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get many values from gRPC session cache: %w", err)
		}

		// Process response items and remove prefix if needed
		for _, item := range resp.Items {
			key := item.Key
			if bag.Prefix != "" {
				key = strings.TrimPrefix(item.Key, bag.Prefix+KeyPrefixDelimiter)
			}
			results[key] = item.Value
		}
	}

	return results, nil
}

// Set stores a value in the cache with the given key.
func (g *GRPCSessionCache) Set(ctx context.Context, key string, value []byte, opt ...types.SessionOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	if bag.Prefix != "" {
		key = bag.Prefix + KeyPrefixDelimiter + key
	}

	req := &v1.SetRequest{
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
func (g *GRPCSessionCache) SetMany(ctx context.Context, values map[string][]byte, opt ...types.SessionOption) error {
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
	// TODO(kans): we may need to chunk if the values are too large for a single gRPC request.
	allKeys := make([]string, 0, len(prefixedValues))
	for key := range prefixedValues {
		allKeys = append(allKeys, key)
	}

	for keys := range Chunk(allKeys, maxKeysPerRequest) {
		// Create chunk of values
		chunkValues := make(map[string][]byte)
		for _, key := range keys {
			chunkValues[key] = prefixedValues[key]
		}

		_, err = g.client.SetMany(ctx, &v1.SetManyRequest{
			SyncId: bag.SyncID,
			Values: chunkValues,
		})
		if err != nil {
			return fmt.Errorf("failed to set many values in gRPC session cache: %w", err)
		}
	}

	return nil
}

// Delete removes a value from the cache by key.
func (g *GRPCSessionCache) Delete(ctx context.Context, key string, opt ...types.SessionOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	if bag.Prefix != "" {
		key = bag.Prefix + KeyPrefixDelimiter + key
	}

	req := &v1.DeleteRequest{
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
func (g *GRPCSessionCache) Clear(ctx context.Context, opt ...types.SessionOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	req := &v1.ClearRequest{
		SyncId: bag.SyncID,
	}

	_, err = g.client.Clear(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to clear gRPC session cache: %w", err)
	}

	return nil
}

func (g *GRPCSessionCache) GetAll(ctx context.Context, opt ...types.SessionOption) (map[string][]byte, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, err
	}

	if bag.Prefix != "" {
		return nil, fmt.Errorf("prefix is not supported for GetAll in gRPC session cache")
	}

	result := make(map[string][]byte)

	pageToken := ""
	for {
		req := &v1.GetAllRequest{
			SyncId:    bag.SyncID,
			PageToken: pageToken,
		}

		resp, err := g.client.GetAll(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("failed to get all values from gRPC session cache: %w", err)
		}

		for _, item := range resp.Items {
			result[item.Key] = item.Value
		}

		// Check if there are more pages
		if resp.PageToken == "" {
			break
		}
		pageToken = resp.PageToken
	}

	return result, nil
}

// Close performs any necessary cleanup when the cache is no longer needed.
func (g *GRPCSessionCache) CloseStore(ctx context.Context) error {
	// No cleanup needed for gRPC client
	return nil
}
