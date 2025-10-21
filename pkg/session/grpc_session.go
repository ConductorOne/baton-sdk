package session

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"iter"
	"net"
	"os"
	"time"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/sdk"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
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
			end := min(i+chunkSize, len(items))
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
func NewGRPCSessionClient(ctx context.Context, accessToken string, dpopKey *jose.JSONWebKey, opt ...sessions.SessionStoreConstructorOption) (v1.BatonSessionServiceClient, error) {
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

// These functions are no longer needed since we're reusing existing credentials

// NewGRPCSessionCache creates a new gRPC session cache instance.
func NewGRPCSessionCache(ctx context.Context, client v1.BatonSessionServiceClient, opt ...sessions.SessionStoreConstructorOption) (sessions.SessionStore, error) {
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
func (g *GRPCSessionCache) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, false, err
	}

	req := &v1.GetRequest{
		SyncId: bag.SyncID,
		Key:    key,
		Prefix: bag.Prefix,
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
func (g *GRPCSessionCache) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, err
	}

	results := make(map[string][]byte)
	// TODO(kans): we may need to chunk if the values are too large for a single gRPC request.
	// The GetMany interface may be backed by gPRC, memory, etc, so we need to handle pagination at the client level.
	for keys := range Chunk(keys, maxKeysPerRequest) {
		resp, err := g.client.GetMany(ctx, &v1.GetManyRequest{
			SyncId: bag.SyncID,
			Keys:   keys,
			Prefix: bag.Prefix,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get many values from gRPC session cache: %w", err)
		}

		for _, item := range resp.Items {
			results[item.Key] = item.Value
		}
	}

	return results, nil
}

// Set stores a value in the cache with the given key.
func (g *GRPCSessionCache) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	req := &v1.SetRequest{
		SyncId: bag.SyncID,
		Key:    key,
		Value:  value,
		Prefix: bag.Prefix,
	}

	_, err = g.client.Set(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to set value in gRPC session cache: %w", err)
	}

	return nil
}

// SetMany stores multiple values in the cache.
func (g *GRPCSessionCache) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	// TODO(kans): we may need to chunk if the values are too large for a single gRPC request.
	allKeys := make([]string, 0, len(values))
	for key := range values {
		allKeys = append(allKeys, key)
	}

	for keys := range Chunk(allKeys, maxKeysPerRequest) {
		chunkValues := make(map[string][]byte)
		for _, key := range keys {
			chunkValues[key] = values[key]
		}

		_, err = g.client.SetMany(ctx, &v1.SetManyRequest{
			SyncId: bag.SyncID,
			Values: chunkValues,
			Prefix: bag.Prefix,
		})
		if err != nil {
			return fmt.Errorf("failed to set many values in gRPC session cache: %w", err)
		}
	}

	return nil
}

// Delete removes a value from the cache by key.
func (g *GRPCSessionCache) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	req := &v1.DeleteRequest{
		SyncId: bag.SyncID,
		Key:    key,
		Prefix: bag.Prefix,
	}

	_, err = g.client.Delete(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to delete value from gRPC session cache: %w", err)
	}

	return nil
}

// Clear removes all values from the cache.
func (g *GRPCSessionCache) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	req := &v1.ClearRequest{
		SyncId: bag.SyncID,
		Prefix: bag.Prefix,
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
func (g *GRPCSessionCache) GetAll(ctx context.Context, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, err
	}

	result := make(map[string][]byte)

	pageToken := ""
	for {
		req := &v1.GetAllRequest{
			SyncId:    bag.SyncID,
			PageToken: pageToken,
			Prefix:    bag.Prefix,
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
