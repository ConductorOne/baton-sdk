package session

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"slices"
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

var _ sessions.SessionStore = (*GRPCSessionStoreClient)(nil)

type GRPCSessionStoreClient struct {
	client v1.BatonSessionServiceClient
}

// applyOptions applies session cache options and returns a configured bag.
func applyOptions(ctx context.Context, opt ...sessions.SessionStoreOption) (*sessions.SessionStoreBag, error) {
	bag := &sessions.SessionStoreBag{}

	for _, option := range opt {
		err := option(ctx, bag)
		if err != nil {
			return nil, err
		}
	}

	if bag.SyncID == "" {
		return nil, fmt.Errorf("no syncID set in options")
	}

	return bag, nil
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

// NewGRPCSessionStore creates a new gRPC session cache instance.
func NewGRPCSessionStore(ctx context.Context, client v1.BatonSessionServiceClient, opt ...sessions.SessionStoreConstructorOption) (sessions.SessionStore, error) {
	// Apply constructor options
	for _, option := range opt {
		var err error
		ctx, err = option(ctx)
		if err != nil {
			return nil, err
		}
	}

	return &GRPCSessionStoreClient{
		client: client,
	}, nil
}

// Get retrieves a value from the cache by key.
func (g *GRPCSessionStoreClient) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, false, err
	}

	req := v1.GetRequest_builder{
		SyncId: bag.SyncID,
		Key:    key,
		Prefix: bag.Prefix,
	}.Build()

	resp, err := g.client.Get(ctx, req)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get value from gRPC session cache: %w", err)
	}

	return resp.GetValue(), resp.GetFound(), nil
}

// GetMany retrieves multiple values from the cache by keys.
func (g *GRPCSessionStoreClient) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, nil, err
	}

	slices.Sort(keys)
	keys = slices.Compact(keys)

	resp, err := g.client.GetMany(ctx, v1.GetManyRequest_builder{
		SyncId: bag.SyncID,
		Keys:   keys,
		Prefix: bag.Prefix,
	}.Build())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get many values from gRPC session cache: %w", err)
	}

	results := make(map[string][]byte, len(resp.Items))
	for _, item := range resp.Items {
		results[item.Key] = item.Value
	}

	return results, resp.UnprocessedKeys, nil
}

// Set stores a value in the cache with the given key.
func (g *GRPCSessionStoreClient) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	req := v1.SetRequest_builder{
		SyncId: bag.SyncID,
		Key:    key,
		Value:  value,
		Prefix: bag.Prefix,
	}.Build()

	_, err = g.client.Set(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to set value in gRPC session cache: %w", err)
	}

	return nil
}

// SetMany stores multiple values in the cache.
func (g *GRPCSessionStoreClient) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	_, err = g.client.SetMany(ctx, v1.SetManyRequest_builder{
		Values: values,
		SyncId: bag.SyncID,
		Prefix: bag.Prefix,
	}.Build())
	if err != nil {
		return fmt.Errorf("failed to set many values in gRPC session cache: %w", err)
	}

	return nil
}

// Delete removes a value from the cache by key.
func (g *GRPCSessionStoreClient) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	req := v1.DeleteRequest_builder{
		SyncId: bag.SyncID,
		Key:    key,
		Prefix: bag.Prefix,
	}.Build()

	_, err = g.client.Delete(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to delete value from gRPC session cache: %w", err)
	}

	return nil
}

// Clear removes all values from the cache.
func (g *GRPCSessionStoreClient) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	req := v1.ClearRequest_builder{
		SyncId: bag.SyncID,
		Prefix: bag.Prefix,
	}.Build()

	_, err = g.client.Clear(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to clear gRPC session cache: %w", err)
	}

	return nil
}

func (g *GRPCSessionStoreClient) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, "", err
	}

	result := make(map[string][]byte)

	req := v1.GetAllRequest_builder{
		SyncId:    bag.SyncID,
		PageToken: pageToken,
		Prefix:    bag.Prefix,
	}.Build()

	resp, err := g.client.GetAll(ctx, req)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get all values from gRPC session cache: %w", err)
	}

	// Add items from this page to the result
	for _, item := range resp.Items {
		result[item.Key] = item.Value
	}

	return result, resp.PageToken, nil
}
