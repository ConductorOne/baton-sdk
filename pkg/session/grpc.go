package session

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	batonv1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types"
)

// GRPCSessionCache implements SessionCache interface using a remote gRPC service.
type GRPCSessionCache struct {
	client    batonv1.SessionCacheServiceClient
	conn      *grpc.ClientConn
	timeout   time.Duration
	authToken string
}

// NewGRPCSessionCache creates a new gRPC-based session cache.
func NewGRPCSessionCache(ctx context.Context, serverAddr string, opts ...GRPCOption) (types.SessionCache, error) {
	config := &grpcConfig{
		timeout: 30 * time.Second,
	}

	for _, opt := range opts {
		opt(config)
	}

	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to session cache server: %w", err)
	}

	client := batonv1.NewSessionCacheServiceClient(conn)

	return &GRPCSessionCache{
		client:    client,
		conn:      conn,
		timeout:   config.timeout,
		authToken: config.authToken,
	}, nil
}

// grpcConfig holds configuration for the gRPC session cache.
type grpcConfig struct {
	timeout   time.Duration
	authToken string
}

// GRPCOption is a function that configures the gRPC session cache.
type GRPCOption func(*grpcConfig)

// WithTimeout sets the timeout for gRPC operations.
func WithTimeout(timeout time.Duration) GRPCOption {
	return func(c *grpcConfig) {
		c.timeout = timeout
	}
}

// WithAuthToken sets the authentication token for gRPC operations.
func WithAuthToken(token string) GRPCOption {
	return func(c *grpcConfig) {
		c.authToken = token
	}
}

// addAuthHeader adds authentication header to the context if token is set.
func (g *GRPCSessionCache) addAuthHeader(ctx context.Context) context.Context {
	if g.authToken != "" {
		md := metadata.New(map[string]string{
			"authorization": "Bearer " + g.authToken,
		})
		return metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

// createTimeoutContext creates a context with timeout.
func (g *GRPCSessionCache) createTimeoutContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, g.timeout)
}

// Get retrieves a value from the cache by namespace and key.
func (g *GRPCSessionCache) Get(ctx context.Context, namespace, key string) ([]byte, bool, error) {
	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.GetRequest{
		SyncId:    "default", // TODO: Get sync_id from context or parameter
		Namespace: namespace,
		Key:       key,
	}

	resp, err := g.client.Get(ctx, req)
	if err != nil {
		return nil, false, fmt.Errorf("gRPC get failed: %w", err)
	}

	return resp.Value, resp.Found, nil
}

// Set stores a value in the cache with the given namespace and key.
func (g *GRPCSessionCache) Set(ctx context.Context, namespace, key string, value []byte) error {
	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.SetRequest{
		SyncId:    "default", // TODO: Get sync_id from context or parameter
		Namespace: namespace,
		Key:       key,
		Value:     value,
	}

	resp, err := g.client.Set(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC set failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("gRPC set operation failed")
	}

	return nil
}

// Delete removes a value from the cache by namespace and key.
func (g *GRPCSessionCache) Delete(ctx context.Context, namespace, key string) error {
	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.DeleteRequest{
		SyncId:    "default", // TODO: Get sync_id from context or parameter
		Namespace: namespace,
		Key:       key,
	}

	resp, err := g.client.Delete(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC delete failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("gRPC delete operation failed")
	}

	return nil
}

// Clear removes all values from the cache.
func (g *GRPCSessionCache) Clear(ctx context.Context) error {
	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	resp, err := g.client.Clear(ctx, &batonv1.ClearRequest{})
	if err != nil {
		return fmt.Errorf("gRPC clear failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("gRPC clear operation failed")
	}

	return nil
}

// GetAll returns all key-value pairs in a namespace.
func (g *GRPCSessionCache) GetAll(ctx context.Context, namespace string) (map[string][]byte, error) {
	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.GetAllRequest{
		SyncId:    "default", // TODO: Get sync_id from context or parameter
		Namespace: namespace,
	}

	resp, err := g.client.GetAll(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC getAll failed: %w", err)
	}

	return resp.Values, nil
}

// GetMany retrieves multiple values from the cache by namespace and keys.
func (g *GRPCSessionCache) GetMany(ctx context.Context, namespace string, keys []string) (map[string][]byte, error) {
	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.GetManyRequest{
		SyncId:    "default", // TODO: Get sync_id from context or parameter
		Namespace: namespace,
		Keys:      keys,
	}

	resp, err := g.client.GetMany(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC getMany failed: %w", err)
	}

	return resp.Values, nil
}

// SetMany stores multiple values in the cache with the given namespace.
func (g *GRPCSessionCache) SetMany(ctx context.Context, namespace string, values map[string][]byte) error {
	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.SetManyRequest{
		SyncId:    "default", // TODO: Get sync_id from context or parameter
		Namespace: namespace,
		Values:    values,
	}

	resp, err := g.client.SetMany(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC setMany failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("gRPC setMany operation failed")
	}

	return nil
}

// Close performs any necessary cleanup when the cache is no longer needed.
func (g *GRPCSessionCache) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}
