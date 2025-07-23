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
	return NewGRPCSessionCacheWithOptions(ctx, serverAddr, opts...)
}

// NewGRPCSessionCacheWithOptions creates a new gRPC-based session cache with constructor options.
func NewGRPCSessionCacheWithOptions(ctx context.Context, serverAddr string, grpcOpts ...GRPCOption) (types.SessionCache, error) {
	config := &grpcConfig{
		timeout: 30 * time.Second,
	}

	for _, opt := range grpcOpts {
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

// Get retrieves a value from the cache by key.
func (g *GRPCSessionCache) Get(ctx context.Context, key string, opt ...types.SessionCacheOption) ([]byte, bool, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, false, err
	}

	if bag.Prefix != "" {
		key = bag.Prefix + "::" + key
	}

	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.GetRequest{
		SyncId:    bag.SyncID,
		Namespace: "",
		Key:       key,
	}

	resp, err := g.client.Get(ctx, req)
	if err != nil {
		return nil, false, fmt.Errorf("gRPC get failed: %w", err)
	}

	return resp.Value, resp.Found, nil
}

// Set stores a value in the cache with the given key.
func (g *GRPCSessionCache) Set(ctx context.Context, key string, value []byte, opt ...types.SessionCacheOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	if bag.Prefix != "" {
		key = bag.Prefix + "::" + key
	}

	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.SetRequest{
		SyncId:    bag.SyncID,
		Namespace: "",
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

// Delete removes a value from the cache by key.
func (g *GRPCSessionCache) Delete(ctx context.Context, key string, opt ...types.SessionCacheOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	if bag.Prefix != "" {
		key = bag.Prefix + "::" + key
	}

	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.DeleteRequest{
		SyncId:    bag.SyncID,
		Namespace: "",
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
func (g *GRPCSessionCache) Clear(ctx context.Context, opt ...types.SessionCacheOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.ClearRequest{
		SyncId: bag.SyncID,
	}

	resp, err := g.client.Clear(ctx, req)
	if err != nil {
		return fmt.Errorf("gRPC clear failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("gRPC clear operation failed")
	}

	return nil
}

// GetAll returns all key-value pairs.
func (g *GRPCSessionCache) GetAll(ctx context.Context, opt ...types.SessionCacheOption) (map[string][]byte, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, err
	}

	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.GetAllRequest{
		SyncId:    bag.SyncID,
		Namespace: "",
	}

	resp, err := g.client.GetAll(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC getAll failed: %w", err)
	}

	return resp.Values, nil
}

// GetMany retrieves multiple values from the cache by keys.
func (g *GRPCSessionCache) GetMany(ctx context.Context, keys []string, opt ...types.SessionCacheOption) (map[string][]byte, error) {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return nil, err
	}

	// Apply prefix to all keys if needed
	if bag.Prefix != "" {
		for i, key := range keys {
			keys[i] = bag.Prefix + "::" + key
		}
	}

	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.GetManyRequest{
		SyncId:    bag.SyncID,
		Namespace: "",
		Keys:      keys,
	}

	resp, err := g.client.GetMany(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("gRPC getMany failed: %w", err)
	}

	return resp.Values, nil
}

// SetMany stores multiple values in the cache.
func (g *GRPCSessionCache) SetMany(ctx context.Context, values map[string][]byte, opt ...types.SessionCacheOption) error {
	bag, err := applyOptions(ctx, opt...)
	if err != nil {
		return err
	}

	// Apply prefix to all keys if needed
	prefixedValues := make(map[string][]byte)
	for key, value := range values {
		if bag.Prefix != "" {
			key = bag.Prefix + "::" + key
		}
		prefixedValues[key] = value
	}

	ctx = g.addAuthHeader(ctx)
	ctx, cancel := g.createTimeoutContext(ctx)
	defer cancel()

	req := &batonv1.SetManyRequest{
		SyncId:    bag.SyncID,
		Namespace: "",
		Values:    prefixedValues,
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

// WithNamespace returns a curried session cache that operates within a fixed namespace.
func (g *GRPCSessionCache) WithNamespace(namespace string) types.SessionCache {
	return &namespacedSessionCache{
		cache:     g,
		namespace: namespace,
	}
}
