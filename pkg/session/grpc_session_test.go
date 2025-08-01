package session

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"os"
	"strings"
	"testing"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/go-jose/go-jose/v4"
	"google.golang.org/grpc"
)

// SimpleMockBatonSessionServiceClient is a simple mock implementation for testing
type SimpleMockBatonSessionServiceClient struct {
	getFunc     func(ctx context.Context, req *v1.BatonServiceGetRequest) (*v1.BatonServiceGetResponse, error)
	getManyFunc func(ctx context.Context, req *v1.BatonServiceGetManyRequest) (*v1.BatonServiceGetManyResponse, error)
	setFunc     func(ctx context.Context, req *v1.BatonServiceSetRequest) (*v1.BatonServiceSetResponse, error)
	setManyFunc func(ctx context.Context, req *v1.BatonServiceSetManyRequest) (*v1.BatonServiceSetManyResponse, error)
	deleteFunc  func(ctx context.Context, req *v1.BatonServiceDeleteRequest) (*v1.BatonServiceDeleteResponse, error)
	clearFunc   func(ctx context.Context, req *v1.BatonServiceClearRequest) (*v1.BatonServiceClearResponse, error)
}

func (m *SimpleMockBatonSessionServiceClient) Get(ctx context.Context, in *v1.BatonServiceGetRequest, opts ...grpc.CallOption) (*v1.BatonServiceGetResponse, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, in)
	}
	return &v1.BatonServiceGetResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) GetMany(ctx context.Context, in *v1.BatonServiceGetManyRequest, opts ...grpc.CallOption) (*v1.BatonServiceGetManyResponse, error) {
	if m.getManyFunc != nil {
		return m.getManyFunc(ctx, in)
	}
	return &v1.BatonServiceGetManyResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) Set(ctx context.Context, in *v1.BatonServiceSetRequest, opts ...grpc.CallOption) (*v1.BatonServiceSetResponse, error) {
	if m.setFunc != nil {
		return m.setFunc(ctx, in)
	}
	return &v1.BatonServiceSetResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) SetMany(ctx context.Context, in *v1.BatonServiceSetManyRequest, opts ...grpc.CallOption) (*v1.BatonServiceSetManyResponse, error) {
	if m.setManyFunc != nil {
		return m.setManyFunc(ctx, in)
	}
	return &v1.BatonServiceSetManyResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) Delete(ctx context.Context, in *v1.BatonServiceDeleteRequest, opts ...grpc.CallOption) (*v1.BatonServiceDeleteResponse, error) {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, in)
	}
	return &v1.BatonServiceDeleteResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) DeleteMany(ctx context.Context, in *v1.BatonServiceDeleteManyRequest, opts ...grpc.CallOption) (*v1.BatonServiceDeleteManyResponse, error) {
	return &v1.BatonServiceDeleteManyResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) Clear(ctx context.Context, in *v1.BatonServiceClearRequest, opts ...grpc.CallOption) (*v1.BatonServiceClearResponse, error) {
	if m.clearFunc != nil {
		return m.clearFunc(ctx, in)
	}
	return &v1.BatonServiceClearResponse{}, nil
}

// WithSyncID returns a SessionCacheOption that sets the sync ID for the operation.
func setSyncIDInContext(ctx context.Context, syncID string) (context.Context, error) {
	// Check if syncID is already set in the context
	if existingSyncID, ok := ctx.Value(types.SyncIDKey{}).(string); ok && existingSyncID != "" {
		if existingSyncID != syncID {
			return ctx, fmt.Errorf("syncID mismatch: context already has syncID %q, cannot set to %q", existingSyncID, syncID)
		}
		// If they match, return the context unchanged
		return ctx, nil
	}
	return context.WithValue(ctx, types.SyncIDKey{}, syncID), nil
}

func TestGRPCSessionCache_Get(t *testing.T) {
	expectedValue := []byte("test-value")
	mockClient := &SimpleMockBatonSessionServiceClient{
		getFunc: func(ctx context.Context, req *v1.BatonServiceGetRequest) (*v1.BatonServiceGetResponse, error) {
			if req.Key == "test-key" && req.SyncId == "test-sync-id" {
				return &v1.BatonServiceGetResponse{Value: expectedValue}, nil
			}
			return &v1.BatonServiceGetResponse{Value: []byte{}}, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx, _ = setSyncIDInContext(ctx, "test-sync-id")

	// Test successful get
	value, found, err := cache.Get(ctx, "test-key")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !found {
		t.Fatal("Expected key to be found")
	}
	if string(value) != string(expectedValue) {
		t.Fatalf("Expected value %s, got %s", string(expectedValue), string(value))
	}

	// Test key not found
	value, found, err = cache.Get(ctx, "non-existent-key")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if found {
		t.Fatal("Expected key to not be found")
	}
	if value != nil {
		t.Fatalf("Expected nil value, got %v", value)
	}
}

func TestGRPCSessionCache_Set(t *testing.T) {
	mockClient := &SimpleMockBatonSessionServiceClient{
		setFunc: func(ctx context.Context, req *v1.BatonServiceSetRequest) (*v1.BatonServiceSetResponse, error) {
			if req.Key == "test-key" && req.SyncId == "test-sync-id" && string(req.Value) == "test-value" {
				return &v1.BatonServiceSetResponse{}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx, _ = setSyncIDInContext(ctx, "test-sync-id")

	value := []byte("test-value")
	err := cache.Set(ctx, "test-key", value)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestGRPCSessionCache_GetMany(t *testing.T) {
	expectedValues := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}

	mockClient := &SimpleMockBatonSessionServiceClient{
		getManyFunc: func(ctx context.Context, req *v1.BatonServiceGetManyRequest) (*v1.BatonServiceGetManyResponse, error) {
			if req.SyncId == "test-sync-id" && len(req.Keys) == 2 {
				return &v1.BatonServiceGetManyResponse{Values: expectedValues}, nil
			}
			return &v1.BatonServiceGetManyResponse{Values: map[string][]byte{}}, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx, _ = setSyncIDInContext(ctx, "test-sync-id")

	keys := []string{"key1", "key2"}
	values, err := cache.GetMany(ctx, keys)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(values) != len(expectedValues) {
		t.Fatalf("Expected %d values, got %d", len(expectedValues), len(values))
	}
	for k, v := range expectedValues {
		if string(values[k]) != string(v) {
			t.Fatalf("Expected value %s for key %s, got %s", string(v), k, string(values[k]))
		}
	}
}

func TestGRPCSessionCache_SetMany(t *testing.T) {
	values := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}

	mockClient := &SimpleMockBatonSessionServiceClient{
		setManyFunc: func(ctx context.Context, req *v1.BatonServiceSetManyRequest) (*v1.BatonServiceSetManyResponse, error) {
			if req.SyncId == "test-sync-id" && len(req.Values) == 2 {
				return &v1.BatonServiceSetManyResponse{}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx, _ = setSyncIDInContext(ctx, "test-sync-id")

	err := cache.SetMany(ctx, values)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestGRPCSessionCache_Delete(t *testing.T) {
	mockClient := &SimpleMockBatonSessionServiceClient{
		deleteFunc: func(ctx context.Context, req *v1.BatonServiceDeleteRequest) (*v1.BatonServiceDeleteResponse, error) {
			if req.Key == "test-key" && req.SyncId == "test-sync-id" {
				return &v1.BatonServiceDeleteResponse{}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx, _ = setSyncIDInContext(ctx, "test-sync-id")

	err := cache.Delete(ctx, "test-key")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestGRPCSessionCache_Clear(t *testing.T) {
	mockClient := &SimpleMockBatonSessionServiceClient{
		clearFunc: func(ctx context.Context, req *v1.BatonServiceClearRequest) (*v1.BatonServiceClearResponse, error) {
			if req.SyncId == "test-sync-id" {
				return &v1.BatonServiceClearResponse{}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx, _ = setSyncIDInContext(ctx, "test-sync-id")

	err := cache.Clear(ctx)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestGRPCSessionCache_GetAll(t *testing.T) {
	mockClient := &SimpleMockBatonSessionServiceClient{}
	cache := &GRPCSessionCache{client: mockClient}

	ctx := context.Background()
	ctx, _ = setSyncIDInContext(ctx, "test-sync-id")

	values, err := cache.GetAll(ctx)
	if err == nil {
		t.Fatal("Expected error for GetAll operation")
	}
	if err.Error() != "GetAll operation is not supported by the gRPC session service" {
		t.Fatalf("Expected specific error message, got %v", err)
	}
	if len(values) != 0 {
		t.Fatalf("Expected empty map, got %v", values)
	}
}

func TestGRPCSessionCache_WithPrefix(t *testing.T) {
	expectedValue := []byte("test-value")
	mockClient := &SimpleMockBatonSessionServiceClient{
		getFunc: func(ctx context.Context, req *v1.BatonServiceGetRequest) (*v1.BatonServiceGetResponse, error) {
			if req.Key == "prefix"+KeyPrefixDelimiter+"test-key" && req.SyncId == "test-sync-id" {
				return &v1.BatonServiceGetResponse{Value: expectedValue}, nil
			}
			return &v1.BatonServiceGetResponse{Value: []byte{}}, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx, _ = setSyncIDInContext(ctx, "test-sync-id")

	// Test with prefix
	value, found, err := cache.Get(ctx, "test-key", WithPrefix("prefix"))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !found {
		t.Fatal("Expected key to be found")
	}
	if string(value) != string(expectedValue) {
		t.Fatalf("Expected value %s, got %s", string(expectedValue), string(value))
	}
}

func TestNewGRPCSessionCache(t *testing.T) {
	mockClient := &SimpleMockBatonSessionServiceClient{}

	cache, err := NewGRPCSessionCache(context.Background(), mockClient)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if cache == nil {
		t.Fatal("Expected non-nil cache")
	}
	// Note: We can't access the client field directly since it's private
	// This test just verifies the constructor works
}

func TestNewGRPCSessionClient(t *testing.T) {
	// Create a test DPoP key
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("Failed to generate test key: %v", err)
	}

	dpopKey := &jose.JSONWebKey{
		Key:       priv,
		KeyID:     "test-key",
		Algorithm: string(jose.EdDSA),
		Use:       "sig",
	}

	// Test with valid access token and DPoP key (should fail to connect but not crash)
	client, err := NewGRPCSessionClient(context.Background(), "test-access-token", dpopKey)
	if err == nil {
		t.Fatal("Expected error when no session service is running")
	}
	// Should get a connection error, not a client creation error
	if client != nil {
		t.Fatal("Expected nil client when connection fails")
	}
}

func TestNewGRPCSessionClient_WithInvalidAddress(t *testing.T) {
	// Test with invalid address format
	os.Setenv("BATON_SESSION_SERVICE_ADDR", "invalid-address")
	defer os.Unsetenv("BATON_SESSION_SERVICE_ADDR")

	// Create a test DPoP key
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("Failed to generate test key: %v", err)
	}

	dpopKey := &jose.JSONWebKey{
		Key:       priv,
		KeyID:     "test-key",
		Algorithm: string(jose.EdDSA),
		Use:       "sig",
	}

	client, err := NewGRPCSessionClient(context.Background(), "test-access-token", dpopKey)
	if err == nil {
		t.Fatal("Expected error for invalid address format")
	}
	if client != nil {
		t.Fatal("Expected nil client for invalid address")
	}
	if !strings.Contains(err.Error(), "invalid session service address") {
		t.Fatalf("Expected error about invalid address, got: %v", err)
	}
}

func TestNewGRPCSessionClient_WithInvalidDPoPKey(t *testing.T) {
	// Test with invalid DPoP key
	invalidKey := &jose.JSONWebKey{
		Key:       "not-a-key",
		KeyID:     "invalid-key",
		Algorithm: string(jose.EdDSA),
		Use:       "sig",
	}

	client, err := NewGRPCSessionClient(context.Background(), "test-access-token", invalidKey)
	if err == nil {
		t.Fatal("Expected error for invalid DPoP key")
	}
	if client != nil {
		t.Fatal("Expected nil client for invalid DPoP key")
	}
	if !strings.Contains(err.Error(), "failed to create dpop proofer") {
		t.Fatalf("Expected error about invalid DPoP key, got: %v", err)
	}
}
