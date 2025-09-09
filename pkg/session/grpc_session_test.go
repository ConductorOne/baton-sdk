//go:build baton_lambda_support

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
	getFunc     func(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error)
	getManyFunc func(ctx context.Context, req *v1.GetManyRequest) (*v1.GetManyResponse, error)
	getAllFunc  func(ctx context.Context, req *v1.GetAllRequest) (*v1.GetAllResponse, error)
	setFunc     func(ctx context.Context, req *v1.SetRequest) (*v1.SetResponse, error)
	setManyFunc func(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error)
	deleteFunc  func(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error)
	clearFunc   func(ctx context.Context, req *v1.ClearRequest) (*v1.ClearResponse, error)
}

func (m *SimpleMockBatonSessionServiceClient) Get(ctx context.Context, in *v1.GetRequest, opts ...grpc.CallOption) (*v1.GetResponse, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, in)
	}
	return &v1.GetResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) GetMany(ctx context.Context, in *v1.GetManyRequest, opts ...grpc.CallOption) (*v1.GetManyResponse, error) {
	if m.getManyFunc != nil {
		return m.getManyFunc(ctx, in)
	}
	return &v1.GetManyResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) GetAll(ctx context.Context, in *v1.GetAllRequest, opts ...grpc.CallOption) (*v1.GetAllResponse, error) {
	if m.getAllFunc != nil {
		return m.getAllFunc(ctx, in)
	}
	return &v1.GetAllResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) Set(ctx context.Context, in *v1.SetRequest, opts ...grpc.CallOption) (*v1.SetResponse, error) {
	if m.setFunc != nil {
		return m.setFunc(ctx, in)
	}
	return &v1.SetResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) SetMany(ctx context.Context, in *v1.SetManyRequest, opts ...grpc.CallOption) (*v1.SetManyResponse, error) {
	if m.setManyFunc != nil {
		return m.setManyFunc(ctx, in)
	}
	return &v1.SetManyResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) Delete(ctx context.Context, in *v1.DeleteRequest, opts ...grpc.CallOption) (*v1.DeleteResponse, error) {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, in)
	}
	return &v1.DeleteResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) DeleteMany(ctx context.Context, in *v1.DeleteManyRequest, opts ...grpc.CallOption) (*v1.DeleteManyResponse, error) {
	return &v1.DeleteManyResponse{}, nil
}

func (m *SimpleMockBatonSessionServiceClient) Clear(ctx context.Context, in *v1.ClearRequest, opts ...grpc.CallOption) (*v1.ClearResponse, error) {
	if m.clearFunc != nil {
		return m.clearFunc(ctx, in)
	}
	return &v1.ClearResponse{}, nil
}

func TestGRPCSessionCache_Get(t *testing.T) {
	expectedValue := []byte("test-value")
	mockClient := &SimpleMockBatonSessionServiceClient{
		getFunc: func(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error) {
			if req.Key == "test-key" && req.SyncId == "test-sync-id" {
				return &v1.GetResponse{Value: expectedValue}, nil
			}
			// Return nil for non-existent keys to indicate "not found"
			return nil, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, types.SyncIDKey{}, "test-sync-id")

	// Test successful get
	value, found, err := cache.Get(ctx, "test-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Expected to find value")
	}
	if string(value) != "test-value" {
		t.Fatalf("Expected value 'test-value', got '%s'", string(value))
	}

	// Test get with non-existent key
	value, found, err = cache.Get(ctx, "non-existent-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if found {
		t.Fatal("Expected not to find value", value)
	}
	if value != nil {
		t.Fatalf("Expected nil value, got %v", value)
	}
}

func TestGRPCSessionCache_Set(t *testing.T) {
	mockClient := &SimpleMockBatonSessionServiceClient{
		setFunc: func(ctx context.Context, req *v1.SetRequest) (*v1.SetResponse, error) {
			if req.Key == "test-key" && req.SyncId == "test-sync-id" && string(req.Value) == "test-value" {
				return &v1.SetResponse{}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, types.SyncIDKey{}, "test-sync-id")

	err := cache.Set(ctx, "test-key", []byte("test-value"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
}

func TestGRPCSessionCache_GetMany(t *testing.T) {
	expectedValues := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}
	mockClient := &SimpleMockBatonSessionServiceClient{
		getManyFunc: func(ctx context.Context, req *v1.GetManyRequest) (*v1.GetManyResponse, error) {
			if req.SyncId == "test-sync-id" {
				// Convert map to items array
				items := make([]*v1.GetManyItem, 0, len(expectedValues))
				for key, value := range expectedValues {
					items = append(items, &v1.GetManyItem{
						Key:   key,
						Value: value,
					})
				}
				return &v1.GetManyResponse{
					Items: items,
				}, nil
			}
			return &v1.GetManyResponse{
				Items: []*v1.GetManyItem{},
			}, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, types.SyncIDKey{}, "test-sync-id")

	values, err := cache.GetMany(ctx, []string{"key1", "key2"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(values))
	}
	if string(values["key1"]) != "value1" {
		t.Fatalf("Expected value 'value1' for key1, got '%s'", string(values["key1"]))
	}
	if string(values["key2"]) != "value2" {
		t.Fatalf("Expected value 'value2' for key2, got '%s'", string(values["key2"]))
	}
}

func TestGRPCSessionCache_GetMany_Pagination(t *testing.T) {
	callCount := 0
	mockClient := &SimpleMockBatonSessionServiceClient{
		getManyFunc: func(ctx context.Context, req *v1.GetManyRequest) (*v1.GetManyResponse, error) {
			callCount++
			if req.SyncId != "test-sync-id" {
				return nil, fmt.Errorf("unexpected sync id: %s", req.SyncId)
			}
			if req.PageToken == "" {
				return &v1.GetManyResponse{
					Items: []*v1.GetManyItem{
						{Key: "key1", Value: []byte("value1")},
					},
					NextPageToken: "p2",
				}, nil
			}
			if req.PageToken == "p2" {
				return &v1.GetManyResponse{
					Items: []*v1.GetManyItem{
						{Key: "key2", Value: []byte("value2")},
					},
				}, nil
			}
			return &v1.GetManyResponse{Items: []*v1.GetManyItem{}}, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, types.SyncIDKey{}, "test-sync-id")

	values, err := cache.GetMany(ctx, []string{"key1", "key2"})
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if callCount != 2 {
		t.Fatalf("expected 2 calls, got %d", callCount)
	}
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(values))
	}
	if string(values["key1"]) != "value1" || string(values["key2"]) != "value2" {
		t.Fatalf("unexpected values: %+v", values)
	}
}

func TestGRPCSessionCache_SetMany(t *testing.T) {
	values := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}

	mockClient := &SimpleMockBatonSessionServiceClient{
		setManyFunc: func(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error) {
			if req.SyncId == "test-sync-id" && len(req.Values) == 2 {
				return &v1.SetManyResponse{}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, types.SyncIDKey{}, "test-sync-id")

	err := cache.SetMany(ctx, values)
	if err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}
}

func TestGRPCSessionCache_Delete(t *testing.T) {
	mockClient := &SimpleMockBatonSessionServiceClient{
		deleteFunc: func(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
			if req.Key == "test-key" && req.SyncId == "test-sync-id" {
				return &v1.DeleteResponse{}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, types.SyncIDKey{}, "test-sync-id")

	err := cache.Delete(ctx, "test-key")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func TestGRPCSessionCache_Clear(t *testing.T) {
	mockClient := &SimpleMockBatonSessionServiceClient{
		clearFunc: func(ctx context.Context, req *v1.ClearRequest) (*v1.ClearResponse, error) {
			if req.SyncId == "test-sync-id" {
				return &v1.ClearResponse{}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, types.SyncIDKey{}, "test-sync-id")

	err := cache.Clear(ctx)
	if err != nil {
		t.Fatalf("Clear failed: %v", err)
	}
}

func TestGRPCSessionCache_GetAll(t *testing.T) {
	expectedValues := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}
	mockClient := &SimpleMockBatonSessionServiceClient{
		getAllFunc: func(ctx context.Context, req *v1.GetAllRequest) (*v1.GetAllResponse, error) {
			if req.SyncId == "test-sync-id" {
				// Convert map to items array
				items := make([]*v1.GetAllItem, 0, len(expectedValues))
				for key, value := range expectedValues {
					items = append(items, &v1.GetAllItem{
						Key:   key,
						Value: value,
					})
				}
				return &v1.GetAllResponse{
					Items: items,
				}, nil
			}
			return &v1.GetAllResponse{
				Items: []*v1.GetAllItem{},
			}, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, types.SyncIDKey{}, "test-sync-id")

	values, err := cache.GetAll(ctx)
	if err != nil {
		t.Fatalf("GetAll failed: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(values))
	}
	if string(values["key1"]) != "value1" {
		t.Fatalf("Expected value 'value1' for key1, got '%s'", string(values["key1"]))
	}
	if string(values["key2"]) != "value2" {
		t.Fatalf("Expected value 'value2' for key2, got '%s'", string(values["key2"]))
	}
}

func TestGRPCSessionCache_GetAll_Pagination(t *testing.T) {
	callCount := 0
	mockClient := &SimpleMockBatonSessionServiceClient{
		getAllFunc: func(ctx context.Context, req *v1.GetAllRequest) (*v1.GetAllResponse, error) {
			callCount++
			if req.SyncId != "test-sync-id" {
				return nil, fmt.Errorf("unexpected sync id: %s", req.SyncId)
			}
			if req.PageToken == "" {
				return &v1.GetAllResponse{
					Items: []*v1.GetAllItem{
						{Key: "a", Value: []byte("1")},
						{Key: "b", Value: []byte("2")},
					},
					NextPageToken: "p2",
				}, nil
			}
			if req.PageToken == "p2" {
				return &v1.GetAllResponse{
					Items: []*v1.GetAllItem{
						{Key: "c", Value: []byte("3")},
					},
				}, nil
			}
			return &v1.GetAllResponse{Items: []*v1.GetAllItem{}}, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, types.SyncIDKey{}, "test-sync-id")

	values, err := cache.GetAll(ctx)
	if err != nil {
		t.Fatalf("GetAll failed: %v", err)
	}
	if callCount != 2 {
		t.Fatalf("expected 2 calls, got %d", callCount)
	}
	if len(values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(values))
	}
	if string(values["a"]) != "1" || string(values["b"]) != "2" || string(values["c"]) != "3" {
		t.Fatalf("unexpected values: %+v", values)
	}
}

func TestGRPCSessionCache_WithPrefix(t *testing.T) {
	expectedValue := []byte("test-value")
	mockClient := &SimpleMockBatonSessionServiceClient{
		getFunc: func(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error) {
			if req.Key == "prefix"+KeyPrefixDelimiter+"test-key" && req.SyncId == "test-sync-id" {
				return &v1.GetResponse{Value: expectedValue}, nil
			}
			return &v1.GetResponse{Value: []byte{}}, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, types.SyncIDKey{}, "test-sync-id")

	// Test with prefix
	value, found, err := cache.Get(ctx, "test-key", WithPrefix("prefix"))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if !found {
		t.Fatal("Expected key to be found")
	}
	if string(value) != "test-value" {
		t.Fatalf("Expected value 'test-value', got '%s'", string(value))
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

	// Test with valid access token and DPoP key
	// The client creation should succeed even when no session service is running
	// because gRPC connections are created lazily
	client, err := NewGRPCSessionClient(context.Background(), "test-access-token", dpopKey)
	if err != nil {
		t.Fatalf("Expected no error during client creation, got %v", err)
	}
	if client == nil {
		t.Fatal("Expected non-nil client")
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
