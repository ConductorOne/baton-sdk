//go:build baton_lambda_support

package session

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"os"
	"strings"
	"testing"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/go-jose/go-jose/v4"
	"google.golang.org/grpc"
)

// SimpleMockBatonSessionServiceClient is a simple mock implementation for testing.
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
	syncID := "test-sync-id-get"
	expectedValue := []byte("test-value")
	mockClient := &SimpleMockBatonSessionServiceClient{
		getFunc: func(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error) {
			if req.Key == "test-key" && req.SyncId == syncID {
				return &v1.GetResponse{Value: expectedValue}, nil
			}
			// Return nil for non-existent keys to indicate "not found"
			return nil, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()

	// Test successful get
	value, found, err := cache.Get(ctx, "test-key", sessions.WithSyncID(syncID))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("Expected to find value")
	}
	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("Expected value '%s', got '%s'", string(expectedValue), string(value))
	}

	// Test get with non-existent key
	value, found, err = cache.Get(ctx, "non-existent-key", sessions.WithSyncID(syncID))
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
			if req.Key == "test-key-set" && req.SyncId == "test-sync-id-set" && string(req.Value) == "test-value-set" {
				return &v1.SetResponse{}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()

	err := cache.Set(ctx, "test-key-set", []byte("test-value-set"), sessions.WithSyncID("test-sync-id-set"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
}

func TestGRPCSessionCache_GetMany(t *testing.T) {
	expectedValues := map[string][]byte{
		"key1": []byte("value1-get-many"),
		"key2": []byte("value2-get-many"),
	}
	mockClient := &SimpleMockBatonSessionServiceClient{
		getManyFunc: func(ctx context.Context, req *v1.GetManyRequest) (*v1.GetManyResponse, error) {
			if req.SyncId == "test-sync-id-get-many" {
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

	values, err := cache.GetMany(ctx, []string{"key1", "key2"}, sessions.WithSyncID("test-sync-id-get-many"))
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(values))
	}
	if string(values["key1"]) != "value1-get-many" {
		t.Fatalf("Expected value 'value1-get-many' for key1, got '%s'", string(values["key1"]))
	}
	if string(values["key2"]) != "value2-get-many" {
		t.Fatalf("Expected value 'value2-get-many' for key2, got '%s'", string(values["key2"]))
	}
}

func TestGRPCSessionCache_GetMany_Pagination(t *testing.T) {
	syncID := "test-sync-id-get-many-pagination"
	callCount := 0
	mockClient := &SimpleMockBatonSessionServiceClient{
		getManyFunc: func(ctx context.Context, req *v1.GetManyRequest) (*v1.GetManyResponse, error) {
			callCount++
			if req.SyncId != syncID {
				return nil, fmt.Errorf("unexpected sync id: %s", req.SyncId)
			}
			// Since we're only requesting 2 keys, they fit in a single chunk
			// Return both values in a single response
			return &v1.GetManyResponse{
				Items: []*v1.GetManyItem{
					{Key: "key1", Value: []byte("value1-get-many-pagination")},
					{Key: "key2", Value: []byte("value2-get-many-pagination")},
				},
			}, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()

	values, err := cache.GetMany(ctx, []string{"key1", "key2"}, sessions.WithSyncID(syncID))
	if err != nil {
		t.Fatalf("GetMany failed: %v", err)
	}
	// With only 2 keys, they fit in a single chunk, so only 1 call is made
	if callCount != 1 {
		t.Fatalf("expected 1 call, got %d", callCount)
	}
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(values))
	}
	if string(values["key1"]) != "value1-get-many-pagination" || string(values["key2"]) != "value2-get-many-pagination" {
		t.Fatalf("unexpected values: %+v", values)
	}
}

func TestGRPCSessionCache_SetMany(t *testing.T) {
	values := map[string][]byte{
		"key1": []byte("value1-set-many"),
		"key2": []byte("value2-set-many"),
	}

	mockClient := &SimpleMockBatonSessionServiceClient{
		setManyFunc: func(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error) {
			if req.SyncId == "test-sync-id-set-many" && len(req.Values) == 2 {
				return &v1.SetManyResponse{}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()

	err := cache.SetMany(ctx, values, sessions.WithSyncID("test-sync-id-set-many"))
	if err != nil {
		t.Fatalf("SetMany failed: %v", err)
	}
}

func TestGRPCSessionCache_Delete(t *testing.T) {
	mockClient := &SimpleMockBatonSessionServiceClient{
		deleteFunc: func(ctx context.Context, req *v1.DeleteRequest) (*v1.DeleteResponse, error) {
			if req.Key == "test-key-delete" && req.SyncId == "test-sync-id-delete" {
				return &v1.DeleteResponse{}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()

	err := cache.Delete(ctx, "test-key-delete", sessions.WithSyncID("test-sync-id-delete"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func TestGRPCSessionCache_Clear(t *testing.T) {
	mockClient := &SimpleMockBatonSessionServiceClient{
		clearFunc: func(ctx context.Context, req *v1.ClearRequest) (*v1.ClearResponse, error) {
			if req.SyncId == "test-sync-id-clear" {
				return &v1.ClearResponse{}, nil
			}
			return nil, fmt.Errorf("unexpected request")
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()

	err := cache.Clear(ctx, sessions.WithSyncID("test-sync-id-clear"))
	if err != nil {
		t.Fatalf("Clear failed: %v", err)
	}
}

func TestGRPCSessionCache_GetAll(t *testing.T) {
	expectedValues := map[string][]byte{
		"key1": []byte("value1-get-all"),
		"key2": []byte("value2-get-all"),
	}
	mockClient := &SimpleMockBatonSessionServiceClient{
		getAllFunc: func(ctx context.Context, req *v1.GetAllRequest) (*v1.GetAllResponse, error) {
			if req.SyncId == "test-sync-id-get-all" {
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

	values, err := cache.GetAll(ctx, sessions.WithSyncID("test-sync-id-get-all"))
	if err != nil {
		t.Fatalf("GetAll failed: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(values))
	}
	if string(values["key1"]) != "value1-get-all" {
		t.Fatalf("Expected value 'value1-get-all' for key1, got '%s'", string(values["key1"]))
	}
	if string(values["key2"]) != "value2-get-all" {
		t.Fatalf("Expected value 'value2-get-all' for key2, got '%s'", string(values["key2"]))
	}
}

func TestGRPCSessionCache_GetAll_Pagination(t *testing.T) {
	syncID := "test-sync-id-get-all-pagination"
	callCount := 0
	mockClient := &SimpleMockBatonSessionServiceClient{
		getAllFunc: func(ctx context.Context, req *v1.GetAllRequest) (*v1.GetAllResponse, error) {
			callCount++
			if req.SyncId != syncID {
				return nil, fmt.Errorf("unexpected sync id: %s", req.SyncId)
			}
			if req.PageToken == "" {
				return &v1.GetAllResponse{
					Items: []*v1.GetAllItem{
						{Key: "a", Value: []byte("1")},
						{Key: "b", Value: []byte("2")},
					},
					PageToken: "p2",
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

	values, err := cache.GetAll(ctx, sessions.WithSyncID(syncID))
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

func TestGRPCSessionCache_GetMany_Chunking(t *testing.T) {
	syncID := "test-sync-id-get-many-chunking"
	t.Run("exactly maxKeysPerRequest keys", func(t *testing.T) {
		callCount := 0
		mockClient := &SimpleMockBatonSessionServiceClient{
			getManyFunc: func(ctx context.Context, req *v1.GetManyRequest) (*v1.GetManyResponse, error) {
				callCount++
				if req.SyncId != syncID {
					return nil, fmt.Errorf("unexpected sync id: %s", req.SyncId)
				}
				if len(req.Keys) != maxKeysPerRequest {
					return nil, fmt.Errorf("expected %d keys, got %d", maxKeysPerRequest, len(req.Keys))
				}
				return &v1.GetManyResponse{
					Items: []*v1.GetManyItem{},
				}, nil
			},
		}

		cache := &GRPCSessionCache{client: mockClient}
		ctx := context.Background()

		// Create exactly maxKeysPerRequest keys
		keys := make([]string, maxKeysPerRequest)
		for i := range maxKeysPerRequest {
			keys[i] = fmt.Sprintf("key-%d", i)
		}

		_, err := cache.GetMany(ctx, keys, sessions.WithSyncID(syncID))
		if err != nil {
			t.Fatalf("GetMany failed: %v", err)
		}

		if callCount != 1 {
			t.Fatalf("Expected 1 call, got %d", callCount)
		}
	})

	t.Run("more than maxKeysPerRequest keys", func(t *testing.T) {
		callCount := 0
		mockClient := &SimpleMockBatonSessionServiceClient{
			getManyFunc: func(ctx context.Context, req *v1.GetManyRequest) (*v1.GetManyResponse, error) {
				callCount++
				if req.SyncId != syncID {
					return nil, fmt.Errorf("unexpected sync id: %s", req.SyncId)
				}
				if len(req.Keys) > maxKeysPerRequest {
					return nil, fmt.Errorf("expected at most %d keys, got %d", maxKeysPerRequest, len(req.Keys))
				}
				return &v1.GetManyResponse{
					Items: []*v1.GetManyItem{},
				}, nil
			},
		}

		cache := &GRPCSessionCache{client: mockClient}
		ctx := context.Background()

		// Create more than maxKeysPerRequest keys
		keys := make([]string, maxKeysPerRequest+50)
		for i := range maxKeysPerRequest + 50 {
			keys[i] = fmt.Sprintf("key-%d", i)
		}

		_, err := cache.GetMany(ctx, keys, sessions.WithSyncID(syncID))
		if err != nil {
			t.Fatalf("GetMany failed: %v", err)
		}

		expectedCalls := 2 // First chunk: 200 keys, second chunk: 50 keys
		if callCount != expectedCalls {
			t.Fatalf("Expected %d calls, got %d", expectedCalls, callCount)
		}
	})

	t.Run("large number of keys requiring multiple chunks", func(t *testing.T) {
		callCount := 0
		mockClient := &SimpleMockBatonSessionServiceClient{
			getManyFunc: func(ctx context.Context, req *v1.GetManyRequest) (*v1.GetManyResponse, error) {
				callCount++
				if req.SyncId != syncID {
					return nil, fmt.Errorf("unexpected sync id: %s", req.SyncId)
				}
				if len(req.Keys) > maxKeysPerRequest {
					return nil, fmt.Errorf("expected at most %d keys, got %d", maxKeysPerRequest, len(req.Keys))
				}
				return &v1.GetManyResponse{
					Items: []*v1.GetManyItem{},
				}, nil
			},
		}

		cache := &GRPCSessionCache{client: mockClient}
		ctx := context.Background()

		// Create 1000 keys (should require 5 chunks of 200 each)
		keys := make([]string, 1000)
		for i := range 1000 {
			keys[i] = fmt.Sprintf("key-%d", i)
		}

		_, err := cache.GetMany(ctx, keys, sessions.WithSyncID(syncID))
		if err != nil {
			t.Fatalf("GetMany failed: %v", err)
		}

		expectedCalls := 5 // 1000 / 200 = 5 chunks
		if callCount != expectedCalls {
			t.Fatalf("Expected %d calls, got %d", expectedCalls, callCount)
		}
	})

	t.Run("chunking preserves key order and values", func(t *testing.T) {
		callCount := 0
		var allRequestedKeys []string
		mockClient := &SimpleMockBatonSessionServiceClient{
			getManyFunc: func(ctx context.Context, req *v1.GetManyRequest) (*v1.GetManyResponse, error) {
				callCount++
				allRequestedKeys = append(allRequestedKeys, req.Keys...)

				// Return some mock values
				items := make([]*v1.GetManyItem, len(req.Keys))
				for i, key := range req.Keys {
					items[i] = &v1.GetManyItem{
						Key:   key,
						Value: []byte(fmt.Sprintf("value-for-%s", key)),
					}
				}
				return &v1.GetManyResponse{
					Items: items,
				}, nil
			},
		}

		cache := &GRPCSessionCache{client: mockClient}
		ctx := context.Background()

		// Create 300 keys
		keys := make([]string, 300)
		for i := range 300 {
			keys[i] = fmt.Sprintf("key-%d", i)
		}

		results, err := cache.GetMany(ctx, keys, sessions.WithSyncID(syncID))
		if err != nil {
			t.Fatalf("GetMany failed: %v", err)
		}

		expectedCalls := 2 // 300 / 200 = 2 chunks (200 + 100)
		if callCount != expectedCalls {
			t.Fatalf("Expected %d calls, got %d", expectedCalls, callCount)
		}

		// Verify all keys were requested
		if len(allRequestedKeys) != 300 {
			t.Fatalf("Expected 300 keys to be requested, got %d", len(allRequestedKeys))
		}

		// Verify all results are returned
		if len(results) != 300 {
			t.Fatalf("Expected 300 results, got %d", len(results))
		}

		// Verify some specific values
		if string(results["key-0"]) != "value-for-key-0" {
			t.Fatalf("Expected 'value-for-key-0', got '%s'", string(results["key-0"]))
		}
		if string(results["key-299"]) != "value-for-key-299" {
			t.Fatalf("Expected 'value-for-key-299', got '%s'", string(results["key-299"]))
		}
	})
}

func TestGRPCSessionCache_SetMany_Chunking(t *testing.T) {
	syncID := "test-sync-id-set-many-chunking"
	t.Run("exactly maxKeysPerRequest values", func(t *testing.T) {
		callCount := 0
		mockClient := &SimpleMockBatonSessionServiceClient{
			setManyFunc: func(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error) {
				callCount++
				if req.SyncId != syncID {
					return nil, fmt.Errorf("unexpected sync id: %s", req.SyncId)
				}
				if len(req.Values) != maxKeysPerRequest {
					return nil, fmt.Errorf("expected %d values, got %d", maxKeysPerRequest, len(req.Values))
				}
				return &v1.SetManyResponse{}, nil
			},
		}

		cache := &GRPCSessionCache{client: mockClient}
		ctx := context.Background()

		// Create exactly maxKeysPerRequest values
		values := make(map[string][]byte, maxKeysPerRequest)
		for i := range maxKeysPerRequest {
			values[fmt.Sprintf("key-%d", i)] = []byte(fmt.Sprintf("value-%d", i))
		}

		err := cache.SetMany(ctx, values, sessions.WithSyncID(syncID))
		if err != nil {
			t.Fatalf("SetMany failed: %v", err)
		}

		if callCount != 1 {
			t.Fatalf("Expected 1 call, got %d", callCount)
		}
	})

	t.Run("more than maxKeysPerRequest values", func(t *testing.T) {
		callCount := 0
		mockClient := &SimpleMockBatonSessionServiceClient{
			setManyFunc: func(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error) {
				callCount++
				if req.SyncId != syncID {
					return nil, fmt.Errorf("unexpected sync id: %s", req.SyncId)
				}
				if len(req.Values) > maxKeysPerRequest {
					return nil, fmt.Errorf("expected at most %d values, got %d", maxKeysPerRequest, len(req.Values))
				}
				return &v1.SetManyResponse{}, nil
			},
		}

		cache := &GRPCSessionCache{client: mockClient}
		ctx := context.Background()

		// Create more than maxKeysPerRequest values
		values := make(map[string][]byte, maxKeysPerRequest+50)
		for i := range maxKeysPerRequest + 50 {
			values[fmt.Sprintf("key-%d", i)] = []byte(fmt.Sprintf("value-%d", i))
		}

		err := cache.SetMany(ctx, values, sessions.WithSyncID(syncID))
		if err != nil {
			t.Fatalf("SetMany failed: %v", err)
		}

		expectedCalls := 2 // First chunk: 200 values, second chunk: 50 values
		if callCount != expectedCalls {
			t.Fatalf("Expected %d calls, got %d", expectedCalls, callCount)
		}
	})

	t.Run("large number of values requiring multiple chunks", func(t *testing.T) {
		callCount := 0
		mockClient := &SimpleMockBatonSessionServiceClient{
			setManyFunc: func(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error) {
				callCount++
				if req.SyncId != syncID {
					return nil, fmt.Errorf("unexpected sync id: %s", req.SyncId)
				}
				if len(req.Values) > maxKeysPerRequest {
					return nil, fmt.Errorf("expected at most %d values, got %d", maxKeysPerRequest, len(req.Values))
				}
				return &v1.SetManyResponse{}, nil
			},
		}

		cache := &GRPCSessionCache{client: mockClient}
		ctx := context.Background()

		// Create 1000 values (should require 5 chunks of 200 each)
		values := make(map[string][]byte, 1000)
		for i := range 1000 {
			values[fmt.Sprintf("key-%d", i)] = []byte(fmt.Sprintf("value-%d", i))
		}

		err := cache.SetMany(ctx, values, sessions.WithSyncID(syncID))
		if err != nil {
			t.Fatalf("SetMany failed: %v", err)
		}

		expectedCalls := 5 // 1000 / 200 = 5 chunks
		if callCount != expectedCalls {
			t.Fatalf("Expected %d calls, got %d", expectedCalls, callCount)
		}
	})

	t.Run("chunking preserves key-value pairs", func(t *testing.T) {
		callCount := 0
		var allRequestedValues []map[string][]byte
		mockClient := &SimpleMockBatonSessionServiceClient{
			setManyFunc: func(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error) {
				callCount++
				allRequestedValues = append(allRequestedValues, req.Values)
				return &v1.SetManyResponse{}, nil
			},
		}

		cache := &GRPCSessionCache{client: mockClient}
		ctx := context.Background()

		// Create 300 values
		values := make(map[string][]byte, 300)
		for i := range 300 {
			values[fmt.Sprintf("key-%d", i)] = []byte(fmt.Sprintf("value-%d", i))
		}

		err := cache.SetMany(ctx, values, sessions.WithSyncID(syncID))
		if err != nil {
			t.Fatalf("SetMany failed: %v", err)
		}

		expectedCalls := 2 // 300 / 200 = 2 chunks (200 + 100)
		if callCount != expectedCalls {
			t.Fatalf("Expected %d calls, got %d", expectedCalls, callCount)
		}

		// Verify all values were sent
		totalValues := 0
		for _, chunk := range allRequestedValues {
			totalValues += len(chunk)
		}
		if totalValues != 300 {
			t.Fatalf("Expected 300 values to be sent, got %d", totalValues)
		}

		// Verify specific key-value pairs are preserved
		found := false
		for _, chunk := range allRequestedValues {
			if val, exists := chunk["key-0"]; exists && string(val) == "value-0" {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("Expected to find key-0 with value-0")
		}

		found = false
		for _, chunk := range allRequestedValues {
			if val, exists := chunk["key-299"]; exists && string(val) == "value-299" {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("Expected to find key-299 with value-299")
		}
	})

	t.Run("chunking with prefix", func(t *testing.T) {
		callCount := 0
		var allRequestedValues []map[string][]byte
		mockClient := &SimpleMockBatonSessionServiceClient{
			setManyFunc: func(ctx context.Context, req *v1.SetManyRequest) (*v1.SetManyResponse, error) {
				callCount++
				allRequestedValues = append(allRequestedValues, req.Values)

				// Verify all keys have the expected prefix
				for key := range req.Values {
					if !strings.HasPrefix(key, "test-prefix:") {
						return nil, fmt.Errorf("expected key to have prefix 'test-prefix:', got '%s'", key)
					}
				}
				return &v1.SetManyResponse{}, nil
			},
		}

		cache := &GRPCSessionCache{client: mockClient}
		ctx := context.Background()

		// Create 300 values
		values := make(map[string][]byte, 300)
		for i := range 300 {
			values[fmt.Sprintf("key-%d", i)] = []byte(fmt.Sprintf("value-%d", i))
		}

		err := cache.SetMany(ctx, values, sessions.WithPrefix("test-prefix"), sessions.WithSyncID(syncID))
		if err != nil {
			t.Fatalf("SetMany failed: %v", err)
		}

		expectedCalls := 2 // 300 / 200 = 2 chunks (200 + 100)
		if callCount != expectedCalls {
			t.Fatalf("Expected %d calls, got %d", expectedCalls, callCount)
		}

		// Verify all values were sent with correct prefix
		totalValues := 0
		for _, chunk := range allRequestedValues {
			totalValues += len(chunk)
		}
		if totalValues != 300 {
			t.Fatalf("Expected 300 values to be sent, got %d", totalValues)
		}
	})
}
