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
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
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
				return &v1.GetResponse{Value: expectedValue, Found: true}, nil
			}
			// Return found=false for non-existent keys
			return &v1.GetResponse{Found: false}, nil
		},
	}

	cache := &GRPCSessionStoreClient{client: mockClient}
	ctx := context.Background()

	// Test successful get
	value, found, err := cache.Get(ctx, "test-key", sessions.WithSyncID(syncID))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, expectedValue, value)

	// Test get with non-existent key
	value, found, err = cache.Get(ctx, "non-existent-key", sessions.WithSyncID(syncID))
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, value)
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

	cache := &GRPCSessionStoreClient{client: mockClient}
	ctx := context.Background()

	err := cache.Set(ctx, "test-key-set", []byte("test-value-set"), sessions.WithSyncID("test-sync-id-set"))
	require.NoError(t, err)
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

	cache := &GRPCSessionStoreClient{client: mockClient}
	ctx := context.Background()

	values, err := cache.GetMany(ctx, []string{"key1", "key2"}, sessions.WithSyncID("test-sync-id-get-many"))
	require.NoError(t, err)
	require.Equal(t, 2, len(values))
	require.Equal(t, "value1-get-many", string(values["key1"]))
	require.Equal(t, "value2-get-many", string(values["key2"]))
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

	cache := &GRPCSessionStoreClient{client: mockClient}
	ctx := context.Background()

	values, err := cache.GetMany(ctx, []string{"key1", "key2"}, sessions.WithSyncID(syncID))
	require.NoError(t, err)
	// With only 2 keys, they fit in a single chunk, so only 1 call is made
	require.Equal(t, 1, callCount)
	require.Equal(t, 2, len(values))
	require.Equal(t, "value1-get-many-pagination", string(values["key1"]))
	require.Equal(t, "value2-get-many-pagination", string(values["key2"]))
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

	cache := &GRPCSessionStoreClient{client: mockClient}
	ctx := context.Background()

	err := cache.SetMany(ctx, values, sessions.WithSyncID("test-sync-id-set-many"))
	require.NoError(t, err)
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

	cache := &GRPCSessionStoreClient{client: mockClient}
	ctx := context.Background()

	err := cache.Delete(ctx, "test-key-delete", sessions.WithSyncID("test-sync-id-delete"))
	require.NoError(t, err)
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

	cache := &GRPCSessionStoreClient{client: mockClient}
	ctx := context.Background()

	err := cache.Clear(ctx, sessions.WithSyncID("test-sync-id-clear"))
	require.NoError(t, err)
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

	cache := &GRPCSessionStoreClient{client: mockClient}
	ctx := context.Background()

	values, pageToken, err := cache.GetAll(ctx, "", sessions.WithSyncID("test-sync-id-get-all"))
	require.NoError(t, err)
	require.Equal(t, 2, len(values))
	require.Equal(t, "value1-get-all", string(values["key1"]))
	require.Equal(t, "value2-get-all", string(values["key2"]))
	require.Equal(t, "", pageToken)
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

	cache := &GRPCSessionStoreClient{client: mockClient}
	ctx := context.Background()

	values, pageToken, err := cache.GetAll(ctx, "", sessions.WithSyncID(syncID))
	require.NoError(t, err)
	// GetAll now only makes a single call - pagination is handled by UnrollGetAll
	require.Equal(t, 1, callCount)
	// Only gets items from the first page
	require.Equal(t, 2, len(values))
	require.Equal(t, "1", string(values["a"]))
	require.Equal(t, "2", string(values["b"]))
	// Returns the page token for the next page
	require.Equal(t, "p2", pageToken)
}

func TestNewGRPCSessionCache(t *testing.T) {
	mockClient := &SimpleMockBatonSessionServiceClient{}

	cache, err := NewGRPCSessionStore(context.Background(), mockClient)
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
	t.Skip("Chunking tests removed - chunking is now handled by Unroll variants")
}

func TestGRPCSessionCache_SetMany_Chunking(t *testing.T) {
	t.Skip("Chunking tests removed - chunking is now handled by Unroll variants")
}

// MockSessionStore is a mock implementation for testing unroll variants.
type MockSessionStore struct {
	getFunc     func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error)
	getManyFunc func(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error)
	getAllFunc  func(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error)
	setFunc     func(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error
	setManyFunc func(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error
	deleteFunc  func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error
	clearFunc   func(ctx context.Context, opt ...sessions.SessionStoreOption) error
}

func (m *MockSessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, key, opt...)
	}
	return nil, false, nil
}

func (m *MockSessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
	if m.getManyFunc != nil {
		return m.getManyFunc(ctx, keys, opt...)
	}
	return make(map[string][]byte), nil
}

func (m *MockSessionStore) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	if m.getAllFunc != nil {
		return m.getAllFunc(ctx, pageToken, opt...)
	}
	return make(map[string][]byte), "", nil
}

func (m *MockSessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	if m.setFunc != nil {
		return m.setFunc(ctx, key, value, opt...)
	}
	return nil
}

func (m *MockSessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	if m.setManyFunc != nil {
		return m.setManyFunc(ctx, values, opt...)
	}
	return nil
}

func (m *MockSessionStore) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, key, opt...)
	}
	return nil
}

func (m *MockSessionStore) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	if m.clearFunc != nil {
		return m.clearFunc(ctx, opt...)
	}
	return nil
}

func TestUnrollGetMany(t *testing.T) {
	t.Run("small number of keys - no chunking needed", func(t *testing.T) {
		callCount := 0
		mockStore := &MockSessionStore{
			getManyFunc: func(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
				callCount++
				require.Equal(t, 50, len(keys))
				result := make(map[string][]byte)
				for _, key := range keys {
					result[key] = []byte(fmt.Sprintf("value-%s", key))
				}
				return result, nil
			},
		}

		// Create 50 keys (less than maxKeysPerRequest = 100)
		keys := make([]string, 50)
		for i := range 50 {
			keys[i] = fmt.Sprintf("key-%d", i)
		}

		ctx := context.Background()
		result, err := UnrollGetMany(ctx, mockStore, keys)
		require.NoError(t, err)
		require.Equal(t, 1, callCount)
		require.Equal(t, 50, len(result))

		// Verify some specific values
		require.Equal(t, "value-key-0", string(result["key-0"]))
		require.Equal(t, "value-key-49", string(result["key-49"]))
	})

	t.Run("large number of keys - requires chunking", func(t *testing.T) {
		callCount := 0
		mockStore := &MockSessionStore{
			getManyFunc: func(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
				callCount++
				require.LessOrEqual(t, len(keys), 100)
				result := make(map[string][]byte)
				for _, key := range keys {
					result[key] = []byte(fmt.Sprintf("value-%s", key))
				}
				return result, nil
			},
		}

		// Create 250 keys (requires 3 chunks: 100 + 100 + 50)
		keys := make([]string, 250)
		for i := range 250 {
			keys[i] = fmt.Sprintf("key-%d", i)
		}

		ctx := context.Background()
		result, err := UnrollGetMany(ctx, mockStore, keys)
		require.NoError(t, err)
		require.Equal(t, 3, callCount)
		require.Equal(t, 250, len(result))

		// Verify some specific values
		require.Equal(t, "value-key-0", string(result["key-0"]))
		require.Equal(t, "value-key-249", string(result["key-249"]))
	})

	t.Run("error handling", func(t *testing.T) {
		mockStore := &MockSessionStore{
			getManyFunc: func(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
				return nil, fmt.Errorf("mock error")
			},
		}

		keys := []string{"key1", "key2"}
		ctx := context.Background()
		_, err := UnrollGetMany(ctx, mockStore, keys)
		require.Error(t, err)
		require.Equal(t, "mock error", err.Error())
	})

	t.Run("empty keys", func(t *testing.T) {
		callCount := 0
		mockStore := &MockSessionStore{
			getManyFunc: func(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
				callCount++
				return make(map[string][]byte), nil
			},
		}

		ctx := context.Background()
		result, err := UnrollGetMany(ctx, mockStore, []string{})
		require.NoError(t, err)
		require.Equal(t, 0, callCount)
		require.Equal(t, 0, len(result))
	})
}

func TestUnrollSetMany(t *testing.T) {
	t.Run("small number of values - no chunking needed", func(t *testing.T) {
		callCount := 0
		mockStore := &MockSessionStore{
			setManyFunc: func(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
				callCount++
				require.Equal(t, 50, len(values))
				return nil
			},
		}

		// Create 50 values (less than maxKeysPerRequest = 100)
		values := make(map[string][]byte, 50)
		for i := range 50 {
			values[fmt.Sprintf("key-%d", i)] = []byte(fmt.Sprintf("value-%d", i))
		}

		ctx := context.Background()
		err := UnrollSetMany(ctx, mockStore, values)
		require.NoError(t, err)
		require.Equal(t, 1, callCount)
	})

	t.Run("large number of values - requires chunking", func(t *testing.T) {
		callCount := 0
		mockStore := &MockSessionStore{
			setManyFunc: func(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
				callCount++
				require.LessOrEqual(t, len(values), 100)
				return nil
			},
		}

		// Create 250 values (requires 3 chunks: 100 + 100 + 50)
		values := make(map[string][]byte, 250)
		for i := range 250 {
			values[fmt.Sprintf("key-%d", i)] = []byte(fmt.Sprintf("value-%d", i))
		}

		ctx := context.Background()
		err := UnrollSetMany(ctx, mockStore, values)
		require.NoError(t, err)
		require.Equal(t, 3, callCount)
	})

	t.Run("error handling", func(t *testing.T) {
		mockStore := &MockSessionStore{
			setManyFunc: func(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
				return fmt.Errorf("mock error")
			},
		}

		values := map[string][]byte{"key1": []byte("value1")}
		ctx := context.Background()
		err := UnrollSetMany(ctx, mockStore, values)
		require.Error(t, err)
		require.Equal(t, "mock error", err.Error())
	})

	t.Run("empty values", func(t *testing.T) {
		callCount := 0
		mockStore := &MockSessionStore{
			setManyFunc: func(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
				callCount++
				return nil
			},
		}

		ctx := context.Background()
		err := UnrollSetMany(ctx, mockStore, make(map[string][]byte))
		require.NoError(t, err)
		require.Equal(t, 0, callCount)
	})
}

func TestUnrollGetAll(t *testing.T) {
	t.Run("single page - no pagination", func(t *testing.T) {
		callCount := 0
		mockStore := &MockSessionStore{
			getAllFunc: func(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
				callCount++
				if pageToken != "" {
					require.Equal(t, "", pageToken)
				}
				return map[string][]byte{
					"key1": []byte("value1"),
					"key2": []byte("value2"),
				}, "", nil // Empty page token means no more pages
			},
		}

		ctx := context.Background()
		result, err := UnrollGetAll(ctx, mockStore)
		require.NoError(t, err)
		require.Equal(t, 1, callCount)
		require.Equal(t, 2, len(result))

		require.Equal(t, "value1", string(result["key1"]))
		require.Equal(t, "value2", string(result["key2"]))
	})

	t.Run("multiple pages - pagination", func(t *testing.T) {
		callCount := 0
		mockStore := &MockSessionStore{
			getAllFunc: func(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
				callCount++
				switch pageToken {
				case "":
					// First page
					return map[string][]byte{
						"key1": []byte("value1"),
						"key2": []byte("value2"),
					}, "page2", nil
				case "page2":
					// Second page
					return map[string][]byte{
						"key3": []byte("value3"),
					}, "", nil // Empty page token means no more pages
				default:
					return nil, "", fmt.Errorf("unexpected page token: %s", pageToken)
				}
			},
		}

		ctx := context.Background()
		result, err := UnrollGetAll(ctx, mockStore)
		require.NoError(t, err)
		require.Equal(t, 2, callCount)
		require.Equal(t, 3, len(result))
		require.Equal(t, "value1", string(result["key1"]))
		require.Equal(t, "value2", string(result["key2"]))
		require.Equal(t, "value3", string(result["key3"]))
	})

	t.Run("error handling", func(t *testing.T) {
		mockStore := &MockSessionStore{
			getAllFunc: func(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
				return nil, "", fmt.Errorf("mock error")
			},
		}

		ctx := context.Background()
		_, err := UnrollGetAll(ctx, mockStore)
		require.Error(t, err)
		require.Equal(t, "mock error", err.Error())
	})

	t.Run("infinite loop protection - same page token", func(t *testing.T) {
		callCount := 0
		mockStore := &MockSessionStore{
			getAllFunc: func(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
				callCount++
				require.LessOrEqual(t, callCount, 10)
				return map[string][]byte{
					"key1": []byte("value1"),
				}, "same-token", nil // Same token returned
			},
		}

		ctx := context.Background()
		_, err := UnrollGetAll(ctx, mockStore)
		require.Error(t, err)
		require.Contains(t, err.Error(), "page token is the same")
	})

	t.Run("empty result", func(t *testing.T) {
		callCount := 0
		mockStore := &MockSessionStore{
			getAllFunc: func(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
				callCount++
				return make(map[string][]byte), "", nil
			},
		}

		ctx := context.Background()
		result, err := UnrollGetAll(ctx, mockStore)
		require.NoError(t, err)
		require.Equal(t, 1, callCount)
		require.Equal(t, 0, len(result))
	})
}
