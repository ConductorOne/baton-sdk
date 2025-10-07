//go:build baton_lambda_support

package session

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/go-jose/go-jose/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// SimpleMockBatonSessionServiceClient is a simple mock implementation for testing
type SimpleMockBatonSessionServiceClient struct {
	getFunc     func(ctx context.Context, req *v1.GetRequest) (*v1.GetResponse, error)
	getManyFunc func(ctx context.Context, req *v1.GetManyRequest) (v1.BatonSessionService_GetManyClient, error)
	getAllFunc  func(ctx context.Context, req *v1.GetAllRequest) (v1.BatonSessionService_GetAllClient, error)
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

func (m *SimpleMockBatonSessionServiceClient) GetMany(ctx context.Context, in *v1.GetManyRequest, opts ...grpc.CallOption) (v1.BatonSessionService_GetManyClient, error) {
	if m.getManyFunc != nil {
		return m.getManyFunc(ctx, in)
	}
	return nil, fmt.Errorf("GetMany not implemented in mock")
}

func (m *SimpleMockBatonSessionServiceClient) GetAll(ctx context.Context, in *v1.GetAllRequest, opts ...grpc.CallOption) (v1.BatonSessionService_GetAllClient, error) {
	if m.getAllFunc != nil {
		return m.getAllFunc(ctx, in)
	}
	return nil, fmt.Errorf("GetAll not implemented in mock")
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

// SimpleMockBatonSessionServiceGetManyClient is a mock implementation of the GetMany stream client
type SimpleMockBatonSessionServiceGetManyClient struct {
	values map[string][]byte
	index  int
	keys   []string
}

func (m *SimpleMockBatonSessionServiceGetManyClient) Recv() (*v1.GetManyResponse, error) {
	if m.keys == nil {
		// Initialize keys slice
		m.keys = make([]string, 0, len(m.values))
		for key := range m.values {
			m.keys = append(m.keys, key)
		}
	}

	if m.index >= len(m.keys) {
		return nil, io.EOF
	}

	key := m.keys[m.index]
	m.index++

	return &v1.GetManyResponse{
		Key:   key,
		Value: m.values[key],
	}, nil
}

func (m *SimpleMockBatonSessionServiceGetManyClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *SimpleMockBatonSessionServiceGetManyClient) Trailer() metadata.MD {
	return nil
}

func (m *SimpleMockBatonSessionServiceGetManyClient) CloseSend() error {
	return nil
}

func (m *SimpleMockBatonSessionServiceGetManyClient) Context() context.Context {
	return context.Background()
}

func (m *SimpleMockBatonSessionServiceGetManyClient) SendMsg(interface{}) error {
	return nil
}

func (m *SimpleMockBatonSessionServiceGetManyClient) RecvMsg(interface{}) error {
	return nil
}

// SimpleMockBatonSessionServiceGetAllClient is a mock implementation of the GetAll stream client
type SimpleMockBatonSessionServiceGetAllClient struct {
	values map[string][]byte
	index  int
	keys   []string
}

func (m *SimpleMockBatonSessionServiceGetAllClient) Recv() (*v1.GetAllResponse, error) {
	if m.keys == nil {
		// Initialize keys slice
		m.keys = make([]string, 0, len(m.values))
		for key := range m.values {
			m.keys = append(m.keys, key)
		}
	}

	if m.index >= len(m.keys) {
		return nil, io.EOF
	}

	key := m.keys[m.index]
	m.index++

	return &v1.GetAllResponse{
		Key:   key,
		Value: m.values[key],
	}, nil
}

func (m *SimpleMockBatonSessionServiceGetAllClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *SimpleMockBatonSessionServiceGetAllClient) Trailer() metadata.MD {
	return nil
}

func (m *SimpleMockBatonSessionServiceGetAllClient) CloseSend() error {
	return nil
}

func (m *SimpleMockBatonSessionServiceGetAllClient) Context() context.Context {
	return context.Background()
}

func (m *SimpleMockBatonSessionServiceGetAllClient) SendMsg(interface{}) error {
	return nil
}

func (m *SimpleMockBatonSessionServiceGetAllClient) RecvMsg(interface{}) error {
	return nil
}

// PagedGetAllClient simulates a paginated GetAll stream
// Each response in the sequence is returned in order, then io.EOF.
type PagedGetAllClient struct {
	responses []*v1.GetAllResponse
	index     int
}

func (p *PagedGetAllClient) Recv() (*v1.GetAllResponse, error) {
	if p.index >= len(p.responses) {
		return nil, io.EOF
	}
	r := p.responses[p.index]
	p.index++
	return r, nil
}

func (p *PagedGetAllClient) Header() (metadata.MD, error) { return nil, nil }
func (p *PagedGetAllClient) Trailer() metadata.MD         { return nil }
func (p *PagedGetAllClient) CloseSend() error             { return nil }
func (p *PagedGetAllClient) Context() context.Context     { return context.Background() }
func (p *PagedGetAllClient) SendMsg(interface{}) error    { return nil }
func (p *PagedGetAllClient) RecvMsg(interface{}) error    { return nil }

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
	ctx = context.WithValue(ctx, sessions.SyncIDKey{}, "test-sync-id")

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
	ctx = context.WithValue(ctx, sessions.SyncIDKey{}, "test-sync-id")

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
		getManyFunc: func(ctx context.Context, req *v1.GetManyRequest) (v1.BatonSessionService_GetManyClient, error) {
			if req.SyncId == "test-sync-id" {
				return &SimpleMockBatonSessionServiceGetManyClient{
					values: expectedValues,
				}, nil
			}
			return &SimpleMockBatonSessionServiceGetManyClient{
				values: make(map[string][]byte),
			}, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, sessions.SyncIDKey{}, "test-sync-id")

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
	ctx = context.WithValue(ctx, sessions.SyncIDKey{}, "test-sync-id")

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
	ctx = context.WithValue(ctx, sessions.SyncIDKey{}, "test-sync-id")

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
	ctx = context.WithValue(ctx, sessions.SyncIDKey{}, "test-sync-id")

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
		getAllFunc: func(ctx context.Context, req *v1.GetAllRequest) (v1.BatonSessionService_GetAllClient, error) {
			if req.SyncId == "test-sync-id" {
				return &SimpleMockBatonSessionServiceGetAllClient{
					values: expectedValues,
				}, nil
			}
			return &SimpleMockBatonSessionServiceGetAllClient{
				values: make(map[string][]byte),
			}, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, sessions.SyncIDKey{}, "test-sync-id")

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
		getAllFunc: func(ctx context.Context, req *v1.GetAllRequest) (v1.BatonSessionService_GetAllClient, error) {
			callCount++
			if req.SyncId != "test-sync-id" {
				return nil, fmt.Errorf("unexpected sync id: %s", req.SyncId)
			}
			if req.PageToken == "" {
				return &PagedGetAllClient{responses: []*v1.GetAllResponse{
					{Key: "a", Value: []byte("1")},
					{Key: "b", Value: []byte("2"), NextPageToken: "p2"},
				}}, nil
			}
			if req.PageToken == "p2" {
				return &PagedGetAllClient{responses: []*v1.GetAllResponse{
					{Key: "c", Value: []byte("3")},
				}}, nil
			}
			return &PagedGetAllClient{responses: nil}, nil
		},
	}

	cache := &GRPCSessionCache{client: mockClient}
	ctx := context.Background()
	ctx = context.WithValue(ctx, sessions.SyncIDKey{}, "test-sync-id")

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
	ctx = context.WithValue(ctx, sessions.SyncIDKey{}, "test-sync-id")

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
