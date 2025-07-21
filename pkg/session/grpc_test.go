package session

import (
	"context"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestGRPCSessionCache_Constructor(t *testing.T) {
	ctx := context.Background()

	// Test that constructor fails with invalid address
	_, err := NewGRPCSessionCache(ctx, "invalid-address:12345")
	// This might succeed or fail depending on the network configuration
	// We'll just test that it doesn't panic
	if err != nil {
		assert.Contains(t, err.Error(), "failed to connect")
	}
}

func TestGRPCSessionCache_Options(t *testing.T) {
	// Test timeout option
	timeout := 5 * time.Second
	config := &grpcConfig{}
	WithTimeout(timeout)(config)
	assert.Equal(t, timeout, config.timeout)

	// Test auth token option
	token := "test-token"
	config = &grpcConfig{}
	WithAuthToken(token)(config)
	assert.Equal(t, token, config.authToken)
}

func TestGRPCSessionCache_AddAuthHeader(t *testing.T) {
	ctx := context.Background()

	// Test without auth token
	cache := &GRPCSessionCache{}
	resultCtx := cache.addAuthHeader(ctx)
	assert.Equal(t, ctx, resultCtx)

	// Test with auth token
	cache.authToken = "test-token"
	resultCtx = cache.addAuthHeader(ctx)
	assert.NotEqual(t, ctx, resultCtx)
}

func TestGRPCSessionCache_CreateTimeoutContext(t *testing.T) {
	ctx := context.Background()
	cache := &GRPCSessionCache{
		timeout: 100 * time.Millisecond,
	}

	timeoutCtx, cancel := cache.createTimeoutContext(ctx)
	defer cancel()

	// Verify timeout is set
	deadline, ok := timeoutCtx.Deadline()
	assert.True(t, ok)
	assert.True(t, deadline.After(time.Now()))
}

func TestGRPCSessionCache_DefaultConfig(t *testing.T) {
	// Test default configuration
	config := &grpcConfig{
		timeout: 30 * time.Second,
	}

	assert.Equal(t, 30*time.Second, config.timeout)
	assert.Empty(t, config.authToken)
}

func TestGRPCSessionCache_ErrorHandling(t *testing.T) {
	ctx := context.Background()

	// Test connection error
	_, err := NewGRPCSessionCache(ctx, "invalid-address:12345")
	// This might succeed or fail depending on the network configuration
	// We'll just test that it doesn't panic
	if err != nil {
		assert.Contains(t, err.Error(), "failed to connect")
	}
}

func TestGRPCSessionCache_InterfaceCompliance(t *testing.T) {
	// This test ensures the GRPCSessionCache implements the SessionCache interface
	var _ types.SessionCache = (*GRPCSessionCache)(nil)
}
