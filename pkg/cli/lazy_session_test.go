package cli

import (
	"context"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSessionStore is a simple mock implementation of SessionStore for testing.
type mockSessionStore struct {
	created bool
	data    map[string][]byte
}

func newMockSessionStore() *mockSessionStore {
	return &mockSessionStore{
		created: true,
		data:    make(map[string][]byte),
	}
}

func (m *mockSessionStore) Get(ctx context.Context, key string, opt ...types.SessionOption) ([]byte, bool, error) {
	value, exists := m.data[key]
	return value, exists, nil
}

func (m *mockSessionStore) GetMany(ctx context.Context, keys []string, opt ...types.SessionOption) (map[string][]byte, error) {
	result := make(map[string][]byte)
	for _, key := range keys {
		if value, exists := m.data[key]; exists {
			result[key] = value
		}
	}
	return result, nil
}

func (m *mockSessionStore) Set(ctx context.Context, key string, value []byte, opt ...types.SessionOption) error {
	m.data[key] = value
	return nil
}

func (m *mockSessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...types.SessionOption) error {
	for k, v := range values {
		m.data[k] = v
	}
	return nil
}

func (m *mockSessionStore) Delete(ctx context.Context, key string, opt ...types.SessionOption) error {
	delete(m.data, key)
	return nil
}

func (m *mockSessionStore) Clear(ctx context.Context, opt ...types.SessionOption) error {
	m.data = make(map[string][]byte)
	return nil
}

func (m *mockSessionStore) GetAll(ctx context.Context, opt ...types.SessionOption) (map[string][]byte, error) {
	result := make(map[string][]byte)
	for k, v := range m.data {
		result[k] = v
	}
	return result, nil
}

func (m *mockSessionStore) CloseStore(ctx context.Context) error {
	return nil
}

func TestWithLazySession(t *testing.T) {
	ctx := context.Background()

	// Track if the constructor was called
	constructorCalled := false
	constructor := func(ctx context.Context, opt ...types.SessionConstructorOption) (types.SessionStore, error) {
		constructorCalled = true
		return newMockSessionStore(), nil
	}

	// Create lazy session
	lazyCtx := WithLazySession(ctx, constructor)

	// Constructor should not be called yet
	assert.False(t, constructorCalled, "Constructor should not be called when creating lazy session")

	// Get the session from context
	session, err := GetSessionFromContext(lazyCtx)
	require.NoError(t, err)
	require.NotNil(t, session)

	// Cast to lazySessionStore to verify it's the lazy wrapper
	lazySession, ok := session.(*lazySessionStore)
	require.True(t, ok, "Session should be a lazySessionStore")

	// Constructor should still not be called
	assert.False(t, constructorCalled, "Constructor should not be called yet")

	// Now call a method - this should trigger the constructor
	err = session.Set(ctx, "test-key", []byte("test-value"))
	require.NoError(t, err)

	// Constructor should now be called
	assert.True(t, constructorCalled, "Constructor should be called when first method is invoked")

	// Verify the session works correctly
	value, found, err := session.Get(ctx, "test-key")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("test-value"), value)

	// Verify subsequent calls don't recreate the session
	constructorCalled = false
	err = session.Set(ctx, "another-key", []byte("another-value"))
	require.NoError(t, err)
	assert.False(t, constructorCalled, "Constructor should not be called again")

	// Verify the lazy session's internal session is properly initialized
	assert.NotNil(t, lazySession.session, "Internal session should be initialized")
	assert.NoError(t, lazySession.err, "No error should be stored")
}

func TestWithLazySessionConstructorError(t *testing.T) {
	ctx := context.Background()

	// Constructor that returns an error
	constructor := func(ctx context.Context, opt ...types.SessionConstructorOption) (types.SessionStore, error) {
		return nil, assert.AnError
	}

	// Create lazy session
	lazyCtx := WithLazySession(ctx, constructor)

	// Get the session from context
	session, err := GetSessionFromContext(lazyCtx)
	require.NoError(t, err)
	require.NotNil(t, session)

	// Try to use the session - this should trigger the constructor and return the error
	err = session.Set(ctx, "test-key", []byte("test-value"))
	assert.Error(t, err)
	assert.Equal(t, assert.AnError, err)
}

// Helper function to get session from context (similar to session.GetSession).
func GetSessionFromContext(ctx context.Context) (types.SessionStore, error) {
	if sessionCache, ok := ctx.Value(types.SessionCacheKey{}).(types.SessionStore); ok {
		return sessionCache, nil
	}
	return nil, assert.AnError
}
