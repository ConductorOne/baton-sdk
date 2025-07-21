package session

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMemorySessionCache(t *testing.T) {
	ctx := context.Background()

	// Test default constructor
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	require.NotNil(t, cache)

	// Test with custom TTL
	cacheWithTTL, err := NewMemorySessionCacheWithTTL(ctx, 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, cacheWithTTL)

	// Test with options
	cacheWithOptions, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	require.NotNil(t, cacheWithOptions)
}

func TestMemorySessionCache_Get(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)

	// Test getting non-existent key
	value, found, err := cache.Get(ctx, "test-namespace", "non-existent-key")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, value)

	// Test getting from non-existent namespace
	value, found, err = cache.Get(ctx, "non-existent-namespace", "key")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, value)
}

func TestMemorySessionCache_Set(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)

	// Test setting a value
	testData := []byte("test-value")
	err = cache.Set(ctx, "test-namespace", "test-key", testData)
	require.NoError(t, err)

	// Test retrieving the set value
	value, found, err := cache.Get(ctx, "test-namespace", "test-key")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, testData, value)

	// Test setting empty value
	err = cache.Set(ctx, "test-namespace", "empty-key", []byte{})
	require.NoError(t, err)

	value, found, err = cache.Get(ctx, "test-namespace", "empty-key")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte{}, value)
}

func TestMemorySessionCache_Delete(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)

	// Set a value first
	testData := []byte("test-value")
	err = cache.Set(ctx, "test-namespace", "test-key", testData)
	require.NoError(t, err)

	// Verify it exists
	value, found, err := cache.Get(ctx, "test-namespace", "test-key")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, testData, value)

	// Delete the value
	err = cache.Delete(ctx, "test-namespace", "test-key")
	require.NoError(t, err)

	// Verify it's gone
	value, found, err = cache.Get(ctx, "test-namespace", "test-key")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, value)

	// Test deleting non-existent key (should not error)
	err = cache.Delete(ctx, "test-namespace", "non-existent-key")
	require.NoError(t, err)

	// Test deleting from non-existent namespace (should not error)
	err = cache.Delete(ctx, "non-existent-namespace", "key")
	require.NoError(t, err)
}

func TestMemorySessionCache_Clear(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)

	// Set multiple values in different namespaces
	err = cache.Set(ctx, "namespace1", "key1", []byte("value1"))
	require.NoError(t, err)
	err = cache.Set(ctx, "namespace1", "key2", []byte("value2"))
	require.NoError(t, err)
	err = cache.Set(ctx, "namespace2", "key1", []byte("value3"))
	require.NoError(t, err)

	// Verify values exist
	_, found, err := cache.Get(ctx, "namespace1", "key1")
	require.NoError(t, err)
	assert.True(t, found)

	_, found, err = cache.Get(ctx, "namespace2", "key1")
	require.NoError(t, err)
	assert.True(t, found)

	// Clear all
	err = cache.Clear(ctx)
	require.NoError(t, err)

	// Verify all values are gone
	_, found, err = cache.Get(ctx, "namespace1", "key1")
	require.NoError(t, err)
	assert.False(t, found)

	_, found, err = cache.Get(ctx, "namespace1", "key2")
	require.NoError(t, err)
	assert.False(t, found)

	_, found, err = cache.Get(ctx, "namespace2", "key1")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestMemorySessionCache_GetAll(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)

	// Test empty namespace
	values, err := cache.GetAll(ctx, "empty-namespace")
	require.NoError(t, err)
	assert.Empty(t, values)

	// Set multiple values in a namespace
	err = cache.Set(ctx, "test-namespace", "key1", []byte("value1"))
	require.NoError(t, err)
	err = cache.Set(ctx, "test-namespace", "key2", []byte("value2"))
	require.NoError(t, err)
	err = cache.Set(ctx, "test-namespace", "key3", []byte("value3"))
	require.NoError(t, err)

	// Get all values from the namespace
	values, err = cache.GetAll(ctx, "test-namespace")
	require.NoError(t, err)
	assert.Len(t, values, 3)
	assert.Equal(t, []byte("value1"), values["key1"])
	assert.Equal(t, []byte("value2"), values["key2"])
	assert.Equal(t, []byte("value3"), values["key3"])

	// Test that other namespace is not affected
	otherValues, err := cache.GetAll(ctx, "other-namespace")
	require.NoError(t, err)
	assert.Empty(t, otherValues)
}

func TestMemorySessionCache_GetMany(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)

	// Set multiple values
	err = cache.Set(ctx, "test-namespace", "key1", []byte("value1"))
	require.NoError(t, err)
	err = cache.Set(ctx, "test-namespace", "key2", []byte("value2"))
	require.NoError(t, err)
	err = cache.Set(ctx, "test-namespace", "key3", []byte("value3"))
	require.NoError(t, err)

	// Test getting multiple existing keys
	keys := []string{"key1", "key2", "key3"}
	values, err := cache.GetMany(ctx, "test-namespace", keys)
	require.NoError(t, err)
	assert.Len(t, values, 3)
	assert.Equal(t, []byte("value1"), values["key1"])
	assert.Equal(t, []byte("value2"), values["key2"])
	assert.Equal(t, []byte("value3"), values["key3"])

	// Test getting mix of existing and non-existing keys
	keys = []string{"key1", "non-existent", "key3"}
	values, err = cache.GetMany(ctx, "test-namespace", keys)
	require.NoError(t, err)
	assert.Len(t, values, 2)
	assert.Equal(t, []byte("value1"), values["key1"])
	assert.Equal(t, []byte("value3"), values["key3"])
	assert.NotContains(t, values, "non-existent")

	// Test getting from non-existent namespace
	values, err = cache.GetMany(ctx, "non-existent-namespace", keys)
	require.NoError(t, err)
	assert.Empty(t, values)

	// Test getting empty key list
	values, err = cache.GetMany(ctx, "test-namespace", []string{})
	require.NoError(t, err)
	assert.Empty(t, values)
}

func TestMemorySessionCache_SetMany(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)

	// Test setting multiple values
	values := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	err = cache.SetMany(ctx, "test-namespace", values)
	require.NoError(t, err)

	// Verify all values were set
	for key, expectedValue := range values {
		value, found, err := cache.Get(ctx, "test-namespace", key)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, expectedValue, value)
	}

	// Test setting empty map
	err = cache.SetMany(ctx, "test-namespace", map[string][]byte{})
	require.NoError(t, err)

	// Test setting values with empty bytes
	emptyValues := map[string][]byte{
		"empty1": []byte{},
		"empty2": []byte{},
	}
	err = cache.SetMany(ctx, "test-namespace", emptyValues)
	require.NoError(t, err)

	for key := range emptyValues {
		value, found, err := cache.Get(ctx, "test-namespace", key)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte{}, value)
	}
}

func TestMemorySessionCache_Concurrency(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)

	// Test concurrent reads and writes
	done := make(chan bool, 10)
	for i := 0; i < 5; i++ {
		go func(id int) {
			defer func() { done <- true }()
			key := fmt.Sprintf("key-%d", id)
			value := []byte(fmt.Sprintf("value-%d", id))

			err := cache.Set(ctx, "concurrent-namespace", key, value)
			require.NoError(t, err)

			retrieved, found, err := cache.Get(ctx, "concurrent-namespace", key)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, value, retrieved)
		}(i)
	}

	for i := 0; i < 5; i++ {
		<-done
	}
}

func TestMemorySessionCache_Close(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)

	// Set some values
	err = cache.Set(ctx, "test-namespace", "key1", []byte("value1"))
	require.NoError(t, err)

	// Close the cache
	err = cache.Close()
	require.NoError(t, err)

	// Verify cache is cleared after close
	values, err := cache.GetAll(ctx, "test-namespace")
	require.NoError(t, err)
	assert.Empty(t, values)
}

func TestMemorySessionCache_TTL(t *testing.T) {
	ctx := context.Background()

	// Create cache with short TTL for testing
	cache, err := NewMemorySessionCacheWithTTL(ctx, 100*time.Millisecond)
	require.NoError(t, err)

	// Set a value
	err = cache.Set(ctx, "test-namespace", "key1", []byte("value1"))
	require.NoError(t, err)

	// Verify it exists immediately
	value, found, err := cache.Get(ctx, "test-namespace", "key1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, []byte("value1"), value)

	// Wait for TTL to expire
	time.Sleep(200 * time.Millisecond)

	// Verify it's expired
	value, found, err = cache.Get(ctx, "test-namespace", "key1")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Nil(t, value)
}

func TestMemorySessionCache_NamespaceIsolation(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)

	// Set same key in different namespaces
	err = cache.Set(ctx, "namespace1", "same-key", []byte("value1"))
	require.NoError(t, err)
	err = cache.Set(ctx, "namespace2", "same-key", []byte("value2"))
	require.NoError(t, err)

	// Verify namespaces are isolated
	value1, found1, err := cache.Get(ctx, "namespace1", "same-key")
	require.NoError(t, err)
	assert.True(t, found1)
	assert.Equal(t, []byte("value1"), value1)

	value2, found2, err := cache.Get(ctx, "namespace2", "same-key")
	require.NoError(t, err)
	assert.True(t, found2)
	assert.Equal(t, []byte("value2"), value2)

	// Verify GetAll respects namespace boundaries
	values1, err := cache.GetAll(ctx, "namespace1")
	require.NoError(t, err)
	assert.Len(t, values1, 1)
	assert.Equal(t, []byte("value1"), values1["same-key"])

	values2, err := cache.GetAll(ctx, "namespace2")
	require.NoError(t, err)
	assert.Len(t, values2, 1)
	assert.Equal(t, []byte("value2"), values2["same-key"])
}
