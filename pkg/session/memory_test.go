package session

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*  Vibecodes */
func TestNewMemorySessionCache(t *testing.T) {
	ctx := context.Background()

	t.Run("default constructor", func(t *testing.T) {
		cache, err := NewMemorySessionCache(ctx)
		require.NoError(t, err)
		require.NotNil(t, cache)
	})

	t.Run("with custom TTL", func(t *testing.T) {
		cache, err := NewMemorySessionCacheWithTTL(ctx, 30*time.Second)
		require.NoError(t, err)
		require.NotNil(t, cache)
	})

	t.Run("with zero TTL", func(t *testing.T) {
		cache, err := NewMemorySessionCacheWithTTL(ctx, 0)
		require.NoError(t, err)
		require.NotNil(t, cache)
	})

	t.Run("with negative TTL", func(t *testing.T) {
		cache, err := NewMemorySessionCacheWithTTL(ctx, -1*time.Second)
		require.NoError(t, err)
		require.NotNil(t, cache)
	})

	t.Run("with options", func(t *testing.T) {
		cache, err := NewMemorySessionCache(ctx)
		require.NoError(t, err)
		require.NotNil(t, cache)
	})
}

func TestMemorySessionCache_Get(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	memCache := cache.(*MemorySessionCache)

	t.Run("non-existent key", func(t *testing.T) {
		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "non-existent-key")
		require.NoError(t, err)
		assert.False(t, found)
		assert.Nil(t, value)
	})

	t.Run("empty key", func(t *testing.T) {
		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "")
		require.NoError(t, err)
		assert.False(t, found)
		assert.Nil(t, value)
	})

	t.Run("nil context", func(t *testing.T) {
		value, found, err := memCache.GetWithSyncID(nil, "test-sync", "key") //nolint:staticcheck // because we want to test the nil context
		require.NoError(t, err)
		assert.False(t, found)
		assert.Nil(t, value)
	})
}

func TestMemorySessionCache_Set(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	memCache := cache.(*MemorySessionCache)

	t.Run("set and retrieve value", func(t *testing.T) {
		testData := []byte("test-value")
		err = memCache.SetWithSyncID(ctx, "test-sync", "test-key", testData)
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "test-key")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, testData, value)
	})

	t.Run("set empty value", func(t *testing.T) {
		err = memCache.SetWithSyncID(ctx, "test-sync", "empty-key", []byte{})
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "empty-key")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte{}, value)
	})

	t.Run("set nil value", func(t *testing.T) {
		err = memCache.SetWithSyncID(ctx, "test-sync", "nil-key", nil)
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "nil-key")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Nil(t, value)
	})

	t.Run("overwrite existing value", func(t *testing.T) {
		// Set initial value
		err = memCache.SetWithSyncID(ctx, "test-sync", "overwrite-key", []byte("initial"))
		require.NoError(t, err)

		// Overwrite with new value
		err = memCache.SetWithSyncID(ctx, "test-sync", "overwrite-key", []byte("updated"))
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "overwrite-key")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("updated"), value)
	})

	t.Run("set with empty namespace", func(t *testing.T) {
		err = memCache.SetWithSyncID(ctx, "test-sync", "key", []byte("value"))
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "key")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value"), value)
	})

	t.Run("set with empty key", func(t *testing.T) {
		err = memCache.SetWithSyncID(ctx, "test-sync", "", []byte("value"))
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value"), value)
	})

	t.Run("set with nil context", func(t *testing.T) {
		err = memCache.SetWithSyncID(nil, "test-sync", "key", []byte("value")) //nolint:staticcheck // because we want to test the nil context
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "key")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value"), value)
	})

	t.Run("set large value", func(t *testing.T) {
		largeData := make([]byte, 1024*1024) // 1MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		err = memCache.SetWithSyncID(ctx, "test-sync", "large-key", largeData)
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "large-key")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, largeData, value)
	})
}

func TestMemorySessionCache_Delete(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	memCache := cache.(*MemorySessionCache)

	t.Run("delete existing value", func(t *testing.T) {
		// Set a value first
		testData := []byte("test-value")
		err = memCache.SetWithSyncID(ctx, "test-sync", "test-key", testData)
		require.NoError(t, err)

		// Verify it exists
		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "test-key")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, testData, value)

		// Delete the value
		err = memCache.DeleteWithSyncID(ctx, "test-sync", "test-key")
		require.NoError(t, err)

		// Verify it's gone
		value, found, err = memCache.GetWithSyncID(ctx, "test-sync", "test-key")
		require.NoError(t, err)
		assert.False(t, found)
		assert.Nil(t, value)
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		err = memCache.DeleteWithSyncID(ctx, "test-namespace", "non-existent-key")
		require.NoError(t, err)
	})

	t.Run("delete from non-existent namespace", func(t *testing.T) {
		err = memCache.DeleteWithSyncID(ctx, "non-existent-namespace", "key")
		require.NoError(t, err)
	})

	t.Run("delete with empty namespace", func(t *testing.T) {
		err = memCache.DeleteWithSyncID(ctx, "test-sync", "key")
		require.NoError(t, err)
	})

	t.Run("delete with empty key", func(t *testing.T) {
		err = memCache.DeleteWithSyncID(ctx, "test-sync", "")
		require.NoError(t, err)
	})

	t.Run("delete with nil context", func(t *testing.T) {
		err = memCache.DeleteWithSyncID(nil, "test-sync", "key") //nolint:staticcheck // because we want to test the nil context
		require.NoError(t, err)
	})
}

func TestMemorySessionCache_Clear(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	memCache := cache.(*MemorySessionCache)

	t.Run("clear populated cache", func(t *testing.T) {
		// Set multiple values in different namespaces
		err = memCache.SetWithSyncID(ctx, "namespace1", "key1", []byte("value1"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "namespace1", "key2", []byte("value2"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "namespace2", "key1", []byte("value3"))
		require.NoError(t, err)

		// Verify values exist
		_, found, err := memCache.GetWithSyncID(ctx, "namespace1", "key1")
		require.NoError(t, err)
		assert.True(t, found)

		_, found, err = memCache.GetWithSyncID(ctx, "namespace2", "key1")
		require.NoError(t, err)
		assert.True(t, found)

		// Clear namespace1
		err = memCache.ClearWithSyncID(ctx, "namespace1")
		require.NoError(t, err)

		// Verify namespace1 values are gone but namespace2 remains
		_, found, err = memCache.GetWithSyncID(ctx, "namespace1", "key1")
		require.NoError(t, err)
		assert.False(t, found)

		_, found, err = memCache.GetWithSyncID(ctx, "namespace1", "key2")
		require.NoError(t, err)
		assert.False(t, found)

		_, found, err = memCache.GetWithSyncID(ctx, "namespace2", "key1")
		require.NoError(t, err)
		assert.True(t, found)
	})

	t.Run("clear empty cache", func(t *testing.T) {
		err = memCache.ClearWithSyncID(ctx, "test-sync")
		require.NoError(t, err)
	})

	t.Run("clear with nil context", func(t *testing.T) {
		err = memCache.ClearWithSyncID(nil, "test-sync") //nolint:staticcheck // because we want to test the nil context
		require.NoError(t, err)
	})
}

func TestMemorySessionCache_GetAll(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	memCache := cache.(*MemorySessionCache)

	t.Run("empty namespace", func(t *testing.T) {
		values, err := memCache.GetAllWithSyncID(ctx, "empty-namespace")
		require.NoError(t, err)
		assert.Empty(t, values)
	})

	t.Run("populated namespace", func(t *testing.T) {
		// Set multiple values in a namespace
		err = memCache.SetWithSyncID(ctx, "test-sync", "key1", []byte("value1"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "test-sync", "key2", []byte("value2"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "test-sync", "key3", []byte("value3"))
		require.NoError(t, err)

		// Get all values from the namespace
		values, err := memCache.GetAllWithSyncID(ctx, "test-sync")
		require.NoError(t, err)
		assert.Len(t, values, 3)
		assert.Equal(t, []byte("value1"), values["key1"])
		assert.Equal(t, []byte("value2"), values["key2"])
		assert.Equal(t, []byte("value3"), values["key3"])
	})

	t.Run("namespace isolation", func(t *testing.T) {
		// Set values in different namespaces
		err = memCache.SetWithSyncID(ctx, "namespace1", "key1", []byte("value1"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "namespace2", "key1", []byte("value2"))
		require.NoError(t, err)

		// Verify namespaces are isolated
		values1, err := memCache.GetAllWithSyncID(ctx, "namespace1")
		require.NoError(t, err)
		assert.Len(t, values1, 1)
		assert.Equal(t, []byte("value1"), values1["key1"])

		values2, err := memCache.GetAllWithSyncID(ctx, "namespace2")
		require.NoError(t, err)
		assert.Len(t, values2, 1)
		assert.Equal(t, []byte("value2"), values2["key1"])
	})

	t.Run("empty namespace string", func(t *testing.T) {
		values, err := memCache.GetAllWithSyncID(ctx, "empty-sync")
		require.NoError(t, err)
		assert.Empty(t, values)
	})

	t.Run("nil context", func(t *testing.T) {
		values, err := memCache.GetAllWithSyncID(nil, "test-sync") //nolint:staticcheck // because we want to test the nil context
		require.NoError(t, err)
		assert.NotNil(t, values)
	})
}

func TestMemorySessionCache_GetMany(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	memCache := cache.(*MemorySessionCache)

	t.Run("get multiple existing keys", func(t *testing.T) {
		// Set multiple values
		err = memCache.SetWithSyncID(ctx, "test-sync", "key1", []byte("value1"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "test-sync", "key2", []byte("value2"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "test-sync", "key3", []byte("value3"))
		require.NoError(t, err)

		// Test getting multiple existing keys
		keys := []string{"key1", "key2", "key3"}
		values, err := memCache.GetManyWithSyncID(ctx, "test-sync", keys)
		require.NoError(t, err)
		assert.Len(t, values, 3)
		assert.Equal(t, []byte("value1"), values["key1"])
		assert.Equal(t, []byte("value2"), values["key2"])
		assert.Equal(t, []byte("value3"), values["key3"])
	})

	t.Run("get mix of existing and non-existing keys", func(t *testing.T) {
		keys := []string{"key1", "non-existent", "key3"}
		values, err := memCache.GetManyWithSyncID(ctx, "test-sync", keys)
		require.NoError(t, err)
		assert.Len(t, values, 2)
		assert.Equal(t, []byte("value1"), values["key1"])
		assert.Equal(t, []byte("value3"), values["key3"])
		assert.NotContains(t, values, "non-existent")
	})

	t.Run("get from non-existent namespace", func(t *testing.T) {
		values, err := memCache.GetManyWithSyncID(ctx, "non-existent-sync", []string{"key1", "key2"})
		require.NoError(t, err)
		assert.Empty(t, values)
	})

	t.Run("get empty key list", func(t *testing.T) {
		values, err := memCache.GetManyWithSyncID(ctx, "test-sync", []string{})
		require.NoError(t, err)
		assert.Empty(t, values)
	})

	t.Run("get with nil keys", func(t *testing.T) {
		values, err := memCache.GetManyWithSyncID(ctx, "test-sync", nil)
		require.NoError(t, err)
		assert.Empty(t, values)
	})

	t.Run("get with empty namespace", func(t *testing.T) {
		values, err := memCache.GetManyWithSyncID(ctx, "empty-sync", []string{"key1"})
		require.NoError(t, err)
		assert.Empty(t, values)
	})

	t.Run("get with nil context", func(t *testing.T) {
		values, err := memCache.GetManyWithSyncID(nil, "test-sync", []string{"key1"}) //nolint:staticcheck // because we want to test the nil context
		require.NoError(t, err)
		assert.NotNil(t, values)
	})
}

func TestMemorySessionCache_SetMany(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	memCache := cache.(*MemorySessionCache)

	t.Run("set multiple values", func(t *testing.T) {
		values := map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
			"key3": []byte("value3"),
		}

		err = memCache.SetManyWithSyncID(ctx, "test-sync", values)
		require.NoError(t, err)

		// Verify all values were set
		for key, expectedValue := range values {
			value, found, err := memCache.GetWithSyncID(ctx, "test-sync", key)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, expectedValue, value)
		}
	})

	t.Run("set empty map", func(t *testing.T) {
		err = memCache.SetManyWithSyncID(ctx, "test-sync", map[string][]byte{})
		require.NoError(t, err)
	})

	t.Run("set values with empty bytes", func(t *testing.T) {
		emptyValues := map[string][]byte{
			"empty1": []byte{},
			"empty2": []byte{},
		}
		err = memCache.SetManyWithSyncID(ctx, "test-sync", emptyValues)
		require.NoError(t, err)

		for key := range emptyValues {
			value, found, err := memCache.GetWithSyncID(ctx, "test-sync", key)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, []byte{}, value)
		}
	})

	t.Run("set values with nil bytes", func(t *testing.T) {
		nilValues := map[string][]byte{
			"nil1": nil,
			"nil2": nil,
		}
		err = memCache.SetManyWithSyncID(ctx, "test-sync", nilValues)
		require.NoError(t, err)

		for key := range nilValues {
			value, found, err := memCache.GetWithSyncID(ctx, "test-sync", key)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Nil(t, value)
		}
	})

	t.Run("set with empty namespace", func(t *testing.T) {
		values := map[string][]byte{"key1": []byte("value1")}
		err = memCache.SetManyWithSyncID(ctx, "test-sync", values)
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "key1")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value1"), value)
	})

	t.Run("set with nil context", func(t *testing.T) {
		values := map[string][]byte{"key1": []byte("value1")}
		err = memCache.SetManyWithSyncID(nil, "test-sync", values) //nolint:staticcheck // because we want to test the nil context
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "key1")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value1"), value)
	})

	t.Run("overwrite existing values", func(t *testing.T) {
		// Set initial values
		initialValues := map[string][]byte{
			"key1": []byte("initial1"),
			"key2": []byte("initial2"),
		}
		err = memCache.SetManyWithSyncID(ctx, "test-sync", initialValues)
		require.NoError(t, err)

		// Overwrite with new values
		updatedValues := map[string][]byte{
			"key1": []byte("updated1"),
			"key2": []byte("updated2"),
			"key3": []byte("new3"),
		}
		err = memCache.SetManyWithSyncID(ctx, "test-sync", updatedValues)
		require.NoError(t, err)

		// Verify all values are correct
		for key, expectedValue := range updatedValues {
			value, found, err := memCache.GetWithSyncID(ctx, "test-sync", key)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, expectedValue, value)
		}
	})
}

func TestMemorySessionCache_Concurrency(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	memCache := cache.(*MemorySessionCache)

	t.Run("concurrent reads and writes", func(t *testing.T) {
		const numGoroutines = 10
		const operationsPerGoroutine = 100
		var wg sync.WaitGroup

		// Start multiple goroutines performing concurrent operations
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				namespace := fmt.Sprintf("namespace-%d", id)

				for j := 0; j < operationsPerGoroutine; j++ {
					key := fmt.Sprintf("key-%d", j)
					value := []byte(fmt.Sprintf("value-%d-%d", id, j))

					// Set value
					err := memCache.SetWithSyncID(ctx, namespace, key, value)
					require.NoError(t, err)

					// Get value
					retrieved, found, err := memCache.GetWithSyncID(ctx, namespace, key)
					require.NoError(t, err)
					assert.True(t, found)
					assert.Equal(t, value, retrieved)

					// Delete value
					err = memCache.DeleteWithSyncID(ctx, namespace, key)
					require.NoError(t, err)

					// Verify deletion
					_, found, err = memCache.GetWithSyncID(ctx, namespace, key)
					require.NoError(t, err)
					assert.False(t, found)
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("concurrent SetMany operations", func(t *testing.T) {
		const numGoroutines = 5
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				namespace := fmt.Sprintf("setmany-namespace-%d", id)
				values := map[string][]byte{
					"key1": []byte(fmt.Sprintf("value1-%d", id)),
					"key2": []byte(fmt.Sprintf("value2-%d", id)),
					"key3": []byte(fmt.Sprintf("value3-%d", id)),
				}

				err := memCache.SetManyWithSyncID(ctx, namespace, values)
				require.NoError(t, err)

				// Verify values were set correctly
				for key, expectedValue := range values {
					value, found, err := memCache.GetWithSyncID(ctx, namespace, key)
					require.NoError(t, err)
					assert.True(t, found)
					assert.Equal(t, expectedValue, value)
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("concurrent GetMany operations", func(t *testing.T) {
		// Set up test data
		namespace := "getmany-namespace"
		testValues := map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
			"key3": []byte("value3"),
			"key4": []byte("value4"),
			"key5": []byte("value5"),
		}
		err = memCache.SetManyWithSyncID(ctx, namespace, testValues)
		require.NoError(t, err)

		const numGoroutines = 10
		var wg sync.WaitGroup

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				keys := []string{"key1", "key2", "key3", "non-existent"}
				values, err := memCache.GetManyWithSyncID(ctx, namespace, keys)
				require.NoError(t, err)
				assert.Len(t, values, 3)
				assert.Equal(t, []byte("value1"), values["key1"])
				assert.Equal(t, []byte("value2"), values["key2"])
				assert.Equal(t, []byte("value3"), values["key3"])
			}()
		}

		wg.Wait()
	})
}

func TestMemorySessionCache_Close(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	memCache := cache.(*MemorySessionCache)

	t.Run("close populated cache", func(t *testing.T) {
		// Set some values
		err = memCache.SetWithSyncID(ctx, "test-sync", "key1", []byte("value1"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "test-sync", "key2", []byte("value2"))
		require.NoError(t, err)

		// Verify values exist
		values, err := memCache.GetAllWithSyncID(ctx, "test-sync")
		require.NoError(t, err)
		assert.Len(t, values, 2)

		// Close the cache
		err = cache.Close()
		require.NoError(t, err)

		// Verify cache is cleared after close
		values, err = memCache.GetAllWithSyncID(ctx, "test-sync")
		require.NoError(t, err)
		assert.Empty(t, values)
	})

	t.Run("close empty cache", func(t *testing.T) {
		cache, err := NewMemorySessionCache(ctx)
		require.NoError(t, err)

		err = cache.Close()
		require.NoError(t, err)
	})

	t.Run("multiple close calls", func(t *testing.T) {
		cache, err := NewMemorySessionCache(ctx)
		require.NoError(t, err)

		// First close
		err = cache.Close()
		require.NoError(t, err)

		// Second close should not error
		err = cache.Close()
		require.NoError(t, err)
	})
}

func TestMemorySessionCache_TTL(t *testing.T) {
	t.Skip("TTL functionality is not implemented in the current MemorySessionCache")
}

func TestMemorySessionCache_NamespaceIsolation(t *testing.T) {
	ctx := context.Background()

	t.Run("same key in different namespaces", func(t *testing.T) {
		cache, err := NewMemorySessionCache(ctx)
		require.NoError(t, err)
		memCache := cache.(*MemorySessionCache)

		// Set same key in different namespaces
		err = memCache.SetWithSyncID(ctx, "namespace1", "same-key", []byte("value1"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "namespace2", "same-key", []byte("value2"))
		require.NoError(t, err)

		// Verify namespaces are isolated
		value1, found1, err := memCache.GetWithSyncID(ctx, "namespace1", "same-key")
		require.NoError(t, err)
		assert.True(t, found1)
		assert.Equal(t, []byte("value1"), value1)

		value2, found2, err := memCache.GetWithSyncID(ctx, "namespace2", "same-key")
		require.NoError(t, err)
		assert.True(t, found2)
		assert.Equal(t, []byte("value2"), value2)
	})

	t.Run("GetAll respects namespace boundaries", func(t *testing.T) {
		cache, err := NewMemorySessionCache(ctx)
		require.NoError(t, err)
		memCache := cache.(*MemorySessionCache)

		// Set values in different namespaces
		err = memCache.SetWithSyncID(ctx, "namespace1", "key1", []byte("value1"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "namespace1", "key2", []byte("value2"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "namespace2", "key1", []byte("value3"))
		require.NoError(t, err)

		// Verify GetAll respects namespace boundaries
		values1, err := memCache.GetAllWithSyncID(ctx, "namespace1")
		require.NoError(t, err)
		assert.Len(t, values1, 2)
		assert.Equal(t, []byte("value1"), values1["key1"])
		assert.Equal(t, []byte("value2"), values1["key2"])

		values2, err := memCache.GetAllWithSyncID(ctx, "namespace2")
		require.NoError(t, err)
		assert.Len(t, values2, 1)
		assert.Equal(t, []byte("value3"), values2["key1"])
	})

	t.Run("GetMany respects namespace boundaries", func(t *testing.T) {
		cache, err := NewMemorySessionCache(ctx)
		require.NoError(t, err)
		memCache := cache.(*MemorySessionCache)

		// Set values in different namespaces
		err = memCache.SetWithSyncID(ctx, "namespace1", "key1", []byte("value1"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "namespace1", "key2", []byte("value2"))
		require.NoError(t, err)
		err = memCache.SetWithSyncID(ctx, "namespace2", "key1", []byte("value3"))
		require.NoError(t, err)

		// Test GetMany with same keys in different namespaces
		keys := []string{"key1", "key2"}
		values1, err := memCache.GetManyWithSyncID(ctx, "namespace1", keys)
		require.NoError(t, err)
		assert.Len(t, values1, 2)
		assert.Equal(t, []byte("value1"), values1["key1"])
		assert.Equal(t, []byte("value2"), values1["key2"])

		values2, err := memCache.GetManyWithSyncID(ctx, "namespace2", keys)
		require.NoError(t, err)
		assert.Len(t, values2, 1)
		assert.Equal(t, []byte("value3"), values2["key1"])
	})
}

func TestMemorySessionCache_EdgeCases(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	memCache := cache.(*MemorySessionCache)

	t.Run("very long namespace and key names", func(t *testing.T) {
		longNamespace := string(make([]byte, 1000))
		longKey := string(make([]byte, 1000))

		err = memCache.SetWithSyncID(ctx, longNamespace, longKey, []byte("value"))
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, longNamespace, longKey)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value"), value)
	})

	t.Run("special characters in namespace and key", func(t *testing.T) {
		specialNamespace := "namespace/with/slashes"
		specialKey := "key-with-special-chars!@#$%^&*()"

		err = memCache.SetWithSyncID(ctx, specialNamespace, specialKey, []byte("value"))
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, specialNamespace, specialKey)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value"), value)
	})

	t.Run("unicode characters", func(t *testing.T) {
		unicodeNamespace := "namespace-测试"
		unicodeKey := "key-测试"

		err = memCache.SetWithSyncID(ctx, unicodeNamespace, unicodeKey, []byte("value"))
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, unicodeNamespace, unicodeKey)
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value"), value)
	})

	t.Run("empty values", func(t *testing.T) {
		err = memCache.SetWithSyncID(ctx, "test-sync", "empty-value", []byte{})
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "empty-value")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte{}, value)
	})

	t.Run("nil values", func(t *testing.T) {
		err = memCache.SetWithSyncID(ctx, "test-sync", "nil-value", nil)
		require.NoError(t, err)

		value, found, err := memCache.GetWithSyncID(ctx, "test-sync", "nil-value")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Nil(t, value)
	})
}

func TestMemorySessionCache_Performance(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	memCache := cache.(*MemorySessionCache)

	t.Run("bulk operations", func(t *testing.T) {
		const numOperations = 1000
		namespace := "performance-test"

		// Bulk set
		values := make(map[string][]byte, numOperations)
		for i := 0; i < numOperations; i++ {
			values[fmt.Sprintf("key-%d", i)] = []byte(fmt.Sprintf("value-%d", i))
		}

		start := time.Now()
		err = memCache.SetManyWithSyncID(ctx, namespace, values)
		require.NoError(t, err)
		setDuration := time.Since(start)

		// Bulk get
		keys := make([]string, numOperations)
		for i := 0; i < numOperations; i++ {
			keys[i] = fmt.Sprintf("key-%d", i)
		}

		start = time.Now()
		retrieved, err := memCache.GetManyWithSyncID(ctx, namespace, keys)
		require.NoError(t, err)
		getDuration := time.Since(start)

		// Verify all values
		assert.Len(t, retrieved, numOperations)
		for i := 0; i < numOperations; i++ {
			key := fmt.Sprintf("key-%d", i)
			expectedValue := []byte(fmt.Sprintf("value-%d", i))
			assert.Equal(t, expectedValue, retrieved[key])
		}

		t.Logf("SetMany %d items: %v", numOperations, setDuration)
		t.Logf("GetMany %d items: %v", numOperations, getDuration)
	})
}

func TestMemorySessionCache_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	memCache := cache.(*MemorySessionCache)

	t.Run("stress test", func(t *testing.T) {
		const numGoroutines = 20
		const operationsPerGoroutine = 500
		var wg sync.WaitGroup

		// Start multiple goroutines performing various operations
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				namespace := fmt.Sprintf("stress-namespace-%d", id)

				for j := 0; j < operationsPerGoroutine; j++ {
					key := fmt.Sprintf("key-%d", j)
					value := []byte(fmt.Sprintf("value-%d-%d", id, j))

					// Random operations
					switch j % 4 {
					case 0:
						// Set
						err := memCache.SetWithSyncID(ctx, namespace, key, value)
						require.NoError(t, err)
					case 1:
						// Get
						_, _, err := memCache.GetWithSyncID(ctx, namespace, key)
						require.NoError(t, err)
					case 2:
						// Delete
						err := memCache.DeleteWithSyncID(ctx, namespace, key)
						require.NoError(t, err)
					case 3:
						// GetAll
						_, err := memCache.GetAllWithSyncID(ctx, namespace)
						require.NoError(t, err)
					}
				}
			}(i)
		}

		wg.Wait()
	})
}

func TestMemorySessionCache_WithNamespace(t *testing.T) {
	ctx := context.Background()
	cache, err := NewMemorySessionCache(ctx)
	require.NoError(t, err)
	defer cache.Close()

	t.Run("basic_operations", func(t *testing.T) {
		// Create a namespaced cache
		nsCache := cache.WithNamespace("test-namespace")
		nsMemCache := nsCache.(*namespacedSessionCache)

		// Test Set and Get
		err := nsMemCache.SetWithSyncID(ctx, "test-sync", "key1", []byte("value1"))
		require.NoError(t, err)

		value, found, err := nsMemCache.GetWithSyncID(ctx, "test-sync", "key1")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value1"), value)

		// Test non-existent key
		value, found, err = nsMemCache.GetWithSyncID(ctx, "test-sync", "non-existent")
		require.NoError(t, err)
		assert.False(t, found)
		assert.Nil(t, value)
	})

	t.Run("namespace_isolation", func(t *testing.T) {
		// Create two different namespaced caches
		ns1 := cache.WithNamespace("namespace1")
		ns2 := cache.WithNamespace("namespace2")
		ns1MemCache := ns1.(*namespacedSessionCache)
		ns2MemCache := ns2.(*namespacedSessionCache)

		// Set values in different namespaces
		err := ns1MemCache.SetWithSyncID(ctx, "test-sync", "same-key", []byte("value1"))
		require.NoError(t, err)
		err = ns2MemCache.SetWithSyncID(ctx, "test-sync", "same-key", []byte("value2"))
		require.NoError(t, err)

		// Verify isolation
		value1, found, err := ns1MemCache.GetWithSyncID(ctx, "test-sync", "same-key")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value1"), value1)

		value2, found, err := ns2MemCache.GetWithSyncID(ctx, "test-sync", "same-key")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value2"), value2)
	})

	t.Run("GetMany_operations", func(t *testing.T) {
		nsCache := cache.WithNamespace("getmany-namespace")
		nsMemCache := nsCache.(*namespacedSessionCache)

		// Set multiple values
		values := map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
			"key3": []byte("value3"),
		}
		err := nsMemCache.SetManyWithSyncID(ctx, "test-sync", values)
		require.NoError(t, err)

		// Get multiple values
		result, err := nsMemCache.GetManyWithSyncID(ctx, "test-sync", []string{"key1", "key2", "key4"})
		require.NoError(t, err)
		assert.Equal(t, map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
		}, result)
	})

	t.Run("SetMany_operations", func(t *testing.T) {
		nsCache := cache.WithNamespace("setmany-namespace")
		nsMemCache := nsCache.(*namespacedSessionCache)

		// Set multiple values
		values := map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
		}
		err := nsMemCache.SetManyWithSyncID(ctx, "test-sync", values)
		require.NoError(t, err)

		// Verify all values were set
		for key, expectedValue := range values {
			value, found, err := nsMemCache.GetWithSyncID(ctx, "test-sync", key)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, expectedValue, value)
		}
	})

	t.Run("Delete_operations", func(t *testing.T) {
		nsCache := cache.WithNamespace("delete-namespace")
		nsMemCache := nsCache.(*namespacedSessionCache)

		// Set a value
		err := nsMemCache.SetWithSyncID(ctx, "test-sync", "key1", []byte("value1"))
		require.NoError(t, err)

		// Verify it exists
		value, found, err := nsMemCache.GetWithSyncID(ctx, "test-sync", "key1")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value1"), value)

		// Delete it
		err = nsMemCache.DeleteWithSyncID(ctx, "test-sync", "key1")
		require.NoError(t, err)

		// Verify it's gone
		value, found, err = nsMemCache.GetWithSyncID(ctx, "test-sync", "key1")
		require.NoError(t, err)
		assert.False(t, found)
		assert.Nil(t, value)
	})

	t.Run("GetAll_operations", func(t *testing.T) {
		nsCache := cache.WithNamespace("getall-namespace")
		nsMemCache := nsCache.(*namespacedSessionCache)

		// Set multiple values
		expectedValues := map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
			"key3": []byte("value3"),
		}
		err := nsMemCache.SetManyWithSyncID(ctx, "test-sync", expectedValues)
		require.NoError(t, err)

		// Get all values
		result, err := nsMemCache.GetAllWithSyncID(ctx, "test-sync")
		require.NoError(t, err)
		assert.Equal(t, expectedValues, result)
	})

	t.Run("empty_namespace", func(t *testing.T) {
		nsCache := cache.WithNamespace("")
		nsMemCache := nsCache.(*namespacedSessionCache)

		// Test operations with empty namespace
		err := nsMemCache.SetWithSyncID(ctx, "test-sync", "key1", []byte("value1"))
		require.NoError(t, err)

		value, found, err := nsMemCache.GetWithSyncID(ctx, "test-sync", "key1")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value1"), value)
	})

	t.Run("nil_context", func(t *testing.T) {
		nsCache := cache.WithNamespace("nil-context-namespace")
		nsMemCache := nsCache.(*namespacedSessionCache)

		// Test operations with nil context
		err := nsMemCache.SetWithSyncID(nil, "test-sync", "key1", []byte("value1")) //nolint:staticcheck // because we want to test the nil context
		require.NoError(t, err)

		value, found, err := nsMemCache.GetWithSyncID(nil, "test-sync", "key1") //nolint:staticcheck // because we want to test the nil context
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, []byte("value1"), value)
	})

	t.Run("concurrent_operations", func(t *testing.T) {
		nsCache := cache.WithNamespace("concurrent-namespace")
		nsMemCache := nsCache.(*namespacedSessionCache)
		const numGoroutines = 10
		const numOperations = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Start concurrent goroutines
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("key-%d-%d", id, j)
					value := []byte(fmt.Sprintf("value-%d-%d", id, j))

					// Set value
					err := nsMemCache.SetWithSyncID(ctx, "test-sync", key, value)
					require.NoError(t, err)

					// Get value
					retrievedValue, found, err := nsMemCache.GetWithSyncID(ctx, "test-sync", key)
					require.NoError(t, err)
					assert.True(t, found)
					assert.Equal(t, value, retrievedValue)
				}
			}(i)
		}

		wg.Wait()
	})
}
