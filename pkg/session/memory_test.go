//go:build baton_lambda_support

package session

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/maypok86/otter/v2"
	"github.com/stretchr/testify/require"
)

// defaultOtterOptions returns default otter options for testing.
func defaultOtterOptions() *otter.Options[string, []byte] {
	return &otter.Options[string, []byte]{
		MaximumSize:      1024 * 1024 * 15, // 15MB
		ExpiryCalculator: otter.ExpiryWriting[string, []byte](10 * time.Minute),
	}
}

func TestMemorySessionCache_Get(t *testing.T) {
	t.Run("successful get with caching", func(t *testing.T) {
		callCount := 0
		mockStore := &MockSessionStore{
			getFunc: func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
				callCount++
				if key == "test-key" {
					return []byte("test-value"), true, nil
				}
				return nil, false, nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		// First call - should hit backing store
		value, found, err := cache.Get(ctx, "test-key", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("test-value"), value)
		require.Equal(t, 1, callCount)

		// Second call - should hit cache
		value, found, err = cache.Get(ctx, "test-key", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("test-value"), value)
		require.Equal(t, 1, callCount, "should not call backing store again")
	})

	t.Run("get non-existent key", func(t *testing.T) {
		mockStore := &MockSessionStore{
			getFunc: func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
				return nil, false, nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		value, found, err := cache.Get(ctx, "non-existent", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.False(t, found)
		require.Nil(t, value)
	})

	t.Run("get with prefix", func(t *testing.T) {
		mockStore := &MockSessionStore{
			getFunc: func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
				if key == "key1" {
					return []byte("value1"), true, nil
				}
				return nil, false, nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		value, found, err := cache.Get(ctx, "key1", sessions.WithSyncID("sync-1"), sessions.WithPrefix("prefix/"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("value1"), value)
	})

	t.Run("error from backing store", func(t *testing.T) {
		mockStore := &MockSessionStore{
			getFunc: func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
				return nil, false, fmt.Errorf("backing store error")
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		_, _, err = cache.Get(ctx, "test-key", sessions.WithSyncID("sync-1"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "backing store error")
	})

	t.Run("sync ID isolation", func(t *testing.T) {
		mockStore := &MockSessionStore{
			getFunc: func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
				return []byte("shared-value"), true, nil
			},
		}

		cache1, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		cache2, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		// Set in sync-1
		err = cache1.Set(ctx, "key1", []byte("value1"), sessions.WithSyncID("sync-1"))
		require.NoError(t, err)

		// Get from sync-1 (should get cached value)
		value, found, err := cache1.Get(ctx, "key1", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("value1"), value)

		// Get from sync-2 (should get from backing store, not from sync-1's cache)
		value, found, err = cache2.Get(ctx, "key1", sessions.WithSyncID("sync-2"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("shared-value"), value)
	})
}

func TestMemorySessionCache_Set(t *testing.T) {
	t.Run("successful set", func(t *testing.T) {
		mockStore := &MockSessionStore{
			setFunc: func(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
				return nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		err = cache.Set(ctx, "test-key", []byte("test-value"), sessions.WithSyncID("sync-1"))
		require.NoError(t, err)

		// Verify it's cached
		value, found, err := cache.Get(ctx, "test-key", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("test-value"), value)
	})

	t.Run("set with prefix", func(t *testing.T) {
		mockStore := &MockSessionStore{
			setFunc: func(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
				return nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		err = cache.Set(ctx, "key1", []byte("value1"), sessions.WithSyncID("sync-1"), sessions.WithPrefix("prefix/"))
		require.NoError(t, err)

		// Should be able to get with the same prefix
		value, found, err := cache.Get(ctx, "key1", sessions.WithSyncID("sync-1"), sessions.WithPrefix("prefix/"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("value1"), value)
	})

	t.Run("error from backing store", func(t *testing.T) {
		mockStore := &MockSessionStore{
			setFunc: func(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
				return fmt.Errorf("backing store error")
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		err = cache.Set(ctx, "test-key", []byte("test-value"), sessions.WithSyncID("sync-1"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "backing store error")
	})
}

func TestMemorySessionCache_Delete(t *testing.T) {
	t.Run("successful delete", func(t *testing.T) {
		mockStore := &MockSessionStore{
			deleteFunc: func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
				return nil
			},
			getFunc: func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
				return nil, false, nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		// Set first
		err = cache.Set(ctx, "test-key", []byte("test-value"), sessions.WithSyncID("sync-1"))
		require.NoError(t, err)

		// Delete
		err = cache.Delete(ctx, "test-key", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)

		// Verify it's deleted
		value, found, err := cache.Get(ctx, "test-key", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.False(t, found)
		require.Nil(t, value)
	})

	t.Run("delete with prefix", func(t *testing.T) {
		mockStore := &MockSessionStore{
			deleteFunc: func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
				return nil
			},
			getFunc: func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
				return nil, false, nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		err = cache.Set(ctx, "key1", []byte("value1"), sessions.WithSyncID("sync-1"), sessions.WithPrefix("prefix/"))
		require.NoError(t, err)

		err = cache.Delete(ctx, "key1", sessions.WithSyncID("sync-1"), sessions.WithPrefix("prefix/"))
		require.NoError(t, err)

		value, found, err := cache.Get(ctx, "key1", sessions.WithSyncID("sync-1"), sessions.WithPrefix("prefix/"))
		require.NoError(t, err)
		require.False(t, found)
		require.Nil(t, value)
	})
}

func TestMemorySessionCache_GetMany(t *testing.T) {
	t.Run("successful getMany", func(t *testing.T) {
		callCount := 0
		mockStore := &MockSessionStore{
			getManyFunc: func(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
				callCount++
				result := make(map[string][]byte)
				for _, key := range keys {
					result[key] = []byte(fmt.Sprintf("value-%s", key))
				}
				return result, nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		// First call
		values, err := cache.GetMany(ctx, []string{"key1", "key2"}, sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.Equal(t, 2, len(values))
		require.Equal(t, []byte("value-key1"), values["key1"])
		require.Equal(t, []byte("value-key2"), values["key2"])
		require.Equal(t, 1, callCount)

		// Second call - should hit cache
		values, err = cache.GetMany(ctx, []string{"key1", "key2"}, sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.Equal(t, 2, len(values))
		require.Equal(t, 1, callCount, "should not call backing store again")
	})

	t.Run("getMany with prefix", func(t *testing.T) {
		keysReceived := []string{}
		mockStore := &MockSessionStore{
			getManyFunc: func(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, error) {
				keysReceived = keys // Capture what keys the backing store receives
				result := make(map[string][]byte)
				for _, key := range keys {
					result[key] = []byte(fmt.Sprintf("value-%s", key))
				}
				return result, nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		values, err := cache.GetMany(ctx, []string{"key1", "key2"}, sessions.WithSyncID("sync-1"), sessions.WithPrefix("prefix/"))
		require.NoError(t, err)
		require.Equal(t, 2, len(values))

		for key := range values {
			require.Contains(t, []string{"key1", "key2"}, key)
		}

		// Verify the backing store received prefixed keys
		// The current implementation passes prefixed keys to the backing store
		require.Contains(t, keysReceived, "prefix/key1", "Backing store receives prefixed keys")
		require.Contains(t, keysReceived, "prefix/key2", "Backing store receives prefixed keys")
	})
}

func TestMemorySessionCache_SetMany(t *testing.T) {
	t.Run("successful setMany", func(t *testing.T) {
		mockStore := &MockSessionStore{
			setManyFunc: func(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
				return nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		values := map[string][]byte{
			"key1": []byte("value1"),
			"key2": []byte("value2"),
		}

		err = cache.SetMany(ctx, values, sessions.WithSyncID("sync-1"))
		require.NoError(t, err)

		// Verify cached
		v1, found, err := cache.Get(ctx, "key1", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("value1"), v1)

		v2, found, err := cache.Get(ctx, "key2", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("value2"), v2)
	})
}

func TestMemorySessionCache_Clear(t *testing.T) {
	t.Run("clear all", func(t *testing.T) {
		mockStore := &MockSessionStore{
			clearFunc: func(ctx context.Context, opt ...sessions.SessionStoreOption) error {
				return nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		// Set some values
		err = cache.Set(ctx, "key1", []byte("value1"), sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		err = cache.Set(ctx, "key2", []byte("value2"), sessions.WithSyncID("sync-1"))
		require.NoError(t, err)

		// Clear
		err = cache.Clear(ctx, sessions.WithSyncID("sync-1"))
		require.NoError(t, err)

		// Verify cleared
		_, found, err := cache.Get(ctx, "key1", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("clear with prefix", func(t *testing.T) {
		mockStore := &MockSessionStore{
			clearFunc: func(ctx context.Context, opt ...sessions.SessionStoreOption) error {
				return nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		// Set with different prefixes
		err = cache.Set(ctx, "key1", []byte("value1"), sessions.WithSyncID("sync-1"), sessions.WithPrefix("prefix1/"))
		require.NoError(t, err)
		err = cache.Set(ctx, "key2", []byte("value2"), sessions.WithSyncID("sync-1"), sessions.WithPrefix("prefix2/"))
		require.NoError(t, err)

		// Clear only prefix1
		err = cache.Clear(ctx, sessions.WithSyncID("sync-1"), sessions.WithPrefix("prefix1/"))
		require.NoError(t, err)

		// prefix1 should be cleared
		_, found, err := cache.Get(ctx, "key1", sessions.WithSyncID("sync-1"), sessions.WithPrefix("prefix1/"))
		require.NoError(t, err)
		require.False(t, found)

		// prefix2 should still exist (but cache won't have it since Clear also clears cache for that prefix)
		// This test verifies the code doesn't crash
	})
}

func TestMemorySessionCache_GetAll(t *testing.T) {
	t.Run("successful getAll", func(t *testing.T) {
		mockStore := &MockSessionStore{
			getAllFunc: func(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
				return map[string][]byte{
					"key1": []byte("value1"),
					"key2": []byte("value2"),
				}, "", nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		values, nextPageToken, err := cache.GetAll(ctx, "", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.Equal(t, 2, len(values))
		require.Equal(t, "", nextPageToken)
		require.Equal(t, []byte("value1"), values["key1"])
		require.Equal(t, []byte("value2"), values["key2"])

		// Verify cached
		v1, found, err := cache.Get(ctx, "key1", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("value1"), v1)
	})
}

func TestMemorySessionCache_EdgeCases(t *testing.T) {
	t.Run("concurrent access", func(t *testing.T) {
		mockStore := &MockSessionStore{
			getFunc: func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
				return []byte("value"), true, nil
			},
		}

		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		// Run concurrent gets
		errCh := make(chan error, 10)
		for i := 0; i < 10; i++ {
			go func() {
				_, _, err := cache.Get(ctx, "key", sessions.WithSyncID("sync-1"))
				errCh <- err
			}()
		}

		for i := 0; i < 10; i++ {
			require.NoError(t, <-errCh)
		}
	})

	t.Run("multiple sync IDs", func(t *testing.T) {
		mockStore := &MockSessionStore{
			getFunc: func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
				return []byte("shared-value"), true, nil
			},
			setFunc: func(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
				return nil
			},
		}

		cache1, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		cache2, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		// Set in multiple sync IDs
		err = cache1.Set(ctx, "key1", []byte("value-sync1"), sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		err = cache2.Set(ctx, "key1", []byte("value-sync2"), sessions.WithSyncID("sync-2"))
		require.NoError(t, err)

		// Verify isolation
		v1, found, err := cache1.Get(ctx, "key1", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("value-sync1"), v1)

		v2, found, err := cache2.Get(ctx, "key1", sessions.WithSyncID("sync-2"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("value-sync2"), v2)
	})

	t.Run("cross-contamination with same cache instance", func(t *testing.T) {
		backingStoreCallCount := 0
		mockStore := &MockSessionStore{
			setFunc: func(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
				return nil
			},
			getFunc: func(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
				backingStoreCallCount++
				// This should be called when getting from sync-2, but currently
				// the cache will return the value from sync-1 instead
				return []byte("value-from-backing-store-sync-2"), true, nil
			},
		}

		// Use a SINGLE cache instance to demonstrate cross-contamination
		cache, err := NewMemorySessionCache(defaultOtterOptions(), mockStore)
		require.NoError(t, err)
		ctx := context.Background()

		// Set a value with sync-1
		err = cache.Set(ctx, "test-key", []byte("value-from-sync-1"), sessions.WithSyncID("sync-1"))
		require.NoError(t, err)

		// Get with sync-1 - should get cached value (no backing store call)
		backingStoreCallCount = 0
		value, found, err := cache.Get(ctx, "test-key", sessions.WithSyncID("sync-1"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("value-from-sync-1"), value)
		require.Equal(t, 0, backingStoreCallCount, "should use cache, not call backing store")

		// Get with DIFFERENT sync-2 - should go to backing store and get different value
		// Currently this will FAIL because the cache doesn't isolate by syncID
		backingStoreCallCount = 0
		value, found, err = cache.Get(ctx, "test-key", sessions.WithSyncID("sync-2"))
		require.NoError(t, err)
		require.True(t, found)
		// This assertion will fail because cache returns value from sync-1 instead
		require.Equal(t, []byte("value-from-backing-store-sync-2"), value,
			"should get value from backing store for different syncID, not cached value from sync-1")
		require.Equal(t, 1, backingStoreCallCount, "should call backing store for different syncID")
	})
}
