package dotc1z

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/session"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/stretchr/testify/require"
)

func TestC1FileSessionStore_Get(t *testing.T) {
	ctx := context.Background()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close()

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("Get non-existent key", func(t *testing.T) {
		value, found, err := c1zFile.Get(ctx, "non-existent", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.False(t, found)
		require.Nil(t, value)
	})

	t.Run("Get existing key", func(t *testing.T) {
		// Set a value first
		err := c1zFile.Set(ctx, "test-key", []byte("test-value"), sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Get the value
		value, found, err := c1zFile.Get(ctx, "test-key", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("test-value"), value)
	})

	t.Run("Get with prefix", func(t *testing.T) {
		// Set a value with prefix
		err := c1zFile.Set(ctx, "prefixed-key", []byte("prefixed-value"),
			sessions.WithSyncID(syncID), session.WithPrefix("test-prefix:"))
		require.NoError(t, err)

		// Get the value with prefix
		value, found, err := c1zFile.Get(ctx, "prefixed-key",
			sessions.WithSyncID(syncID), session.WithPrefix("test-prefix:"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("prefixed-value"), value)
	})

	t.Run("Get without sync ID", func(t *testing.T) {
		_, _, err := c1zFile.Get(ctx, "test-key")
		require.Error(t, err)
		require.Contains(t, err.Error(), "sync id is required")
	})

	t.Run("Get with wrong sync ID", func(t *testing.T) {
		value, found, err := c1zFile.Get(ctx, "test-key", sessions.WithSyncID("wrong-sync-id"))
		require.NoError(t, err)
		require.False(t, found)
		require.Nil(t, value)
	})
}

func TestC1FileSessionStore_Set(t *testing.T) {
	ctx := context.Background()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close()

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("Set new key", func(t *testing.T) {
		err := c1zFile.Set(ctx, "new-key", []byte("new-value"), sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify it was set
		value, found, err := c1zFile.Get(ctx, "new-key", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("new-value"), value)
	})

	t.Run("Set existing key (upsert)", func(t *testing.T) {
		// Set initial value
		err := c1zFile.Set(ctx, "existing-key", []byte("initial-value"), sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Update the value
		err = c1zFile.Set(ctx, "existing-key", []byte("updated-value"), sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify it was updated
		value, found, err := c1zFile.Get(ctx, "existing-key", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("updated-value"), value)
	})

	t.Run("Set with prefix", func(t *testing.T) {
		err := c1zFile.Set(ctx, "prefixed-key", []byte("prefixed-value"),
			sessions.WithSyncID(syncID), session.WithPrefix("prefix:"))
		require.NoError(t, err)

		// Verify it was set with prefix
		value, found, err := c1zFile.Get(ctx, "prefixed-key",
			sessions.WithSyncID(syncID), session.WithPrefix("prefix:"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("prefixed-value"), value)
	})

	t.Run("Set without sync ID", func(t *testing.T) {
		err := c1zFile.Set(ctx, "test-key", []byte("test-value"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "sync id is required")
	})

	t.Run("Set empty value", func(t *testing.T) {
		err := c1zFile.Set(ctx, "empty-key", []byte(""), sessions.WithSyncID(syncID))
		require.NoError(t, err)

		value, found, err := c1zFile.Get(ctx, "empty-key", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte(nil), value)
	})
}

func TestC1FileSessionStore_GetMany(t *testing.T) {
	ctx := context.Background()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close()

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("GetMany empty keys", func(t *testing.T) {
		result, err := c1zFile.GetMany(ctx, []string{}, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("GetMany non-existent keys", func(t *testing.T) {
		result, err := c1zFile.GetMany(ctx, []string{"non-existent-1", "non-existent-2"}, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("GetMany existing keys", func(t *testing.T) {
		// Set some values
		err := c1zFile.Set(ctx, "key1", []byte("value1"), sessions.WithSyncID(syncID))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "key2", []byte("value2"), sessions.WithSyncID(syncID))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "key3", []byte("value3"), sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Get multiple keys
		result, err := c1zFile.GetMany(ctx, []string{"key1", "key2", "key3"}, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.Equal(t, []byte("value1"), result["key1"])
		require.Equal(t, []byte("value2"), result["key2"])
		require.Equal(t, []byte("value3"), result["key3"])
	})

	t.Run("GetMany mixed existing and non-existent keys", func(t *testing.T) {
		result, err := c1zFile.GetMany(ctx, []string{"key1", "non-existent", "key2"}, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Equal(t, []byte("value1"), result["key1"])
		require.Equal(t, []byte("value2"), result["key2"])
	})

	t.Run("GetMany with prefix", func(t *testing.T) {
		// Set values with prefix
		err := c1zFile.Set(ctx, "prefixed-key1", []byte("prefixed-value1"),
			sessions.WithSyncID(syncID), session.WithPrefix("test-prefix:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "prefixed-key2", []byte("prefixed-value2"),
			sessions.WithSyncID(syncID), session.WithPrefix("test-prefix:"))
		require.NoError(t, err)

		// Get multiple keys with prefix
		result, err := c1zFile.GetMany(ctx, []string{"prefixed-key1", "prefixed-key2"},
			sessions.WithSyncID(syncID), session.WithPrefix("test-prefix:"))
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Equal(t, []byte("prefixed-value1"), result["prefixed-key1"])
		require.Equal(t, []byte("prefixed-value2"), result["prefixed-key2"])
	})

	t.Run("GetMany without sync ID", func(t *testing.T) {
		_, err := c1zFile.GetMany(ctx, []string{"key1"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "sync id is required")
	})
}

func TestC1FileSessionStore_SetMany(t *testing.T) {
	ctx := context.Background()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close()

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("SetMany empty map", func(t *testing.T) {
		err := c1zFile.SetMany(ctx, map[string][]byte{}, sessions.WithSyncID(syncID))
		require.NoError(t, err)
	})

	t.Run("SetMany multiple keys", func(t *testing.T) {
		values := map[string][]byte{
			"batch-key1": []byte("batch-value1"),
			"batch-key2": []byte("batch-value2"),
			"batch-key3": []byte("batch-value3"),
		}

		err := c1zFile.SetMany(ctx, values, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify all values were set
		for key, expectedValue := range values {
			value, found, err := c1zFile.Get(ctx, key, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, expectedValue, value)
		}
	})

	t.Run("SetMany with prefix", func(t *testing.T) {
		values := map[string][]byte{
			"batch-prefixed-key1": []byte("batch-prefixed-value1"),
			"batch-prefixed-key2": []byte("batch-prefixed-value2"),
		}

		err := c1zFile.SetMany(ctx, values,
			sessions.WithSyncID(syncID), session.WithPrefix("batch-prefix:"))
		require.NoError(t, err)

		// Verify all values were set with prefix
		for key, expectedValue := range values {
			value, found, err := c1zFile.Get(ctx, key,
				sessions.WithSyncID(syncID), session.WithPrefix("batch-prefix:"))
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, expectedValue, value)
		}
	})

	t.Run("SetMany without sync ID", func(t *testing.T) {
		values := map[string][]byte{"test-key": []byte("test-value")}
		err := c1zFile.SetMany(ctx, values)
		require.Error(t, err)
		require.Contains(t, err.Error(), "sync id is required")
	})

	t.Run("SetMany with empty values", func(t *testing.T) {
		values := map[string][]byte{
			"empty-key1": []byte(""),
			"empty-key2": []byte(""),
		}

		err := c1zFile.SetMany(ctx, values, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify empty values were set
		for key := range values {
			value, found, err := c1zFile.Get(ctx, key, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []byte(nil), value)
		}
	})
}

func TestC1FileSessionStore_Delete(t *testing.T) {
	ctx := context.Background()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close()

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("Delete non-existent key", func(t *testing.T) {
		err := c1zFile.Delete(ctx, "non-existent-key", sessions.WithSyncID(syncID))
		require.NoError(t, err)
	})

	t.Run("Delete existing key", func(t *testing.T) {
		// Set a value first
		err := c1zFile.Set(ctx, "to-delete", []byte("will-be-deleted"), sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify it exists
		value, found, err := c1zFile.Get(ctx, "to-delete", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("will-be-deleted"), value)

		// Delete it
		err = c1zFile.Delete(ctx, "to-delete", sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify it's gone
		value, found, err = c1zFile.Get(ctx, "to-delete", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.False(t, found)
		require.Nil(t, value)
	})

	t.Run("Delete with prefix", func(t *testing.T) {
		// Set a value with prefix
		err := c1zFile.Set(ctx, "prefixed-to-delete", []byte("will-be-deleted"),
			sessions.WithSyncID(syncID), session.WithPrefix("delete-prefix:"))
		require.NoError(t, err)

		// Verify it exists
		value, found, err := c1zFile.Get(ctx, "prefixed-to-delete",
			sessions.WithSyncID(syncID), session.WithPrefix("delete-prefix:"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("will-be-deleted"), value)

		// Delete it with prefix
		err = c1zFile.Delete(ctx, "prefixed-to-delete",
			sessions.WithSyncID(syncID), session.WithPrefix("delete-prefix:"))
		require.NoError(t, err)

		// Verify it's gone
		value, found, err = c1zFile.Get(ctx, "prefixed-to-delete",
			sessions.WithSyncID(syncID), session.WithPrefix("delete-prefix:"))
		require.NoError(t, err)
		require.False(t, found)
		require.Nil(t, value)
	})

	t.Run("Delete without sync ID", func(t *testing.T) {
		err := c1zFile.Delete(ctx, "test-key")
		require.Error(t, err)
		require.Contains(t, err.Error(), "sync id is required")
	})
}

func TestC1FileSessionStore_Clear(t *testing.T) {
	ctx := context.Background()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close()

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("Clear empty store", func(t *testing.T) {
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)
	})

	t.Run("Clear all keys", func(t *testing.T) {
		// Set multiple values
		err := c1zFile.Set(ctx, "clear-key1", []byte("clear-value1"), sessions.WithSyncID(syncID))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "clear-key2", []byte("clear-value2"), sessions.WithSyncID(syncID))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "clear-key3", []byte("clear-value3"), sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify they exist
		all, err := c1zFile.GetAll(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, all, 3)

		// Clear all
		err = c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify they're all gone
		all, err = c1zFile.GetAll(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Empty(t, all)
	})

	t.Run("Clear with prefix", func(t *testing.T) {
		// Set values with different prefixes
		err := c1zFile.Set(ctx, "prefix1-key1", []byte("prefix1-value1"),
			sessions.WithSyncID(syncID), session.WithPrefix("prefix1:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "prefix1-key2", []byte("prefix1-value2"),
			sessions.WithSyncID(syncID), session.WithPrefix("prefix1:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "prefix2-key1", []byte("prefix2-value1"),
			sessions.WithSyncID(syncID), session.WithPrefix("prefix2:"))
		require.NoError(t, err)

		// Clear only prefix1
		err = c1zFile.Clear(ctx, sessions.WithSyncID(syncID), session.WithPrefix("prefix1:"))
		require.NoError(t, err)

		// Verify prefix1 keys are gone but prefix2 remains
		_, found, err := c1zFile.Get(ctx, "prefix1-key1",
			sessions.WithSyncID(syncID), session.WithPrefix("prefix1:"))
		require.NoError(t, err)
		require.False(t, found)

		err = c1zFile.Clear(ctx, sessions.WithSyncID(syncID), session.WithPrefix("a%"))
		require.NoError(t, err)

		value, found, err := c1zFile.Get(ctx, "prefix2-key1",
			sessions.WithSyncID(syncID), session.WithPrefix("prefix2:"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("prefix2-value1"), value)
	})

	t.Run("Clear without sync ID", func(t *testing.T) {
		err := c1zFile.Clear(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "sync id is required")
	})
}

func TestC1FileSessionStore_GetAll(t *testing.T) {
	ctx := context.Background()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close()

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("GetAll empty store", func(t *testing.T) {
		result, err := c1zFile.GetAll(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Empty(t, result)
	})

	t.Run("GetAll with multiple keys", func(t *testing.T) {
		// Set multiple values
		err := c1zFile.Set(ctx, "getall-key1", []byte("getall-value1"), sessions.WithSyncID(syncID))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "getall-key2", []byte("getall-value2"), sessions.WithSyncID(syncID))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "getall-key3", []byte("getall-value3"), sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Get all
		result, err := c1zFile.GetAll(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.Equal(t, []byte("getall-value1"), result["getall-key1"])
		require.Equal(t, []byte("getall-value2"), result["getall-key2"])
		require.Equal(t, []byte("getall-value3"), result["getall-key3"])
	})

	t.Run("GetAll with prefix", func(t *testing.T) {
		// Set values with different prefixes
		err := c1zFile.Set(ctx, "getall-prefix1-key1", []byte("getall-prefix1-value1"),
			sessions.WithSyncID(syncID), session.WithPrefix("getall-prefix1:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "getall-prefix1-key2", []byte("getall-prefix1-value2"),
			sessions.WithSyncID(syncID), session.WithPrefix("getall-prefix1:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "getall-prefix2-key1", []byte("getall-prefix2-value1"),
			sessions.WithSyncID(syncID), session.WithPrefix("getall-prefix2:"))
		require.NoError(t, err)

		// Get all with prefix1
		result, err := c1zFile.GetAll(ctx, sessions.WithSyncID(syncID), session.WithPrefix("getall-prefix1:"))
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Equal(t, []byte("getall-prefix1-value1"), result["getall-prefix1-key1"])
		require.Equal(t, []byte("getall-prefix1-value2"), result["getall-prefix1-key2"])
	})

	t.Run("GetAll without sync ID", func(t *testing.T) {
		_, err := c1zFile.GetAll(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "sync id is required")
	})
}

func TestC1FileSessionStore_Isolation(t *testing.T) {
	ctx := context.Background()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close()

	syncID1, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	// End the first sync to allow starting a new one
	err = c1zFile.EndSync(ctx)
	require.NoError(t, err)

	syncID2, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("Sync ID isolation", func(t *testing.T) {
		// Set values in different sync IDs
		err := c1zFile.Set(ctx, "isolated-key", []byte("sync1-value"), sessions.WithSyncID(syncID1))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "isolated-key", []byte("sync2-value"), sessions.WithSyncID(syncID2))
		require.NoError(t, err)

		// Verify isolation
		value1, found1, err := c1zFile.Get(ctx, "isolated-key", sessions.WithSyncID(syncID1))
		require.NoError(t, err)
		require.True(t, found1)
		require.Equal(t, []byte("sync1-value"), value1)

		value2, found2, err := c1zFile.Get(ctx, "isolated-key", sessions.WithSyncID(syncID2))
		require.NoError(t, err)
		require.True(t, found2)
		require.Equal(t, []byte("sync2-value"), value2)

		// Verify they don't interfere with each other
		require.NotEqual(t, value1, value2)
	})

	t.Run("Prefix isolation", func(t *testing.T) {
		// Set values with different prefixes in same sync
		err := c1zFile.Set(ctx, "prefix-key", []byte("prefix1-value"),
			sessions.WithSyncID(syncID1), session.WithPrefix("prefix1:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "prefix-key", []byte("prefix2-value"),
			sessions.WithSyncID(syncID1), session.WithPrefix("prefix2:"))
		require.NoError(t, err)

		// Verify prefix isolation
		value1, found1, err := c1zFile.Get(ctx, "prefix-key",
			sessions.WithSyncID(syncID1), session.WithPrefix("prefix1:"))
		require.NoError(t, err)
		require.True(t, found1)
		require.Equal(t, []byte("prefix1-value"), value1)

		value2, found2, err := c1zFile.Get(ctx, "prefix-key",
			sessions.WithSyncID(syncID1), session.WithPrefix("prefix2:"))
		require.NoError(t, err)
		require.True(t, found2)
		require.Equal(t, []byte("prefix2-value"), value2)

		// Verify they don't interfere with each other
		require.NotEqual(t, value1, value2)
	})
}

func TestC1FileSessionStore_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close()

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("Concurrent reads and writes", func(t *testing.T) {
		// Start multiple goroutines doing concurrent operations
		done := make(chan bool, 10)
		errCh := make(chan error, 10)
		// Concurrent writers
		for i := 0; i < 5; i++ {
			go func(i int) {
				defer func() { done <- true }()
				key := fmt.Sprintf("concurrent-key-%d", i)
				value := []byte(fmt.Sprintf("concurrent-value-%d", i))
				errCh <- c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			}(i)
		}

		// Concurrent readers
		for i := 0; i < 5; i++ {
			go func(i int) {
				defer func() { done <- true }()
				key := fmt.Sprintf("concurrent-key-%d", i)
				_, _, err := c1zFile.Get(ctx, key, sessions.WithSyncID(syncID))
				errCh <- err
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < 10; i++ {
			<-done
		}

		close(errCh)
		for err := range errCh {
			require.NoError(t, err)
		}

		// Verify final state
		all, err := c1zFile.GetAll(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, all, 5) // Should have 5 keys
	})
}

func TestC1FileSessionStore_ErrorHandling(t *testing.T) {
	ctx := context.Background()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close()

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("Invalid options", func(t *testing.T) {
		// Test with invalid option (this should not happen in practice, but test error handling)
		invalidOption := func(ctx context.Context, bag *sessions.SessionStoreBag) error {
			return fmt.Errorf("invalid option")
		}

		_, _, err := c1zFile.Get(ctx, "test-key", invalidOption)
		require.Error(t, err)
		require.Contains(t, err.Error(), "error applying session option")
	})

	t.Run("Context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		_, _, err := c1zFile.Get(cancelCtx, "test-key", sessions.WithSyncID(syncID))
		require.Error(t, err)
		require.Contains(t, err.Error(), "context canceled")
	})
}

func TestC1FileSessionStore_Performance(t *testing.T) {
	ctx := context.Background()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close()

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("Large batch operations", func(t *testing.T) {
		// Test SetMany with large number of keys
		largeValues := make(map[string][]byte)
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("large-batch-key-%d", i)
			value := []byte(fmt.Sprintf("large-batch-value-%d", i))
			largeValues[key] = value
		}

		err := c1zFile.SetMany(ctx, largeValues, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify all values were set
		all, err := c1zFile.GetAll(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, all, 1000)

		// Test GetMany with large number of keys
		keys := make([]string, 0, 1000)
		for key := range largeValues {
			keys = append(keys, key)
		}

		result, err := c1zFile.GetMany(ctx, keys, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, result, 1000)
	})

	t.Run("Large values", func(t *testing.T) {
		// Test with large values
		largeValue := make([]byte, 1024*1024) // 1MB
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		err := c1zFile.Set(ctx, "large-value-key", largeValue, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		value, found, err := c1zFile.Get(ctx, "large-value-key", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, largeValue, value)
	})
}
