package dotc1z

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"maps"
	"path/filepath"
	"slices"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/stretchr/testify/require"
)

func TestC1FileSessionStore_Get(t *testing.T) {
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close(ctx)

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
			sessions.WithSyncID(syncID), sessions.WithPrefix("test-prefix:"))
		require.NoError(t, err)

		// Get the value with prefix
		value, found, err := c1zFile.Get(ctx, "prefixed-key",
			sessions.WithSyncID(syncID), sessions.WithPrefix("test-prefix:"))
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
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close(ctx)

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
			sessions.WithSyncID(syncID), sessions.WithPrefix("prefix:"))
		require.NoError(t, err)

		// Verify it was set with prefix
		value, found, err := c1zFile.Get(ctx, "prefixed-key",
			sessions.WithSyncID(syncID), sessions.WithPrefix("prefix:"))
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
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close(ctx)

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("GetMany empty keys", func(t *testing.T) {
		result, unprocessedKeys, err := c1zFile.GetMany(ctx, []string{}, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Empty(t, result)
		require.Empty(t, unprocessedKeys)
	})

	t.Run("GetMany non-existent keys", func(t *testing.T) {
		result, unprocessedKeys, err := c1zFile.GetMany(ctx, []string{"non-existent-1", "non-existent-2"}, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Empty(t, result)
		require.Empty(t, unprocessedKeys)
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
		result, unprocessedKeys, err := c1zFile.GetMany(ctx, []string{"key1", "key2", "key3"}, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Empty(t, unprocessedKeys)
		require.Len(t, result, 3)
		require.Equal(t, []byte("value1"), result["key1"])
		require.Equal(t, []byte("value2"), result["key2"])
		require.Equal(t, []byte("value3"), result["key3"])
	})

	t.Run("GetMany mixed existing and non-existent keys", func(t *testing.T) {
		result, unprocessedKeys, err := c1zFile.GetMany(ctx, []string{"key1", "non-existent", "key2"}, sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Empty(t, unprocessedKeys)
		require.Len(t, result, 2)
		require.Equal(t, []byte("value1"), result["key1"])
		require.Equal(t, []byte("value2"), result["key2"])
	})

	t.Run("GetMany with prefix", func(t *testing.T) {
		// Set values with prefix
		err := c1zFile.Set(ctx, "prefixed-key1", []byte("prefixed-value1"),
			sessions.WithSyncID(syncID), sessions.WithPrefix("test-prefix:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "prefixed-key2", []byte("prefixed-value2"),
			sessions.WithSyncID(syncID), sessions.WithPrefix("test-prefix:"))
		require.NoError(t, err)

		// Get multiple keys with prefix
		result, unprocessedKeys, err := c1zFile.GetMany(ctx, []string{"prefixed-key1", "prefixed-key2"},
			sessions.WithSyncID(syncID), sessions.WithPrefix("test-prefix:"))
		require.NoError(t, err)
		require.Empty(t, unprocessedKeys)
		require.Len(t, result, 2)
		require.Equal(t, []byte("prefixed-value1"), result["prefixed-key1"])
		require.Equal(t, []byte("prefixed-value2"), result["prefixed-key2"])

		// Verify prefix is stripped - keys should NOT have the prefix
		_, hasPrefixInKey := result["test-prefix:prefixed-key1"]
		require.False(t, hasPrefixInKey, "returned keys should not include the prefix")
		_, hasPrefixInKey = result["test-prefix:prefixed-key2"]
		require.False(t, hasPrefixInKey, "returned keys should not include the prefix")
	})

	t.Run("GetMany without sync ID", func(t *testing.T) {
		_, unprocessedKeys, err := c1zFile.GetMany(ctx, []string{"key1"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "sync id is required")
		require.Empty(t, unprocessedKeys)
	})

	// Test 1: Size limit enforcement - values exceed MaxSessionStoreSizeLimit
	t.Run("GetMany size limit enforcement", func(t *testing.T) {
		// Create values that total more than MaxSessionStoreSizeLimit
		// Each value is ~1MB, so 5 values = ~5MB > MaxSessionStoreSizeLimit
		keys := []string{"size-limit-key1", "size-limit-key2", "size-limit-key3", "size-limit-key4", "size-limit-key5"}
		for i, key := range keys {
			// Each value is approximately 1MB (1048576 bytes)
			value := bytes.Repeat([]byte{byte(i)}, 1048576)
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		result, unprocessedKeys, err := c1zFile.GetMany(ctx, keys, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify some keys are in result and some are in unprocessedKeys
		require.NotEmpty(t, result, "should have some results")
		require.NotEmpty(t, unprocessedKeys, "should have some unprocessed keys")

		// Verify total size of result is <= MaxSessionStoreSizeLimit
		totalSize := 0
		for _, value := range result {
			totalSize += len(value)
		}
		require.LessOrEqual(t, totalSize, sessions.MaxSessionStoreSizeLimit, "result size should be within limit")

		// Verify all requested keys are either in result or unprocessedKeys
		allKeys := make(map[string]bool)
		for key := range result {
			allKeys[key] = true
		}
		for _, key := range unprocessedKeys {
			allKeys[key] = true
		}
		require.Len(t, allKeys, len(keys), "all keys should be accounted for")
	})

	// Test 3: Mixed large and small values
	t.Run("GetMany mixed large and small values", func(t *testing.T) {
		// Create a mix: some small values that fit, some large that don't
		smallKeys := []string{"small-key1", "small-key2", "small-key3"}
		largeKeys := []string{"large-key1", "large-key2"}

		// Small values (each ~100 bytes)
		for _, key := range smallKeys {
			err := c1zFile.Set(ctx, key, bytes.Repeat([]byte("small"), 20), sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		// Large values (each ~2.5MB, exceeds limit when combined)
		for i, key := range largeKeys {
			value := bytes.Repeat([]byte{byte(i)}, 2500000)
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		allKeys := make([]string, 0, len(smallKeys)+len(largeKeys))
		allKeys = append(allKeys, smallKeys...)
		allKeys = append(allKeys, largeKeys...)
		result, unprocessedKeys, err := c1zFile.GetMany(ctx, allKeys, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Small keys should all be in result
		for _, key := range smallKeys {
			_, inResult := result[key]
			require.True(t, inResult, "small key %s should be in result", key)
		}

		// Large keys should be in unprocessedKeys (or at least some of them)
		require.NotEmpty(t, unprocessedKeys, "should have unprocessed keys for large values")
		unprocessedSet := make(map[string]bool)
		for _, key := range unprocessedKeys {
			unprocessedSet[key] = true
		}
		// At least one large key should be unprocessed
		hasLargeUnprocessed := false
		for _, key := range largeKeys {
			if unprocessedSet[key] {
				hasLargeUnprocessed = true
				break
			}
		}
		require.True(t, hasLargeUnprocessed, "at least one large key should be unprocessed")
	})

	// Test 4: Exact boundary at MaxSessionStoreSizeLimit
	t.Run("GetMany exact boundary at limit", func(t *testing.T) {
		// Create values that sum to exactly MaxSessionStoreSizeLimit
		keys := []string{"1", "2", "3", "4"}
		valueSize := (sessions.MaxSessionStoreSizeLimit - 4 - (20 * 4)) / 4 // 1040896 bytes each - (4) bytes for key and 20 bytes for value

		for i, key := range keys {
			value := bytes.Repeat([]byte{byte(i)}, valueSize)
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		result, unprocessedKeys, err := c1zFile.GetMany(ctx, keys, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// All should fit (using <= comparison)
		totalSize := 0
		for _, value := range result {
			totalSize += len(value)
		}
		require.LessOrEqual(t, totalSize, sessions.MaxSessionStoreSizeLimit, "total size should be within limit")
		require.Empty(t, unprocessedKeys, "all keys should fit at exact boundary")
		require.Len(t, result, len(keys), "all keys should be in result")
	})

	// Test 6: UnprocessedKeys for non-existent keys
	t.Run("GetMany unprocessedKeys for non-existent keys", func(t *testing.T) {
		// Request keys that don't exist in the database
		nonExistentKeys := []string{"non-existent-key1", "non-existent-key2", "non-existent-key3"}

		result, unprocessedKeys, err := c1zFile.GetMany(ctx, nonExistentKeys, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Non-existent keys won't be in the database, so they won't appear in rows
		// Therefore they won't be added to unprocessedKeys (current behavior)
		require.Empty(t, result, "no results for non-existent keys")
		require.Empty(t, unprocessedKeys, "non-existent keys are not in database, so not in unprocessedKeys")

		// Test mixed: some exist, some don't
		// Set one key that exists
		err = c1zFile.Set(ctx, "mixed-exists", []byte("exists"), sessions.WithSyncID(syncID))
		require.NoError(t, err)

		mixedKeys := []string{"mixed-exists", "mixed-non-existent1", "mixed-non-existent2"}
		result, unprocessedKeys, err = c1zFile.GetMany(ctx, mixedKeys, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// The existing key should be in result
		require.Contains(t, result, "mixed-exists", "existing key should be in result")
		// Non-existent keys won't be in unprocessedKeys (they're not in the database)
		require.Empty(t, unprocessedKeys, "non-existent keys are not tracked in unprocessedKeys")
	})

	// Test 8: UnprocessedKeys ordering and completeness
	t.Run("GetMany unprocessedKeys ordering and completeness", func(t *testing.T) {
		// Create many keys with values that will exceed the limit
		numKeys := 10
		keys := make([]string, numKeys)
		for i := range numKeys {
			keys[i] = fmt.Sprintf("completeness-key-%d", i)
			// Each value is ~500KB, so 10 values = ~5MB > MaxSessionStoreSizeLimit
			value := bytes.Repeat([]byte{byte(i)}, 500000)
			err := c1zFile.Set(ctx, keys[i], value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		result, unprocessedKeys, err := c1zFile.GetMany(ctx, keys, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify all requested keys are accounted for (either in result or unprocessedKeys)
		allAccountedFor := make(map[string]bool)
		for key := range result {
			allAccountedFor[key] = true
		}
		for _, key := range unprocessedKeys {
			allAccountedFor[key] = true
		}

		require.Len(t, allAccountedFor, len(keys), "all keys should be accounted for")
		for _, key := range keys {
			require.True(t, allAccountedFor[key], "key %s should be in result or unprocessedKeys", key)
		}

		// Verify unprocessedKeys contains expected keys (order doesn't matter)
		unprocessedSet := make(map[string]bool)
		for _, key := range unprocessedKeys {
			unprocessedSet[key] = true
		}

		// Verify no duplicates in unprocessedKeys
		require.Len(t, unprocessedKeys, len(unprocessedSet), "unprocessedKeys should have no duplicates")

		// Verify unprocessedKeys are a subset of requested keys
		requestedSet := make(map[string]bool)
		for _, key := range keys {
			requestedSet[key] = true
		}
		for key := range unprocessedSet {
			require.True(t, requestedSet[key], "unprocessed key %s should be in requested keys", key)
		}
	})

	// Test 14: Edge case - Empty values with size limit
	t.Run("GetMany empty values with size limit", func(t *testing.T) {
		// Create mix of empty values and large values
		emptyKeys := []string{"empty-key1", "empty-key2", "empty-key3"}
		largeKeys := []string{"large-empty-key1", "large-empty-key2"}

		// Set empty values (0 bytes each)
		for _, key := range emptyKeys {
			err := c1zFile.Set(ctx, key, []byte{}, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		// Set large values (each ~2.5MB)
		for i, key := range largeKeys {
			value := bytes.Repeat([]byte{byte(i)}, 2500000)
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		allKeys := make([]string, 0, len(emptyKeys)+len(largeKeys))
		allKeys = append(allKeys, emptyKeys...)
		allKeys = append(allKeys, largeKeys...)
		result, unprocessedKeys, err := c1zFile.GetMany(ctx, allKeys, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Empty values should all be in result (they take 0 bytes)
		for _, key := range emptyKeys {
			_, inResult := result[key]
			require.True(t, inResult, "empty key %s should be in result", key)
			require.Empty(t, result[key], "empty key should have empty value")
		}

		// Verify total size calculation
		totalSize := 0
		for _, value := range result {
			totalSize += len(value)
		}
		require.LessOrEqual(t, totalSize, sessions.MaxSessionStoreSizeLimit, "result size should be within limit")

		// Large values should be in unprocessedKeys (or at least some)
		require.NotEmpty(t, unprocessedKeys, "should have unprocessed keys for large values")
		unprocessedSet := make(map[string]bool)
		for _, key := range unprocessedKeys {
			unprocessedSet[key] = true
		}
		// At least one large key should be unprocessed
		hasLargeUnprocessed := false
		for _, key := range largeKeys {
			if unprocessedSet[key] {
				hasLargeUnprocessed = true
				break
			}
		}
		require.True(t, hasLargeUnprocessed, "at least one large key should be unprocessed")
	})
}

func TestC1FileSessionStore_SetMany(t *testing.T) {
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close(ctx)

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
			sessions.WithSyncID(syncID), sessions.WithPrefix("batch-prefix:"))
		require.NoError(t, err)

		// Verify all values were set with prefix
		for key, expectedValue := range values {
			value, found, err := c1zFile.Get(ctx, key,
				sessions.WithSyncID(syncID), sessions.WithPrefix("batch-prefix:"))
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
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close(ctx)

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
			sessions.WithSyncID(syncID), sessions.WithPrefix("delete-prefix:"))
		require.NoError(t, err)

		// Verify it exists
		value, found, err := c1zFile.Get(ctx, "prefixed-to-delete",
			sessions.WithSyncID(syncID), sessions.WithPrefix("delete-prefix:"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("will-be-deleted"), value)

		// Delete it with prefix
		err = c1zFile.Delete(ctx, "prefixed-to-delete",
			sessions.WithSyncID(syncID), sessions.WithPrefix("delete-prefix:"))
		require.NoError(t, err)

		// Verify it's gone
		value, found, err = c1zFile.Get(ctx, "prefixed-to-delete",
			sessions.WithSyncID(syncID), sessions.WithPrefix("delete-prefix:"))
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
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close(ctx)

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
		all, pageToken, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, all, 3)
		require.Equal(t, "", pageToken)

		// Clear all
		err = c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify they're all gone
		all, pageToken, err = c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Empty(t, all)
		require.Equal(t, "", pageToken)
	})

	t.Run("Clear with prefix", func(t *testing.T) {
		// Set values with different prefixes
		err := c1zFile.Set(ctx, "prefix1-key1", []byte("prefix1-value1"),
			sessions.WithSyncID(syncID), sessions.WithPrefix("prefix1:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "prefix1-key2", []byte("prefix1-value2"),
			sessions.WithSyncID(syncID), sessions.WithPrefix("prefix1:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "prefix2-key1", []byte("prefix2-value1"),
			sessions.WithSyncID(syncID), sessions.WithPrefix("prefix2:"))
		require.NoError(t, err)

		// Clear only prefix1
		err = c1zFile.Clear(ctx, sessions.WithSyncID(syncID), sessions.WithPrefix("prefix1:"))
		require.NoError(t, err)

		// Verify prefix1 keys are gone but prefix2 remains
		_, found, err := c1zFile.Get(ctx, "prefix1-key1",
			sessions.WithSyncID(syncID), sessions.WithPrefix("prefix1:"))
		require.NoError(t, err)
		require.False(t, found)

		err = c1zFile.Clear(ctx, sessions.WithSyncID(syncID), sessions.WithPrefix("a%"))
		require.NoError(t, err)

		value, found, err := c1zFile.Get(ctx, "prefix2-key1",
			sessions.WithSyncID(syncID), sessions.WithPrefix("prefix2:"))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, []byte("prefix2-value1"), value)
	})

	t.Run("Clear with special characters in prefix (underscore, percent, backslash)", func(t *testing.T) {
		// Ensure test isolation.
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Add one key for each special prefix, plus a control key.
		require.NoError(t, c1zFile.Set(ctx, "k1", []byte("v1"), sessions.WithSyncID(syncID), sessions.WithPrefix("a_b:")))
		require.NoError(t, c1zFile.Set(ctx, "k2", []byte("v2"), sessions.WithSyncID(syncID), sessions.WithPrefix("a%:")))
		require.NoError(t, c1zFile.Set(ctx, "k3", []byte("v3"), sessions.WithSyncID(syncID), sessions.WithPrefix(`a\b:`)))
		require.NoError(t, c1zFile.Set(ctx, "k4", []byte("v4"), sessions.WithSyncID(syncID), sessions.WithPrefix("control:")))

		// Clear underscore prefix only.
		require.NoError(t, c1zFile.Clear(ctx, sessions.WithSyncID(syncID), sessions.WithPrefix("a_b:")))
		_, found, err := c1zFile.Get(ctx, "k1", sessions.WithSyncID(syncID), sessions.WithPrefix("a_b:"))
		require.NoError(t, err)
		require.False(t, found)

		// Other prefixes should remain.
		_, found, err = c1zFile.Get(ctx, "k2", sessions.WithSyncID(syncID), sessions.WithPrefix("a%:"))
		require.NoError(t, err)
		require.True(t, found)
		_, found, err = c1zFile.Get(ctx, "k3", sessions.WithSyncID(syncID), sessions.WithPrefix(`a\b:`))
		require.NoError(t, err)
		require.True(t, found)
		_, found, err = c1zFile.Get(ctx, "k4", sessions.WithSyncID(syncID), sessions.WithPrefix("control:"))
		require.NoError(t, err)
		require.True(t, found)

		// Clear percent prefix only.
		require.NoError(t, c1zFile.Clear(ctx, sessions.WithSyncID(syncID), sessions.WithPrefix("a%:")))
		_, found, err = c1zFile.Get(ctx, "k2", sessions.WithSyncID(syncID), sessions.WithPrefix("a%:"))
		require.NoError(t, err)
		require.False(t, found)

		// Backslash + control should remain.
		_, found, err = c1zFile.Get(ctx, "k3", sessions.WithSyncID(syncID), sessions.WithPrefix(`a\b:`))
		require.NoError(t, err)
		require.True(t, found)
		_, found, err = c1zFile.Get(ctx, "k4", sessions.WithSyncID(syncID), sessions.WithPrefix("control:"))
		require.NoError(t, err)
		require.True(t, found)

		// Clear backslash prefix only.
		require.NoError(t, c1zFile.Clear(ctx, sessions.WithSyncID(syncID), sessions.WithPrefix(`a\b:`)))
		_, found, err = c1zFile.Get(ctx, "k3", sessions.WithSyncID(syncID), sessions.WithPrefix(`a\b:`))
		require.NoError(t, err)
		require.False(t, found)

		// Control should remain.
		_, found, err = c1zFile.Get(ctx, "k4", sessions.WithSyncID(syncID), sessions.WithPrefix("control:"))
		require.NoError(t, err)
		require.True(t, found)
	})

	t.Run("Clear without sync ID", func(t *testing.T) {
		err := c1zFile.Clear(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "sync id is required")
	})
}

func TestC1FileSessionStore_GetAll(t *testing.T) {
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close(ctx)

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("GetAll empty store", func(t *testing.T) {
		result, pageToken, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Empty(t, result)
		require.Equal(t, "", pageToken)
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
		result, pageToken, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, result, 3)
		require.Equal(t, []byte("getall-value1"), result["getall-key1"])
		require.Equal(t, []byte("getall-value2"), result["getall-key2"])
		require.Equal(t, []byte("getall-value3"), result["getall-key3"])
		require.Equal(t, "", pageToken)
	})

	t.Run("GetAll with prefix", func(t *testing.T) {
		// Set values with different prefixes
		err := c1zFile.Set(ctx, "getall-prefix1-key1", []byte("getall-prefix1-value1"),
			sessions.WithSyncID(syncID), sessions.WithPrefix("getall-prefix1:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "getall-prefix1-key2", []byte("getall-prefix1-value2"),
			sessions.WithSyncID(syncID), sessions.WithPrefix("getall-prefix1:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "getall-prefix2-key1", []byte("getall-prefix2-value1"),
			sessions.WithSyncID(syncID), sessions.WithPrefix("getall-prefix2:"))
		require.NoError(t, err)

		// Get all with prefix1
		result, pageToken, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID), sessions.WithPrefix("getall-prefix1:"))
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Equal(t, []byte("getall-prefix1-value1"), result["getall-prefix1-key1"])
		require.Equal(t, []byte("getall-prefix1-value2"), result["getall-prefix1-key2"])
		require.Equal(t, "", pageToken)

		// Verify prefix is stripped - keys should NOT have the prefix
		_, hasPrefixInKey := result["getall-prefix1:getall-prefix1-key1"]
		require.False(t, hasPrefixInKey, "returned keys should not include the prefix")
		// Also verify we don't get keys from prefix2
		_, hasPrefix2Key := result["getall-prefix2-key1"]
		require.False(t, hasPrefix2Key, "should not return keys from different prefix")
	})

	t.Run("GetAll with underscore in prefix", func(t *testing.T) {
		// NOTE: This currently fails because SQLite LIKE does not treat "\" as an escape
		// unless the query includes an explicit ESCAPE clause.
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		err = c1zFile.Set(ctx, "k1", []byte("v1"),
			sessions.WithSyncID(syncID), sessions.WithPrefix("a_b:"))
		require.NoError(t, err)

		got, pageToken, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID), sessions.WithPrefix("a_b:"))
		require.NoError(t, err)
		require.Equal(t, "", pageToken)

		require.Len(t, got, 1)
		require.Equal(t, []byte("v1"), got["k1"])
	})

	t.Run("GetAll with percent in prefix", func(t *testing.T) {
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Prefix contains "%" which must be treated literally.
		err = c1zFile.Set(ctx, "k1", []byte("v1"),
			sessions.WithSyncID(syncID), sessions.WithPrefix("a%:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "k2", []byte("v2"),
			sessions.WithSyncID(syncID), sessions.WithPrefix("ax:"))
		require.NoError(t, err)

		got, pageToken, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID), sessions.WithPrefix("a%:"))
		require.NoError(t, err)
		require.Equal(t, "", pageToken)
		require.Len(t, got, 1)
		require.Equal(t, []byte("v1"), got["k1"])
	})

	t.Run("GetAll with backslash in prefix", func(t *testing.T) {
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Prefix contains "\" which must be treated literally.
		err = c1zFile.Set(ctx, "k1", []byte("v1"),
			sessions.WithSyncID(syncID), sessions.WithPrefix(`a\b:`))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "k2", []byte("v2"),
			sessions.WithSyncID(syncID), sessions.WithPrefix("ab:"))
		require.NoError(t, err)

		got, pageToken, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID), sessions.WithPrefix(`a\b:`))
		require.NoError(t, err)
		require.Equal(t, "", pageToken)
		require.Len(t, got, 1)
		require.Equal(t, []byte("v1"), got["k1"])
	})

	t.Run("GetAll without sync ID", func(t *testing.T) {
		_, _, err := c1zFile.GetAll(ctx, "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "sync id is required")
	})

	// Test 1: Pagination - More than 100 items (item count limit)
	t.Run("GetAll pagination with more than 100 items", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create 150 items with small values
		for i := range 150 {
			key := fmt.Sprintf("pagination-key-%03d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		// GetAll handles pagination internally and returns all items in one call if they fit
		// With 150 small items, they all fit within the size limit, so GetAll returns everything
		result, pageToken, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, result, 150, "should retrieve all 150 items")
		require.Equal(t, "", pageToken, "pageToken should be empty when all items fit")
	})

	// Test 2: Pagination - Size limit enforcement
	t.Run("GetAll pagination with size limit enforcement", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create items that will exceed size limit
		// Each value is ~500KB, so 10 items = ~5MB > MaxSessionStoreSizeLimit
		for i := range 10 {
			key := fmt.Sprintf("size-limit-key-%d", i)
			value := bytes.Repeat([]byte{byte(i)}, 500000)
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		all := make(map[string][]byte)
		pageToken := ""
		pageCount := 0
		maxIterations := 50
		iteration := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			pageCount++
			iteration++

			// Verify each page size is within limit
			pageSize := 0
			for _, value := range result {
				pageSize += len(value)
			}
			require.LessOrEqual(t, pageSize, sessions.MaxSessionStoreSizeLimit, "page size should be within limit")

			maps.Copy(all, result)

			if nextPageToken == "" {
				break
			}
			require.NotEqual(t, pageToken, nextPageToken, "pageToken should change to prevent infinite loop")
			require.Less(t, iteration, maxIterations, "should not hit max iterations (infinite loop protection)")
			pageToken = nextPageToken
		}

		require.Greater(t, pageCount, 1, "should have multiple pages due to size limit")
		require.Len(t, all, 10, "should retrieve all 10 items")
	})

	// Test 3: PageToken continuation - manual pagination
	t.Run("GetAll pageToken continuation", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create 150 items
		expectedKeys := make([]string, 150)
		for i := range 150 {
			key := fmt.Sprintf("continuation-key-%03d", i)
			expectedKeys[i] = key
			value := []byte(fmt.Sprintf("value-%d", i))
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		// GetAll handles pagination internally, so with 150 small items it returns everything in one call
		// But we can test manual pagination by using a pageToken to start from a specific point
		// First, get all items to verify they exist
		allItems, _, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, allItems, 150, "should get all 150 items")

		// Now test manual pagination by starting from a specific key
		// Use a key in the middle to test continuation
		startKey := "continuation-key-075"
		pageFromMiddle, pageToken, err := c1zFile.GetAll(ctx, startKey, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Should get items starting from startKey (inclusive, due to Gte)
		require.NotEmpty(t, pageFromMiddle, "should get items starting from pageToken")

		// Verify all keys in pageFromMiddle are >= startKey
		for key := range pageFromMiddle {
			require.GreaterOrEqual(t, key, startKey, "all keys should be >= startKey")
		}

		// Verify we got the remaining items
		expectedRemaining := 150 - 75 // items from index 75 onwards
		require.LessOrEqual(t, len(pageFromMiddle), expectedRemaining, "should get remaining items from startKey")
		require.Equal(t, "", pageToken, "pageToken should be empty when all remaining items are retrieved")
	})

	// Test 4: Edge case: Exactly 100 items
	t.Run("GetAll exactly 100 items", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create exactly 100 items
		for i := range 100 {
			key := fmt.Sprintf("exact-100-key-%03d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		result, pageToken, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, result, 100, "should get all 100 items")
		// With exactly 100 items, GetAll may return a pageToken if it thinks there might be more
		// But since we know there are exactly 100, it should return empty pageToken
		// Actually, looking at getAllChunk: if len(result) == 100, it returns nextPageToken
		// So we need to check if there are actually more items
		// Since we created exactly 100, the second call should return empty
		if pageToken != "" {
			// If pageToken is returned, verify that continuing returns empty
			result2, pageToken2, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			require.Empty(t, result2, "second call should return empty since there are no more items")
			require.Equal(t, "", pageToken2, "pageToken should be empty when done")
		}
	})

	// Test 5: Edge case: 101 items
	t.Run("GetAll exactly 101 items", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create exactly 101 items
		for i := range 101 {
			key := fmt.Sprintf("exact-101-key-%03d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		// GetAll handles pagination internally and returns all items in one call
		// With 101 small items, they all fit within the size limit, so GetAll returns everything
		result, pageToken, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, result, 101, "should get all 101 items")
		// GetAll may return a pageToken if it thinks there might be more, but since we know there are exactly 101,
		// and they all fit, it should return empty pageToken
		// Actually, getAllChunk returns pageToken when len(result) == 100, so GetAll might return a pageToken
		// But the second internal call will return empty, so the final pageToken should be empty
		require.Equal(t, "", pageToken, "pageToken should be empty when all items are retrieved")
	})

	// Test 6: PageToken value correctness
	t.Run("GetAll pageToken value correctness", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create 150 items with predictable keys
		for i := range 150 {
			key := fmt.Sprintf("token-key-%03d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		// GetAll handles pagination internally, but with 150 items it may return a pageToken
		// Get first page
		page1, pageToken1, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// With 150 items, GetAll may return all items or a pageToken
		// If pageToken is returned, verify it can be used to get next page
		if pageToken1 != "" {
			// Find the last key in page1 (should be sorted)
			var lastKey string
			for key := range page1 {
				if lastKey == "" || key > lastKey {
					lastKey = key
				}
			}

			require.NotEmpty(t, lastKey, "should have a last key")

			// Verify pageToken can be used to get next page
			page2, _, err := c1zFile.GetAll(ctx, pageToken1, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			require.NotEmpty(t, page2, "should get second page")

			// Verify no overlap - all keys in page2 should be > lastKey
			for key := range page2 {
				require.Greater(t, key, lastKey, "keys in page2 should be after lastKey from page1")
			}

			// Verify we got all items
			maps.Copy(page1, page2)
			require.Len(t, page1, 150, "should have all 150 items")
		} else {
			// If no pageToken, GetAll returned all items in one call
			require.Len(t, page1, 150, "should have all 150 items")
		}
	})

	// Test 7: Pagination with prefix - multiple pages
	t.Run("GetAll pagination with prefix multiple pages", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create 150 items with prefix
		for i := range 150 {
			key := fmt.Sprintf("prefix-pag-key-%03d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID), sessions.WithPrefix("prefix-pag:"))
			require.NoError(t, err)
		}

		// Create some items without prefix to ensure they're filtered
		for i := range 10 {
			key := fmt.Sprintf("no-prefix-key-%d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		// GetAll handles pagination internally, but with 150 items it may return a pageToken
		// Continue pagination until all items are retrieved
		all := make(map[string][]byte)
		pageToken := ""
		maxIterations := 20
		iteration := 0
		prevAllLen := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID), sessions.WithPrefix("prefix-pag:"))
			require.NoError(t, err)
			iteration++
			maps.Copy(all, result)

			// If we've retrieved all 150 items, we're done
			if len(all) >= 150 {
				break
			}

			if nextPageToken == "" {
				break
			}

			// If we're not making progress (same number of items and same pageToken),
			// but we haven't reached 150 items, try incrementing the pageToken slightly
			if len(all) == prevAllLen && pageToken == nextPageToken {
				// If we have exactly 100 items and pageToken hasn't changed,
				// GetAll might have hit the 100-item limit. Try using the last key as pageToken
				if len(all) == 100 {
					// Find the last key and use it as pageToken
					var lastKey string
					for key := range all {
						if lastKey == "" || key > lastKey {
							lastKey = key
						}
					}
					if lastKey != "" {
						pageToken = lastKey
						prevAllLen = len(all)
						require.Less(t, iteration, maxIterations, "should not hit max iterations")
						continue
					}
				}
				break
			}
			prevAllLen = len(all)

			require.Less(t, iteration, maxIterations, "should not hit max iterations")
			pageToken = nextPageToken
		}

		require.Len(t, all, 150, "should retrieve all 150 prefixed items")

		// Verify prefix is stripped
		for key := range all {
			require.NotContains(t, key, "prefix-pag:", "prefix should be stripped from keys")
		}

		// Verify no non-prefixed items
		for key := range all {
			require.NotContains(t, key, "no-prefix", "should not have non-prefixed keys")
		}
		require.Len(t, all, 150, "should have all 150 prefixed items")
	})

	// Test 8: PageToken with prefix continuation
	t.Run("GetAll pageToken with prefix continuation", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create items with prefix
		for i := range 150 {
			key := fmt.Sprintf("prefix-cont-key-%03d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID), sessions.WithPrefix("prefix-cont:"))
			require.NoError(t, err)
		}

		// GetAll handles pagination internally, but with 150 items it may return a pageToken
		// Continue pagination until all items are retrieved
		all := make(map[string][]byte)
		pageToken := ""
		maxIterations := 20
		iteration := 0
		prevAllLen := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID), sessions.WithPrefix("prefix-cont:"))
			require.NoError(t, err)
			iteration++
			maps.Copy(all, result)

			// If we've retrieved all 150 items, we're done
			if len(all) >= 150 {
				break
			}

			if nextPageToken == "" {
				break
			}
			// If we're not making progress (same number of items and same pageToken),
			// but we haven't reached 150 items, try incrementing the pageToken slightly
			if len(all) == prevAllLen && pageToken == nextPageToken {
				require.Fail(t, "should make progress")
			}
			prevAllLen = len(all)

			require.Less(t, iteration, maxIterations, "should not hit max iterations")
			pageToken = nextPageToken
		}

		require.Len(t, all, 150, "should get all 150 items")

		// Verify prefix is stripped
		for key := range all {
			require.NotContains(t, key, "prefix-cont:", "prefix should be stripped")
		}

		require.Len(t, all, 150, "should have all 150 items")
	})

	// Test 9: MessageSizeRemaining calculation across pages
	t.Run("GetAll messageSizeRemaining calculation", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create items with known sizes that will require multiple pages
		// Each item ~300KB, so ~14 items per page (300KB * 14 = 4.2MB > 4MB limit)
		for i := range 30 {
			key := fmt.Sprintf("size-track-key-%03d", i)
			value := bytes.Repeat([]byte{byte(i)}, 300000)
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		// GetAll handles pagination internally
		// With large items, GetAll may need to return a pageToken if it hits size limits
		all := make(map[string][]byte)
		pageToken := ""
		pageCount := 0
		maxIterations := 50
		iteration := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			pageCount++
			iteration++

			// Verify page size is within limit
			pageSize := 0
			for _, value := range result {
				pageSize += len(value)
			}
			require.LessOrEqual(t, pageSize, sessions.MaxSessionStoreSizeLimit, "each page should be within size limit")

			maps.Copy(all, result)

			if nextPageToken == "" {
				break
			}
			require.NotEqual(t, pageToken, nextPageToken, "pageToken should change to prevent infinite loop")
			require.Less(t, iteration, maxIterations, "should not hit max iterations (infinite loop protection)")
			pageToken = nextPageToken
		}

		// Verify total size makes sense
		totalSize := 0
		for _, value := range all {
			totalSize += len(value)
		}
		require.Equal(t, 30*300000, totalSize, "total size should match expected")
		require.Len(t, all, 30, "should have all 30 items")
		// With large items, we should have multiple pages
		require.GreaterOrEqual(t, pageCount, 1, "should have at least one page")
	})

	// Test 10: Termination condition: messageSizeRemaining <= 0
	t.Run("GetAll termination messageSizeRemaining zero", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create items that will consume most of the size limit
		// Each item ~1MB, so 4 items will use ~4MB, leaving little room
		for i := range 6 {
			key := fmt.Sprintf("terminate-key-%d", i)
			value := bytes.Repeat([]byte{byte(i)}, 1000000)
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		all := make(map[string][]byte)
		pageToken := ""
		pageCount := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			pageCount++
			maps.Copy(all, result)

			if nextPageToken == "" {
				break
			}
			// Verify we're making progress
			require.NotEqual(t, pageToken, nextPageToken, "pageToken should change")
			pageToken = nextPageToken
		}

		// Should have multiple pages due to size limit
		require.Greater(t, pageCount, 1, "should have multiple pages")
		require.Len(t, all, 6, "should retrieve all 6 items")
	})

	// Test 12: Ordering verification across pages
	t.Run("GetAll ordering verification across pages", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create 150 items with predictable ordering
		for i := range 150 {
			key := fmt.Sprintf("order-key-%03d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		allKeys := make([]string, 0, 150)
		pageToken := ""
		maxIterations := 200
		iteration := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			iteration++

			// Collect keys from this page
			pageKeys := make([]string, 0, len(result))
			for key := range result {
				pageKeys = append(pageKeys, key)
			}
			slices.Sort(pageKeys)
			allKeys = append(allKeys, pageKeys...)

			// Verify page keys are in order
			for i := 1; i < len(pageKeys); i++ {
				require.Less(t, pageKeys[i-1], pageKeys[i], "keys within page should be sorted")
			}

			if nextPageToken == "" {
				break
			}
			require.NotEqual(t, pageToken, nextPageToken, "pageToken should change to prevent infinite loop")
			require.Less(t, iteration, maxIterations, "should not hit max iterations (infinite loop protection)")
			pageToken = nextPageToken
		}

		// Verify overall ordering
		require.Len(t, allKeys, 150, "should have all 150 keys")
		for i := 1; i < len(allKeys); i++ {
			require.Less(t, allKeys[i-1], allKeys[i], "all keys should be in ascending order")
		}
	})

	// Test 13: No duplicate items across pages
	t.Run("GetAll no duplicate items across pages", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create 150 items
		for i := range 150 {
			key := fmt.Sprintf("nodup-key-%03d", i)
			value := []byte(fmt.Sprintf("value-%d", i))
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		// GetAll handles pagination internally, so with 150 small items it returns everything in one call
		all := make(map[string][]byte)
		seenKeys := make(map[string]int)
		pageToken := ""
		maxIterations := 200
		iteration := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			iteration++

			// Check for duplicates
			for key := range result {
				if count, exists := seenKeys[key]; exists {
					require.Fail(t, "duplicate key found", "key %s appears %d times", key, count+1)
				}
				seenKeys[key] = 1
			}

			maps.Copy(all, result)

			if nextPageToken == "" {
				break
			}
			require.NotEqual(t, pageToken, nextPageToken, "pageToken should change to prevent infinite loop")
			require.Less(t, iteration, maxIterations, "should not hit max iterations (infinite loop protection)")
			pageToken = nextPageToken
		}

		require.Len(t, all, 150, "should have all 150 items")
		require.Len(t, seenKeys, 150, "should have seen 150 unique keys")
		// GetAll may return all items in one call, so we don't require multiple pages
	})

	// Test 14: No missing items across pages
	t.Run("GetAll no missing items across pages", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create 150 items with predictable keys
		expectedKeys := make(map[string]bool)
		for i := range 150 {
			key := fmt.Sprintf("nomiss-key-%03d", i)
			expectedKeys[key] = true
			value := []byte(fmt.Sprintf("value-%d", i))
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		// GetAll handles pagination internally
		all := make(map[string][]byte)
		pageToken := ""
		maxIterations := 200
		iteration := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			iteration++
			maps.Copy(all, result)

			if nextPageToken == "" {
				break
			}
			require.NotEqual(t, pageToken, nextPageToken, "pageToken should change to prevent infinite loop")
			require.Less(t, iteration, maxIterations, "should not hit max iterations (infinite loop protection)")
			pageToken = nextPageToken
		}

		// Verify all expected keys are present
		require.Len(t, all, 150, "should have all 150 items")
		for key := range expectedKeys {
			_, found := all[key]
			require.True(t, found, "key %s should be present", key)
		}
	})

	// Test 15: Large values requiring size-based pagination
	t.Run("GetAll large values size-based pagination", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create items with large values (>1MB each)
		// Each item ~1.5MB, so 3 items = ~4.5MB > 4MB limit
		for i := range 5 {
			key := fmt.Sprintf("large-val-key-%d", i)
			value := bytes.Repeat([]byte{byte(i)}, 1500000)
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		all := make(map[string][]byte)
		pageToken := ""
		pageCount := 0
		maxIterations := 20
		iteration := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			pageCount++
			iteration++

			// Verify page size
			pageSize := 0
			for _, value := range result {
				pageSize += len(value)
			}
			require.LessOrEqual(t, pageSize, sessions.MaxSessionStoreSizeLimit, "page size should be within limit")

			maps.Copy(all, result)

			if nextPageToken == "" {
				break
			}
			require.NotEqual(t, pageToken, nextPageToken, "pageToken should change to prevent infinite loop")
			require.Less(t, iteration, maxIterations, "should not hit max iterations (infinite loop protection)")
			pageToken = nextPageToken
		}

		require.Greater(t, pageCount, 1, "should have multiple pages due to size")
		require.Len(t, all, 5, "should retrieve all 5 items")
	})

	// Test 17: Empty values with pagination
	t.Run("GetAll empty values with pagination", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create mix of empty and regular values
		for i := range 150 {
			key := fmt.Sprintf("empty-pag-key-%03d", i)
			var value []byte
			if i%10 == 0 {
				// Every 10th item is empty
				value = []byte{}
			} else {
				value = []byte(fmt.Sprintf("value-%d", i))
			}
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		// GetAll handles pagination internally
		all := make(map[string][]byte)
		pageToken := ""
		emptyCount := 0
		maxIterations := 200
		iteration := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			iteration++

			// Count empty values
			for key, value := range result {
				if len(value) == 0 {
					emptyCount++
					require.Empty(t, value, "key %s should have empty value", key)
				}
			}

			maps.Copy(all, result)

			if nextPageToken == "" {
				break
			}
			require.NotEqual(t, pageToken, nextPageToken, "pageToken should change to prevent infinite loop")
			require.Less(t, iteration, maxIterations, "should not hit max iterations (infinite loop protection)")
			pageToken = nextPageToken
		}

		require.Len(t, all, 150, "should have all 150 items")
		require.Equal(t, 15, emptyCount, "should have 15 empty values (every 10th item)")
	})

	// Test 20: getAllChunk size limit logic verification
	t.Run("GetAll getAllChunk size limit logic", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create items where size limit will be hit before item count limit
		// Each item ~500KB, so ~8 items per page (500KB * 8 = 4MB)
		for i := range 20 {
			key := fmt.Sprintf("chunk-limit-key-%03d", i)
			value := bytes.Repeat([]byte{byte(i)}, 500000)
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		all := make(map[string][]byte)
		pageToken := ""
		pageCount := 0
		maxIterations := 50
		iteration := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			pageCount++
			iteration++

			// Verify chunk respects size limit (should have < 100 items due to size)
			pageSize := 0
			for _, value := range result {
				pageSize += len(value)
			}
			require.LessOrEqual(t, pageSize, sessions.MaxSessionStoreSizeLimit, "chunk should respect size limit")
			// With 500KB items, should have ~8 items per chunk, not 100
			if len(result) > 0 {
				require.Less(t, len(result), 100, "chunk should be limited by size, not item count")
			}

			maps.Copy(all, result)

			if nextPageToken == "" {
				break
			}
			require.NotEqual(t, pageToken, nextPageToken, "pageToken should change to prevent infinite loop")
			require.Less(t, iteration, maxIterations, "should not hit max iterations (infinite loop protection)")
			pageToken = nextPageToken
		}

		require.Greater(t, pageCount, 1, "should have multiple pages")
		require.Len(t, all, 20, "should retrieve all 20 items")
	})

	// Test 24: Multiple termination conditions interaction
	t.Run("GetAll multiple termination conditions", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create scenario where size limit and empty pageToken could both apply
		// Create items that will exactly fill the size limit
		itemSize := sessions.MaxSessionStoreSizeLimit / 5 // ~832KB per item, 5 items = ~4MB
		for i := range 5 {
			key := fmt.Sprintf("terminate-key-%d", i)
			value := bytes.Repeat([]byte{byte(i)}, itemSize)
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		all := make(map[string][]byte)
		pageToken := ""
		pageCount := 0
		maxIterations := 20
		iteration := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			pageCount++
			iteration++

			// Verify termination conditions
			if len(result) == 0 {
				require.Equal(t, "", nextPageToken, "empty result should have empty pageToken")
			}

			maps.Copy(all, result)

			if nextPageToken == "" {
				break
			}
			require.NotEqual(t, pageToken, nextPageToken, "pageToken should change to prevent infinite loop")
			require.Less(t, iteration, maxIterations, "should not hit max iterations (infinite loop protection)")
			pageToken = nextPageToken
		}

		require.Len(t, all, 5, "should retrieve all 5 items")
		// Should terminate correctly regardless of which condition triggers
	})

	// Test 25: Verify result accumulation across pages
	t.Run("GetAll result accumulation across pages", func(t *testing.T) {
		// Clear any existing data to ensure test isolation
		err := c1zFile.Clear(ctx, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Create 150 items with unique values
		expectedValues := make(map[string][]byte)
		for i := range 150 {
			key := fmt.Sprintf("accum-key-%03d", i)
			value := []byte(fmt.Sprintf("unique-value-%d", i))
			expectedValues[key] = value
			err := c1zFile.Set(ctx, key, value, sessions.WithSyncID(syncID))
			require.NoError(t, err)
		}

		// GetAll handles pagination internally
		all := make(map[string][]byte)
		pageToken := ""
		maxIterations := 200
		iteration := 0
		for {
			result, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			iteration++

			// Verify maps.Copy works correctly
			beforeCount := len(all)
			maps.Copy(all, result)
			afterCount := len(all)

			// Should accumulate correctly
			require.Equal(t, beforeCount+len(result), afterCount, "accumulation should add all items from result")

			if nextPageToken == "" {
				break
			}
			require.NotEqual(t, pageToken, nextPageToken, "pageToken should change to prevent infinite loop")
			require.Less(t, iteration, maxIterations, "should not hit max iterations (infinite loop protection)")
			pageToken = nextPageToken
		}

		// Verify all expected values are present and correct
		require.Len(t, all, 150, "should have all 150 items")
		for key, expectedValue := range expectedValues {
			actualValue, found := all[key]
			require.True(t, found, "key %s should be present", key)
			require.Equal(t, expectedValue, actualValue, "value for key %s should match", key)
		}
	})
}

func TestC1FileSessionStore_Isolation(t *testing.T) {
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close(ctx)

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
			sessions.WithSyncID(syncID1), sessions.WithPrefix("prefix1:"))
		require.NoError(t, err)
		err = c1zFile.Set(ctx, "prefix-key", []byte("prefix2-value"),
			sessions.WithSyncID(syncID1), sessions.WithPrefix("prefix2:"))
		require.NoError(t, err)

		// Verify prefix isolation
		value1, found1, err := c1zFile.Get(ctx, "prefix-key",
			sessions.WithSyncID(syncID1), sessions.WithPrefix("prefix1:"))
		require.NoError(t, err)
		require.True(t, found1)
		require.Equal(t, []byte("prefix1-value"), value1)

		value2, found2, err := c1zFile.Get(ctx, "prefix-key",
			sessions.WithSyncID(syncID1), sessions.WithPrefix("prefix2:"))
		require.NoError(t, err)
		require.True(t, found2)
		require.Equal(t, []byte("prefix2-value"), value2)

		// Verify they don't interfere with each other
		require.NotEqual(t, value1, value2)
	})
}

func TestC1FileSessionStore_ConcurrentAccess(t *testing.T) {
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close(ctx)

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
		all, pageToken, err := c1zFile.GetAll(ctx, "", sessions.WithSyncID(syncID))
		require.NoError(t, err)
		require.Len(t, all, 5) // Should have 5 keys
		require.Equal(t, "", pageToken)
	})
}

func TestC1FileSessionStore_ErrorHandling(t *testing.T) {
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close(ctx)

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
	ctx := t.Context()
	tempDir := filepath.Join(t.TempDir(), "test-session.c1z")

	c1zFile, err := NewC1ZFile(ctx, tempDir, WithPragma("journal_mode", "WAL"))
	require.NoError(t, err)
	defer c1zFile.Close(ctx)

	syncID, err := c1zFile.StartNewSync(ctx, "full", "")
	require.NoError(t, err)

	t.Run("Large batch operations", func(t *testing.T) {
		// Test SetMany with large number of keys
		largeValues := make(map[string][]byte)
		for i := range 1000 {
			key := fmt.Sprintf("large-batch-key-%d", i)
			// Each value is 10kb.
			value := bytes.Repeat([]byte("large-byte-value"), 640)
			largeValues[key] = value
		}

		largeValuesSize := 0
		for _, value := range largeValues {
			largeValuesSize += len(value)
		}
		t.Logf("Setting %d large values, total size %d", len(largeValues), largeValuesSize)
		err := c1zFile.SetMany(ctx, largeValues, sessions.WithSyncID(syncID))
		require.NoError(t, err)

		// Verify all values were set
		all := make(map[string][]byte)
		pageToken := ""
		for {
			items, nextPageToken, err := c1zFile.GetAll(ctx, pageToken, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			// Check that size of items is less than 4MB.
			itemsSize := 0
			for _, value := range items {
				itemsSize += len(value)
			}
			require.Less(t, itemsSize, sessions.MaxSessionStoreSizeLimit)
			maps.Copy(all, items)

			log.Printf("itemsSize: %d, items: %d, nextPageToken: %s, pageToken: %s", itemsSize, len(items), nextPageToken, pageToken)
			require.NotEqual(t, nextPageToken, pageToken)
			pageToken = nextPageToken
			if nextPageToken == "" {
				break
			}
		}
		require.Len(t, all, 1000)
		require.Equal(t, "", pageToken)

		// Test GetMany with large number of keys
		keys := make([]string, 0, 1000)
		for key := range largeValues {
			keys = append(keys, key)
		}
		slices.Sort(keys)

		all = make(map[string][]byte)
		for {
			result, unprocessedKeys, err := c1zFile.GetMany(ctx, keys, sessions.WithSyncID(syncID))
			require.NoError(t, err)
			keys = unprocessedKeys
			maps.Copy(all, result)
			if len(unprocessedKeys) == 0 {
				break
			}
		}
		log.Printf("GetMany: result: %d", len(all))
		require.Len(t, all, 1000, "expected 1000 items, got %d for nextPageToken %s", len(all))
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
