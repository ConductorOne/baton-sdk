package pebble

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPebbleEngine(t *testing.T) {
	ctx := context.Background()

	t.Run("successful creation with default options", func(t *testing.T) {
		tempDir := t.TempDir()

		engine, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		require.NotNil(t, engine)

		// Verify engine properties
		assert.True(t, engine.isOpen())
		assert.NotNil(t, engine.db)
		assert.Equal(t, filepath.Join(tempDir, "pebble.db"), engine.dbPath)
		assert.Equal(t, tempDir, engine.workingDir)
		assert.NotNil(t, engine.keyEncoder)
		assert.NotNil(t, engine.valueCodec)
		assert.NotNil(t, engine.options)

		// Verify default options
		opts := engine.options
		assert.Equal(t, int64(64<<20), opts.CacheSize)
		assert.Equal(t, int64(32<<20), opts.WriteBufferSize)
		assert.Equal(t, 1000, opts.MaxOpenFiles)
		assert.Equal(t, 10, opts.BloomFilterBits)
		assert.Equal(t, 4096, opts.BlockSize)
		assert.Equal(t, 1, opts.CompactionConcurrency)
		assert.Equal(t, 5*time.Second, opts.SlowQueryThreshold)
		assert.Equal(t, 1000, opts.BatchSizeLimit)
		assert.Equal(t, SyncPolicyPerBatch, opts.SyncPolicy)
		assert.False(t, opts.MetricsEnabled)
		assert.Equal(t, time.Minute, opts.MetricsInterval)

		// Clean up
		err = engine.Close()
		assert.NoError(t, err)
	})

	t.Run("successful creation with custom options", func(t *testing.T) {
		tempDir := t.TempDir()

		engine, err := NewPebbleEngine(ctx, tempDir,
			WithCacheSize(128<<20),
			WithWriteBufferSize(64<<20),
			WithMaxOpenFiles(2000),
			WithSlowQueryThreshold(10*time.Second),
			WithSyncPolicy(SyncPolicyNoSync),
		)
		require.NoError(t, err)
		require.NotNil(t, engine)

		// Verify custom options
		opts := engine.options
		assert.Equal(t, int64(128<<20), opts.CacheSize)
		assert.Equal(t, int64(64<<20), opts.WriteBufferSize)
		assert.Equal(t, 2000, opts.MaxOpenFiles)
		assert.Equal(t, 10*time.Second, opts.SlowQueryThreshold)
		assert.Equal(t, SyncPolicyNoSync, opts.SyncPolicy)

		// Clean up
		err = engine.Close()
		assert.NoError(t, err)
	})

	t.Run("empty working directory", func(t *testing.T) {
		_, err := NewPebbleEngine(ctx, "")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "working directory cannot be empty")
	})

	t.Run("invalid working directory permissions", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("Skipping permission test when running as root")
		}

		// Create a directory with no write permissions
		tempDir := t.TempDir()
		restrictedDir := filepath.Join(tempDir, "restricted")
		err := os.Mkdir(restrictedDir, 0444) // Read-only
		require.NoError(t, err)

		_, err = NewPebbleEngine(ctx, restrictedDir)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not writable")
	})

	t.Run("invalid options", func(t *testing.T) {
		tempDir := t.TempDir()

		testCases := []struct {
			name   string
			option PebbleOption
			errMsg string
		}{
			{
				name:   "negative cache size",
				option: WithCacheSize(-1),
				errMsg: "cache size must be positive",
			},
			{
				name:   "zero cache size",
				option: WithCacheSize(0),
				errMsg: "cache size must be positive",
			},
			{
				name:   "negative write buffer size",
				option: WithWriteBufferSize(-1),
				errMsg: "write buffer size must be positive",
			},
			{
				name:   "zero max open files",
				option: WithMaxOpenFiles(0),
				errMsg: "max open files must be positive",
			},
			{
				name:   "negative slow query threshold",
				option: WithSlowQueryThreshold(-time.Second),
				errMsg: "slow query threshold must be non-negative",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := NewPebbleEngine(ctx, tempDir, tc.option)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errMsg)
			})
		}
	})
}

func TestPebbleEngine_Close(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	t.Run("successful close", func(t *testing.T) {
		engine, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		require.True(t, engine.isOpen())

		err = engine.Close()
		assert.NoError(t, err)
		assert.False(t, engine.isOpen())
		assert.Nil(t, engine.db)
	})

	t.Run("double close", func(t *testing.T) {
		engine, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)

		// First close
		err = engine.Close()
		assert.NoError(t, err)

		// Second close should not error
		err = engine.Close()
		assert.NoError(t, err)
	})

	t.Run("close already closed engine", func(t *testing.T) {
		engine := &PebbleEngine{} // Empty engine, db is nil
		err := engine.Close()
		assert.NoError(t, err)
	})
}

func TestPebbleEngine_Dirty(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	t.Run("initially not dirty", func(t *testing.T) {
		engine, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		defer engine.Close()

		assert.False(t, engine.Dirty())
	})

	t.Run("becomes dirty after marking", func(t *testing.T) {
		engine, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		defer engine.Close()

		assert.False(t, engine.Dirty())

		engine.markDirty()
		assert.True(t, engine.Dirty())
	})
}

func TestPebbleEngine_OutputFilepath(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	t.Run("successful filepath retrieval", func(t *testing.T) {
		engine, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		defer engine.Close()

		expectedPath := filepath.Join(tempDir, "pebble.db")
		path, err := engine.OutputFilepath()
		assert.NoError(t, err)
		assert.Equal(t, expectedPath, path)
	})

	t.Run("empty database path", func(t *testing.T) {
		engine := &PebbleEngine{} // Empty engine
		_, err := engine.OutputFilepath()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database path not set")
	})

	t.Run("non-existent database path", func(t *testing.T) {
		engine := &PebbleEngine{
			dbPath: "/non/existent/path",
		}
		_, err := engine.OutputFilepath()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})
}

func TestPebbleEngine_SyncIDManagement(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	t.Run("current sync ID", func(t *testing.T) {
		// Initially empty
		assert.Equal(t, "", engine.getCurrentSyncID())

		// Set and get
		engine.setCurrentSyncID("sync-123")
		assert.Equal(t, "sync-123", engine.getCurrentSyncID())

		// Update
		engine.setCurrentSyncID("sync-456")
		assert.Equal(t, "sync-456", engine.getCurrentSyncID())
	})

	t.Run("view sync ID", func(t *testing.T) {
		// Initially empty
		assert.Equal(t, "", engine.getViewSyncID())

		// Set and get
		engine.setViewSyncID("view-123")
		assert.Equal(t, "view-123", engine.getViewSyncID())

		// Update
		engine.setViewSyncID("view-456")
		assert.Equal(t, "view-456", engine.getViewSyncID())
	})
}

func TestPebbleEngine_ValidationMethods(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	t.Run("isOpen and validateOpen with open database", func(t *testing.T) {
		engine, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)
		defer engine.Close()

		assert.True(t, engine.isOpen())
		assert.NoError(t, engine.validateOpen())
	})

	t.Run("isOpen and validateOpen with closed database", func(t *testing.T) {
		engine, err := NewPebbleEngine(ctx, tempDir)
		require.NoError(t, err)

		err = engine.Close()
		require.NoError(t, err)

		assert.False(t, engine.isOpen())
		err = engine.validateOpen()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database is not open")
	})
}

func TestValidateOptions(t *testing.T) {
	t.Run("valid options", func(t *testing.T) {
		opts := &PebbleOptions{
			CacheSize:             64 << 20,
			WriteBufferSize:       32 << 20,
			MaxOpenFiles:          1000,
			BloomFilterBits:       10,
			BlockSize:             4096,
			CompactionConcurrency: 1,
			SlowQueryThreshold:    5 * time.Second,
			BatchSizeLimit:        1000,
			MetricsInterval:       time.Minute,
		}

		err := validateOptions(opts)
		assert.NoError(t, err)
	})

	t.Run("invalid options", func(t *testing.T) {
		testCases := []struct {
			name   string
			opts   *PebbleOptions
			errMsg string
		}{
			{
				name: "negative cache size",
				opts: &PebbleOptions{
					CacheSize:             -1,
					WriteBufferSize:       32 << 20,
					MaxOpenFiles:          1000,
					BloomFilterBits:       10,
					BlockSize:             4096,
					CompactionConcurrency: 1,
					SlowQueryThreshold:    5 * time.Second,
					BatchSizeLimit:        1000,
					MetricsInterval:       time.Minute,
				},
				errMsg: "cache size must be positive",
			},
			{
				name: "zero write buffer size",
				opts: &PebbleOptions{
					CacheSize:             64 << 20,
					WriteBufferSize:       0,
					MaxOpenFiles:          1000,
					BloomFilterBits:       10,
					BlockSize:             4096,
					CompactionConcurrency: 1,
					SlowQueryThreshold:    5 * time.Second,
					BatchSizeLimit:        1000,
					MetricsInterval:       time.Minute,
				},
				errMsg: "write buffer size must be positive",
			},
			{
				name: "negative max open files",
				opts: &PebbleOptions{
					CacheSize:             64 << 20,
					WriteBufferSize:       32 << 20,
					MaxOpenFiles:          -1,
					BloomFilterBits:       10,
					BlockSize:             4096,
					CompactionConcurrency: 1,
					SlowQueryThreshold:    5 * time.Second,
					BatchSizeLimit:        1000,
					MetricsInterval:       time.Minute,
				},
				errMsg: "max open files must be positive",
			},
			{
				name: "negative bloom filter bits",
				opts: &PebbleOptions{
					CacheSize:             64 << 20,
					WriteBufferSize:       32 << 20,
					MaxOpenFiles:          1000,
					BloomFilterBits:       -1,
					BlockSize:             4096,
					CompactionConcurrency: 1,
					SlowQueryThreshold:    5 * time.Second,
					BatchSizeLimit:        1000,
					MetricsInterval:       time.Minute,
				},
				errMsg: "bloom filter bits must be non-negative",
			},
			{
				name: "zero block size",
				opts: &PebbleOptions{
					CacheSize:             64 << 20,
					WriteBufferSize:       32 << 20,
					MaxOpenFiles:          1000,
					BloomFilterBits:       10,
					BlockSize:             0,
					CompactionConcurrency: 1,
					SlowQueryThreshold:    5 * time.Second,
					BatchSizeLimit:        1000,
					MetricsInterval:       time.Minute,
				},
				errMsg: "block size must be positive",
			},
			{
				name: "zero compaction concurrency",
				opts: &PebbleOptions{
					CacheSize:             64 << 20,
					WriteBufferSize:       32 << 20,
					MaxOpenFiles:          1000,
					BloomFilterBits:       10,
					BlockSize:             4096,
					CompactionConcurrency: 0,
					SlowQueryThreshold:    5 * time.Second,
					BatchSizeLimit:        1000,
					MetricsInterval:       time.Minute,
				},
				errMsg: "compaction concurrency must be positive",
			},
			{
				name: "negative slow query threshold",
				opts: &PebbleOptions{
					CacheSize:             64 << 20,
					WriteBufferSize:       32 << 20,
					MaxOpenFiles:          1000,
					BloomFilterBits:       10,
					BlockSize:             4096,
					CompactionConcurrency: 1,
					SlowQueryThreshold:    -time.Second,
					BatchSizeLimit:        1000,
					MetricsInterval:       time.Minute,
				},
				errMsg: "slow query threshold must be non-negative",
			},
			{
				name: "zero batch size limit",
				opts: &PebbleOptions{
					CacheSize:             64 << 20,
					WriteBufferSize:       32 << 20,
					MaxOpenFiles:          1000,
					BloomFilterBits:       10,
					BlockSize:             4096,
					CompactionConcurrency: 1,
					SlowQueryThreshold:    5 * time.Second,
					BatchSizeLimit:        0,
					MetricsInterval:       time.Minute,
				},
				errMsg: "batch size limit must be positive",
			},
			{
				name: "zero metrics interval",
				opts: &PebbleOptions{
					CacheSize:             64 << 20,
					WriteBufferSize:       32 << 20,
					MaxOpenFiles:          1000,
					BloomFilterBits:       10,
					BlockSize:             4096,
					CompactionConcurrency: 1,
					SlowQueryThreshold:    5 * time.Second,
					BatchSizeLimit:        1000,
					MetricsInterval:       0,
				},
				errMsg: "metrics interval must be positive",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := validateOptions(tc.opts)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errMsg)
			})
		}
	})
}

func TestValidateDatabaseConnection(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Open a test database
	db, err := pebble.Open(dbPath, &pebble.Options{})
	require.NoError(t, err)
	defer db.Close()

	t.Run("successful validation", func(t *testing.T) {
		err := validateDatabaseConnection(db)
		assert.NoError(t, err)
	})
}

func TestPebbleEngine_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()

	engine, err := NewPebbleEngine(ctx, tempDir)
	require.NoError(t, err)
	defer engine.Close()

	t.Run("concurrent sync ID operations", func(t *testing.T) {
		const numGoroutines = 10
		const numOperations = 100

		done := make(chan bool, numGoroutines)

		// Start multiple goroutines performing sync ID operations
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer func() { done <- true }()

				for j := 0; j < numOperations; j++ {
					syncID := fmt.Sprintf("sync-%d-%d", id, j)

					// Test current sync ID operations
					engine.setCurrentSyncID(syncID)
					retrievedID := engine.getCurrentSyncID()
					assert.NotEmpty(t, retrievedID)

					// Test view sync ID operations
					engine.setViewSyncID(syncID)
					retrievedViewID := engine.getViewSyncID()
					assert.NotEmpty(t, retrievedViewID)

					// Test dirty flag operations
					engine.markDirty()
					assert.True(t, engine.Dirty())
				}
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}
	})
}
