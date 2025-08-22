package pebble

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/conductorone/baton-sdk/pkg/dotc1z/engine"
)

var tracer = otel.Tracer("baton-sdk/pkg.dotc1z.engine.pebble")

var _ engine.StorageEngine = (*PebbleEngine)(nil)

// PerformanceMetrics tracks performance statistics for the Pebble engine.
type PerformanceMetrics struct {
	mu sync.RWMutex

	// Operation counters
	ReadOps     int64
	WriteOps    int64
	DeleteOps   int64
	IteratorOps int64

	// Operation timings (in nanoseconds)
	TotalReadTime     int64
	TotalWriteTime    int64
	TotalDeleteTime   int64
	TotalIteratorTime int64

	// Slow query tracking
	SlowQueries    int64
	LastSlowQuery  time.Time
	SlowQueryLimit time.Duration

	// Compaction stats
	CompactionOps   int64
	CompactionBytes int64
}

// RecordRead records a read operation with timing.
func (pm *PerformanceMetrics) RecordRead(duration time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.ReadOps++
	pm.TotalReadTime += duration.Nanoseconds()
}

// RecordWrite records a write operation with timing.
func (pm *PerformanceMetrics) RecordWrite(duration time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.WriteOps++
	pm.TotalWriteTime += duration.Nanoseconds()
	
	// Check for slow queries
	if pm.SlowQueryLimit > 0 && duration > pm.SlowQueryLimit {
		pm.SlowQueries++
		pm.LastSlowQuery = time.Now()
	}
}

// RecordDelete records a delete operation with timing.
func (pm *PerformanceMetrics) RecordDelete(duration time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.DeleteOps++
	pm.TotalDeleteTime += duration.Nanoseconds()
}

// RecordIterator records an iterator operation with timing.
func (pm *PerformanceMetrics) RecordIterator(duration time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.IteratorOps++
	pm.TotalIteratorTime += duration.Nanoseconds()
}

// GetStats returns current performance statistics.
func (pm *PerformanceMetrics) GetStats() map[string]interface{} {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	
	stats := make(map[string]interface{})
	stats["read_ops"] = pm.ReadOps
	stats["write_ops"] = pm.WriteOps
	stats["delete_ops"] = pm.DeleteOps
	stats["iterator_ops"] = pm.IteratorOps
	stats["slow_queries"] = pm.SlowQueries
	
	if pm.ReadOps > 0 {
		stats["avg_read_time_ns"] = pm.TotalReadTime / pm.ReadOps
	}
	if pm.WriteOps > 0 {
		stats["avg_write_time_ns"] = pm.TotalWriteTime / pm.WriteOps
	}
	if pm.DeleteOps > 0 {
		stats["avg_delete_time_ns"] = pm.TotalDeleteTime / pm.DeleteOps
	}
	if pm.IteratorOps > 0 {
		stats["avg_iterator_time_ns"] = pm.TotalIteratorTime / pm.IteratorOps
	}
	
	return stats
}

// PebbleEngine implements the StorageEngine interface using Pebble as the underlying storage.
type PebbleEngine struct {
	db            *pebble.DB
	currentSyncID string
	viewSyncID    string
	dbPath        string
	dbUpdated     bool
	workingDir    string

	// Key management
	keyEncoder *KeyEncoder
	valueCodec *ValueCodec

	// Index management
	indexManager *IndexManager

	// Pagination management
	paginator *Paginator

	// Maintenance management
	integrityChecker  *IntegrityChecker
	compactionManager *CompactionManager

	// Performance monitoring
	metrics *PerformanceMetrics

	// Observability
	observability *ObservabilityManager

	// Configuration
	options *PebbleOptions

	// Synchronization
	mu sync.RWMutex
}

// PebbleOptions contains configuration options for the Pebble engine.
type PebbleOptions struct {
	// Database options
	CacheSize       int64
	WriteBufferSize int64
	MaxOpenFiles    int

	// Performance tuning
	BloomFilterBits       int
	BlockSize             int
	CompactionConcurrency int

	// Operational settings
	SlowQueryThreshold time.Duration
	BatchSizeLimit     int
	SyncPolicy         SyncPolicy

	// Monitoring
	MetricsEnabled  bool
	MetricsInterval time.Duration
	Logger          Logger
}

// SyncPolicy defines when to sync writes to disk.
type SyncPolicy int

const (
	SyncPolicyNoSync SyncPolicy = iota
	SyncPolicyPerBatch
	SyncPolicyPerOperation
)

// PebbleOption is a functional option for configuring PebbleEngine.
type PebbleOption func(*PebbleOptions)

// WithCacheSize sets the cache size for the Pebble database.
func WithCacheSize(size int64) PebbleOption {
	return func(opts *PebbleOptions) {
		opts.CacheSize = size
	}
}

// WithWriteBufferSize sets the write buffer size for the Pebble database.
func WithWriteBufferSize(size int64) PebbleOption {
	return func(opts *PebbleOptions) {
		opts.WriteBufferSize = size
	}
}

// WithMaxOpenFiles sets the maximum number of open files for the Pebble database.
func WithMaxOpenFiles(count int) PebbleOption {
	return func(opts *PebbleOptions) {
		opts.MaxOpenFiles = count
	}
}

// WithSlowQueryThreshold sets the threshold for logging slow queries.
func WithSlowQueryThreshold(threshold time.Duration) PebbleOption {
	return func(opts *PebbleOptions) {
		opts.SlowQueryThreshold = threshold
	}
}

// WithSyncPolicy sets the sync policy for write operations.
func WithSyncPolicy(policy SyncPolicy) PebbleOption {
	return func(opts *PebbleOptions) {
		opts.SyncPolicy = policy
	}
}

// WithBatchSizeLimit sets the maximum batch size for write operations.
func WithBatchSizeLimit(limit int) PebbleOption {
	return func(opts *PebbleOptions) {
		opts.BatchSizeLimit = limit
	}
}

// WithMetricsEnabled enables performance metrics collection.
func WithMetricsEnabled(enabled bool) PebbleOption {
	return func(opts *PebbleOptions) {
		opts.MetricsEnabled = enabled
	}
}

// WithMetricsInterval sets the interval for metrics collection.
func WithMetricsInterval(interval time.Duration) PebbleOption {
	return func(opts *PebbleOptions) {
		opts.MetricsInterval = interval
	}
}

// WithBloomFilterBits sets the number of bits per key for bloom filters.
func WithBloomFilterBits(bits int) PebbleOption {
	return func(opts *PebbleOptions) {
		opts.BloomFilterBits = bits
	}
}

// WithBlockSize sets the block size for SST files.
func WithBlockSize(size int) PebbleOption {
	return func(opts *PebbleOptions) {
		opts.BlockSize = size
	}
}

// WithCompactionConcurrency sets the number of concurrent compaction threads.
func WithCompactionConcurrency(concurrency int) PebbleOption {
	return func(opts *PebbleOptions) {
		opts.CompactionConcurrency = concurrency
	}
}

// WithLogger sets the logger for observability.
func WithLogger(logger Logger) PebbleOption {
	return func(opts *PebbleOptions) {
		opts.Logger = logger
	}
}

// NewPebbleEngine creates a new PebbleEngine instance.
func NewPebbleEngine(ctx context.Context, workingDir string, opts ...PebbleOption) (*PebbleEngine, error) {
	ctx, span := tracer.Start(ctx, "NewPebbleEngine")
	defer span.End()

	// Validate input parameters
	if workingDir == "" {
		return nil, fmt.Errorf("pebble: working directory cannot be empty")
	}

	// Create working directory if it doesn't exist
	err := os.MkdirAll(workingDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("pebble: could not create working directory %q: %w", workingDir, err)
	}

	// Verify working directory is writable
	testFile := filepath.Join(workingDir, ".pebble_test")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return nil, fmt.Errorf("pebble: working directory %q is not writable: %w", workingDir, err)
	}
	os.Remove(testFile) // Clean up test file

	// Set default options
	options := &PebbleOptions{
		CacheSize:             64 << 20, // 64MB
		WriteBufferSize:       32 << 20, // 32MB
		MaxOpenFiles:          1000,
		BloomFilterBits:       10,
		BlockSize:             4096,
		CompactionConcurrency: 1,
		SlowQueryThreshold:    5 * time.Second,
		BatchSizeLimit:        1000,
		SyncPolicy:            SyncPolicyPerBatch,
		MetricsEnabled:        false,
		MetricsInterval:       time.Minute,
	}

	// Apply options
	for _, opt := range opts {
		opt(options)
	}

	// Validate options
	if err := validateOptions(options); err != nil {
		return nil, fmt.Errorf("pebble: invalid options: %w", err)
	}

	dbPath := filepath.Join(workingDir, "pebble.db")

	// Configure Pebble options
	pebbleOpts := &pebble.Options{
		Cache:                    pebble.NewCache(options.CacheSize),
		MemTableSize:             uint64(options.WriteBufferSize),
		MaxOpenFiles:             options.MaxOpenFiles,
		Comparer:                 pebble.DefaultComparer,
		DisableWAL:               options.SyncPolicy == SyncPolicyNoSync,
		MaxConcurrentCompactions: func() int { return options.CompactionConcurrency },
	}

	// Open the database
	db, err := pebble.Open(dbPath, pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("pebble: could not open database at %q: %w", dbPath, err)
	}

	// Validate database connection by performing a simple operation
	if err := validateDatabaseConnection(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("pebble: database validation failed: %w", err)
	}

	engine := &PebbleEngine{
		db:         db,
		dbPath:     dbPath,
		workingDir: workingDir,
		options:    options,
		keyEncoder: NewKeyEncoder(),
		valueCodec: NewValueCodec(),
	}

	// Initialize index manager after engine is created
	engine.indexManager = NewIndexManager(engine)

	// Initialize paginator
	engine.paginator = NewPaginator(engine)

	// Initialize maintenance managers
	engine.integrityChecker = NewIntegrityChecker(engine)
	engine.compactionManager = NewCompactionManager(engine)

	// Initialize performance metrics
	engine.metrics = &PerformanceMetrics{
		SlowQueryLimit: options.SlowQueryThreshold,
	}

	// Initialize observability manager
	logger := options.Logger
	if logger == nil {
		logger = &NoOpLogger{} // Default to no-op logger
	}
	engine.observability = NewObservabilityManager(engine, logger)

	// Start periodic reporting if metrics are enabled
	if options.MetricsEnabled && options.MetricsInterval > 0 {
		engine.observability.StartPeriodicReporting(ctx, options.MetricsInterval)
	}

	return engine, nil
}

// validateOptions validates the PebbleOptions configuration.
func validateOptions(opts *PebbleOptions) error {
	if opts.CacheSize <= 0 {
		return fmt.Errorf("cache size must be positive, got %d", opts.CacheSize)
	}
	if opts.WriteBufferSize <= 0 {
		return fmt.Errorf("write buffer size must be positive, got %d", opts.WriteBufferSize)
	}
	if opts.MaxOpenFiles <= 0 {
		return fmt.Errorf("max open files must be positive, got %d", opts.MaxOpenFiles)
	}
	if opts.BloomFilterBits < 0 {
		return fmt.Errorf("bloom filter bits must be non-negative, got %d", opts.BloomFilterBits)
	}
	if opts.BlockSize <= 0 {
		return fmt.Errorf("block size must be positive, got %d", opts.BlockSize)
	}
	if opts.CompactionConcurrency <= 0 {
		return fmt.Errorf("compaction concurrency must be positive, got %d", opts.CompactionConcurrency)
	}
	if opts.SlowQueryThreshold < 0 {
		return fmt.Errorf("slow query threshold must be non-negative, got %v", opts.SlowQueryThreshold)
	}
	if opts.BatchSizeLimit <= 0 {
		return fmt.Errorf("batch size limit must be positive, got %d", opts.BatchSizeLimit)
	}
	if opts.MetricsInterval <= 0 {
		return fmt.Errorf("metrics interval must be positive, got %v", opts.MetricsInterval)
	}
	return nil
}

// validateDatabaseConnection performs a simple validation of the database connection.
func validateDatabaseConnection(db *pebble.DB) error {
	// Test basic write/read operation
	testKey := []byte("__pebble_test_key__")
	testValue := []byte("test")

	// Write test data (use NoSync to avoid WAL issues during validation)
	if err := db.Set(testKey, testValue, pebble.NoSync); err != nil {
		return fmt.Errorf("failed to write test data: %w", err)
	}

	// Read test data
	value, closer, err := db.Get(testKey)
	if err != nil {
		return fmt.Errorf("failed to read test data: %w", err)
	}
	defer closer.Close()

	if string(value) != string(testValue) {
		return fmt.Errorf("test data mismatch: expected %q, got %q", testValue, value)
	}

	// Clean up test data
	if err := db.Delete(testKey, pebble.NoSync); err != nil {
		return fmt.Errorf("failed to delete test data: %w", err)
	}

	return nil
}

// Core engine methods

// Close closes the Pebble engine and releases resources.
func (pe *PebbleEngine) Close() error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	if pe.db == nil {
		return nil // Already closed
	}

	err := pe.db.Close()
	pe.db = nil
	return err
}

// Dirty returns whether the database has been modified.
func (pe *PebbleEngine) Dirty() bool {
	pe.mu.RLock()
	defer pe.mu.RUnlock()
	return pe.dbUpdated
}

// OutputFilepath returns the path to the database file.
func (pe *PebbleEngine) OutputFilepath() (string, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	if pe.dbPath == "" {
		return "", fmt.Errorf("database path not set")
	}

	// Check if database file exists
	if _, err := os.Stat(pe.dbPath); os.IsNotExist(err) {
		return "", fmt.Errorf("database file does not exist: %s", pe.dbPath)
	}

	return pe.dbPath, nil
}

// AttachFile attaches another storage engine (not implemented for Pebble).
func (pe *PebbleEngine) AttachFile(other engine.StorageEngine, dbName string) (engine.AttachedStorageEngine, error) {
	return nil, fmt.Errorf("attach file not implemented for Pebble engine")
}

// Helper methods

// markDirty marks the database as modified.
func (pe *PebbleEngine) markDirty() {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	pe.dbUpdated = true
}

// getCurrentSyncID returns the current sync ID.
func (pe *PebbleEngine) getCurrentSyncID() string {
	pe.mu.RLock()
	defer pe.mu.RUnlock()
	return pe.currentSyncID
}

// setCurrentSyncID sets the current sync ID.
func (pe *PebbleEngine) setCurrentSyncID(syncID string) {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	pe.currentSyncID = syncID
}

// getViewSyncID returns the view sync ID.
func (pe *PebbleEngine) getViewSyncID() string {
	pe.mu.RLock()
	defer pe.mu.RUnlock()
	return pe.viewSyncID
}

// setViewSyncID sets the view sync ID.
func (pe *PebbleEngine) setViewSyncID(syncID string) {
	pe.mu.Lock()
	defer pe.mu.Unlock()
	pe.viewSyncID = syncID
}

// isOpen returns whether the database is open.
func (pe *PebbleEngine) isOpen() bool {
	pe.mu.RLock()
	defer pe.mu.RUnlock()
	return pe.db != nil
}

// validateOpen validates that the database is open.
func (pe *PebbleEngine) validateOpen() error {
	if !pe.isOpen() {
		return fmt.Errorf("database is not open")
	}
	return nil
}

// currentSyncTime returns the current timestamp for sync operations.
func (pe *PebbleEngine) currentSyncTime() time.Time {
	return time.Now()
}

// getSyncIDForQuery resolves the sync ID to use for a query based on annotations.
func (pe *PebbleEngine) getSyncIDForQuery(ctx context.Context, annotationsList []*anypb.Any) (string, error) {
	if len(annotationsList) > 0 {
		for _, annotation := range annotationsList {
			// Check if this annotation contains sync ID information
			if annotation != nil {
				// For now, skip annotation processing - this needs proper implementation
				// TODO: Implement proper sync ID annotation checking
				continue
			}
		}
	}

	// Use current sync ID if set
	if currentSyncID := pe.getCurrentSyncID(); currentSyncID != "" {
		return currentSyncID, nil
	}

	// Use view sync ID if set
	if viewSyncID := pe.getViewSyncID(); viewSyncID != "" {
		return viewSyncID, nil
	}

	// Fall back to latest finished sync
	fullSyncs, _, err := pe.getAllFinishedSyncs(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get finished syncs: %w", err)
	}

	if len(fullSyncs) == 0 {
		return "", fmt.Errorf("no finished sync available")
	}

	// Use the most recent full sync
	return fullSyncs[len(fullSyncs)-1].ID, nil
}

// Pagination helpers

// encodePageToken encodes a key as a page token.
func (pe *PebbleEngine) encodePageToken(key []byte) (string, error) {
	return pe.paginator.EncodeToken(key)
}

// encodePageTokenWithSize encodes a key and page size as a page token.
func (pe *PebbleEngine) encodePageTokenWithSize(key []byte, pageSize uint32) (string, error) {
	// Use the basic EncodeToken for now - TODO: implement size handling
	return pe.paginator.EncodeToken(key)
}

// decodePageToken decodes a page token to get the key.
func (pe *PebbleEngine) decodePageToken(token string) (*PaginationToken, error) {
	return pe.paginator.DecodeToken(token)
}

// Sync options helper

// getSyncOptions returns the appropriate sync options based on the engine configuration.
func (pe *PebbleEngine) getSyncOptions() *pebble.WriteOptions {
	switch pe.options.SyncPolicy {
	case SyncPolicyPerOperation:
		return pebble.Sync
	case SyncPolicyPerBatch:
		return pebble.Sync
	case SyncPolicyNoSync:
		return pebble.NoSync
	default:
		return pebble.NoSync
	}
}

// Entity update helpers

// shouldUpdateEntity checks if an entity should be updated based on timestamp.
func (pe *PebbleEngine) shouldUpdateEntity(key []byte, currentTime time.Time) (bool, error) {
	existingValue, closer, err := pe.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return true, nil // Entity doesn't exist, should update
		}
		return false, fmt.Errorf("failed to get existing entity: %w", err)
	}
	defer closer.Close()

	// Decode existing timestamp
	existingTimestamp, err := pe.valueCodec.GetDiscoveredAt(existingValue)
	if err != nil {
		return true, nil // Can't decode existing, assume should update
	}

	// Update if current timestamp is newer
	return currentTime.After(existingTimestamp), nil
}

// shouldUpdateEntityWithBatch checks if an entity should be updated using a batch.
func (pe *PebbleEngine) shouldUpdateEntityWithBatch(batch *pebble.Batch, key []byte, currentTime time.Time) (bool, error) {
	// First check if there's a pending update in the batch
	// For simplicity, we'll check the main database for now
	// A more sophisticated implementation could track batch updates
	return pe.shouldUpdateEntity(key, currentTime)
}

// Key counting helper

// countKeysWithPrefix counts keys with a specific prefix.
func (pe *PebbleEngine) countKeysWithPrefix(prefix []byte) (int64, error) {
	upperBound := make([]byte, len(prefix))
	copy(upperBound, prefix)
	// Increment the last byte to create upper bound
	for i := len(upperBound) - 1; i >= 0; i-- {
		if upperBound[i] < 255 {
			upperBound[i]++
			break
		}
		upperBound[i] = 0
		if i == 0 {
			// Overflow case - use nil as upper bound
			upperBound = nil
			break
		}
	}

	iter, err := pe.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	count := int64(0)
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	return count, iter.Error()
}

// Performance and observability methods

// GetPerformanceStats returns performance statistics.
func (pe *PebbleEngine) GetPerformanceStats() map[string]interface{} {
	if pe.metrics == nil {
		return make(map[string]interface{})
	}
	return pe.metrics.GetStats()
}

// ResetPerformanceStats resets performance statistics.
func (pe *PebbleEngine) ResetPerformanceStats() {
	if pe.metrics == nil {
		return
	}
	
	pe.metrics.mu.Lock()
	defer pe.metrics.mu.Unlock()
	
	pe.metrics.ReadOps = 0
	pe.metrics.WriteOps = 0
	pe.metrics.DeleteOps = 0
	pe.metrics.IteratorOps = 0
	pe.metrics.TotalReadTime = 0
	pe.metrics.TotalWriteTime = 0
	pe.metrics.TotalDeleteTime = 0
	pe.metrics.TotalIteratorTime = 0
	pe.metrics.SlowQueries = 0
	pe.metrics.CompactionOps = 0
	pe.metrics.CompactionBytes = 0
}

// GetObservabilityManager returns the observability manager.
func (pe *PebbleEngine) GetObservabilityManager() *ObservabilityManager {
	return pe.observability
}

// GetSystemStats returns system statistics.
func (pe *PebbleEngine) GetSystemStats(ctx context.Context) (map[string]interface{}, error) {
	if pe.observability == nil {
		return make(map[string]interface{}), nil
	}
	return pe.observability.GetSystemStats(ctx)
}

// GetHealthStatus returns health status.
func (pe *PebbleEngine) GetHealthStatus(ctx context.Context) (map[string]interface{}, error) {
	if pe.observability == nil {
		return map[string]interface{}{
			"status":    "unknown",
			"timestamp": time.Now().Unix(),
			"issues":    []string{"observability not initialized"},
		}, nil
	}
	return pe.observability.GetHealthStatus(ctx)
}

// Maintenance methods

// IntegrityChecker returns the integrity checker for maintenance operations.
func (pe *PebbleEngine) IntegrityChecker() *IntegrityChecker {
	return pe.integrityChecker
}

// CompactionManager returns the compaction manager for maintenance operations.
func (pe *PebbleEngine) CompactionManager() *CompactionManager {
	return pe.compactionManager
}

// Internal sync management helpers

// getAllFinishedSyncs retrieves all finished syncs, separated by type (full vs partial).
func (pe *PebbleEngine) getAllFinishedSyncs(ctx context.Context) ([]*SyncRun, []*SyncRun, error) {
	ctx, span := tracer.Start(ctx, "PebbleEngine.getAllFinishedSyncs")
	defer span.End()

	prefix := pe.keyEncoder.EncodePrefixKey(KeyTypeSyncRun, "")
	upperBound := pe.keyEncoder.EncodePrefixKey(KeyTypeSyncRun, "~")

	iter, err := pe.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	var fullSyncs []*SyncRun
	var partialSyncs []*SyncRun

	for iter.First(); iter.Valid(); iter.Next() {
		// Decode the asset envelope
		_, data, err := pe.valueCodec.DecodeAsset(iter.Value())
		if err != nil {
			continue // Skip invalid entries
		}

		// Unmarshal from JSON
		syncRun := &SyncRun{}
		if err := json.Unmarshal(data, syncRun); err != nil {
			continue // Skip invalid entries
		}

		// Only include finished syncs
		if syncRun.EndedAt == 0 {
			continue
		}

		if syncRun.Type == "partial" {
			partialSyncs = append(partialSyncs, syncRun)
		} else {
			fullSyncs = append(fullSyncs, syncRun)
		}
	}

	// Sort by creation time (most recent first)
	sort.Slice(fullSyncs, func(i, j int) bool {
		return fullSyncs[i].StartedAt > fullSyncs[j].StartedAt
	})
	sort.Slice(partialSyncs, func(i, j int) bool {
		return partialSyncs[i].StartedAt > partialSyncs[j].StartedAt
	})

	return fullSyncs, partialSyncs, nil
}


// deleteSyncData deletes all data associated with a sync.
func (pe *PebbleEngine) deleteSyncData(ctx context.Context, syncID string) error {
	ctx, span := tracer.Start(ctx, "PebbleEngine.deleteSyncData")
	defer span.End()

	batch := pe.db.NewBatch()
	defer batch.Close()

	// List of entity types to clean up
	entityTypes := []KeyType{
		KeyTypeResourceType,
		KeyTypeResource,
		KeyTypeEntitlement,
		KeyTypeGrant,
		KeyTypeAsset,
	}

	// Collect start-end key pairs for compaction
	var compactionRanges [][2][]byte
	
	// Use RangeDelete for efficient bulk deletion of each entity type
	for _, entityType := range entityTypes {
		startKey := pe.keyEncoder.EncodePrefixKey(entityType, syncID)
		// Create proper end key by replacing the final separator (0x00) with 0x01
		endKey := make([]byte, len(startKey))
		copy(endKey, startKey)
		endKey[len(endKey)-1] = byte(0x01) // Replace final separator with 0x01
		
		compactionRanges = append(compactionRanges, [2][]byte{startKey, endKey})
		
		if err := batch.DeleteRange(startKey, endKey, nil); err != nil {
			return fmt.Errorf("failed to range delete entity type %v: %w", entityType, err)
		}
	}

	// Delete all indexes for this syncID (indexes that have syncID as first component)
	indexTypes := []IndexType{
		IndexEntitlementsByResource,
		IndexGrantsByResource,
		IndexGrantsByPrincipal,
		IndexGrantsByEntitlement,
	}

	for _, indexType := range indexTypes {
		startKey := pe.keyEncoder.EncodeIndexPrefixKey(indexType, syncID)
		// Create proper end key by replacing the final separator (0x00) with 0x01
		endKey := make([]byte, len(startKey))
		copy(endKey, startKey)
		endKey[len(endKey)-1] = byte(0x01) // Replace final separator with 0x01
		
		compactionRanges = append(compactionRanges, [2][]byte{startKey, endKey})
		
		if err := batch.DeleteRange(startKey, endKey, nil); err != nil {
			return fmt.Errorf("failed to range delete index type %v: %w", indexType, err)
		}
	}

	// Delete the sync run record itself
	syncRunKey := pe.keyEncoder.EncodeSyncRunKey(syncID)
	if err := batch.Delete(syncRunKey, nil); err != nil {
		return fmt.Errorf("failed to delete sync run: %w", err)
	}

	// Commit the batch
	if err := batch.Commit(pe.getSyncOptions()); err != nil {
		return fmt.Errorf("failed to commit sync deletion: %w", err)
	}

	// Trigger manual compaction for all deleted ranges to reclaim space
	for _, compactionRange := range compactionRanges {
		if err := pe.db.Compact(compactionRange[0], compactionRange[1], false); err != nil {
			// Log error but don't fail - compaction is optimization, not critical
			// TODO: Add proper logging when logger is available
			_ = err
		}
	}

	pe.markDirty()
	return nil
}

// triggerManualCompaction triggers a manual compaction.
func (pe *PebbleEngine) triggerManualCompaction(ctx context.Context) error {
	if pe.compactionManager != nil {
		return pe.compactionManager.TriggerManualCompaction(ctx)
	}
	// Fallback to basic Pebble compaction
	return pe.db.Compact(nil, nil, false)
}