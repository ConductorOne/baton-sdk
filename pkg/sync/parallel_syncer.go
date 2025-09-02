package sync

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

var _ Syncer = (*parallelSyncer)(nil)

var parallelTracer = otel.Tracer("baton-sdk/parallel-sync")

// StateInterface defines the minimal interface needed by helper methods
// This allows helper methods to work with either the sequential syncer's state machine
// or the parallel syncer's local state context
type StateInterface interface {
	PageToken(ctx context.Context) string
	NextPage(ctx context.Context, pageToken string) error
	SetNeedsExpansion()
	SetHasExternalResourcesGrants()
	ShouldFetchRelatedResources() bool
}

// ActionDecision represents the decision made by a helper method
// This allows the caller to decide how to handle the result
type ActionDecision struct {
	ShouldContinue       bool   // Whether to continue processing (e.g., more pages)
	NextPageToken        string // Page token for next page, if applicable
	Action               string // What action to take: "next_page", "finish", "continue"
	NeedsExpansion       bool   // Whether grant expansion is needed
	HasExternalResources bool   // Whether external resources were found
	ShouldFetchRelated   bool   // Whether related resources should be fetched
}

// LocalStateContext provides local state management for parallel syncer
// This implements StateInterface without sharing the global state machine
type LocalStateContext struct {
	resourceID           *v2.ResourceId
	pageToken            string
	needsExpansion       bool
	hasExternalResources bool
	shouldFetchRelated   bool
}

// NewLocalStateContext creates a new local state context for a resource
func NewLocalStateContext(resourceID *v2.ResourceId) *LocalStateContext {
	return &LocalStateContext{
		resourceID:           resourceID,
		pageToken:            "",
		needsExpansion:       false,
		hasExternalResources: false,
		shouldFetchRelated:   true, // Default to true for backward compatibility
	}
}

// PageToken returns the current page token for this resource
func (lsc *LocalStateContext) PageToken(ctx context.Context) string {
	return lsc.pageToken
}

// NextPage updates the page token for the next page
func (lsc *LocalStateContext) NextPage(ctx context.Context, pageToken string) error {
	lsc.pageToken = pageToken
	return nil
}

// SetNeedsExpansion marks that grant expansion is needed
func (lsc *LocalStateContext) SetNeedsExpansion() {
	lsc.needsExpansion = true
}

// SetHasExternalResourcesGrants marks that external resources were found
func (lsc *LocalStateContext) SetHasExternalResourcesGrants() {
	lsc.hasExternalResources = true
}

// ShouldFetchRelatedResources returns whether related resources should be fetched
func (lsc *LocalStateContext) ShouldFetchRelatedResources() bool {
	return lsc.shouldFetchRelated
}

// ParallelSyncConfig holds configuration for parallel sync operations
type ParallelSyncConfig struct {
	// Number of worker goroutines to use for parallel processing
	WorkerCount int
	// Default bucket for resource types not explicitly configured
	// If a resource type doesn't specify a sync_bucket, this default will be used
	DefaultBucket string
}

// DefaultParallelSyncConfig returns a default configuration
func DefaultParallelSyncConfig() *ParallelSyncConfig {
	return &ParallelSyncConfig{
		WorkerCount:   1,
		DefaultBucket: "", // Empty string means each resource type gets its own bucket
	}
}

// WithWorkerCount sets the number of worker goroutines
func (c *ParallelSyncConfig) WithWorkerCount(count int) *ParallelSyncConfig {
	if count > 0 {
		c.WorkerCount = count
	}
	return c
}

// WithDefaultBucket sets the default bucket for resource types that don't specify a sync_bucket
func (c *ParallelSyncConfig) WithDefaultBucket(bucket string) *ParallelSyncConfig {
	c.DefaultBucket = bucket
	return c
}

// task represents a unit of work for the parallel syncer
type task struct {
	Action       Action
	ResourceID   string
	Priority     int              // Higher priority tasks are processed first
	ResourceType *v2.ResourceType // The resource type for this task
}

// taskQueue manages the distribution of tasks to workers using dynamic bucketing
type taskQueue struct {
	bucketQueues map[string]chan *task // Map of bucket name to task channel
	config       *ParallelSyncConfig
	mu           sync.RWMutex
	closed       bool
}

// newTaskQueue creates a new task queue
func newTaskQueue(config *ParallelSyncConfig) *taskQueue {
	// Initialize with an empty map of bucket queues
	// Buckets will be created dynamically as tasks are added
	return &taskQueue{
		bucketQueues: make(map[string]chan *task),
		config:       config,
	}
}

// AddTask adds a task to the appropriate queue
func (q *taskQueue) AddTask(t *task) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return errors.New("task queue is closed")
	}

	// Determine the bucket for this task
	bucket := q.getBucketForTask(t)

	// Create the bucket queue if it doesn't exist
	if _, exists := q.bucketQueues[bucket]; !exists {
		queueSize := q.config.WorkerCount * 10
		q.bucketQueues[bucket] = make(chan *task, queueSize)
	}

	// Add the task to the appropriate bucket queue
	select {
	case q.bucketQueues[bucket] <- t:
		// Log task addition for debugging
		l := ctxzap.Extract(context.Background())
		l.Info("task added to queue",
			zap.String("bucket", bucket),
			zap.String("operation", t.Action.Op.String()),
			zap.String("resource_type", t.Action.ResourceTypeID),
			zap.Int("queue_length", len(q.bucketQueues[bucket])))
		return nil
	default:
		return fmt.Errorf("bucket queue '%s' is full", bucket)
	}
}

// getBucketForTask determines the bucket for a task based on the resource type's sync_bucket
func (q *taskQueue) getBucketForTask(t *task) string {
	// If the resource type has an explicit sync_bucket, use it
	if t.ResourceType != nil && t.ResourceType.SyncBucket != "" {
		return t.ResourceType.SyncBucket
	}

	// If no explicit bucket and default is empty, create a unique bucket per resource type
	if q.config.DefaultBucket == "" {
		return fmt.Sprintf("resource-type-%s", t.Action.ResourceTypeID)
	}

	// Otherwise use the configured default bucket
	return q.config.DefaultBucket
}

// GetTask retrieves the next task with intelligent bucket selection
func (q *taskQueue) GetTask() (*task, error) {
	q.mu.Lock() // Use write lock to make the operation atomic
	defer q.mu.Unlock()

	// Debug logging
	l := ctxzap.Extract(context.Background())
	l.Debug("GetTask called",
		zap.Int("total_buckets", len(q.bucketQueues)),
		zap.Any("bucket_names", getMapKeys(q.bucketQueues)))

	if len(q.bucketQueues) == 0 {
		l.Debug("no buckets available")
		return nil, errors.New("no buckets available")
	}

	// First, try to find a bucket with available tasks
	var availableBuckets []string
	for bucketName, queue := range q.bucketQueues {
		queueLen := len(queue)
		l.Debug("checking bucket", zap.String("bucket", bucketName), zap.Int("queue_length", queueLen))
		if queueLen > 0 {
			availableBuckets = append(availableBuckets, bucketName)
		}
	}

	l.Debug("available buckets", zap.Strings("buckets", availableBuckets))

	if len(availableBuckets) == 0 {
		l.Debug("no tasks available in any bucket")
		return nil, errors.New("no tasks available")
	}

	// Try to get a task from each available bucket in round-robin order
	// Use a more robust approach that handles the case where a queue becomes empty
	for _, bucketName := range availableBuckets {
		queue := q.bucketQueues[bucketName]

		// Double-check the queue still has items before trying to read
		if len(queue) == 0 {
			l.Debug("bucket queue became empty", zap.String("bucket", bucketName))
			continue
		}

		select {
		case t := <-queue:
			l.Debug("retrieved task from bucket", zap.String("bucket", bucketName))
			return t, nil
		default:
			l.Debug("bucket queue empty when trying to read", zap.String("bucket", bucketName))
			continue
		}
	}

	l.Debug("failed to get task from any available bucket")
	return nil, errors.New("no tasks available")
}

// getMapKeys returns the keys of a map as a slice
func getMapKeys(m map[string]chan *task) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// GetTaskFromBucket retrieves a task from a specific bucket
func (q *taskQueue) GetTaskFromBucket(bucketName string) (*task, error) {
	q.mu.Lock() // Use write lock to make the operation atomic
	defer q.mu.Unlock()

	queue, exists := q.bucketQueues[bucketName]
	if !exists {
		return nil, fmt.Errorf("bucket '%s' does not exist", bucketName)
	}

	select {
	case t := <-queue:
		return t, nil
	default:
		return nil, errors.New("no tasks available in bucket")
	}
}

// GetBucketStats returns statistics about each bucket
func (q *taskQueue) GetBucketStats() map[string]int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	stats := make(map[string]int)
	for bucketName, queue := range q.bucketQueues {
		stats[bucketName] = len(queue)
	}
	return stats
}

// Close closes the task queue
func (q *taskQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.closed {
		// Close all bucket queues
		for _, queue := range q.bucketQueues {
			close(queue)
		}
		q.closed = true
	}
}

// worker represents a worker goroutine that processes tasks
type worker struct {
	id           int
	taskQueue    *taskQueue
	syncer       *parallelSyncer
	ctx          context.Context
	cancel       context.CancelFunc
	wg           *sync.WaitGroup
	rateLimited  atomic.Bool
	isProcessing atomic.Bool
}

// newWorker creates a new worker
func newWorker(id int, taskQueue *taskQueue, syncer *parallelSyncer, ctx context.Context, wg *sync.WaitGroup) *worker {
	workerCtx, cancel := context.WithCancel(ctx)
	return &worker{
		id:        id,
		taskQueue: taskQueue,
		syncer:    syncer,
		ctx:       workerCtx,
		cancel:    cancel,
		wg:        wg,
	}
}

// Start starts the worker with bucket-aware task processing and work-stealing
func (w *worker) Start() {
	defer w.wg.Done()

	l := ctxzap.Extract(w.ctx)
	l.Debug("worker started", zap.Int("worker_id", w.id))

	// Track which bucket this worker is currently working on
	currentBucket := ""
	consecutiveFailures := 0
	maxConsecutiveFailures := 3

	for {
		select {
		case <-w.ctx.Done():
			l.Debug("worker stopped", zap.Int("worker_id", w.id))
			return
		default:

			// Try to get a task, with preference for the current bucket if we're making progress
			task, err := w.taskQueue.GetTask()
			if err != nil {
				// No tasks available, wait a bit
				l.Debug("no tasks available, waiting", zap.Int("worker_id", w.id), zap.Error(err))
				time.Sleep(100 * time.Millisecond)
				continue
			}
			l.Info("worker got task", zap.Int("worker_id", w.id), zap.String("task_op", task.Action.Op.String()))

			// Track which bucket we're working on
			taskBucket := w.taskQueue.getBucketForTask(task)
			if taskBucket != currentBucket {
				l.Debug("worker switching buckets",
					zap.Int("worker_id", w.id),
					zap.String("from_bucket", currentBucket),
					zap.String("to_bucket", taskBucket))
				currentBucket = taskBucket
				consecutiveFailures = 0
			}

			// Add detailed task information logging
			l.Info("processing task details",
				zap.Int("worker_id", w.id),
				zap.String("task_op", task.Action.Op.String()),
				zap.String("resource_type", task.Action.ResourceTypeID),
				zap.String("page_token", task.Action.PageToken),
				zap.String("bucket", taskBucket))

			// Set processing flag
			w.isProcessing.Store(true)

			// Process the task
			if err := w.processTask(task); err != nil {
				l.Error("failed to process task",
					zap.Int("worker_id", w.id),
					zap.String("bucket", taskBucket),
					zap.String("operation", task.Action.Op.String()),
					zap.String("resource_type", task.Action.ResourceTypeID),
					zap.Error(err))

				consecutiveFailures++

				// Check if this is a rate limit error
				if w.isRateLimitError(err) {
					w.rateLimited.Store(true)

					// If we're hitting rate limits in the current bucket, consider switching
					if consecutiveFailures >= maxConsecutiveFailures {
						l.Info("worker hitting rate limits in bucket, will try other buckets",
							zap.Int("worker_id", w.id),
							zap.String("bucket", taskBucket),
							zap.Int("consecutive_failures", consecutiveFailures))

						// Force bucket switch on next iteration
						currentBucket = ""
						consecutiveFailures = 0
					}

					// Wait before retrying with bucket-specific delay
					delay := w.getBucketRateLimitDelay(taskBucket)
					time.Sleep(delay)
				} else {
					// Non-rate-limit error, reset rate limit flag
					w.rateLimited.Store(false)
				}
			} else {
				// Task succeeded, reset failure counters
				w.rateLimited.Store(false)
				consecutiveFailures = 0
			}

			// Reset processing flag
			w.isProcessing.Store(false)
		}
	}
}

// processTask processes a single task
func (w *worker) processTask(task *task) error {
	ctx, span := parallelTracer.Start(w.ctx, "worker.processTask")
	defer span.End()

	span.SetAttributes(
		attribute.Int("worker_id", w.id),
		attribute.String("operation", task.Action.Op.String()),
		attribute.String("resource_type", task.Action.ResourceTypeID),
	)

	switch task.Action.Op {
	case SyncResourcesOp:
		return w.syncer.syncResources(ctx, task.Action)
	case SyncEntitlementsOp:
		if task.Action.ResourceID != "" {
			// Process specific resource's entitlements
			return w.syncer.syncEntitlementsForResource(ctx, task.Action)
		} else {
			// Fallback to resource type processing (for backward compatibility)
			return w.syncer.syncEntitlementsForResourceType(ctx, task.Action)
		}
	case SyncGrantsOp:
		if task.Action.ResourceID != "" {
			// Process specific resource's grants
			return w.syncer.syncGrantsForResource(ctx, task.Action)
		} else {
			// Fallback to resource type processing (for backward compatibility)
			return w.syncer.syncGrantsForResourceType(ctx, task.Action)
		}
	default:
		return fmt.Errorf("unsupported operation: %s", task.Action.Op.String())
	}
}

// isRateLimitError checks if an error is a rate limit error
func (w *worker) isRateLimitError(err error) bool {
	// Check for rate limit annotations in the error
	if err == nil {
		return false
	}

	// This is a simplified check - in practice, you'd want to check the actual
	// error type returned by the connector for rate limiting
	return status.Code(err) == codes.ResourceExhausted ||
		errors.Is(err, sql.ErrConnDone) // Placeholder for rate limit detection
}

// getBucketRateLimitDelay returns the appropriate delay for a bucket based on rate limiting
func (w *worker) getBucketRateLimitDelay(bucket string) time.Duration {
	// Different buckets can have different rate limit delays
	// This allows for bucket-specific rate limiting strategies

	switch {
	case strings.Contains(bucket, "rate-limited"):
		return 2 * time.Second // Longer delay for rate-limited buckets
	case strings.Contains(bucket, "fast-apis"):
		return 100 * time.Millisecond // Shorter delay for fast APIs
	default:
		return 1 * time.Second // Default delay
	}
}

// Stop stops the worker
func (w *worker) Stop() {
	w.cancel()
}

// parallelSyncer extends the base syncer with parallel processing capabilities
type parallelSyncer struct {
	syncer    *SequentialSyncer
	config    *ParallelSyncConfig
	taskQueue *taskQueue
	workers   []*worker
	workerWg  sync.WaitGroup
	mu        sync.RWMutex
}

// NewParallelSyncer creates a new parallel syncer
func NewParallelSyncer(baseSyncer *SequentialSyncer, config *ParallelSyncConfig) *parallelSyncer {
	if config == nil {
		config = DefaultParallelSyncConfig()
	}

	return &parallelSyncer{
		syncer: baseSyncer,
		config: config,
	}
}

// Sync implements the Syncer interface using parallel processing
func (ps *parallelSyncer) Sync(ctx context.Context) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.Sync")
	defer span.End()

	l := ctxzap.Extract(ctx)

	// Initialize the sync
	if err := ps.initializeSync(ctx); err != nil {
		return err
	}

	// Create task queue
	ps.taskQueue = newTaskQueue(ps.config)
	defer ps.taskQueue.Close()

	// Start workers
	if err := ps.startWorkers(ctx); err != nil {
		return err
	}
	defer ps.stopWorkers()

	// Generate initial tasks
	if err := ps.generateInitialTasks(ctx); err != nil {
		return err
	}

	// Wait for all tasks to complete
	if err := ps.waitForCompletion(ctx); err != nil {
		return err
	}

	// Now that all parallel processing is complete, run grant expansion sequentially
	if err := ps.syncGrantExpansion(ctx); err != nil {
		l.Error("failed to run grant expansion", zap.Error(err))
		return fmt.Errorf("failed to run grant expansion: %w", err)
	}

	// Run external resources sync if configured
	if ps.syncer.externalResourceReader != nil {
		if err := ps.syncExternalResources(ctx); err != nil {
			l.Error("failed to run external resources sync", zap.Error(err))
			return fmt.Errorf("failed to run external resources sync: %w", err)
		}
	}

	// Finalize sync
	if err := ps.finalizeSync(ctx); err != nil {
		return err
	}

	return nil
}

// initializeSync performs the initial sync setup
func (ps *parallelSyncer) initializeSync(ctx context.Context) error {
	// Load store and validate connector (reuse existing logic)
	if err := ps.syncer.loadStore(ctx); err != nil {
		return err
	}

	_, err := ps.syncer.connector.Validate(ctx, &v2.ConnectorServiceValidateRequest{})
	if err != nil {
		return err
	}

	// Start or resume sync
	_, _, err = ps.syncer.startOrResumeSync(ctx)
	if err != nil {
		return err
	}

	// Set up state
	currentStep, err := ps.syncer.store.CurrentSyncStep(ctx)
	if err != nil {
		return err
	}

	state := &state{}
	if err := state.Unmarshal(currentStep); err != nil {
		return err
	}
	ps.syncer.state = state

	// Set progress counts to parallel mode for thread safety
	if ps.syncer.counts != nil {
		ps.syncer.counts.SetSequentialMode(false)
	}

	return nil
}

// startWorkers starts all worker goroutines
func (ps *parallelSyncer) startWorkers(ctx context.Context) error {
	ps.workers = make([]*worker, ps.config.WorkerCount)

	for i := 0; i < ps.config.WorkerCount; i++ {
		worker := newWorker(i, ps.taskQueue, ps, ctx, &ps.workerWg)
		ps.workers[i] = worker
		ps.workerWg.Add(1)
		go worker.Start()
	}

	return nil
}

// stopWorkers stops all workers
func (ps *parallelSyncer) stopWorkers() {
	for _, worker := range ps.workers {
		worker.Stop()
	}
	ps.workerWg.Wait()
}

// areWorkersIdle checks if all workers are currently idle (not processing tasks)
func (ps *parallelSyncer) areWorkersIdle() bool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for _, worker := range ps.workers {
		if worker.isProcessing.Load() {
			return false
		}
	}
	return true
}

// generateInitialTasks creates the initial set of tasks following the original sync workflow
func (ps *parallelSyncer) generateInitialTasks(ctx context.Context) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.generateInitialTasks")
	defer span.End()

	l := ctxzap.Extract(ctx)

	// Follow the exact same workflow as the original sync
	// 1. Start with resource types
	// 2. Then resources for each resource type (sequentially within each resource type)
	// 3. Then entitlements for each resource type (sequentially within each resource type)
	// 4. Then grants for each resource type (sequentially within each resource type)
	// 5. Then grant expansion and external resources

	// First, sync resource types
	if err := ps.syncResourceTypes(ctx); err != nil {
		l.Error("failed to sync resource types", zap.Error(err))
		return err
	}

	// Get all resource types and create resource sync tasks
	resp, err := ps.syncer.store.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
	if err != nil {
		l.Error("failed to list resource types", zap.Error(err))
		return err
	}

	// Group resource types by their buckets for better task organization
	bucketGroups := make(map[string][]*v2.ResourceType)
	for _, rt := range resp.List {
		bucket := ps.getBucketForResourceType(rt)
		bucketGroups[bucket] = append(bucketGroups[bucket], rt)
	}

	// Create tasks for each bucket, ensuring sequential processing within each bucket
	for _, resourceTypes := range bucketGroups {
		l := ctxzap.Extract(ctx)

		// Create tasks for this bucket
		for _, rt := range resourceTypes {
			// Create task to sync resources for this resource type
			task := &task{
				Action: Action{
					Op:             SyncResourcesOp,
					ResourceTypeID: rt.Id,
				},
				Priority:     1,
				ResourceType: rt, // Include the resource type for bucket determination
			}

			if err := ps.taskQueue.AddTask(task); err != nil {
				l.Error("failed to add resource sync task", zap.Error(err))
				return fmt.Errorf("failed to add resource sync task for resource type %s: %w", rt.Id, err)
			}
		}
	}

	// Note: Grant expansion and external resources tasks are NOT added here
	// They are added after ALL resource types are completely processed
	// This ensures the correct order: resources → entitlements → grants → grant expansion → external resources

	return nil
}

// getBucketForResourceType determines the bucket for a resource type
func (ps *parallelSyncer) getBucketForResourceType(rt *v2.ResourceType) string {
	// If the resource type has an explicit sync_bucket, use it
	if rt.SyncBucket != "" {
		return rt.SyncBucket
	}

	// If no explicit bucket and default is empty, create a unique bucket per resource type
	if ps.config.DefaultBucket == "" {
		return fmt.Sprintf("resource-type-%s", rt.Id)
	}

	// Otherwise use the configured default bucket
	return ps.config.DefaultBucket
}

// waitForCompletion waits for all tasks to complete with bucket-aware monitoring
func (ps *parallelSyncer) waitForCompletion(ctx context.Context) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.waitForCompletion")
	defer span.End()

	l := ctxzap.Extract(ctx)

	// Monitor task completion with periodic status updates
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	lastTaskCount := 0
	noProgressCount := 0
	maxNoProgressCount := 6 // 30 seconds without progress

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Get current bucket statistics
			bucketStats := ps.taskQueue.GetBucketStats()
			totalTasks := 0
			for _, count := range bucketStats {
				totalTasks += count
			}

			// Log progress
			if len(bucketStats) > 0 {
				// Debug: Log which buckets still have active tasks
				activeBuckets := make([]string, 0)
				for bucketName, taskCount := range bucketStats {
					if taskCount > 0 && bucketName != "resource-type-" {
						activeBuckets = append(activeBuckets, fmt.Sprintf("%s:%d", bucketName, taskCount))
					}
				}
			}

			// Check if we're making progress
			if totalTasks == lastTaskCount {
				noProgressCount++
				if noProgressCount >= maxNoProgressCount {
					l.Warn("no task progress detected, assuming completion",
						zap.Int("no_progress_count", noProgressCount),
						zap.Int("total_tasks", totalTasks))
					return nil
				}
			} else {
				noProgressCount = 0
				lastTaskCount = totalTasks
			}

			// Check if all resource-specific tasks are complete
			// We need to ensure ALL resource types have finished processing
			if totalTasks == 0 {
				// Double-check that we're truly done with resource processing
				// Look for any active resource processing in the bucket stats
				allResourceProcessingComplete := true
				for bucketName, taskCount := range bucketStats {
					// Skip the default bucket (used for final tasks)
					if bucketName == "resource-type-" {
						continue
					}
					if taskCount > 0 {
						allResourceProcessingComplete = false
						break
					}
				}

				if allResourceProcessingComplete {
					// Additional safety check: wait a bit more to ensure workers are truly idle
					time.Sleep(2 * time.Second)

					// Check one more time to ensure no new tasks appeared
					finalBucketStats := ps.taskQueue.GetBucketStats()
					finalTotalTasks := 0
					for _, count := range finalBucketStats {
						finalTotalTasks += count
					}

					if finalTotalTasks == 0 {
						// Final check: ensure all workers are actually idle
						if ps.areWorkersIdle() {
							return nil
						} else {
							// Reset progress counters since we're not done yet
							noProgressCount = 0
							lastTaskCount = finalTotalTasks
						}
					} else {
						// Reset progress counters since we're not done yet
						noProgressCount = 0
						lastTaskCount = finalTotalTasks
					}
				}
			}
		}
	}
}

// syncGrantExpansion handles grant expansion by delegating to the base syncer
func (ps *parallelSyncer) syncGrantExpansion(ctx context.Context) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.syncGrantExpansion")
	defer span.End()

	// The base syncer's SyncGrantExpansion expects to have actions in its state stack
	// We need to set up the proper state context before calling it
	ps.syncer.state.PushAction(ctx, Action{
		Op: SyncGrantExpansionOp,
	})

	// Delegate to the base syncer's grant expansion logic
	// This ensures we get the exact same behavior as the sequential sync
	err := ps.syncer.SyncGrantExpansion(ctx)

	// Clean up the state
	ps.syncer.state.FinishAction(ctx)

	return err
}

// syncExternalResources handles external resources by delegating to the base syncer
func (ps *parallelSyncer) syncExternalResources(ctx context.Context) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.syncExternalResources")
	defer span.End()

	// The base syncer's SyncExternalResources expects to have actions in its state stack
	// We need to set up the proper state context before calling it
	ps.syncer.state.PushAction(ctx, Action{
		Op: SyncExternalResourcesOp,
	})

	// Delegate to the base syncer's external resources logic
	// This ensures we get the exact same behavior as the sequential sync
	err := ps.syncer.SyncExternalResources(ctx)

	// Clean up the state
	ps.syncer.state.FinishAction(ctx)

	return err
}

// finalizeSync performs final sync cleanup
func (ps *parallelSyncer) finalizeSync(ctx context.Context) error {
	// End sync
	if err := ps.syncer.store.EndSync(ctx); err != nil {
		return err
	}

	// Cleanup
	if err := ps.syncer.store.Cleanup(ctx); err != nil {
		return err
	}

	_, err := ps.syncer.connector.Cleanup(ctx, &v2.ConnectorServiceCleanupRequest{})
	if err != nil {
		ctxzap.Extract(ctx).Error("error clearing connector caches", zap.Error(err))
	}

	return nil
}

// syncResourceTypes syncs resource types (equivalent to SyncResourceTypes)
func (ps *parallelSyncer) syncResourceTypes(ctx context.Context) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.syncResourceTypes")
	defer span.End()

	// This replicates the exact logic from the original SyncResourceTypes
	resp, err := ps.syncer.connector.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{})
	if err != nil {
		return err
	}

	err = ps.syncer.store.PutResourceTypes(ctx, resp.List...)
	if err != nil {
		return err
	}

	ps.syncer.counts.AddResourceTypes(len(resp.List))

	return nil
}

// syncResources processes resources for a specific resource type (equivalent to SyncResources)
func (ps *parallelSyncer) syncResources(ctx context.Context, action Action) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.syncResources")
	defer span.End()

	l := ctxzap.Extract(ctx)

	// Add panic recovery to catch any unexpected errors
	defer func() {
		if r := recover(); r != nil {
			l.Error("panic in syncResources",
				zap.String("resource_type", action.ResourceTypeID),
				zap.Any("panic", r))
		}
	}()

	// This replicates the exact logic from the original SyncResources
	req := &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: action.ResourceTypeID,
		PageToken:      action.PageToken,
	}

	// If this is a child resource task, set the parent resource ID
	if action.ParentResourceID != "" {
		req.ParentResourceId = &v2.ResourceId{
			ResourceType: action.ParentResourceTypeID,
			Resource:     action.ParentResourceID,
		}
	}

	resp, err := ps.syncer.connector.ListResources(ctx, req)
	if err != nil {
		l.Error("failed to list resources", zap.Error(err))
		return err
	}

	// Store resources
	if len(resp.List) > 0 {
		err = ps.syncer.store.PutResources(ctx, resp.List...)
		if err != nil {
			l.Error("failed to store resources", zap.Error(err))
			return err
		}
	}

	// Update progress counts
	resourceTypeId := action.ResourceTypeID
	ps.syncer.counts.AddResources(resourceTypeId, len(resp.List))

	// Log progress
	if len(resp.List) > 0 {
		ps.syncer.counts.LogResourcesProgress(ctx, resourceTypeId)
	} else {
		// Even with no resources, we should log progress
		ps.syncer.counts.LogResourcesProgress(ctx, resourceTypeId)
	}

	// Process each resource (handle sub-resources)
	for _, r := range resp.List {
		// Use the base syncer's getSubResources method
		if err := ps.syncer.getSubResources(ctx, r); err != nil {
			l.Error("failed to process sub-resources", zap.Error(err))
			return err
		}
	}

	// Handle pagination - if there are more pages, create a task for the next page
	if resp.NextPageToken != "" {
		nextPageTask := &task{
			Action: Action{
				Op:             SyncResourcesOp,
				ResourceTypeID: action.ResourceTypeID,
				PageToken:      resp.NextPageToken,
			},
			Priority: 1,
		}

		if err := ps.taskQueue.AddTask(nextPageTask); err != nil {
			return fmt.Errorf("failed to add next page task for resource type %s: %w", action.ResourceTypeID, err)
		}

		return nil // Don't create entitlement/grant tasks yet, wait for all pages
	}

	// Get all resources for this resource type to create individual tasks
	allResourcesResp, err := ps.syncer.store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: action.ResourceTypeID,
		PageToken:      "",
	})
	if err != nil {
		l.Error("failed to list resources for task creation", zap.Error(err))
		return err
	}
	// Check if this resource type has child resource types that need to be processed
	// We need to process child resources before entitlements and grants
	if err := ps.processChildResourceTypes(ctx, action.ResourceTypeID); err != nil {
		l.Error("failed to process child resource types", zap.Error(err))
		return err
	}

	// Create individual tasks for each resource's entitlements and grants
	for _, resource := range allResourcesResp.List {
		// Check if we should skip entitlements and grants for this resource
		shouldSkip, err := ps.shouldSkipEntitlementsAndGrants(ctx, resource)
		if err != nil {
			l.Error("failed to check if resource should be skipped", zap.Error(err))
			return err
		}
		if shouldSkip {
			continue
		}

		// Create task to sync entitlements for this specific resource
		entitlementsTask := &task{
			Action: Action{
				Op:             SyncEntitlementsOp,
				ResourceTypeID: action.ResourceTypeID,
				ResourceID:     resource.Id.Resource,
			},
			Priority: 2,
		}

		if err := ps.taskQueue.AddTask(entitlementsTask); err != nil {
			return fmt.Errorf("failed to add entitlements task for resource %s: %w", resource.Id.Resource, err)
		}

		// Create task to sync grants for this specific resource
		grantsTask := &task{
			Action: Action{
				Op:             SyncGrantsOp,
				ResourceTypeID: action.ResourceTypeID,
				ResourceID:     resource.Id.Resource,
			},
			Priority: 3,
		}

		if err := ps.taskQueue.AddTask(grantsTask); err != nil {
			l.Error("failed to add grants task", zap.Error(err))
			return fmt.Errorf("failed to add grants task for resource %s: %w", resource.Id.Resource, err)
		}
	}

	return nil
}

// processChildResourceTypes processes child resource types for a given parent resource type
func (ps *parallelSyncer) processChildResourceTypes(ctx context.Context, parentResourceTypeID string) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.processChildResourceTypes")
	defer span.End()

	l := ctxzap.Extract(ctx)

	// Get all resources of the parent resource type
	resp, err := ps.syncer.store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: parentResourceTypeID,
		PageToken:      "",
	})
	if err != nil {
		l.Error("failed to list parent resources", zap.Error(err))
		return err
	}

	// For each parent resource, check if it has child resource types
	for _, parentResource := range resp.List {
		if err := ps.processChildResourcesForParent(ctx, parentResource); err != nil {
			l.Error("failed to process child resources for parent",
				zap.Error(err),
				zap.String("parent_resource_id", parentResource.Id.Resource),
				zap.String("parent_resource_type", parentResource.Id.ResourceType))
			return err
		}
	}

	return nil
}

// processChildResourcesForParent processes child resources for a specific parent resource
func (ps *parallelSyncer) processChildResourcesForParent(ctx context.Context, parentResource *v2.Resource) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.processChildResourcesForParent")
	defer span.End()

	// Check for ChildResourceType annotations
	for _, annotation := range parentResource.Annotations {
		var childResourceType v2.ChildResourceType
		if err := annotation.UnmarshalTo(&childResourceType); err != nil {
			// Not a ChildResourceType annotation, skip
			continue
		}

		childResourceTypeID := childResourceType.ResourceTypeId

		// Create a task to sync child resources for this parent
		childResourcesTask := &task{
			Action: Action{
				Op:                   SyncResourcesOp,
				ResourceTypeID:       childResourceTypeID,
				ParentResourceTypeID: parentResource.Id.ResourceType,
				ParentResourceID:     parentResource.Id.Resource,
			},
			Priority: 1, // Lower priority than parent resources
		}

		if err := ps.taskQueue.AddTask(childResourcesTask); err != nil {
			return fmt.Errorf("failed to add child resources task for %s under parent %s: %w",
				childResourceTypeID, parentResource.Id.Resource, err)
		}
	}

	return nil
}

// syncEntitlementsForResourceType processes entitlements for all resources of a resource type
func (ps *parallelSyncer) syncEntitlementsForResourceType(ctx context.Context, action Action) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.syncEntitlementsForResourceType")
	defer span.End()

	l := ctxzap.Extract(ctx)

	// Get all resources for this resource type
	resp, err := ps.syncer.store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: action.ResourceTypeID,
		PageToken:      action.PageToken,
	})
	if err != nil {
		l.Error("failed to list resources for entitlements", zap.Error(err))
		return err
	}

	// Process each resource's entitlements sequentially
	for _, r := range resp.List {
		// Check if we should skip entitlements for this resource
		shouldSkip, err := ps.shouldSkipEntitlementsAndGrants(ctx, r)
		if err != nil {
			return err
		}
		if shouldSkip {
			continue
		}

		// Create local state context for this resource
		localState := NewLocalStateContext(r.Id)

		// Use our state-agnostic method to sync entitlements for this specific resource
		decision, err := ps.syncEntitlementsForResourceLogic(ctx, r.Id, localState)
		if err != nil {
			l.Error("failed to sync entitlements for resource",
				zap.String("resource_type", r.Id.ResourceType),
				zap.String("resource_id", r.Id.Resource),
				zap.Error(err))
			return err
		}

		// Handle pagination if needed
		for decision.ShouldContinue && decision.Action == "next_page" {

			// Update the local state with the new page token before continuing
			if err := localState.NextPage(ctx, decision.NextPageToken); err != nil {
				l.Error("failed to update local state with next page token",
					zap.String("resource_type", r.Id.ResourceType),
					zap.String("page_token", decision.NextPageToken),
					zap.Error(err))
				return err
			}

			// Continue with next page
			decision, err = ps.syncEntitlementsForResourceLogic(ctx, r.Id, localState)
			if err != nil {
				l.Error("failed to sync entitlements for resource on next page",
					zap.String("resource_type", r.Id.ResourceType),
					zap.String("resource_id", r.Id.Resource),
					zap.Error(err))
				return err
			}
		}
	}

	return nil
}

// syncEntitlementsForResource processes entitlements for a specific resource
func (ps *parallelSyncer) syncEntitlementsForResource(ctx context.Context, action Action) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.syncEntitlementsForResource")
	defer span.End()

	l := ctxzap.Extract(ctx)
	// Create resource ID from action
	resourceID := &v2.ResourceId{
		ResourceType: action.ResourceTypeID,
		Resource:     action.ResourceID,
	}

	// Create local state context for this resource
	localState := NewLocalStateContext(resourceID)

	// Use existing logic but for single resource
	decision, err := ps.syncEntitlementsForResourceLogic(ctx, resourceID, localState)
	if err != nil {
		l.Error("failed to sync entitlements for resource",
			zap.String("resource_type", action.ResourceTypeID),
			zap.String("resource_id", action.ResourceID),
			zap.Error(err))
		return err
	}

	// Handle pagination if needed
	for decision.ShouldContinue && decision.Action == "next_page" {
		// Update the local state with the new page token before continuing
		if err := localState.NextPage(ctx, decision.NextPageToken); err != nil {
			l.Error("failed to update local state with next page token",
				zap.String("resource_type", action.ResourceTypeID),
				zap.String("resource_id", action.ResourceID),
				zap.String("page_token", decision.NextPageToken),
				zap.Error(err))
			return err
		}

		// Continue with next page
		decision, err = ps.syncEntitlementsForResourceLogic(ctx, resourceID, localState)
		if err != nil {
			l.Error("failed to sync entitlements for resource on next page",
				zap.String("resource_type", action.ResourceTypeID),
				zap.String("resource_id", action.ResourceID),
				zap.Error(err))
			return err
		}
	}

	return nil
}

// syncGrantsForResource processes grants for a specific resource
func (ps *parallelSyncer) syncGrantsForResource(ctx context.Context, action Action) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.syncGrantsForResource")
	defer span.End()

	l := ctxzap.Extract(ctx)
	// Create resource ID from action
	resourceID := &v2.ResourceId{
		ResourceType: action.ResourceTypeID,
		Resource:     action.ResourceID,
	}

	// Create local state context for this resource
	localState := NewLocalStateContext(resourceID)

	// Use existing logic but for single resource
	decision, err := ps.syncGrantsForResourceLogic(ctx, resourceID, localState)
	if err != nil {
		l.Error("failed to sync grants for resource",
			zap.String("resource_type", action.ResourceTypeID),
			zap.String("resource_id", action.ResourceID),
			zap.Error(err))
		return err
	}

	// Handle pagination if needed
	for decision.ShouldContinue && decision.Action == "next_page" {
		// Update the local state with the new page token before continuing
		if err := localState.NextPage(ctx, decision.NextPageToken); err != nil {
			l.Error("failed to update local state with next page token",
				zap.String("resource_type", action.ResourceTypeID),
				zap.String("resource_id", action.ResourceID),
				zap.String("page_token", decision.NextPageToken),
				zap.Error(err))
			return err
		}

		// Continue with next page
		decision, err = ps.syncGrantsForResourceLogic(ctx, resourceID, localState)
		if err != nil {
			l.Error("failed to sync grants for resource on next page",
				zap.String("resource_type", action.ResourceTypeID),
				zap.String("resource_id", action.ResourceID),
				zap.Error(err))
			return err
		}
	}

	return nil
}

// syncGrantsForResourceType processes grants for all resources of a resource type
func (ps *parallelSyncer) syncGrantsForResourceType(ctx context.Context, action Action) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.syncGrantsForResourceType")
	defer span.End()

	l := ctxzap.Extract(ctx)

	// Get all resources for this resource type
	resp, err := ps.syncer.store.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId: action.ResourceTypeID,
		PageToken:      action.PageToken,
	})
	if err != nil {
		l.Error("failed to list resources for grants", zap.Error(err))
		return err
	}

	// Process each resource's grants sequentially
	for _, r := range resp.List {
		// Check if we should skip grants for this resource
		shouldSkip, err := ps.shouldSkipEntitlementsAndGrants(ctx, r)
		if err != nil {
			return err
		}
		if shouldSkip {
			continue
		}

		// Create local state context for this resource
		localState := NewLocalStateContext(r.Id)

		// Use our state-agnostic method to sync grants for this specific resource
		decision, err := ps.syncGrantsForResourceLogic(ctx, r.Id, localState)
		if err != nil {
			l.Error("failed to sync grants for resource",
				zap.String("resource_type", r.Id.ResourceType),
				zap.String("resource_id", r.Id.Resource),
				zap.Error(err))
			return err
		}

		// Handle pagination if needed
		for decision.ShouldContinue && decision.Action == "next_page" {
			// Update the local state with the new page token before continuing
			if err := localState.NextPage(ctx, decision.NextPageToken); err != nil {
				l.Error("failed to update local state with next page token",
					zap.String("resource_type", r.Id.ResourceType),
					zap.String("resource_id", r.Id.Resource),
					zap.String("page_token", decision.NextPageToken),
					zap.Error(err))
				return err
			}

			// Continue with next page
			decision, err = ps.syncGrantsForResourceLogic(ctx, r.Id, localState)
			if err != nil {
				l.Error("failed to sync grants for resource on next page",
					zap.String("resource_type", r.Id.ResourceType),
					zap.String("resource_id", r.Id.Resource),
					zap.Error(err))
				return err
			}
		}
	}

	return nil
}

// syncGrantsForResourceLogic contains the core logic for syncing grants for a resource
// This method is state-agnostic and returns an ActionDecision for the caller to handle
func (ps *parallelSyncer) syncGrantsForResourceLogic(ctx context.Context, resourceID *v2.ResourceId, state StateInterface) (*ActionDecision, error) {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.syncGrantsForResourceLogic")
	defer span.End()

	l := ctxzap.Extract(ctx)

	// Get the resource from the store
	resourceResponse, err := ps.syncer.store.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: resourceID,
	})
	if err != nil {
		return nil, fmt.Errorf("error fetching resource '%s': %w", resourceID.Resource, err)
	}

	resource := resourceResponse.Resource

	var prevSyncID string
	var prevEtag *v2.ETag
	var etagMatch bool
	var grants []*v2.Grant

	resourceAnnos := annotations.Annotations(resource.GetAnnotations())
	pageToken := state.PageToken(ctx)

	prevSyncID, prevEtag, err = ps.syncer.fetchResourceForPreviousSync(ctx, resourceID)
	if err != nil {
		return nil, err
	}
	resourceAnnos.Update(prevEtag)
	resource.Annotations = resourceAnnos

	resp, err := ps.syncer.connector.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{Resource: resource, PageToken: pageToken})
	if err != nil {
		return nil, err
	}

	// Fetch any etagged grants for this resource
	var etaggedGrants []*v2.Grant
	etaggedGrants, etagMatch, err = ps.syncer.fetchEtaggedGrantsForResource(ctx, resource, prevEtag, prevSyncID, resp)
	if err != nil {
		return nil, err
	}
	grants = append(grants, etaggedGrants...)

	// We want to process any grants from the previous sync first so that if there is a conflict, the newer data takes precedence
	grants = append(grants, resp.List...)

	// Process grants and collect state information
	needsExpansion := false
	hasExternalResources := false
	shouldFetchRelated := state.ShouldFetchRelatedResources()

	for _, grant := range grants {
		grantAnnos := annotations.Annotations(grant.GetAnnotations())
		if grantAnnos.Contains(&v2.GrantExpandable{}) {
			needsExpansion = true
			state.SetNeedsExpansion()
		}
		if grantAnnos.ContainsAny(&v2.ExternalResourceMatchAll{}, &v2.ExternalResourceMatch{}, &v2.ExternalResourceMatchID{}) {
			hasExternalResources = true
			state.SetHasExternalResourcesGrants()
		}

		if !shouldFetchRelated {
			continue
		}
		// Some connectors emit grants for other resources. If we're doing a partial sync, check if it exists and queue a fetch if not.
		entitlementResource := grant.GetEntitlement().GetResource()
		_, err := ps.syncer.store.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
			ResourceId: entitlementResource.GetId(),
		})
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}

			erId := entitlementResource.GetId()
			prId := entitlementResource.GetParentResourceId()
			resource, err := ps.syncer.getResourceFromConnector(ctx, erId, prId)
			if err != nil {
				l.Error("error fetching entitlement resource", zap.Error(err))
				return nil, err
			}
			if resource == nil {
				continue
			}
			if err := ps.syncer.store.PutResources(ctx, resource); err != nil {
				return nil, err
			}
		}
	}

	// Store the grants
	err = ps.syncer.store.PutGrants(ctx, grants...)
	if err != nil {
		return nil, err
	}

	// Update progress counts
	ps.syncer.counts.AddGrantsProgress(resourceID.ResourceType, len(grants))
	ps.syncer.counts.LogGrantsProgress(ctx, resourceID.ResourceType)

	// We may want to update the etag on the resource. If we matched a previous etag, then we should use that.
	// Otherwise, we should use the etag from the response if provided.
	var updatedETag *v2.ETag

	if etagMatch {
		updatedETag = prevEtag
	} else {
		newETag := &v2.ETag{}
		respAnnos := annotations.Annotations(resp.GetAnnotations())
		ok, err := respAnnos.Pick(newETag)
		if err != nil {
			return nil, err
		}
		if ok {
			updatedETag = newETag
		}
	}

	if updatedETag != nil {
		resourceAnnos.Update(updatedETag)
		resource.Annotations = resourceAnnos
		err = ps.syncer.store.PutResources(ctx, resource)
		if err != nil {
			return nil, err
		}
	}

	// Check if we need to continue with pagination
	if resp.NextPageToken != "" {
		return &ActionDecision{
			ShouldContinue:       true,
			NextPageToken:        resp.NextPageToken,
			Action:               "next_page",
			NeedsExpansion:       needsExpansion,
			HasExternalResources: hasExternalResources,
			ShouldFetchRelated:   shouldFetchRelated,
		}, nil
	}

	// No more pages, action is complete
	return &ActionDecision{
		ShouldContinue:       false,
		Action:               "finish",
		NeedsExpansion:       needsExpansion,
		HasExternalResources: hasExternalResources,
		ShouldFetchRelated:   shouldFetchRelated,
	}, nil
}

// syncEntitlementsForResourceLogic contains the core logic for syncing entitlements for a resource
// This method is state-agnostic and returns an ActionDecision for the caller to handle
func (ps *parallelSyncer) syncEntitlementsForResourceLogic(ctx context.Context, resourceID *v2.ResourceId, state StateInterface) (*ActionDecision, error) {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.syncEntitlementsForResourceLogic")
	defer span.End()

	// Get the resource from the store
	resourceResponse, err := ps.syncer.store.GetResource(ctx, &reader_v2.ResourcesReaderServiceGetResourceRequest{
		ResourceId: resourceID,
	})
	if err != nil {
		return nil, fmt.Errorf("error fetching resource '%s': %w", resourceID.Resource, err)
	}

	resource := resourceResponse.Resource
	pageToken := state.PageToken(ctx)

	// Call the connector to list entitlements for this resource
	resp, err := ps.syncer.connector.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{
		Resource:  resource,
		PageToken: pageToken,
	})
	if err != nil {
		return nil, err
	}

	// Store the entitlements
	err = ps.syncer.store.PutEntitlements(ctx, resp.List...)
	if err != nil {
		return nil, err
	}

	// Update progress counts
	ps.syncer.counts.AddEntitlementsProgress(resourceID.ResourceType, len(resp.List))
	ps.syncer.counts.LogEntitlementsProgress(ctx, resourceID.ResourceType)

	// Check if we need to continue with pagination
	if resp.NextPageToken != "" {
		return &ActionDecision{
			ShouldContinue: true,
			NextPageToken:  resp.NextPageToken,
			Action:         "next_page",
		}, nil
	}

	// No more pages, action is complete
	return &ActionDecision{
		ShouldContinue: false,
		Action:         "finish",
	}, nil
}

// getSubResources fetches the sub resource types from a resources' annotations (replicating original logic)
func (ps *parallelSyncer) getSubResources(ctx context.Context, parent *v2.Resource) error {
	ctx, span := parallelTracer.Start(ctx, "parallelSyncer.getSubResources")
	defer span.End()

	for _, a := range parent.Annotations {
		if a.MessageIs((*v2.ChildResourceType)(nil)) {
			crt := &v2.ChildResourceType{}
			err := a.UnmarshalTo(crt)
			if err != nil {
				return err
			}

			// Create task for child resource type
			childTask := &task{
				Action: Action{
					Op:                   SyncResourcesOp,
					ResourceTypeID:       crt.ResourceTypeId,
					ParentResourceID:     parent.Id.Resource,
					ParentResourceTypeID: parent.Id.ResourceType,
				},
				Priority: 1,
			}

			if err := ps.taskQueue.AddTask(childTask); err != nil {
				return fmt.Errorf("failed to add child resource task: %w", err)
			}
		}
	}

	return nil
}

// shouldSkipEntitlementsAndGrants checks if entitlements and grants should be skipped for a resource
func (ps *parallelSyncer) shouldSkipEntitlementsAndGrants(ctx context.Context, r *v2.Resource) (bool, error) {
	// This replicates the logic from the original shouldSkipEntitlementsAndGrants method
	// Check if the resource has the SkipEntitlementsAndGrants annotation

	for _, a := range r.Annotations {
		if a.MessageIs((*v2.SkipEntitlementsAndGrants)(nil)) {
			return true, nil
		}
	}

	return false, nil
}

// Close implements the Syncer interface
func (ps *parallelSyncer) Close(ctx context.Context) error {
	// Stop all workers
	ps.stopWorkers()

	// Close the task queue
	if ps.taskQueue != nil {
		ps.taskQueue.Close()
	}

	// Call the base syncer's Close method
	return ps.syncer.Close(ctx)
}

// GetBucketStats returns statistics about all buckets
func (ps *parallelSyncer) GetBucketStats() map[string]int {
	if ps.taskQueue == nil {
		return make(map[string]int)
	}
	return ps.taskQueue.GetBucketStats()
}

// GetWorkerStatus returns the status of all workers
func (ps *parallelSyncer) GetWorkerStatus() []map[string]interface{} {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	status := make([]map[string]interface{}, len(ps.workers))
	for i, worker := range ps.workers {
		status[i] = map[string]interface{}{
			"worker_id":    worker.id,
			"rate_limited": worker.rateLimited.Load(),
		}
	}
	return status
}

// NewParallelSyncerFromSyncer creates a parallel syncer from an existing syncer
func NewParallelSyncerFromSyncer(s Syncer, config *ParallelSyncConfig) (*parallelSyncer, error) {
	// Try to cast to the concrete syncer type
	if baseSyncer, ok := s.(*SequentialSyncer); ok {
		return NewParallelSyncer(baseSyncer, config), nil
	}

	return nil, fmt.Errorf("cannot create parallel syncer from syncer type: %T", s)
}

// syncGrantsForResourceSequential is the refactored version that returns ActionDecision
// This can be called by the sequential syncer to get the same behavior but with explicit control
func (ps *parallelSyncer) syncGrantsForResourceSequential(ctx context.Context, resourceID *v2.ResourceId) (*ActionDecision, error) {
	// Create a state interface that delegates to the base syncer's state
	stateWrapper := &sequentialStateWrapper{syncer: ps.syncer}
	return ps.syncGrantsForResourceLogic(ctx, resourceID, stateWrapper)
}

// syncEntitlementsForResourceSequential is the refactored version that returns ActionDecision
// This can be called by the sequential syncer to get the same behavior but with explicit control
func (ps *parallelSyncer) syncEntitlementsForResourceSequential(ctx context.Context, resourceID *v2.ResourceId) (*ActionDecision, error) {
	// Create a state interface that delegates to the base syncer's state
	stateWrapper := &sequentialStateWrapper{syncer: ps.syncer}
	return ps.syncEntitlementsForResourceLogic(ctx, resourceID, stateWrapper)
}

// sequentialStateWrapper implements StateInterface by delegating to the base syncer's state
// This allows the sequential syncer to use the refactored methods while maintaining its state machine
type sequentialStateWrapper struct {
	syncer *SequentialSyncer
}

func (sw *sequentialStateWrapper) PageToken(ctx context.Context) string {
	return sw.syncer.state.PageToken(ctx)
}

func (sw *sequentialStateWrapper) NextPage(ctx context.Context, pageToken string) error {
	return sw.syncer.state.NextPage(ctx, pageToken)
}

func (sw *sequentialStateWrapper) SetNeedsExpansion() {
	sw.syncer.state.SetNeedsExpansion()
}

func (sw *sequentialStateWrapper) SetHasExternalResourcesGrants() {
	sw.syncer.state.SetHasExternalResourcesGrants()
}

func (sw *sequentialStateWrapper) ShouldFetchRelatedResources() bool {
	return sw.syncer.state.ShouldFetchRelatedResources()
}
