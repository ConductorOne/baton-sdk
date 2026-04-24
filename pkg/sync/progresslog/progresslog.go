package progresslog

import (
	"context"
	"sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const defaultMaxLogFrequency = 10 * time.Second

type rwMutex interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

// noOpMutex fakes a mutex for sequential sync.
type noOpMutex struct{}

func (m *noOpMutex) Lock()    {}
func (m *noOpMutex) Unlock()  {}
func (m *noOpMutex) RLock()   {}
func (m *noOpMutex) RUnlock() {}

type ProgressLog struct {
	resourceTypes        int
	resources            map[string]int
	entitlementsProgress map[string]int
	lastEntitlementLog   map[string]time.Time
	grantsProgress       map[string]int
	lastGrantLog         map[string]time.Time
	mu                   rwMutex // If noOpMutex, sequential mode is enabled. If sync.RWMutex, parallel mode is enabled.
	l                    *zap.Logger
	maxLogFrequency      time.Duration

	// Optional cross-step db-size tracking for LogExpandProgress. Populated
	// via WithDBSizeProvider at construction time or SetDBSizeProvider after
	// loadStore; nil means "size-free" expand logs. Held here (not on
	// Expander) because the syncer recreates the Expander per RunSingleStep,
	// so any state on it resets every step and the periodic size log would
	// never fire in production.
	//
	// Guarded by expandMu — a dedicated mutex separate from p.mu so the
	// provider's stat I/O doesn't run under the shared hot-path mutex that
	// AddResources / AddGrantsProgress contend on.
	expandMu         sync.Mutex
	lastActionLog    time.Time // rate-limit timestamp for LogExpandProgress
	dbSize           connectorstore.DBSizeProvider
	lastLoggedDBSize int64
	hasLoggedDBSize  bool // false until the first successful size read; gates the delta field
}

type Option func(*ProgressLog)

func WithLogger(l *zap.Logger) Option {
	return func(p *ProgressLog) {
		// Don't allow a nil logger to be set, as that would cause a panic.
		if l != nil {
			p.l = l
		}
	}
}

// WithSequentialMode enables/disables mutex protection for sequential sync.
func WithSequentialMode(sequential bool) Option {
	return func(p *ProgressLog) {
		if sequential {
			p.mu = &noOpMutex{}
		} else {
			p.mu = &sync.RWMutex{}
		}
	}
}

func WithLogFrequency(logFrequency time.Duration) Option {
	return func(p *ProgressLog) {
		p.maxLogFrequency = logFrequency
	}
}

// WithDBSizeProvider attaches an optional connectorstore.DBSizeProvider to
// this ProgressLog. When set, LogExpandProgress will include
// decompressed_bytes and decompressed_bytes_delta (growth since the previous
// log) in its output.
//
// This is one of two attachment points — the syncer also calls
// SetDBSizeProvider after loadStore resolves the store, because many callers
// construct NewSyncer via WithC1ZPath (store is nil at NewProgressCounts
// time) and can only be wired once loadStore has run.
//
// nil is a valid value (equivalent to not setting it) and the log falls
// back to the pre-existing action-count-only output.
//
// The log is primarily useful during long grant expansions for catching
// pathological growth before the decoder cap trips on a subsequent read.
// Compare the emitted bytes against BATON_DECODER_MAX_DECODED_SIZE_MB
// (default 3 GiB; see pkg/dotc1z/decoder.go). If a tenant's db size is
// approaching that cap, the per-tenant WithDecoderMaxDecodedSize override
// is the operator's next step.
func WithDBSizeProvider(p connectorstore.DBSizeProvider) Option {
	return func(o *ProgressLog) {
		o.dbSize = p
	}
}

// SetDBSizeProvider updates the DBSizeProvider attached to this ProgressLog.
// Intended for callers (notably the syncer) that construct the ProgressLog
// before the store is known (e.g. WithC1ZPath path, where the store is
// loaded from disk later by loadStore). Idempotent; safe to call multiple
// times — the last non-nil value wins if called from multiple sites.
// Passing nil clears the provider.
func (p *ProgressLog) SetDBSizeProvider(provider connectorstore.DBSizeProvider) {
	p.expandMu.Lock()
	defer p.expandMu.Unlock()
	p.dbSize = provider
}

func NewProgressCounts(ctx context.Context, opts ...Option) *ProgressLog {
	p := &ProgressLog{
		resources:            make(map[string]int),
		entitlementsProgress: make(map[string]int),
		lastEntitlementLog:   make(map[string]time.Time),
		grantsProgress:       make(map[string]int),
		lastGrantLog:         make(map[string]time.Time),
		l:                    ctxzap.Extract(ctx),
		maxLogFrequency:      defaultMaxLogFrequency,
		mu:                   &noOpMutex{}, // Default to sequential mode for backward compatibility
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

func (p *ProgressLog) LogResourceTypesProgress(ctx context.Context) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.l.Info("Synced resource types", zap.Int("count", p.resourceTypes))
}

func (p *ProgressLog) LogResourcesProgress(ctx context.Context, resourceType string) {
	var resources int
	p.mu.RLock()
	resources = p.resources[resourceType]
	p.mu.RUnlock()

	p.l.Info("Synced resources", zap.String("resource_type_id", resourceType), zap.Int("count", resources))
}

func (p *ProgressLog) LogEntitlementsProgress(ctx context.Context, resourceType string) {
	var entitlementsProgress, resources int
	var lastLogTime time.Time

	p.mu.RLock()
	entitlementsProgress = p.entitlementsProgress[resourceType]
	resources = p.resources[resourceType]
	lastLogTime = p.lastEntitlementLog[resourceType]
	p.mu.RUnlock()

	if resources == 0 {
		// if resuming sync, resource counts will be zero, so don't calculate percentage. just log every 10 seconds.
		if time.Since(lastLogTime) > p.maxLogFrequency {
			p.l.Info("Syncing entitlements",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", entitlementsProgress),
			)
			p.mu.Lock()
			p.lastEntitlementLog[resourceType] = time.Now()
			p.mu.Unlock()
		}
		return
	}

	percentComplete := (entitlementsProgress * 100) / resources

	switch {
	case percentComplete == 100:
		p.l.Info("Synced entitlements",
			zap.String("resource_type_id", resourceType),
			zap.Int("count", entitlementsProgress),
			zap.Int("total", resources),
		)
		p.mu.Lock()
		p.lastEntitlementLog[resourceType] = time.Time{}
		p.mu.Unlock()
	case time.Since(lastLogTime) > p.maxLogFrequency:
		if entitlementsProgress > resources {
			p.l.Warn("more entitlement resources than resources",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", entitlementsProgress),
				zap.Int("total", resources),
			)
		} else {
			p.l.Info("Syncing entitlements",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", entitlementsProgress),
				zap.Int("total", resources),
				zap.Int("percent_complete", percentComplete),
			)
		}
		p.mu.Lock()
		p.lastEntitlementLog[resourceType] = time.Now()
		p.mu.Unlock()
	}
}

func (p *ProgressLog) LogGrantsProgress(ctx context.Context, resourceType string) {
	var grantsProgress, resources int
	var lastLogTime time.Time

	p.mu.RLock()
	grantsProgress = p.grantsProgress[resourceType]
	resources = p.resources[resourceType]
	lastLogTime = p.lastGrantLog[resourceType]
	p.mu.RUnlock()

	if resources == 0 {
		// if resuming sync, resource counts will be zero, so don't calculate percentage. just log every 10 seconds.
		if time.Since(lastLogTime) > p.maxLogFrequency {
			p.l.Info("Syncing grants",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", grantsProgress),
			)
			p.mu.Lock()
			p.lastGrantLog[resourceType] = time.Now()
			p.mu.Unlock()
		}
		return
	}

	percentComplete := (grantsProgress * 100) / resources

	switch {
	case percentComplete == 100:
		p.l.Info("Synced grants",
			zap.String("resource_type_id", resourceType),
			zap.Int("count", grantsProgress),
			zap.Int("total", resources),
		)
		p.mu.Lock()
		p.lastGrantLog[resourceType] = time.Time{}
		p.mu.Unlock()
	case time.Since(lastLogTime) > p.maxLogFrequency:
		if grantsProgress > resources {
			p.l.Warn("more grant resources than resources",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", grantsProgress),
				zap.Int("total", resources),
			)
		} else {
			p.l.Info("Syncing grants",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", grantsProgress),
				zap.Int("total", resources),
				zap.Int("percent_complete", percentComplete),
			)
		}
		p.mu.Lock()
		p.lastGrantLog[resourceType] = time.Now()
		p.mu.Unlock()
	}
}

// LogExpandProgress emits an Info-level "Expanding grants" log at most once
// per maxLogFrequency window. When a DBSizeProvider is attached (production:
// *dotc1z.C1File), includes the live uncompressed db size and the growth
// since the previous emitted log (`decompressed_bytes_delta`) — the signal
// that surfaces non-linear expansion growth before the next saveC1z.
//
// All expand-log state (lastActionLog, dbSize, lastLoggedDBSize,
// hasLoggedDBSize) is guarded by p.expandMu, a mutex dedicated to this
// function. It is deliberately separate from p.mu so the provider's stat
// calls can run under the lock (simple defer pattern) without serialising
// against the sync-hot-path counters (AddResources / AddGrantsProgress),
// which contend on p.mu.
func (p *ProgressLog) LogExpandProgress(ctx context.Context, actions []*expand.EntitlementGraphAction) {
	p.expandMu.Lock()
	defer p.expandMu.Unlock()

	if time.Since(p.lastActionLog) < p.maxLogFrequency {
		return
	}
	p.lastActionLog = time.Now()

	fields := []zap.Field{zap.Int("actions_remaining", len(actions))}

	if p.dbSize != nil {
		if sz, err := p.dbSize.CurrentDBSizeBytes(); err == nil {
			fields = append(fields, zap.Int64("decompressed_bytes", sz))
			// Omit the delta on the very first sample — it would equal the
			// full size and create a spurious spike in any Datadog graph of
			// growth rate. Subsequent samples compute delta relative to the
			// previous emitted size, which is the real signal operators want.
			if p.hasLoggedDBSize {
				fields = append(fields, zap.Int64("decompressed_bytes_delta", sz-p.lastLoggedDBSize))
			}
			p.lastLoggedDBSize = sz
			p.hasLoggedDBSize = true
		}
	}

	ctxzap.Extract(ctx).Info("Expanding grants", fields...)
}

// Thread-safe methods for parallel syncer

// AddResourceTypes safely adds to the resource types count.
func (p *ProgressLog) AddResourceTypes(count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resourceTypes += count
}

// AddResources safely adds to the resources count for a specific resource type.
func (p *ProgressLog) AddResources(resourceType string, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resources[resourceType] += count
}

// AddEntitlementsProgress safely adds to the entitlements progress count for a specific resource type.
func (p *ProgressLog) AddEntitlementsProgress(resourceType string, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.entitlementsProgress[resourceType] += count
}

// AddGrantsProgress safely adds to the grants progress count for a specific resource type.
func (p *ProgressLog) AddGrantsProgress(resourceType string, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.grantsProgress[resourceType] += count
}
