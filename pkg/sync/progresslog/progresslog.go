package progresslog

import (
	"context"
	"sync"
	"time"

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
	lastActionLog        time.Time
	mu                   rwMutex // If noOpMutex, sequential mode is enabled. If sync.RWMutex, parallel mode is enabled.
	l                    *zap.Logger
	maxLogFrequency      time.Duration
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

func NewProgressCounts(ctx context.Context, opts ...Option) *ProgressLog {
	p := &ProgressLog{
		resources:            make(map[string]int),
		entitlementsProgress: make(map[string]int),
		lastEntitlementLog:   make(map[string]time.Time),
		grantsProgress:       make(map[string]int),
		lastGrantLog:         make(map[string]time.Time),
		lastActionLog:        time.Time{},
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

func (p *ProgressLog) LogExpandProgress(ctx context.Context, actions []*expand.EntitlementGraphAction) {
	actionsLen := len(actions)

	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.lastActionLog) < p.maxLogFrequency {
		return
	}
	p.lastActionLog = time.Now()

	l := ctxzap.Extract(ctx)
	l.Info("Expanding grants", zap.Int("actions_remaining", actionsLen))
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
