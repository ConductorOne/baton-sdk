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
	ResourceTypes        int
	Resources            map[string]int
	EntitlementsProgress map[string]int
	LastEntitlementLog   map[string]time.Time
	GrantsProgress       map[string]int
	LastGrantLog         map[string]time.Time
	LastActionLog        time.Time
	mu                   rwMutex // If noOpMutex, sequential mode is enabled. If sync.RWMutex, parallel mode is enabled.
	l                    *zap.Logger
	maxLogFrequency      time.Duration
}

type Option func(*ProgressLog)

func WithLogger(l *zap.Logger) Option {
	return func(p *ProgressLog) {
		p.l = l
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
		Resources:            make(map[string]int),
		EntitlementsProgress: make(map[string]int),
		LastEntitlementLog:   make(map[string]time.Time),
		GrantsProgress:       make(map[string]int),
		LastGrantLog:         make(map[string]time.Time),
		LastActionLog:        time.Time{},
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
	p.l.Info("Synced resource types", zap.Int("count", p.ResourceTypes))
}

func (p *ProgressLog) LogResourcesProgress(ctx context.Context, resourceType string) {
	var resources int
	p.mu.RLock()
	resources = p.Resources[resourceType]
	p.mu.RUnlock()

	p.l.Info("Synced resources", zap.String("resource_type_id", resourceType), zap.Int("count", resources))
}

func (p *ProgressLog) LogEntitlementsProgress(ctx context.Context, resourceType string) {
	var entitlementsProgress, resources int
	var lastLogTime time.Time

	p.mu.RLock()
	entitlementsProgress = p.EntitlementsProgress[resourceType]
	resources = p.Resources[resourceType]
	lastLogTime = p.LastEntitlementLog[resourceType]
	p.mu.RUnlock()

	if resources == 0 {
		// if resuming sync, resource counts will be zero, so don't calculate percentage. just log every 10 seconds.
		if time.Since(lastLogTime) > p.maxLogFrequency {
			p.l.Info("Syncing entitlements",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", entitlementsProgress),
			)
			p.mu.Lock()
			p.LastEntitlementLog[resourceType] = time.Now()
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
		p.LastEntitlementLog[resourceType] = time.Time{}
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
		p.LastEntitlementLog[resourceType] = time.Now()
		p.mu.Unlock()
	}
}

func (p *ProgressLog) LogGrantsProgress(ctx context.Context, resourceType string) {
	var grantsProgress, resources int
	var lastLogTime time.Time

	p.mu.RLock()
	grantsProgress = p.GrantsProgress[resourceType]
	resources = p.Resources[resourceType]
	lastLogTime = p.LastGrantLog[resourceType]
	p.mu.RUnlock()

	if resources == 0 {
		// if resuming sync, resource counts will be zero, so don't calculate percentage. just log every 10 seconds.
		if time.Since(lastLogTime) > p.maxLogFrequency {
			p.l.Info("Syncing grants",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", grantsProgress),
			)
			p.mu.Lock()
			p.LastGrantLog[resourceType] = time.Now()
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
		p.LastGrantLog[resourceType] = time.Time{}
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
		p.LastGrantLog[resourceType] = time.Now()
		p.mu.Unlock()
	}
}

func (p *ProgressLog) LogExpandProgress(ctx context.Context, actions []*expand.EntitlementGraphAction) {
	actionsLen := len(actions)

	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.LastActionLog) < p.maxLogFrequency {
		return
	}
	p.LastActionLog = time.Now()

	l := ctxzap.Extract(ctx)
	l.Info("Expanding grants", zap.Int("actions_remaining", actionsLen))
}

// Thread-safe methods for parallel syncer

// AddResourceTypes safely adds to the resource types count.
func (p *ProgressLog) AddResourceTypes(count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ResourceTypes += count
}

// AddResources safely adds to the resources count for a specific resource type.
func (p *ProgressLog) AddResources(resourceType string, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Resources[resourceType] += count
}

// AddEntitlementsProgress safely adds to the entitlements progress count for a specific resource type.
func (p *ProgressLog) AddEntitlementsProgress(resourceType string, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.EntitlementsProgress[resourceType] += count
}

// AddGrantsProgress safely adds to the grants progress count for a specific resource type.
func (p *ProgressLog) AddGrantsProgress(resourceType string, count int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.GrantsProgress[resourceType] += count
}

// SetSequentialMode enables/disables mutex protection for sequential sync.
// TODO: Remove this method and use the WithSequentialMode option instead.
func (p *ProgressLog) SetSequentialMode(sequential bool) {
	if sequential {
		p.mu = &noOpMutex{}
	} else {
		p.mu = &sync.RWMutex{}
	}
}
