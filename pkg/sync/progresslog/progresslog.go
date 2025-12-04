package progresslog

import (
	"context"
	"sync"
	"time"

	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const maxLogFrequency = 10 * time.Second

type ProgressLog struct {
	ResourceTypes        int
	Resources            map[string]int
	EntitlementsProgress map[string]int
	LastEntitlementLog   map[string]time.Time
	GrantsProgress       map[string]int
	LastGrantLog         map[string]time.Time
	LastActionLog        time.Time
	mu                   sync.RWMutex // Protect concurrent access to maps
	sequentialMode       bool         // Disable mutex protection for sequential sync
}

func NewProgressCounts() *ProgressLog {
	return &ProgressLog{
		Resources:            make(map[string]int),
		EntitlementsProgress: make(map[string]int),
		LastEntitlementLog:   make(map[string]time.Time),
		GrantsProgress:       make(map[string]int),
		LastGrantLog:         make(map[string]time.Time),
		LastActionLog:        time.Time{},
		sequentialMode:       true, // Default to sequential mode for backward compatibility
	}
}

func (p *ProgressLog) LogResourceTypesProgress(ctx context.Context) {
	l := ctxzap.Extract(ctx)
	l.Info("Synced resource types", zap.Int("count", p.ResourceTypes))
}

func (p *ProgressLog) LogResourcesProgress(ctx context.Context, resourceType string) {
	var resources int
	if p.sequentialMode {
		resources = p.Resources[resourceType]
	} else {
		p.mu.RLock()
		resources = p.Resources[resourceType]
		p.mu.RUnlock()
	}

	l := ctxzap.Extract(ctx)
	l.Info("Synced resources", zap.String("resource_type_id", resourceType), zap.Int("count", resources))
}

func (p *ProgressLog) LogEntitlementsProgress(ctx context.Context, resourceType string) {
	var entitlementsProgress, resources int
	var lastLogTime time.Time

	if p.sequentialMode {
		entitlementsProgress = p.EntitlementsProgress[resourceType]
		resources = p.Resources[resourceType]
		lastLogTime = p.LastEntitlementLog[resourceType]
	} else {
		p.mu.RLock()
		entitlementsProgress = p.EntitlementsProgress[resourceType]
		resources = p.Resources[resourceType]
		lastLogTime = p.LastEntitlementLog[resourceType]
		p.mu.RUnlock()
	}

	l := ctxzap.Extract(ctx)
	if resources == 0 {
		// if resuming sync, resource counts will be zero, so don't calculate percentage. just log every 10 seconds.
		if time.Since(lastLogTime) > maxLogFrequency {
			l.Info("Syncing entitlements",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", entitlementsProgress),
			)
			if !p.sequentialMode {
				p.mu.Lock()
				p.LastEntitlementLog[resourceType] = time.Now()
				p.mu.Unlock()
			} else {
				p.LastEntitlementLog[resourceType] = time.Now()
			}
		}
		return
	}

	percentComplete := (entitlementsProgress * 100) / resources

	switch {
	case percentComplete == 100:
		l.Info("Synced entitlements",
			zap.String("resource_type_id", resourceType),
			zap.Int("count", entitlementsProgress),
			zap.Int("total", resources),
		)
		if !p.sequentialMode {
			p.mu.Lock()
			p.LastEntitlementLog[resourceType] = time.Time{}
			p.mu.Unlock()
		} else {
			p.LastEntitlementLog[resourceType] = time.Time{}
		}
	case time.Since(lastLogTime) > maxLogFrequency:
		if entitlementsProgress > resources {
			l.Warn("more entitlement resources than resources",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", entitlementsProgress),
				zap.Int("total", resources),
			)
		} else {
			l.Info("Syncing entitlements",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", entitlementsProgress),
				zap.Int("total", resources),
				zap.Int("percent_complete", percentComplete),
			)
		}
		if !p.sequentialMode {
			p.mu.Lock()
			p.LastEntitlementLog[resourceType] = time.Now()
			p.mu.Unlock()
		} else {
			p.LastEntitlementLog[resourceType] = time.Now()
		}
	}
}

func (p *ProgressLog) LogGrantsProgress(ctx context.Context, resourceType string) {
	var grantsProgress, resources int
	var lastLogTime time.Time

	if p.sequentialMode {
		grantsProgress = p.GrantsProgress[resourceType]
		resources = p.Resources[resourceType]
		lastLogTime = p.LastGrantLog[resourceType]
	} else {
		p.mu.RLock()
		grantsProgress = p.GrantsProgress[resourceType]
		resources = p.Resources[resourceType]
		lastLogTime = p.LastGrantLog[resourceType]
		p.mu.RUnlock()
	}

	l := ctxzap.Extract(ctx)
	if resources == 0 {
		// if resuming sync, resource counts will be zero, so don't calculate percentage. just log every 10 seconds.
		if time.Since(lastLogTime) > maxLogFrequency {
			l.Info("Syncing grants",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", grantsProgress),
			)
			if !p.sequentialMode {
				p.mu.Lock()
				p.LastGrantLog[resourceType] = time.Now()
				p.mu.Unlock()
			} else {
				p.LastGrantLog[resourceType] = time.Now()
			}
		}
		return
	}

	percentComplete := (grantsProgress * 100) / resources

	switch {
	case percentComplete == 100:
		l.Info("Synced grants",
			zap.String("resource_type_id", resourceType),
			zap.Int("count", grantsProgress),
			zap.Int("total", resources),
		)
		if !p.sequentialMode {
			p.mu.Lock()
			p.LastGrantLog[resourceType] = time.Time{}
			p.mu.Unlock()
		} else {
			p.LastGrantLog[resourceType] = time.Time{}
		}
	case time.Since(lastLogTime) > maxLogFrequency:
		if grantsProgress > resources {
			l.Warn("more grant resources than resources",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", grantsProgress),
				zap.Int("total", resources),
			)
		} else {
			l.Info("Syncing grants",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", grantsProgress),
				zap.Int("total", resources),
				zap.Int("percent_complete", percentComplete),
			)
		}
		if !p.sequentialMode {
			p.mu.Lock()
			p.LastGrantLog[resourceType] = time.Now()
			p.mu.Unlock()
		} else {
			p.LastGrantLog[resourceType] = time.Now()
		}
	}
}

func (p *ProgressLog) LogExpandProgress(ctx context.Context, actions []*expand.EntitlementGraphAction) {
	actionsLen := len(actions)

	if p.sequentialMode {
		if time.Since(p.LastActionLog) < maxLogFrequency {
			return
		}
		p.LastActionLog = time.Now()
	} else {
		p.mu.Lock()
		if time.Since(p.LastActionLog) < maxLogFrequency {
			p.mu.Unlock()
			return
		}
		p.LastActionLog = time.Now()
		p.mu.Unlock()
	}

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
func (p *ProgressLog) SetSequentialMode(sequential bool) {
	p.sequentialMode = sequential
}
