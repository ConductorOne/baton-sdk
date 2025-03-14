package progress

import (
	"context"
	"time"

	"github.com/conductorone/baton-sdk/pkg/sync/expand"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type ProgressCounts struct {
	ResourceTypes        int            // count of resource types
	Resources            map[string]int // map of resource type id to resource count
	EntitlementsProgress map[string]int // map of resource type id to entitlement count
	LastEntitlementLog   map[string]time.Time
	GrantsProgress       map[string]int // map of resource type id to grant count
	LastGrantLog         map[string]time.Time
	LastActionLog        time.Time
}

const maxLogFrequency = 10 * time.Second

// TODO: use a mutex or a syncmap for when syncer code becomes parallel
func NewProgressCounts() *ProgressCounts {
	return &ProgressCounts{
		Resources:            make(map[string]int),
		EntitlementsProgress: make(map[string]int),
		LastEntitlementLog:   make(map[string]time.Time),
		GrantsProgress:       make(map[string]int),
		LastGrantLog:         make(map[string]time.Time),
		LastActionLog:        time.Time{},
	}
}

func (p *ProgressCounts) LogResourceTypesProgress(ctx context.Context) {
	l := ctxzap.Extract(ctx)
	l.Info("Synced resource types", zap.Int("count", p.ResourceTypes))
}

func (p *ProgressCounts) LogResourcesProgress(ctx context.Context, resourceType string) {
	l := ctxzap.Extract(ctx)
	resources := p.Resources[resourceType]
	l.Info("Synced resources", zap.String("resource_type_id", resourceType), zap.Int("count", resources))
}

func (p *ProgressCounts) LogEntitlementsProgress(ctx context.Context, resourceType string) {
	entitlementsProgress := p.EntitlementsProgress[resourceType]
	resources := p.Resources[resourceType]

	l := ctxzap.Extract(ctx)
	if resources == 0 {
		// if resuming sync, resource counts will be zero, so don't calculate percentage. just log every 10 seconds.
		if time.Since(p.LastEntitlementLog[resourceType]) > maxLogFrequency {
			l.Info("Syncing entitlements",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", entitlementsProgress),
			)
			p.LastEntitlementLog[resourceType] = time.Now()
		}
		return
	}

	percentComplete := (entitlementsProgress * 100) / resources

	switch {
	case entitlementsProgress > resources:
		l.Error("more entitlement resources than resources",
			zap.String("resource_type_id", resourceType),
			zap.Int("synced", entitlementsProgress),
			zap.Int("total", resources),
		)
	case percentComplete == 100:
		l.Info("Synced entitlements",
			zap.String("resource_type_id", resourceType),
			zap.Int("count", entitlementsProgress),
			zap.Int("total", resources),
		)
		p.LastEntitlementLog[resourceType] = time.Time{}
	case time.Since(p.LastEntitlementLog[resourceType]) > maxLogFrequency:
		l.Info("Syncing entitlements",
			zap.String("resource_type_id", resourceType),
			zap.Int("synced", entitlementsProgress),
			zap.Int("total", resources),
			zap.Int("percent_complete", percentComplete),
		)
		p.LastEntitlementLog[resourceType] = time.Now()
	}
}

func (p *ProgressCounts) LogGrantsProgress(ctx context.Context, resourceType string) {
	grantsProgress := p.GrantsProgress[resourceType]
	resources := p.Resources[resourceType]

	l := ctxzap.Extract(ctx)
	if resources == 0 {
		// if resuming sync, resource counts will be zero, so don't calculate percentage. just log every 10 seconds.
		if time.Since(p.LastGrantLog[resourceType]) > maxLogFrequency {
			l.Info("Syncing grants",
				zap.String("resource_type_id", resourceType),
				zap.Int("synced", grantsProgress),
			)
			p.LastGrantLog[resourceType] = time.Now()
		}
		return
	}

	percentComplete := (grantsProgress * 100) / resources

	switch {
	case grantsProgress > resources:
		l.Error("more grant resources than resources",
			zap.String("resource_type_id", resourceType),
			zap.Int("synced", grantsProgress),
			zap.Int("total", resources),
		)
	case percentComplete == 100:
		l.Info("Synced grants",
			zap.String("resource_type_id", resourceType),
			zap.Int("count", grantsProgress),
			zap.Int("total", resources),
		)
		p.LastGrantLog[resourceType] = time.Time{}
	case time.Since(p.LastGrantLog[resourceType]) > maxLogFrequency:
		l.Info("Syncing grants",
			zap.String("resource_type_id", resourceType),
			zap.Int("synced", grantsProgress),
			zap.Int("total", resources),
			zap.Int("percent_complete", percentComplete),
		)
		p.LastGrantLog[resourceType] = time.Now()
	}
}

func (p *ProgressCounts) LogExpandProgress(ctx context.Context, actions []*expand.EntitlementGraphAction) {
	actionsLen := len(actions)
	if time.Since(p.LastActionLog) < maxLogFrequency {
		return
	}
	p.LastActionLog = time.Now()

	l := ctxzap.Extract(ctx)
	l.Info("Expanding grants", zap.Int("actions_remaining", actionsLen))
}
