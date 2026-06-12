package equivalence

import (
	"context"
	"fmt"
	"sort"
	"sync"

	v3 "github.com/conductorone/baton-sdk/pb/c1/storage/v3"
)

// MemoryRef is a deterministic in-memory implementation of Reference,
// used as the source-of-truth in equivalence tests. Backed by a
// nested map keyed by (sync_id, external_id). All operations are
// linearizable under MemoryRef.mu.
//
// Iteration order is by external_id (lexicographic) for stable
// ordering across runs.
type MemoryRef struct {
	mu          sync.Mutex
	bySync      map[string]map[string]*v3.GrantRecord // syncID -> externalID -> record
	currentSync string
}

// NewMemoryRef returns an empty in-memory reference.
func NewMemoryRef() *MemoryRef {
	return &MemoryRef{bySync: map[string]map[string]*v3.GrantRecord{}}
}

func (m *MemoryRef) SetCurrentSync(syncID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentSync = syncID
	return nil
}

func (m *MemoryRef) resolveSync(syncID string) string {
	if syncID == "" {
		return m.currentSync
	}
	return syncID
}

func (m *MemoryRef) PutGrantRecord(_ context.Context, r *v3.GrantRecord) error {
	if r == nil {
		return fmt.Errorf("memoryref: nil record")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// GrantRecord values do not carry a sync_id in v3; writer paths bind
	// grants to the current sync's keyspace.
	sid := m.resolveSync("")
	if sid == "" {
		return fmt.Errorf("memoryref: no sync_id")
	}
	bucket := m.bySync[sid]
	if bucket == nil {
		bucket = map[string]*v3.GrantRecord{}
		m.bySync[sid] = bucket
	}
	bucket[r.GetExternalId()] = r
	return nil
}

func (m *MemoryRef) GetGrantRecord(_ context.Context, syncID, externalID string) (*v3.GrantRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sid := m.resolveSync(syncID)
	if sid == "" {
		return nil, fmt.Errorf("memoryref: no sync_id")
	}
	bucket := m.bySync[sid]
	if bucket == nil {
		return nil, fmt.Errorf("memoryref: not found")
	}
	r, ok := bucket[externalID]
	if !ok {
		return nil, fmt.Errorf("memoryref: not found")
	}
	return r, nil
}

func (m *MemoryRef) DeleteGrantRecord(_ context.Context, syncID, externalID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sid := m.resolveSync(syncID)
	bucket := m.bySync[sid]
	if bucket == nil {
		return nil
	}
	delete(bucket, externalID)
	return nil
}

func (m *MemoryRef) snapshot(syncID string) []*v3.GrantRecord {
	bucket := m.bySync[m.resolveSync(syncID)]
	out := make([]*v3.GrantRecord, 0, len(bucket))
	for _, r := range bucket {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].GetExternalId() < out[j].GetExternalId()
	})
	return out
}

func (m *MemoryRef) IterateGrantsBySync(_ context.Context, syncID string, yield func(*v3.GrantRecord) bool) error {
	m.mu.Lock()
	snap := m.snapshot(syncID)
	m.mu.Unlock()
	for _, r := range snap {
		if !yield(r) {
			return nil
		}
	}
	return nil
}

func (m *MemoryRef) IterateGrantsByEntitlement(_ context.Context, syncID, entitlementID string, yield func(*v3.GrantRecord) bool) error {
	m.mu.Lock()
	snap := m.snapshot(syncID)
	m.mu.Unlock()
	for _, r := range snap {
		if r.GetEntitlement().GetEntitlementId() != entitlementID {
			continue
		}
		if !yield(r) {
			return nil
		}
	}
	return nil
}

func (m *MemoryRef) IterateGrantsByPrincipal(_ context.Context, syncID, principalRT, principalID string, yield func(*v3.GrantRecord) bool) error {
	m.mu.Lock()
	snap := m.snapshot(syncID)
	m.mu.Unlock()
	for _, r := range snap {
		p := r.GetPrincipal()
		if p.GetResourceTypeId() != principalRT || p.GetResourceId() != principalID {
			continue
		}
		if !yield(r) {
			return nil
		}
	}
	return nil
}
