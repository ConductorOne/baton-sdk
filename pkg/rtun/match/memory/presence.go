package memory

import (
	"context"
	"sync"
	"time"
)

// Presence is an in-memory implementation of match.Presence for tests and
// single-node deployments. It stores per-(client, server) leases and global
// client ports.
type Presence struct {
	mu     sync.RWMutex
	leases map[string]map[string]time.Time // clientID -> serverID -> expiry
	ports  map[string][]uint32             // clientID -> ports
}

func NewPresence() *Presence {
	return &Presence{
		leases: make(map[string]map[string]time.Time),
		ports:  make(map[string][]uint32),
	}
}

func (p *Presence) Announce(ctx context.Context, clientID string, serverID string, ttl time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.leases[clientID] == nil {
		p.leases[clientID] = make(map[string]time.Time)
	}
	p.leases[clientID][serverID] = time.Now().Add(ttl)
	return nil
}

func (p *Presence) Revoke(ctx context.Context, clientID string, serverID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if inner := p.leases[clientID]; inner != nil {
		delete(inner, serverID)
		if len(inner) == 0 {
			delete(p.leases, clientID)
			delete(p.ports, clientID)
		}
	}
	return nil
}

func (p *Presence) Locations(ctx context.Context, clientID string) ([]string, error) {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	inner := p.leases[clientID]
	if inner == nil {
		return nil, nil
	}
	for s, exp := range inner {
		if !exp.IsZero() && now.After(exp) {
			delete(inner, s)
		}
	}
	if len(inner) == 0 {
		delete(p.leases, clientID)
		delete(p.ports, clientID)
		return nil, nil
	}
	servers := make([]string, 0, len(inner))
	for s := range inner {
		servers = append(servers, s)
	}
	return servers, nil
}

func (p *Presence) SetPorts(ctx context.Context, clientID string, ports []uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ports == nil {
		delete(p.ports, clientID)
		return nil
	}
	cp := append([]uint32(nil), ports...)
	p.ports[clientID] = cp
	return nil
}

func (p *Presence) Ports(ctx context.Context, clientID string) ([]uint32, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ports := p.ports[clientID]
	if ports == nil {
		return nil, nil
	}
	return append([]uint32(nil), ports...), nil
}
