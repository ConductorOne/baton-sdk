package memory

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrServerNotFound indicates a server ID has no active advertisement.
var ErrServerNotFound = errors.New("rtun/match: server not found")

// Directory is an in-memory Directory for tests and single-node deployments.
type Directory struct {
	mu      sync.RWMutex
	servers map[string]record // serverID -> record
}

type record struct {
	addr    string
	expires time.Time
}

// NewDirectory returns an in-memory Directory.
func NewDirectory() *Directory {
	return &Directory{
		servers: make(map[string]record),
	}
}

func (d *Directory) Advertise(ctx context.Context, serverID string, addr string, ttl time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers[serverID] = record{addr: addr, expires: time.Now().Add(ttl)}
	return nil
}

func (d *Directory) Revoke(ctx context.Context, serverID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.servers, serverID)
	return nil
}

func (d *Directory) Resolve(ctx context.Context, serverID string) (string, error) {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	rec, ok := d.servers[serverID]
	if !ok {
		return "", ErrServerNotFound
	}
	if !rec.expires.IsZero() && now.After(rec.expires) {
		delete(d.servers, serverID)
		return "", ErrServerNotFound
	}
	return rec.addr, nil
}
