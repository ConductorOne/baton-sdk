package server

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"

	"github.com/conductorone/baton-sdk/pkg/rtun/transport"
)

type Registry struct {
	mu       sync.RWMutex
	sessions map[string]*transport.Session
	m        *serverMetrics
}

func NewRegistry(opts ...Option) *Registry {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	r := &Registry{sessions: make(map[string]*transport.Session)}
	if o.metrics != nil {
		r.m = newServerMetrics(o.metrics)
	}
	return r
}

// DialContext dials ONLY if this process owns the client link.
// addr: "rtun://<clientID>:<port>" where clientID is URL-safe and port is required.
// clientID must not contain unescaped colons or slashes; use url.PathEscape if needed.
func (r *Registry) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	// Parse rtun://clientID:port
	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "rtun" {
		return nil, fmt.Errorf("rtun: invalid scheme: %s", u.Scheme)
	}
	host := u.Host
	clientID, portStr, err := net.SplitHostPort(host)
	if err != nil {
		return nil, fmt.Errorf("rtun: missing or invalid port in '%s': %w", addr, err)
	}
	pu, err := strconv.ParseUint(portStr, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("rtun: invalid port in '%s': %w", addr, err)
	}
	port := uint32(pu)

	r.mu.RLock()
	s := r.sessions[clientID]
	r.mu.RUnlock()
	if s == nil {
		if r.m != nil {
			r.m.reverseDialNotFound(ctx)
		}
		return nil, fmt.Errorf("rtun: client not connected: %s", clientID)
	}
	conn, err := s.Open(ctx, port)
	if err == nil {
		if r.m != nil {
			r.m.reverseDialSuccess(ctx)
		}
	}
	return conn, err
}

// Register binds a client's Session to this Registry under the given clientID.
func (r *Registry) Register(ctx context.Context, clientID string, s *transport.Session) {
	r.mu.Lock()
	r.sessions[clientID] = s
	r.mu.Unlock()
	if r.m != nil {
		r.m.registryRegister(ctx)
	}
}

// Unregister removes a client's Session binding.
func (r *Registry) Unregister(ctx context.Context, clientID string) {
	r.mu.Lock()
	delete(r.sessions, clientID)
	r.mu.Unlock()
	if r.m != nil {
		r.m.registryUnregister(ctx)
	}
}
