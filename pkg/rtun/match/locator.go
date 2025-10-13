package match

import (
	"context"
	"hash/fnv"
)

// Locator selects the owning server for a client based on presence and rendezvous hashing.
type Locator struct {
	Presence Presence
}

// OwnerOf returns the server that currently owns the client's link along with the client's ports.
// It uses Presence to list available servers and rendezvous hashing to choose among them.
func (l *Locator) OwnerOf(ctx context.Context, clientID string) (serverID string, ports []uint32, err error) {
	if l == nil || l.Presence == nil {
		return "", nil, ErrNotImplemented
	}
	servers, err := l.Presence.Locations(ctx, clientID)
	if err != nil {
		return "", nil, err
	}
	if len(servers) == 0 {
		return "", nil, ErrClientOffline
	}
	owner := rendezvousChoose(clientID, servers)
	ports, err = l.Presence.Ports(ctx, clientID)
	if err != nil {
		return "", nil, err
	}
	return owner, ports, nil
}

func rendezvousChoose(clientID string, servers []string) string {
	var best string
	var bestVal uint64
	var have bool
	for _, s := range servers {
		h := fnv.New64a()
		_, _ = h.Write([]byte(clientID))
		_, _ = h.Write([]byte("|"))
		_, _ = h.Write([]byte(s))
		v := h.Sum64()
		if !have || v > bestVal {
			bestVal = v
			best = s
			have = true
		}
	}
	return best
}
