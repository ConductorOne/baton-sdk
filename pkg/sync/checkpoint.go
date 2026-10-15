package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"sync"
	"time"
)

type syncCheckpoint struct {
	mu       sync.Mutex
	interval time.Duration
	last     time.Time
}

func (c *syncCheckpoint) write(force bool, commit func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !force && !c.last.IsZero() && time.Since(c.last) < c.interval {
		return nil
	}
	c.last = time.Now()
	return commit()
}
