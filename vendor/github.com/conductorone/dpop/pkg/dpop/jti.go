package dpop

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

// CheckAndStoreJTI validates and stores a JTI.
// Returns error if the JTI has been seen before.
// The implementation should enforce a reasonable TTL and handle cleanup.
type CheckAndStoreJTI func(ctx context.Context, jti string) error

// MemoryJTIStore is a simple in-memory implementation of JTIStore using ttlcache
type MemoryJTIStore struct {
	cache *ttlcache.Cache[string, struct{}]
}

// MemoryJTIStoreOptions configures the behavior of the memory JTI store
type MemoryJTIStoreOptions struct {
	// TTL is the time-to-live for stored JTIs
	TTL time.Duration
}

// DefaultMemoryJTIStoreOptions returns the default options for the memory JTI store
func DefaultMemoryJTIStoreOptions() *MemoryJTIStoreOptions {
	return &MemoryJTIStoreOptions{
		TTL: 10 * time.Minute,
	}
}

// NewMemoryJTIStore creates a new in-memory JTI store
func NewMemoryJTIStore() *MemoryJTIStore {
	return NewMemoryJTIStoreWithOptions(DefaultMemoryJTIStoreOptions())
}

// NewMemoryJTIStoreWithOptions creates a new in-memory JTI store with custom options
func NewMemoryJTIStoreWithOptions(opts *MemoryJTIStoreOptions) *MemoryJTIStore {
	if opts == nil {
		opts = DefaultMemoryJTIStoreOptions()
	}

	cache := ttlcache.New[string, struct{}](
		ttlcache.WithTTL[string, struct{}](opts.TTL),
	)

	// Start the background cleanup goroutine
	go cache.Start()

	return &MemoryJTIStore{
		cache: cache,
	}
}

// Stop cleanly shuts down the JTI store's background cleanup goroutine
func (s *MemoryJTIStore) Stop() {
	s.cache.Stop()
}

// makeKey creates a key from JTI
func (s *MemoryJTIStore) makeKey(jti string) string {
	h := sha256.New()
	h.Write([]byte(jti))
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

// CheckAndStoreJTI implements JTIStore.CheckAndStoreJTI
func (s *MemoryJTIStore) CheckAndStoreJTI(ctx context.Context, jti string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	s.cache.DeleteExpired()
	key := s.makeKey(jti)

	_, found := s.cache.GetOrSet(key, struct{}{})

	if found {
		return fmt.Errorf("duplicate jti")
	}

	return nil
}
