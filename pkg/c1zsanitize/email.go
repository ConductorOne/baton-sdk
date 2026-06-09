package c1zsanitize

import (
	"strings"
	"sync"
)

// domainMap holds the deterministic original→sanitized domain mapping
// for a single c1z sanitization run. Same source domain maps to the
// same destination domain across every email encountered in the c1z;
// distinct source domains never collide on the destination side.
//
// The map is populated lazily as emails are sanitized. The mapping
// itself is purely a function of the per-c1z secret, so the map's
// only purpose is to avoid recomputing the HMAC for repeated lookups.
type domainMap struct {
	mu sync.Mutex
	m  map[string]string
}

func newDomainMap() *domainMap {
	return &domainMap{m: map[string]string{}}
}

// lookup maps a source domain to its deterministic sentinel domain. idFn is
// the sanitizer's reused hot-path HMAC (s.id), not the package-level
// SanitizeID, so domains do not pay a fresh HMAC key schedule per call. The
// HMAC is computed OUTSIDE the lock; the lock only guards the cache map, so a
// burst of distinct domains no longer serializes the hashing. A rare
// duplicate computation under contention is harmless — idFn is a pure
// function of (secret, input).
func (d *domainMap) lookup(idFn func(string) string, domain string) string {
	d.mu.Lock()
	if v, ok := d.m[domain]; ok {
		d.mu.Unlock()
		return v
	}
	d.mu.Unlock()

	v := "dom-" + strings.ToLower(idFn(domain)) + ".example"

	d.mu.Lock()
	if existing, ok := d.m[domain]; ok {
		d.mu.Unlock()
		return existing
	}
	d.m[domain] = v
	d.mu.Unlock()
	return v
}

// sanitizeEmail HMACs the localpart and assigns a deterministic
// sentinel domain through the per-c1z domain map. Cross-tenant
// "all from @acme.com" shapes are preserved without leaking acme.com.
// An input lacking a single '@' is HMACed wholesale — the caller may
// have passed something that looks like an email but isn't. idFn is the
// sanitizer's reused hot-path HMAC (s.id); the package-level SanitizeID
// reference implementation is no longer on this path.
func sanitizeEmail(idFn func(string) string, dm *domainMap, addr string) string {
	at := strings.LastIndexByte(addr, '@')
	if at < 0 {
		return idFn(addr)
	}
	local := addr[:at]
	domain := addr[at+1:]
	return idFn(local) + "@" + dm.lookup(idFn, domain)
}
