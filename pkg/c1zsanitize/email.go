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

func (d *domainMap) lookup(secret []byte, domain string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if v, ok := d.m[domain]; ok {
		return v
	}
	v := "dom-" + strings.ToLower(SanitizeID(secret, domain)) + ".example"
	d.m[domain] = v
	return v
}

// sanitizeEmail HMACs the localpart and assigns a deterministic
// sentinel domain through the per-c1z domain map. Cross-tenant
// "all from @acme.com" shapes are preserved without leaking acme.com.
// An input lacking a single '@' is HMACed wholesale — the caller may
// have passed something that looks like an email but isn't.
func sanitizeEmail(secret []byte, dm *domainMap, addr string) string {
	at := strings.LastIndexByte(addr, '@')
	if at < 0 {
		return SanitizeID(secret, addr)
	}
	local := addr[:at]
	domain := addr[at+1:]
	return SanitizeID(secret, local) + "@" + dm.lookup(secret, domain)
}
