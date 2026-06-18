package c1zsanitize

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// emailIDFn mirrors the hot-path s.id used in production while keeping the
// test self-contained: a pure function of (secret, input).
func emailIDFn(secret []byte) func(string) string {
	return func(in string) string { return SanitizeID(secret, in) }
}

func TestSanitizeEmailPreservesShape(t *testing.T) {
	secret := bytes32("s")
	dm := newDomainMap()
	got := sanitizeEmail(emailIDFn(secret), dm, "john.doe@acme.com")
	require.Contains(t, got, "@", "expected '@' to survive")
}

func TestSanitizeEmailSameDomainMapsConsistently(t *testing.T) {
	secret := bytes32("s")
	dm := newDomainMap()
	idFn := emailIDFn(secret)
	a := sanitizeEmail(idFn, dm, "alice@acme.com")
	b := sanitizeEmail(idFn, dm, "bob@acme.com")
	aDom := a[strings.LastIndexByte(a, '@')+1:]
	bDom := b[strings.LastIndexByte(b, '@')+1:]
	require.Equal(t, aDom, bDom, "expected same source domain to map consistently")
}

func TestSanitizeEmailDistinctDomainsDistinctOutputs(t *testing.T) {
	secret := bytes32("s")
	dm := newDomainMap()
	idFn := emailIDFn(secret)
	a := sanitizeEmail(idFn, dm, "alice@acme.com")
	b := sanitizeEmail(idFn, dm, "alice@example.com")
	aDom := a[strings.LastIndexByte(a, '@')+1:]
	bDom := b[strings.LastIndexByte(b, '@')+1:]
	require.NotEqual(t, aDom, bDom, "expected distinct source domains to map distinctly")
}

func TestSanitizeEmailNoAtTreatsAsID(t *testing.T) {
	secret := bytes32("s")
	dm := newDomainMap()
	got := sanitizeEmail(emailIDFn(secret), dm, "no-at-sign-here")
	require.NotContains(t, got, "@", "expected no '@' in non-email output")
}
