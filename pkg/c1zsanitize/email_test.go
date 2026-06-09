package c1zsanitize

import (
	"strings"
	"testing"
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
	if !strings.ContainsRune(got, '@') {
		t.Fatalf("expected '@' to survive, got %q", got)
	}
}

func TestSanitizeEmailSameDomainMapsConsistently(t *testing.T) {
	secret := bytes32("s")
	dm := newDomainMap()
	idFn := emailIDFn(secret)
	a := sanitizeEmail(idFn, dm, "alice@acme.com")
	b := sanitizeEmail(idFn, dm, "bob@acme.com")
	aDom := a[strings.LastIndexByte(a, '@')+1:]
	bDom := b[strings.LastIndexByte(b, '@')+1:]
	if aDom != bDom {
		t.Fatalf("expected same source domain to map consistently; got %q vs %q", aDom, bDom)
	}
}

func TestSanitizeEmailDistinctDomainsDistinctOutputs(t *testing.T) {
	secret := bytes32("s")
	dm := newDomainMap()
	idFn := emailIDFn(secret)
	a := sanitizeEmail(idFn, dm, "alice@acme.com")
	b := sanitizeEmail(idFn, dm, "alice@example.com")
	aDom := a[strings.LastIndexByte(a, '@')+1:]
	bDom := b[strings.LastIndexByte(b, '@')+1:]
	if aDom == bDom {
		t.Fatalf("expected distinct source domains to map distinctly; both %q", aDom)
	}
}

func TestSanitizeEmailNoAtTreatsAsID(t *testing.T) {
	secret := bytes32("s")
	dm := newDomainMap()
	got := sanitizeEmail(emailIDFn(secret), dm, "no-at-sign-here")
	if strings.ContainsRune(got, '@') {
		t.Fatalf("expected no '@' in non-email output; got %q", got)
	}
}
