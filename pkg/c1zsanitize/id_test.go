package c1zsanitize

import (
	"strings"
	"testing"
)

func TestSanitizeIDDeterministic(t *testing.T) {
	secret := bytes32("test-secret")
	a := SanitizeID(secret, "user-123")
	b := SanitizeID(secret, "user-123")
	if a != b {
		t.Fatalf("expected same input to yield same output; got %q vs %q", a, b)
	}
}

func TestSanitizeIDDistinctInputsDistinctOutputs(t *testing.T) {
	secret := bytes32("test-secret")
	a := SanitizeID(secret, "user-123")
	b := SanitizeID(secret, "user-456")
	if a == b {
		t.Fatalf("expected distinct inputs to yield distinct outputs; both %q", a)
	}
}

func TestSanitizeIDDifferentSecrets(t *testing.T) {
	a := SanitizeID(bytes32("secret-a"), "user-123")
	b := SanitizeID(bytes32("secret-b"), "user-123")
	if a == b {
		t.Fatalf("expected different secrets to yield different outputs; both %q", a)
	}
}

func TestSanitizeIDEmptyInputEmptyOutput(t *testing.T) {
	if got := SanitizeID(bytes32("s"), ""); got != "" {
		t.Fatalf("expected empty output for empty input, got %q", got)
	}
}

func TestSanitizeIDNoPadding(t *testing.T) {
	got := SanitizeID(bytes32("s"), "anything")
	if strings.ContainsRune(got, '=') {
		t.Fatalf("expected unpadded base32, got %q", got)
	}
}

func bytes32(seed string) []byte {
	b := make([]byte, 32)
	copy(b, seed)
	return b
}
