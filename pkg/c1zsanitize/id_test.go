package c1zsanitize

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSanitizeIDDeterministic(t *testing.T) {
	secret := bytes32("test-secret")
	a := SanitizeID(secret, "user-123")
	b := SanitizeID(secret, "user-123")
	require.Equal(t, a, b, "expected same input to yield same output")
}

func TestSanitizeIDDistinctInputsDistinctOutputs(t *testing.T) {
	secret := bytes32("test-secret")
	a := SanitizeID(secret, "user-123")
	b := SanitizeID(secret, "user-456")
	require.NotEqual(t, a, b, "expected distinct inputs to yield distinct outputs")
}

func TestSanitizeIDDifferentSecrets(t *testing.T) {
	a := SanitizeID(bytes32("secret-a"), "user-123")
	b := SanitizeID(bytes32("secret-b"), "user-123")
	require.NotEqual(t, a, b, "expected different secrets to yield different outputs")
}

func TestSanitizeIDEmptyInputEmptyOutput(t *testing.T) {
	require.Empty(t, SanitizeID(bytes32("s"), ""), "expected empty output for empty input")
}

func TestSanitizeIDNoPadding(t *testing.T) {
	got := SanitizeID(bytes32("s"), "anything")
	require.NotContains(t, got, "=", "expected unpadded base32")
}

func bytes32(seed string) []byte {
	b := make([]byte, 32)
	copy(b, seed)
	return b
}
