package anonymize

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const testEmailAddress = "john.doe@example.com"

func TestHasher_Deterministic(t *testing.T) {
	h := NewHasher("test-salt")

	// Same input should produce same output
	hash1 := h.Hash("test-input")
	hash2 := h.Hash("test-input")
	require.Equal(t, hash1, hash2, "Hash should be deterministic")

	// Different inputs should produce different outputs
	hash3 := h.Hash("different-input")
	require.NotEqual(t, hash1, hash3, "Different inputs should produce different hashes")
}

func TestHasher_DifferentSalts(t *testing.T) {
	h1 := NewHasher("salt1")
	h2 := NewHasher("salt2")

	// Same input with different salts should produce different outputs
	hash1 := h1.Hash("test-input")
	hash2 := h2.Hash("test-input")
	require.NotEqual(t, hash1, hash2, "Different salts should produce different hashes")
}

func TestHasher_HashN(t *testing.T) {
	h := NewHasher("test-salt")

	hash8 := h.HashN("test-input", 8)
	require.Len(t, hash8, 8, "HashN should truncate to specified length")

	hash16 := h.HashN("test-input", 16)
	require.Len(t, hash16, 16, "HashN should truncate to specified length")

	// hash8 should be a prefix of hash16
	require.True(t, hash16[:8] == hash8, "Shorter hash should be prefix of longer hash")
}

func TestHasher_AnonymizeEmail(t *testing.T) {
	h := NewHasher("test-salt")

	original := testEmailAddress
	email := h.AnonymizeEmail(original)
	require.Contains(t, email, "@", "Anonymized email should contain @")
	require.Len(t, email, 33, "Anonymized email should be 16+1+16 chars (hash@hash)")
	require.NotEqual(t, original, email, "Anonymized email should be different from original")

	// Same input should produce same output
	email2 := h.AnonymizeEmail(original)
	require.Equal(t, email, email2, "Email anonymization should be deterministic")
}

func TestHasher_AnonymizeDisplayName(t *testing.T) {
	h := NewHasher("test-salt")

	original := "John Doe"
	name := h.AnonymizeDisplayName(original)
	require.Len(t, name, 16, "Anonymized name should be 16 chars")
	require.NotEqual(t, original, name, "Anonymized name should be different from original")

	// Second call should be deterministic
	name2 := h.AnonymizeDisplayName(original)
	require.Equal(t, name, name2, "Display name anonymization should be deterministic")
}

func TestHasher_AnonymizeLogin(t *testing.T) {
	h := NewHasher("test-salt")

	original := "johndoe"
	login := h.AnonymizeLogin(original)
	require.Len(t, login, max(32, len(original)), "Anonymized login should be max(32, original length)")
	require.NotEqual(t, original, login, "Anonymized login should be different from original")

	// Same input should produce same output
	login2 := h.AnonymizeLogin(original)
	require.Equal(t, login, login2, "Login anonymization should be deterministic")
}

func TestHasher_AnonymizeEmployeeID(t *testing.T) {
	h := NewHasher("test-salt")

	original := "EMP001"
	empID := h.AnonymizeEmployeeID(original)
	require.Len(t, empID, max(32, len(original)), "Anonymized employee ID should be max(32, original length)")
	require.NotEqual(t, original, empID, "Anonymized employee ID should be different from original")
}

func TestHasher_AnonymizeResourceID(t *testing.T) {
	h := NewHasher("test-salt")

	original := "resource-123"
	resID := h.AnonymizeResourceID(original)
	require.Len(t, resID, max(32, len(original)), "Anonymized resource ID should be max(32, original length)")
	require.NotEqual(t, original, resID, "Anonymized resource ID should be different from original")
}

func TestHasher_AnonymizeExternalID(t *testing.T) {
	h := NewHasher("test-salt")

	original := "ext-123"
	extID := h.AnonymizeExternalID(original)
	require.Len(t, extID, max(32, len(original)), "Anonymized external ID should be max(32, original length)")
	require.NotEqual(t, original, extID, "Anonymized external ID should be different from original")
}

func TestHasher_AnonymizeURL(t *testing.T) {
	h := NewHasher("test-salt")

	url := h.AnonymizeURL("https://example.com/help")
	require.Contains(t, url, "https://example.com/", "Anonymized URL should use example.com domain")
}

func TestHasher_AnonymizeStructuredName(t *testing.T) {
	h := NewHasher("test-salt")

	originalGiven := "John"
	givenName := h.AnonymizeGivenName(originalGiven)
	require.Len(t, givenName, 16, "Anonymized given name should be 16 chars")
	require.NotEqual(t, originalGiven, givenName, "Anonymized given name should be different from original")

	originalFamily := "Doe"
	familyName := h.AnonymizeFamilyName(originalFamily)
	require.Len(t, familyName, 16, "Anonymized family name should be 16 chars")
	require.NotEqual(t, originalFamily, familyName, "Anonymized family name should be different from original")

	originalMiddle := "Robert"
	middleName := h.AnonymizeMiddleName(originalMiddle)
	require.Len(t, middleName, 16, "Anonymized middle name should be 16 chars")
	require.NotEqual(t, originalMiddle, middleName, "Anonymized middle name should be different from original")
}

func TestDefaultConfig(t *testing.T) {
	config := defaultConfig()

	require.NotEmpty(t, config.Salt, "Default config should have a salt")
}

func TestNew(t *testing.T) {
	a := newWithDefaults()
	require.NotNil(t, a, "NewWithDefaults should return an Anonymizer")
	require.NotNil(t, a.hasher, "Anonymizer should have a hasher")
}

func TestNew_WithEmptySalt(t *testing.T) {
	require.Panics(t, func() {
		New(Config{
			Salt: "",
		})
	}, "New should panic with empty salt")
}

func TestShouldDeleteAssets(t *testing.T) {
	a := newWithDefaults()
	require.True(t, a.ShouldDeleteAssets(), "Assets should always be deleted during anonymization")
}

func TestShouldClearSessionStore(t *testing.T) {
	a := newWithDefaults()
	require.True(t, a.ShouldClearSessionStore(), "Session store should always be cleared")
}
