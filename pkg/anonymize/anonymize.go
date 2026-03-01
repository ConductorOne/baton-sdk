// Package anonymize provides functionality to anonymize c1z files by replacing
// personally identifiable information (PII) and sensitive data with deterministic,
// anonymized equivalents while preserving structural relationships.
package anonymize

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// Config holds configuration options for the anonymization process.
type Config struct {
	// Salt is used as a key for HMAC-based hashing to generate deterministic
	// but unpredictable anonymized values. Different salts produce different outputs.
	// Required.
	Salt string
}

// defaultConfig returns a Config with sensible default values.
func defaultConfig() Config {
	return Config{
		Salt: "baton-anonymize-default-salt",
	}
}

// Anonymizer handles the anonymization of c1z file data.
type Anonymizer struct {
	config    Config
	hasher    *Hasher
	timestamp time.Time // Single timestamp used for all anonymized timestamps
}

// New creates a new Anonymizer with the given configuration.
func New(config Config) *Anonymizer {
	if config.Salt == "" {
		panic("salt is required")
	}
	return &Anonymizer{
		config:    config,
		hasher:    NewHasher(config.Salt),
		timestamp: time.Now(),
	}
}

// AnonymizedTimestamp returns the single timestamp used for all anonymized records.
func (a *Anonymizer) AnonymizedTimestamp() *timestamppb.Timestamp {
	return timestamppb.New(a.timestamp)
}

// newWithDefaults creates a new Anonymizer with default configuration.
func newWithDefaults() *Anonymizer {
	return New(defaultConfig())
}

// Hasher provides deterministic hashing for anonymization.
// It uses HMAC-SHA256 with a salt to generate consistent but unpredictable values.
type Hasher struct {
	salt []byte
}

// NewHasher creates a new Hasher with the given salt.
func NewHasher(salt string) *Hasher {
	return &Hasher{
		salt: []byte(salt),
	}
}

// Hash generates a deterministic hash of the input string.
// The same input with the same salt always produces the same output.
func (h *Hasher) Hash(input string) string {
	mac := hmac.New(sha256.New, h.salt)
	mac.Write([]byte(input))
	return hex.EncodeToString(mac.Sum(nil))
}

// HashN generates a deterministic hash truncated to n characters.
func (h *Hasher) HashN(input string, n int) string {
	hash := h.Hash(input)
	if len(hash) < n {
		return hash
	}
	return hash[:n]
}

// AnonymizeEmail generates an anonymized email address using the full hash digest, split around an @ symbol.
func (h *Hasher) AnonymizeEmail(email string) string {
	e := h.HashN(email, 32)
	return e[:len(e)/2] + "@" + e[len(e)/2:]
}

// AnonymizeDisplayName generates an anonymized display name using the full hash digest.
func (h *Hasher) AnonymizeDisplayName(name string) string {
	return h.HashN(name, 16)
}

// AnonymizeLogin generates an anonymized login/username using the full hash digest.
func (h *Hasher) AnonymizeLogin(login string) string {
	return h.HashN(login, max(32, len(login)))
}

// AnonymizeEmployeeID generates an anonymized employee ID using the full hash digest.
func (h *Hasher) AnonymizeEmployeeID(empID string) string {
	return h.HashN(empID, max(32, len(empID)))
}

// AnonymizeResourceID generates an anonymized resource ID, preserving the original length.
func (h *Hasher) AnonymizeResourceID(resourceID string) string {
	return h.HashN(resourceID, max(32, len(resourceID)))
}

// AnonymizeExternalID generates an anonymized external ID, preserving the original length.
func (h *Hasher) AnonymizeExternalID(externalID string) string {
	return h.HashN(externalID, max(32, len(externalID)))
}

// AnonymizeURL generates an anonymized URL.
func (h *Hasher) AnonymizeURL(url string) string {
	return "https://example.com/" + h.HashN(url, 16)
}

// AnonymizeGivenName generates an anonymized given name using the full hash digest.
func (h *Hasher) AnonymizeGivenName(name string) string {
	return h.HashN(name, 16)
}

// AnonymizeFamilyName generates an anonymized family name using the full hash digest.
func (h *Hasher) AnonymizeFamilyName(name string) string {
	return h.HashN(name, 16)
}

// AnonymizeMiddleName generates an anonymized middle name using the full hash digest.
func (h *Hasher) AnonymizeMiddleName(name string) string {
	return h.HashN(name, 16)
}

// AnonymizeResourceType generates an anonymized resource type name using the full hash digest.
// Uses deterministic hashing so the same type always maps to the same value.
func (h *Hasher) AnonymizeResourceType(resourceType string) string {
	return h.HashN(resourceType, 8)
}
