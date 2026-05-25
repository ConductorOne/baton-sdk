package c1zsanitize

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
)

// MinSecretBytes is the minimum length of a per-c1z secret. Anything
// shorter is rejected by Sanitize; in practice operators should use 32
// random bytes from a CSPRNG.
const MinSecretBytes = 32

// idEncoding is base32 without padding so emitted identifiers stay
// stable-length and free of '=' which some downstream tools quote.
var idEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// idTruncationBytes is the number of HMAC output bytes that survive
// truncation. 12 bytes = 96 bits — well above the birthday bound for
// any plausible tenant size (1M users → ~10⁻¹⁰ collision probability).
const idTruncationBytes = 12

// SanitizeID returns a deterministic, irreversible transform of input
// under the per-c1z secret. Same input → same output within a c1z;
// different across c1zs whose secrets differ.
//
// Empty input returns empty output so callers can transform optional
// fields without checking presence first.
func SanitizeID(secret []byte, input string) string {
	if input == "" {
		return ""
	}
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(input))
	sum := h.Sum(nil)
	return idEncoding.EncodeToString(sum[:idTruncationBytes])
}
