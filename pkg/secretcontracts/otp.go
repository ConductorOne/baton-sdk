package secretcontracts

import (
	"encoding/base32"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

// otpAuthURI mirrors the receiver's otpauth parse for the cases that decide
// acceptance: scheme, host type, label, secret, algorithm, digits and HOTP
// counter. wantType is "" for either type, or "hotp".
func otpAuthURI(value, wantType string) bool {
	value = strings.TrimSpace(value)
	if strings.ContainsAny(value, "#") {
		return false
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "otpauth" || parsed.User != nil || parsed.Port() != "" ||
		parsed.Fragment != "" || !strings.HasPrefix(parsed.Path, "/") || !utf8.ValidString(parsed.Path) {
		return false
	}
	kind := strings.ToLower(parsed.Hostname())
	if kind != "totp" && kind != "hotp" {
		return false
	}
	if wantType != "" && kind != wantType {
		return false
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return false
	}
	for _, name := range []string{"secret", "issuer", "algorithm", "digits", "period", "counter", "encoder"} {
		if len(query[name]) > 1 {
			return false
		}
	}
	secret := strings.ToUpper(strings.TrimSpace(query.Get("secret")))
	if secret == "" || strings.ContainsAny(secret, "\r\n") {
		return false
	}
	if rem := len(secret) % 8; rem != 0 {
		secret += strings.Repeat("=", 8-rem)
	}
	if _, err := base32.StdEncoding.DecodeString(secret); err != nil {
		return false
	}
	if algorithm, ok := query["algorithm"]; ok {
		switch strings.ToLower(algorithm[0]) {
		case "sha1", "sha256", "sha512":
		default:
			return false
		}
	}
	if digits, ok := query["digits"]; ok {
		if number, err := strconv.ParseUint(digits[0], 10, 64); err == nil && (number < 1 || number > 10) {
			return false
		}
	}
	if kind == "hotp" {
		if counter, ok := query["counter"]; ok {
			if _, err := strconv.ParseUint(counter[0], 10, 64); err != nil {
				return false
			}
		}
	}
	return true
}
