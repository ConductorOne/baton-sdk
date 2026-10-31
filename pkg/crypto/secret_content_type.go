package crypto

import (
	"bytes"
	"encoding/json"
	"io"
	"regexp"
)

var secretContentTypePattern = regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*$`)

// ValidSecretContentType reports whether a type identifier can be declared.
func ValidSecretContentType(value string) bool {
	return len(value) <= 128 && secretContentTypePattern.MatchString(value)
}

// TypedOutputContextMatches checks the type-bearing recipient context.
func TypedOutputContextMatches(encoded []byte, contentType string) bool {
	if len(encoded) == 0 || len(encoded) > 16*1024 {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		name, ok := token.(string)
		if !ok {
			return false
		}
		if _, duplicate := fields[name]; duplicate {
			return false
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return false
		}
		fields[name] = value
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') {
		return false
	}
	if _, err := decoder.Token(); err != io.EOF {
		return false
	}
	var purpose, actualType string
	var version uint32
	if json.Unmarshal(fields["purpose"], &purpose) != nil || json.Unmarshal(fields["version"], &version) != nil ||
		json.Unmarshal(fields["content_type"], &actualType) != nil {
		return false
	}
	return purpose == "c1/fk-credential-delivery" && version == 1 && actualType == contentType
}
