package actions

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwe"
)

var secretContentTypePattern = regexp.MustCompile(`^[a-z0-9]+([._-][a-z0-9]+)*$`)

func validateActionSecretContentTypes(schema *v2.BatonActionSchema) error {
	for _, field := range schema.GetArguments() {
		if field.HasSecretContentType() {
			return errors.New("secret content type is only valid on secret action returns")
		}
	}
	for _, field := range schema.GetReturnTypes() {
		if !field.HasSecretContentType() {
			continue
		}
		value := field.GetSecretContentType()
		if !field.GetIsSecret() || len(value) > 128 || !secretContentTypePattern.MatchString(value) {
			return errors.New("secret action return content type is invalid")
		}
	}
	return nil
}

func snapshotSecretContentTypes(fields []*config.Field) map[string]string {
	types := make(map[string]string)
	for _, field := range fields {
		if !field.GetIsSecret() {
			continue
		}
		value := "generic"
		if field.HasSecretContentType() {
			value = field.GetSecretContentType()
		}
		types[field.GetName()] = value
	}
	return types
}

// This checks caller type agreement before producer effects; it does not authenticate the caller or validate secret payloads.
func validateTypedOutputContext(types map[string]string, configs []*v2.EncryptionConfig) error {
	contentType := ""
	for _, value := range types {
		if value != "generic" {
			contentType = value
		}
	}
	if contentType == "" {
		return nil
	}
	if len(types) != 1 {
		return errors.New("typed secret action output requires exactly one secret return")
	}
	matched := false
	for _, config := range configs {
		if strings.ToLower(strings.TrimSpace(config.GetProvider())) != jwe.EncryptionProvider {
			continue
		}
		if !typedOutputContextMatches(config.GetJwkPublicKeyConfig().GetAdditionalAuthenticatedData(), contentType) {
			return errors.New("typed secret action output requires a matching full-knowledge recipient context")
		}
		matched = true
	}
	if !matched {
		return errors.New("typed secret action output requires a full-knowledge JWE recipient")
	}
	return nil
}

func typedOutputContextMatches(encoded []byte, contentType string) bool {
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
