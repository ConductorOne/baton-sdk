package actions

import (
	"errors"
	"strings"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwe"
)

func validateActionSecretContentTypes(schema *v2.BatonActionSchema) error {
	for _, field := range schema.GetArguments() {
		if field.HasSecretContentType() {
			return errors.New("secret content type is only valid on secret action returns")
		}
	}
	secretReturns := 0
	hasTypedReturn := false
	for _, field := range schema.GetReturnTypes() {
		if field.GetIsSecret() {
			secretReturns++
		}
		if !field.HasSecretContentType() {
			continue
		}
		value := field.GetSecretContentType()
		if !field.GetIsSecret() || !crypto.ValidSecretContentType(value) {
			return errors.New("secret action return content type is invalid")
		}
		if value != "generic" {
			hasTypedReturn = true
		}
	}
	if hasTypedReturn && secretReturns != 1 {
		return errors.New("typed secret action output requires exactly one secret return")
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
		if !crypto.TypedOutputContextMatches(config.GetJwkPublicKeyConfig().GetAdditionalAuthenticatedData(), contentType) {
			return errors.New("typed secret action output requires a matching full-knowledge recipient context")
		}
		matched = true
	}
	if !matched {
		return errors.New("typed secret action output requires a full-knowledge JWE recipient")
	}
	return nil
}
