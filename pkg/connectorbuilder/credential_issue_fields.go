package connectorbuilder

import (
	"fmt"
	"strings"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/crypto"
	"google.golang.org/protobuf/proto"
)

// CredentialIssueFields resolves direct declarations and the legacy request schema.
func CredentialIssueFields(descriptor *v2.CredentialIssueOptionDescriptor) (*v2.CredentialIssueRequestSchema, error) {
	legacy := descriptor.GetRequestSchema() //nolint:staticcheck // Read prototype declarations during the transition.
	if len(descriptor.GetArguments()) == 0 && len(descriptor.GetConstraints()) == 0 {
		return legacy, nil
	}
	current := v2.CredentialIssueRequestSchema_builder{Fields: descriptor.GetArguments(), Constraints: descriptor.GetConstraints()}.Build()
	if legacy != nil {
		if err := ValidateCredentialIssueRequestSchema(legacy); err != nil {
			return nil, err
		}
		left := proto.Clone(legacy).(*v2.CredentialIssueRequestSchema)
		right := proto.Clone(current).(*v2.CredentialIssueRequestSchema)
		for _, schema := range []*v2.CredentialIssueRequestSchema{left, right} {
			for _, field := range schema.GetFields() {
				if field != nil {
					field.SetContentType("")
					field.SetTags(nil)
				}
			}
		}
		if !proto.Equal(left, right) {
			return nil, fmt.Errorf("conflicting credential issue request declarations")
		}
	}
	return current, nil
}

// CredentialIssueOutputField resolves the declared output and legacy logical type.
func CredentialIssueOutputField(descriptor *v2.CredentialIssueOptionDescriptor) (crypto.FieldOutput, error) {
	result := crypto.FieldOutput{LogicalType: "generic"}
	if descriptor.HasSecretContentType() { //nolint:staticcheck // Read prototype declarations during the transition.
		if !crypto.ValidSecretContentType(descriptor.GetSecretContentType()) { //nolint:staticcheck // Read prototype declarations during the transition.
			return result, fmt.Errorf("invalid secret content type")
		}
		result.LogicalType = descriptor.GetSecretContentType() //nolint:staticcheck // Read prototype declarations during the transition.
	}
	fields := descriptor.GetReturnTypes()
	if len(fields) == 0 {
		return result, nil
	}
	if len(fields) != 1 || fields[0] == nil {
		return result, fmt.Errorf("credential return declaration requires exactly one field")
	}
	field := fields[0]
	if credentialIssueHasUnknownFields(field.ProtoReflect()) {
		return result, fmt.Errorf("credential return contains unsupported fields")
	}
	if strings.TrimSpace(field.GetName()) == "" || !field.GetIsRequired() || !field.GetIsSecret() || field.WhichField() != config.Field_StringField_case {
		return result, fmt.Errorf("credential return must be a named required secret string")
	}
	meta, err := crypto.ResolveFieldOutput(field)
	if err != nil {
		return result, err
	}
	if descriptor.HasSecretContentType() && meta.HasType && result.LogicalType != meta.LogicalType { //nolint:staticcheck // Read prototype declarations during the transition.
		return result, fmt.Errorf("conflicting credential output types")
	}
	if !meta.HasType {
		meta.LogicalType = result.LogicalType
	}
	return meta, nil
}

// CredentialIssueValidationSchema returns the effective input contract without presentation metadata.
// A nil result preserves the legacy no-schema representation.
func CredentialIssueValidationSchema(descriptor *v2.CredentialIssueOptionDescriptor) (*v2.CredentialIssueRequestSchema, error) {
	schema, err := CredentialIssueFields(descriptor)
	if err != nil {
		return nil, err
	}
	if err := ValidateCredentialIssueRequestSchema(schema); err != nil {
		return nil, err
	}
	if schema == nil {
		return nil, nil
	}
	result := proto.Clone(schema).(*v2.CredentialIssueRequestSchema)
	for _, field := range result.GetFields() {
		field.SetContentType("")
		field.SetTags(nil)
	}
	return result, nil
}
