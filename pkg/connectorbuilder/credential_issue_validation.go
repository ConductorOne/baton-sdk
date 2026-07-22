package connectorbuilder

import (
	"fmt"
	"math"
	"slices"
	"strings"

	configv1 "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/field"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func credentialOptionKind(options *v2.LocalCredentialOptions) v2.CapabilityDetailCredentialOption {
	if options == nil {
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED
	}
	switch options.WhichOptions() {
	case v2.LocalCredentialOptions_ApiKey_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY
	case v2.LocalCredentialOptions_Keypair_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_KEYPAIR
	case v2.LocalCredentialOptions_Token_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN
	case v2.LocalCredentialOptions_ClientSecret_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET
	default:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED
	}
}

func credentialOptionKindFromRequest(options *v2.CredentialOptions) v2.CapabilityDetailCredentialOption {
	if options == nil {
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED
	}
	switch options.WhichOptions() {
	case v2.CredentialOptions_ApiKey_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY
	case v2.CredentialOptions_Keypair_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_KEYPAIR
	case v2.CredentialOptions_Token_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN
	case v2.CredentialOptions_ClientSecret_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET
	default:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED
	}
}

func validateCredentialIssueInput(input *CredentialIssueInput, details *v2.CredentialDetailsCredentialIssue) error {
	kind := credentialOptionKind(input.CredentialOptions)
	if kind == v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED {
		return fmt.Errorf("unsupported credential option")
	}
	var descriptor *v2.CredentialIssueOptionDescriptor
	for _, candidate := range details.GetOptions() {
		if candidate.GetOption() == kind {
			descriptor = candidate
			break
		}
	}
	if descriptor == nil {
		return fmt.Errorf("credential option %s is not advertised by connector", kind)
	}
	if keypair := input.CredentialOptions.GetKeypair(); keypair != nil {
		if keypair.GetProfile() == nil || !slices.ContainsFunc(descriptor.GetKeyProfiles(), func(profile *v2.KeyGenerationProfile) bool {
			return proto.Equal(profile, keypair.GetProfile())
		}) {
			return fmt.Errorf("requested key generation profile is not advertised by connector")
		}
	}
	if apiKey := input.CredentialOptions.GetApiKey(); apiKey != nil && !descriptor.GetCustomScopesAllowed() {
		for _, scope := range apiKey.GetScopes() {
			if !slices.Contains(descriptor.GetScopes(), scope) {
				return fmt.Errorf("scope %q is not advertised by connector", scope)
			}
		}
	}
	if token := input.CredentialOptions.GetToken(); token != nil {
		if token.GetAudience() != "" && !descriptor.GetAudienceSupported() {
			return fmt.Errorf("connector does not support token audience")
		}
		if !descriptor.GetCustomScopesAllowed() {
			for _, scope := range token.GetScopes() {
				if !slices.Contains(descriptor.GetScopes(), scope) {
					return fmt.Errorf("scope %q is not advertised by connector", scope)
				}
			}
		}
	}
	if constraints := input.IssuanceConstraints; constraints != nil && constraints.GetLifetime() != nil {
		if err := constraints.GetLifetime().CheckValid(); err != nil || constraints.GetLifetime().AsDuration() <= 0 {
			return fmt.Errorf("issuance lifetime must be a valid positive duration")
		}
		capability := descriptor.GetLifetime()
		if capability == nil {
			return fmt.Errorf("connector does not support a caller-selected issuance lifetime")
		}
		requested := constraints.GetLifetime().AsDuration()
		if capability.GetMin() != nil && requested < capability.GetMin().AsDuration() {
			return fmt.Errorf("issuance lifetime is below connector minimum")
		}
		if capability.GetMax() != nil && requested > capability.GetMax().AsDuration() {
			return fmt.Errorf("issuance lifetime exceeds connector maximum")
		}
		if capability.GetGranularity() != nil {
			granularity := capability.GetGranularity().AsDuration()
			if granularity <= 0 || requested%granularity != 0 {
				return fmt.Errorf("issuance lifetime does not match connector granularity")
			}
		}
	}
	return validateCredentialConnectorParameters(descriptor.GetConnectorParameters(), input.ConnectorParameters)
}

func validateCredentialConnectorParameters(schema *configv1.Configuration, values *structpb.Struct) error {
	provided := map[string]*structpb.Value{}
	if values != nil {
		provided = values.GetFields()
	}
	if schema == nil {
		if len(provided) != 0 {
			return fmt.Errorf("connector parameters are not supported")
		}
		return nil
	}
	fields := make(map[string]*configv1.Field, len(schema.GetFields()))
	for _, definition := range schema.GetFields() {
		if definition.GetIsSecret() {
			return fmt.Errorf("connector parameter %q must not be secret", definition.GetName())
		}
		fields[definition.GetName()] = definition
	}
	for name := range provided {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("unknown connector parameter %q", name)
		}
	}
	for name, definition := range fields {
		value, present := provided[name]
		if definition.GetIsRequired() && (!present || value.GetKind() == nil || value.GetNullValue() == structpb.NullValue_NULL_VALUE) {
			return fmt.Errorf("connector parameter %q is required", name)
		}
		if !present || value.GetKind() == nil || value.GetNullValue() == structpb.NullValue_NULL_VALUE {
			continue
		}
		switch definition.WhichField() {
		case configv1.Field_StringField_case:
			if _, ok := value.GetKind().(*structpb.Value_StringValue); !ok {
				return fmt.Errorf("connector parameter %q must be a string", name)
			}
			if err := field.ValidateStringRules(definition.GetStringField().GetRules(), value.GetStringValue(), name); err != nil {
				return err
			}
		case configv1.Field_IntField_case:
			number, ok := value.GetKind().(*structpb.Value_NumberValue)
			if !ok || math.Trunc(number.NumberValue) != number.NumberValue || number.NumberValue > math.MaxInt || number.NumberValue < math.MinInt {
				return fmt.Errorf("connector parameter %q must be an integer", name)
			}
			if err := field.ValidateIntRules(definition.GetIntField().GetRules(), int(number.NumberValue), name); err != nil {
				return err
			}
		case configv1.Field_BoolField_case:
			if _, ok := value.GetKind().(*structpb.Value_BoolValue); !ok {
				return fmt.Errorf("connector parameter %q must be a boolean", name)
			}
			if err := field.ValidateBoolRules(definition.GetBoolField().GetRules(), value.GetBoolValue(), name); err != nil {
				return err
			}
		case configv1.Field_StringSliceField_case:
			list, ok := value.GetKind().(*structpb.Value_ListValue)
			if !ok {
				return fmt.Errorf("connector parameter %q must be a string list", name)
			}
			items := make([]string, 0, len(list.ListValue.GetValues()))
			for _, item := range list.ListValue.GetValues() {
				if _, ok := item.GetKind().(*structpb.Value_StringValue); !ok {
					return fmt.Errorf("connector parameter %q must contain only strings", name)
				}
				items = append(items, item.GetStringValue())
			}
			if err := field.ValidateRepeatedStringRules(definition.GetStringSliceField().GetRules(), items, name); err != nil {
				return err
			}
		case configv1.Field_StringMapField_case:
			object, ok := value.GetKind().(*structpb.Value_StructValue)
			if !ok {
				return fmt.Errorf("connector parameter %q must be an object", name)
			}
			plain := make(map[string]any, len(object.StructValue.GetFields()))
			for key, item := range object.StructValue.GetFields() {
				plain[key] = item.AsInterface()
			}
			if err := field.ValidateStringMapRules(definition.GetStringMapField().GetRules(), plain, name); err != nil {
				return err
			}
		default:
			return fmt.Errorf("connector parameter %q uses unsupported input type", name)
		}
	}
	return validateCredentialParameterConstraints(schema, provided)
}

func validateCredentialParameterConstraints(schema *configv1.Configuration, values map[string]*structpb.Value) error {
	present := func(name string) bool {
		value, ok := values[name]
		return ok && value.GetKind() != nil && value.GetNullValue() != structpb.NullValue_NULL_VALUE && strings.TrimSpace(fmt.Sprint(value.AsInterface())) != ""
	}
	for _, constraint := range schema.GetConstraints() {
		count := 0
		for _, name := range constraint.GetFieldNames() {
			if present(name) {
				count++
			}
		}
		switch constraint.GetKind() {
		case configv1.ConstraintKind_CONSTRAINT_KIND_REQUIRED_TOGETHER:
			if count != 0 && count != len(constraint.GetFieldNames()) {
				return fmt.Errorf("connector parameters %v must be provided together", constraint.GetFieldNames())
			}
		case configv1.ConstraintKind_CONSTRAINT_KIND_AT_LEAST_ONE:
			if count == 0 {
				return fmt.Errorf("at least one of connector parameters %v is required", constraint.GetFieldNames())
			}
		case configv1.ConstraintKind_CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE:
			if count > 1 {
				return fmt.Errorf("connector parameters %v are mutually exclusive", constraint.GetFieldNames())
			}
		case configv1.ConstraintKind_CONSTRAINT_KIND_DEPENDENT_ON:
			if count > 0 {
				for _, secondary := range constraint.GetSecondaryFieldNames() {
					if !present(secondary) {
						return fmt.Errorf("connector parameter %q requires %q", constraint.GetFieldNames()[0], secondary)
					}
				}
			}
		case configv1.ConstraintKind_CONSTRAINT_KIND_UNSPECIFIED:
			return fmt.Errorf("connector parameter constraint kind must be specified")
		}
	}
	return nil
}
