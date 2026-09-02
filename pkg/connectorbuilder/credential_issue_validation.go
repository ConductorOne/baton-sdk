package connectorbuilder

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/field"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

var credentialIssueRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// maxCredentialIssueSecretResourceTypeIDBytes bounds secret_resource_type_id on
// both the descriptor and the request. The descriptor's matching proto rules
// never run: nothing on the capabilities or issuance path calls the generated
// Validate().
const maxCredentialIssueSecretResourceTypeIDBytes = 1024

// credentialIssueDescriptorKey identifies one advertised issuance option. A
// credential shape alone cannot: a connector may mint several kinds of
// credential that share a shape and differ only in what they come back as.
type credentialIssueDescriptorKey struct {
	option               v2.CapabilityDetailCredentialOption
	secretResourceTypeID string
}

// resolveCredentialIssueDescriptor looks up the one descriptor a request's
// CredentialIssueOptions selects: the oneof arm gives the shape, and
// secret_resource_type_id gives the kind within that shape. Both halves are
// required, so the pair always names at most one advertised descriptor.
func resolveCredentialIssueDescriptor(
	details *v2.CredentialDetailsCredentialIssue,
	options *v2.CredentialIssueOptions,
) (*v2.CredentialIssueOptionDescriptor, error) {
	kind := credentialIssueOptionKind(options)
	if kind == v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED {
		return nil, fmt.Errorf("unsupported credential option")
	}
	secretResourceTypeID := options.GetSecretResourceTypeId()
	if secretResourceTypeID == "" {
		return nil, fmt.Errorf("credential_options.secret_resource_type_id is required")
	}
	if len(secretResourceTypeID) > maxCredentialIssueSecretResourceTypeIDBytes {
		return nil, fmt.Errorf("credential_options.secret_resource_type_id must be at most %d bytes", maxCredentialIssueSecretResourceTypeIDBytes)
	}
	var advertisedForKind []string
	for _, candidate := range details.GetOptions() {
		if candidate.GetOption() != kind {
			continue
		}
		if candidate.GetSecretResourceTypeId() == secretResourceTypeID {
			return candidate, nil
		}
		advertisedForKind = append(advertisedForKind, strconv.Quote(candidate.GetSecretResourceTypeId()))
	}
	if len(advertisedForKind) == 0 {
		return nil, fmt.Errorf("credential option %s is not advertised by connector", kind)
	}
	return nil, fmt.Errorf("credential option %s does not produce secret resource type %q; it produces %s",
		kind, secretResourceTypeID, strings.Join(advertisedForKind, ", "))
}

func credentialIssueOptionKind(options *v2.CredentialIssueOptions) v2.CapabilityDetailCredentialOption {
	if options == nil {
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED
	}
	switch options.WhichOptions() {
	case v2.CredentialIssueOptions_ApiKey_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY
	case v2.CredentialIssueOptions_Keypair_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_KEYPAIR
	case v2.CredentialIssueOptions_Token_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN
	case v2.CredentialIssueOptions_ClientSecret_case:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_CLIENT_SECRET
	default:
		return v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_UNSPECIFIED
	}
}

func validateCredentialIssueInput(input *CredentialIssueInput, details *v2.CredentialDetailsCredentialIssue, now time.Time) (*v2.CredentialIssueOptionDescriptor, error) {
	if input == nil || input.IdentityID == nil {
		return nil, fmt.Errorf("identity id is required")
	}
	if len(input.RequestID) == 0 || len(input.RequestID) > 128 || !credentialIssueRequestIDPattern.MatchString(input.RequestID) {
		return nil, fmt.Errorf("request id must be 1..128 characters containing only letters, digits, underscore, or hyphen")
	}
	descriptor, err := resolveCredentialIssueDescriptor(details, input.CredentialOptions)
	if err != nil {
		return nil, err
	}
	if descriptor.GetResourceMode() == v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_UNSPECIFIED {
		return nil, fmt.Errorf("credential resource mode must be advertised")
	}
	if err := validateCredentialIssueRequestData(descriptor.GetRequestSchema(), input.RequestData); err != nil {
		return nil, err
	}
	if keypair := input.CredentialOptions.GetKeypair(); keypair != nil {
		if err := validateKeyGenerationProfile(keypair.GetProfile()); err != nil {
			return nil, err
		}
		if !slices.ContainsFunc(descriptor.GetKeyProfiles(), func(profile *v2.KeyGenerationProfile) bool {
			return proto.Equal(profile, keypair.GetProfile())
		}) {
			return nil, fmt.Errorf("requested key generation profile is not advertised by connector")
		}
	}
	if apiKey := input.CredentialOptions.GetApiKey(); apiKey != nil {
		if err := validateRequestedValues("scope", apiKey.GetScopes(), descriptor.GetScopes(), descriptor.GetCustomScopesAllowed()); err != nil {
			return nil, err
		}
	}
	if token := input.CredentialOptions.GetToken(); token != nil {
		if err := validateRequestedValues("scope", token.GetScopes(), descriptor.GetScopes(), descriptor.GetCustomScopesAllowed()); err != nil {
			return nil, err
		}
		if err := validateRequestedValues("audience", token.GetAudiences(), descriptor.GetAudiences(), descriptor.GetCustomAudiencesAllowed()); err != nil {
			return nil, err
		}
	}
	if input.ExpiresAt != nil {
		if err := input.ExpiresAt.CheckValid(); err != nil {
			return nil, fmt.Errorf("expires_at must be valid: %w", err)
		}
		remaining := input.ExpiresAt.AsTime().Sub(now)
		if remaining <= 0 {
			return nil, fmt.Errorf("expires_at must be in the future")
		}
		capability := descriptor.GetExpiry()
		if capability == nil {
			return nil, fmt.Errorf("connector does not support caller-selected expiry")
		}
		if capability.GetMin() != nil && remaining < capability.GetMin().AsDuration() {
			return nil, fmt.Errorf("requested expiry is below connector minimum")
		}
		if capability.GetMax() != nil && remaining > capability.GetMax().AsDuration() {
			return nil, fmt.Errorf("requested expiry exceeds connector maximum")
		}
	}
	return descriptor, nil
}

func validateCredentialIssueRequestSchema(schema *v2.CredentialIssueRequestSchema) error {
	if schema == nil {
		return nil
	}
	fields := make(map[string]*config.Field, len(schema.GetFields()))
	for _, schemaField := range schema.GetFields() {
		if schemaField == nil || strings.TrimSpace(schemaField.GetName()) == "" {
			return fmt.Errorf("request schema field name is required")
		}
		name := schemaField.GetName()
		if _, ok := fields[name]; ok {
			return fmt.Errorf("duplicate request schema field %q", name)
		}
		fields[name] = schemaField
		switch schemaField.WhichField() {
		case config.Field_StringField_case:
			if rules := schemaField.GetStringField().GetRules(); rules != nil {
				if rules.HasPattern() {
					if _, err := regexp.CompilePOSIX(rules.GetPattern()); err != nil {
						return fmt.Errorf("request schema field %q has invalid pattern: %w", name, err)
					}
				}
				if rules.HasMinLen() && rules.HasMaxLen() && rules.GetMinLen() > rules.GetMaxLen() {
					return fmt.Errorf("request schema field %q has minimum length greater than maximum", name)
				}
			}
		case config.Field_IntField_case:
			if rules := schemaField.GetIntField().GetRules(); rules != nil {
				if rules.HasGte() && rules.HasLte() && rules.GetGte() > rules.GetLte() {
					return fmt.Errorf("request schema field %q has minimum greater than maximum", name)
				}
				if rules.HasGt() && rules.HasLt() && rules.GetGt() >= rules.GetLt() {
					return fmt.Errorf("request schema field %q has empty integer range", name)
				}
			}
		case config.Field_BoolField_case, config.Field_StringMapField_case:
		case config.Field_StringSliceField_case:
			rules := schemaField.GetStringSliceField().GetRules()
			if rules != nil && rules.HasMinItems() && rules.HasMaxItems() && rules.GetMinItems() > rules.GetMaxItems() {
				return fmt.Errorf("request schema field %q has minimum items greater than maximum", name)
			}
			if rules != nil && rules.HasItemRules() && rules.GetItemRules().HasPattern() {
				if _, err := regexp.CompilePOSIX(rules.GetItemRules().GetPattern()); err != nil {
					return fmt.Errorf("request schema field %q has invalid item pattern: %w", name, err)
				}
			}
		default:
			return fmt.Errorf("request schema field %q has unsupported type", name)
		}
	}
	for _, constraint := range schema.GetConstraints() {
		if constraint == nil {
			return fmt.Errorf("request schema constraint is required")
		}
		if constraint.GetKind() == config.ConstraintKind_CONSTRAINT_KIND_UNSPECIFIED {
			return fmt.Errorf("request schema constraint kind is required")
		}
		if len(constraint.GetFieldNames()) == 0 {
			return fmt.Errorf("request schema constraint fields are required")
		}
		if duplicate := firstDuplicate(constraint.GetFieldNames()); duplicate != "" {
			return fmt.Errorf("request schema constraint repeats field %q", duplicate)
		}
		if duplicate := firstDuplicate(constraint.GetSecondaryFieldNames()); duplicate != "" {
			return fmt.Errorf("request schema constraint repeats secondary field %q", duplicate)
		}
		for _, name := range append(slices.Clone(constraint.GetFieldNames()), constraint.GetSecondaryFieldNames()...) {
			if _, ok := fields[name]; !ok {
				return fmt.Errorf("request schema constraint refers to unknown field %q", name)
			}
		}
		if constraint.GetKind() == config.ConstraintKind_CONSTRAINT_KIND_DEPENDENT_ON && len(constraint.GetSecondaryFieldNames()) == 0 {
			return fmt.Errorf("request schema dependent-on constraint requires secondary fields")
		}
		if constraint.GetKind() != config.ConstraintKind_CONSTRAINT_KIND_DEPENDENT_ON && len(constraint.GetFieldNames()) < 2 {
			return fmt.Errorf("request schema constraint requires at least two fields")
		}
	}
	return nil
}

func firstDuplicate(values []string) string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			return value
		}
		seen[value] = struct{}{}
	}
	return ""
}

func validateCredentialIssueRequestData(schema *v2.CredentialIssueRequestSchema, data *structpb.Struct) error {
	if err := validateCredentialIssueRequestSchema(schema); err != nil {
		return fmt.Errorf("invalid request schema: %w", err)
	}
	values := map[string]*structpb.Value(nil)
	if data != nil {
		values = data.GetFields()
	}
	fields := make(map[string]*config.Field, len(schema.GetFields()))
	for _, schemaField := range schema.GetFields() {
		fields[schemaField.GetName()] = schemaField
	}
	for name := range values {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("request data contains unknown field %q", name)
		}
	}
	present := make(map[string]bool, len(values))
	for name, schemaField := range fields {
		value, ok := values[name]
		_, isNull := value.GetKind().(*structpb.Value_NullValue)
		if !ok || value == nil || value.GetKind() == nil || isNull {
			if schemaField.GetIsRequired() {
				return fmt.Errorf("request data field %q is required", name)
			}
			continue
		}
		present[name] = true
		if err := validateCredentialIssueRequestValue(schemaField, value); err != nil {
			return err
		}
		if schemaField.GetIsRequired() && credentialIssueRequestValueIsEmpty(value) {
			return fmt.Errorf("request data field %q is required", name)
		}
	}
	for _, constraint := range schema.GetConstraints() {
		if err := validateCredentialIssueConstraint(constraint, present); err != nil {
			return err
		}
	}
	return nil
}

func credentialIssueRequestValueIsEmpty(value *structpb.Value) bool {
	switch kind := value.GetKind().(type) {
	case *structpb.Value_StringValue:
		return kind.StringValue == ""
	case *structpb.Value_ListValue:
		return len(kind.ListValue.GetValues()) == 0
	case *structpb.Value_StructValue:
		return len(kind.StructValue.GetFields()) == 0
	default:
		return false
	}
}

func validateCredentialIssueRequestValue(schemaField *config.Field, value *structpb.Value) error {
	name := schemaField.GetName()
	switch schemaField.WhichField() {
	case config.Field_StringField_case:
		kind, ok := value.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return fmt.Errorf("request data field %q must be a string", name)
		}
		if err := field.ValidateStringRules(schemaField.GetStringField().GetRules(), kind.StringValue, name); err != nil {
			return err
		}
	case config.Field_IntField_case:
		kind, ok := value.GetKind().(*structpb.Value_NumberValue)
		const maxSafeJSONInteger = float64(1<<53 - 1)
		if !ok || math.IsNaN(kind.NumberValue) || math.IsInf(kind.NumberValue, 0) || math.Trunc(kind.NumberValue) != kind.NumberValue || kind.NumberValue < -maxSafeJSONInteger || kind.NumberValue > maxSafeJSONInteger {
			return fmt.Errorf("request data field %q must be an integer", name)
		}
		if err := field.ValidateIntRules(schemaField.GetIntField().GetRules(), int(kind.NumberValue), name); err != nil {
			return err
		}
	case config.Field_BoolField_case:
		kind, ok := value.GetKind().(*structpb.Value_BoolValue)
		if !ok {
			return fmt.Errorf("request data field %q must be a boolean", name)
		}
		if err := field.ValidateBoolRules(schemaField.GetBoolField().GetRules(), kind.BoolValue, name); err != nil {
			return err
		}
	case config.Field_StringSliceField_case:
		kind, ok := value.GetKind().(*structpb.Value_ListValue)
		if !ok {
			return fmt.Errorf("request data field %q must be a string list", name)
		}
		items := make([]string, 0, len(kind.ListValue.GetValues()))
		for _, item := range kind.ListValue.GetValues() {
			stringItem, ok := item.GetKind().(*structpb.Value_StringValue)
			if !ok {
				return fmt.Errorf("request data field %q must contain only strings", name)
			}
			items = append(items, stringItem.StringValue)
		}
		if err := field.ValidateRepeatedStringRules(schemaField.GetStringSliceField().GetRules(), items, name); err != nil {
			return err
		}
	case config.Field_StringMapField_case:
		kind, ok := value.GetKind().(*structpb.Value_StructValue)
		if !ok {
			return fmt.Errorf("request data field %q must be an object", name)
		}
		if err := field.ValidateStringMapRules(schemaField.GetStringMapField().GetRules(), kind.StructValue.AsMap(), name); err != nil {
			return err
		}
	default:
		return fmt.Errorf("request data field %q has unsupported type", name)
	}
	return nil
}

func validateCredentialIssueConstraint(constraint *config.Constraint, present map[string]bool) error {
	countPresent := func(names []string) int {
		seen := make(map[string]struct{}, len(names))
		count := 0
		for _, name := range names {
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			if present[name] {
				count++
			}
		}
		return count
	}
	primaryCount := countPresent(constraint.GetFieldNames())
	switch constraint.GetKind() {
	case config.ConstraintKind_CONSTRAINT_KIND_REQUIRED_TOGETHER:
		if primaryCount > 0 && primaryCount < len(constraint.GetFieldNames()) {
			return fmt.Errorf("request data fields required together: %v", constraint.GetFieldNames())
		}
	case config.ConstraintKind_CONSTRAINT_KIND_AT_LEAST_ONE:
		if primaryCount == 0 {
			return fmt.Errorf("request data requires at least one of: %v", constraint.GetFieldNames())
		}
	case config.ConstraintKind_CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE:
		if primaryCount > 1 {
			return fmt.Errorf("request data fields are mutually exclusive: %v", constraint.GetFieldNames())
		}
	case config.ConstraintKind_CONSTRAINT_KIND_DEPENDENT_ON:
		if primaryCount > 0 && countPresent(constraint.GetSecondaryFieldNames()) < len(constraint.GetSecondaryFieldNames()) {
			return fmt.Errorf("request data fields %v depend on %v", constraint.GetFieldNames(), constraint.GetSecondaryFieldNames())
		}
	default:
		return fmt.Errorf("unknown request schema constraint kind %v", constraint.GetKind())
	}
	return nil
}

func validateRequestedValues(kind string, requested []string, advertised []string, customAllowed bool) error {
	seen := make(map[string]struct{}, len(requested))
	for _, value := range requested {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s must not be empty", kind)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("duplicate %s %q", kind, value)
		}
		seen[value] = struct{}{}
		if !customAllowed && !slices.Contains(advertised, value) {
			return fmt.Errorf("%s %q is not advertised by connector", kind, value)
		}
	}
	return nil
}

func validateKeyGenerationProfile(profile *v2.KeyGenerationProfile) error {
	if profile == nil {
		return fmt.Errorf("key generation profile is required")
	}
	switch profile.GetKty() {
	case "RSA":
		if !profile.HasRsaModulusBits() || profile.HasCrv() {
			return fmt.Errorf("RSA profile requires rsa_modulus_bits and no curve")
		}
		bits := profile.GetRsaModulusBits()
		if bits < 2048 || bits > 16384 || bits%256 != 0 {
			return fmt.Errorf("RSA modulus bits must be 2048..16384 in 256-bit increments")
		}
	case "EC":
		if !profile.HasCrv() || profile.HasRsaModulusBits() || !slices.Contains([]string{"P-256", "P-384", "P-521"}, profile.GetCrv()) {
			return fmt.Errorf("EC profile requires a recognized P-256, P-384, or P-521 curve")
		}
	case "OKP":
		if !profile.HasCrv() || profile.HasRsaModulusBits() || !slices.Contains([]string{"Ed25519", "Ed448", "X25519", "X448"}, profile.GetCrv()) {
			return fmt.Errorf("OKP profile requires a recognized curve")
		}
	default:
		return fmt.Errorf("unsupported JWK key type %q", profile.GetKty())
	}
	return nil
}
