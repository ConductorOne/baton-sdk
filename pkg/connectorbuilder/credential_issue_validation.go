package connectorbuilder

import (
	"errors"
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
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

var credentialIssueRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// ErrInvalidCredentialIssueRequestSchema identifies connector-owned schema
// errors separately from caller-owned request data errors.
var ErrInvalidCredentialIssueRequestSchema = errors.New("invalid credential issue request schema")

const (
	maxSafeJSONInteger                   = int64(1<<53 - 1)
	maxCredentialIssueRequestFields      = 64
	maxCredentialIssueRequestConstraints = 64
	maxCredentialIssueRequestDataBytes   = 64 * 1024
	maxCredentialIssueCollectionItems    = 64
)

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
	if err := ValidateCredentialIssueRequestData(descriptor.GetRequestSchema(), input.RequestData); err != nil {
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

// ValidateCredentialIssueRequestSchema validates connector-owned credential
// request schema structure before the capability is published or consumed.
func ValidateCredentialIssueRequestSchema(schema *v2.CredentialIssueRequestSchema) error {
	if schema == nil {
		return nil
	}
	if len(schema.GetFields()) > maxCredentialIssueRequestFields {
		return fmt.Errorf("request schema must not contain more than %d fields", maxCredentialIssueRequestFields)
	}
	if len(schema.GetConstraints()) > maxCredentialIssueRequestConstraints {
		return fmt.Errorf("request schema must not contain more than %d constraints", maxCredentialIssueRequestConstraints)
	}
	fields := make(map[string]*config.Field, len(schema.GetFields()))
	for _, schemaField := range schema.GetFields() {
		if schemaField == nil || strings.TrimSpace(schemaField.GetName()) == "" {
			return fmt.Errorf("request schema field name is required")
		}
		name := schemaField.GetName()
		if schemaField.GetIsSecret() {
			return fmt.Errorf("request schema field %q must not be secret", name)
		}
		if _, ok := fields[name]; ok {
			return fmt.Errorf("duplicate request schema field %q", name)
		}
		fields[name] = schemaField
		switch schemaField.WhichField() {
		case config.Field_StringField_case:
			if schemaField.GetStringField().GetType() != config.StringFieldType_STRING_FIELD_TYPE_TEXT_UNSPECIFIED {
				return fmt.Errorf("request schema field %q has unsupported string field type", name)
			}
			if len(schemaField.GetStringField().GetAllowedExtensions()) > 0 {
				return fmt.Errorf("request schema field %q must not declare file extensions", name)
			}
			if rules := schemaField.GetStringField().GetRules(); rules != nil {
				if err := validateCredentialIssueStringRuleBounds(name, rules, false); err != nil {
					return err
				}
				if rules.HasPattern() {
					if _, err := regexp.CompilePOSIX(rules.GetPattern()); err != nil {
						return fmt.Errorf("request schema field %q has invalid pattern: %w", name, err)
					}
				}
			}
		case config.Field_IntField_case:
			if rules := schemaField.GetIntField().GetRules(); rules != nil {
				if err := validateCredentialIssueIntRuleBounds(rules); err != nil {
					return fmt.Errorf("request schema field %q: %w", name, err)
				}
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
			if rules != nil && rules.HasMinItems() && rules.GetMinItems() > maxCredentialIssueCollectionItems {
				return fmt.Errorf("request schema field %q minimum items must not exceed %d", name, maxCredentialIssueCollectionItems)
			}
			if rules != nil && rules.HasMaxItems() && rules.GetMaxItems() > maxCredentialIssueCollectionItems {
				return fmt.Errorf("request schema field %q maximum items must not exceed %d", name, maxCredentialIssueCollectionItems)
			}
			if rules != nil && rules.HasMinItems() && rules.HasMaxItems() && rules.GetMinItems() > rules.GetMaxItems() {
				return fmt.Errorf("request schema field %q has minimum items greater than maximum", name)
			}
			if rules != nil && rules.HasItemRules() {
				if err := validateCredentialIssueStringRuleBounds(name, rules.GetItemRules(), true); err != nil {
					return err
				}
			}
			if rules != nil && rules.HasItemRules() && rules.GetItemRules().HasPattern() {
				if _, err := regexp.CompilePOSIX(rules.GetItemRules().GetPattern()); err != nil {
					return fmt.Errorf("request schema field %q has invalid item pattern: %w", name, err)
				}
			}
		default:
			return fmt.Errorf("request schema field %q has unsupported type", name)
		}
		if err := validateCredentialIssueFieldDefaults(schemaField); err != nil {
			return err
		}
	}
	for _, constraint := range schema.GetConstraints() {
		if constraint == nil {
			return fmt.Errorf("request schema constraint is required")
		}
		switch constraint.GetKind() {
		case config.ConstraintKind_CONSTRAINT_KIND_REQUIRED_TOGETHER,
			config.ConstraintKind_CONSTRAINT_KIND_AT_LEAST_ONE,
			config.ConstraintKind_CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE,
			config.ConstraintKind_CONSTRAINT_KIND_DEPENDENT_ON:
		case config.ConstraintKind_CONSTRAINT_KIND_UNSPECIFIED:
			return fmt.Errorf("request schema constraint kind is required")
		default:
			return fmt.Errorf("request schema constraint kind %v is unsupported", constraint.GetKind())
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

func validateCredentialIssueStringRuleBounds(name string, rules *config.StringRules, listItem bool) error {
	if rules.HasLen() && credentialIssueRequestStringValueSize(name, rules.GetLen(), listItem) > maxCredentialIssueRequestDataBytes {
		return fmt.Errorf("request schema field %q exact length cannot fit within the %d-byte request data limit", name, maxCredentialIssueRequestDataBytes)
	}
	if rules.HasMinLen() && credentialIssueRequestStringValueSize(name, rules.GetMinLen(), listItem) > maxCredentialIssueRequestDataBytes {
		return fmt.Errorf("request schema field %q minimum length cannot fit within the %d-byte request data limit", name, maxCredentialIssueRequestDataBytes)
	}
	if rules.HasMinLen() && rules.HasMaxLen() && rules.GetMinLen() > rules.GetMaxLen() {
		return fmt.Errorf("request schema field %q has minimum length greater than maximum", name)
	}
	return nil
}

func credentialIssueRequestStringValueSize(name string, length uint64, listItem bool) int {
	if length > maxCredentialIssueRequestDataBytes {
		return maxCredentialIssueRequestDataBytes + 1
	}
	stringValueSize := protowire.SizeTag(3) + protowire.SizeBytes(int(length))
	valueSize := stringValueSize
	if listItem {
		listSize := protowire.SizeTag(1) + protowire.SizeBytes(stringValueSize)
		valueSize = protowire.SizeTag(6) + protowire.SizeBytes(listSize)
	}
	mapEntrySize := protowire.SizeTag(1) + protowire.SizeBytes(len(name)) + protowire.SizeTag(2) + protowire.SizeBytes(valueSize)
	return protowire.SizeTag(1) + protowire.SizeBytes(mapEntrySize)
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

func validateCredentialIssueFieldDefaults(schemaField *config.Field) error {
	name := schemaField.GetName()
	validate := func(kind string, value *structpb.Value) error {
		if err := validateCredentialIssueRequestValue(schemaField, value); err != nil {
			return fmt.Errorf("request schema field %q has invalid %s: %w", name, kind, err)
		}
		return nil
	}
	switch schemaField.WhichField() {
	case config.Field_StringField_case:
		fieldConfig := schemaField.GetStringField()
		for _, candidate := range []struct{ kind, value string }{
			{"default value", fieldConfig.GetDefaultValue()},
			{"suggested value", fieldConfig.GetSuggestedValue()},
		} {
			if candidate.value != "" {
				if err := validate(candidate.kind, structpb.NewStringValue(candidate.value)); err != nil {
					return err
				}
			}
		}
	case config.Field_IntField_case:
		fieldConfig := schemaField.GetIntField()
		for _, candidate := range []struct {
			kind  string
			value int64
		}{
			{"default value", fieldConfig.GetDefaultValue()},
			{"suggested value", fieldConfig.GetSuggestedValue()},
		} {
			if candidate.value != 0 {
				if err := validate(candidate.kind, structpb.NewNumberValue(float64(candidate.value))); err != nil {
					return err
				}
			}
		}
	case config.Field_BoolField_case:
		fieldConfig := schemaField.GetBoolField()
		for _, candidate := range []struct {
			kind  string
			value bool
		}{
			{"default value", fieldConfig.GetDefaultValue()},
			{"suggested value", fieldConfig.GetSuggestedValue()},
		} {
			if candidate.value {
				if err := validate(candidate.kind, structpb.NewBoolValue(candidate.value)); err != nil {
					return err
				}
			}
		}
	case config.Field_StringSliceField_case:
		fieldConfig := schemaField.GetStringSliceField()
		for _, candidate := range []struct {
			kind  string
			value []string
		}{
			{"default value", fieldConfig.GetDefaultValue()},
			{"suggested value", fieldConfig.GetSuggestedValue()},
		} {
			if len(candidate.value) > 0 {
				values := make([]*structpb.Value, 0, len(candidate.value))
				for _, item := range candidate.value {
					values = append(values, structpb.NewStringValue(item))
				}
				if err := validate(candidate.kind, structpb.NewListValue(&structpb.ListValue{Values: values})); err != nil {
					return err
				}
			}
		}
	case config.Field_StringMapField_case:
		fieldConfig := schemaField.GetStringMapField()
		for _, candidate := range []struct {
			kind  string
			value map[string]*anypb.Any
		}{
			{"default value", fieldConfig.GetDefaultValue()},
			{"suggested value", fieldConfig.GetSuggestedValue()},
		} {
			if len(candidate.value) > 0 {
				structValue, err := credentialIssueConfigMapValue(candidate.value)
				if err != nil {
					return fmt.Errorf("request schema field %q has invalid %s: %w", name, candidate.kind, err)
				}
				if err := validate(candidate.kind, structpb.NewStructValue(structValue)); err != nil {
					return err
				}
			}
		}
	default:
	}
	return nil
}

func credentialIssueConfigMapValue(values map[string]*anypb.Any) (*structpb.Struct, error) {
	converted := make(map[string]*structpb.Value, len(values))
	for name, value := range values {
		if value == nil {
			return nil, fmt.Errorf("map entry %q is nil", name)
		}
		convertedValue := &structpb.Value{}
		if err := value.UnmarshalTo(convertedValue); err != nil {
			return nil, fmt.Errorf("map entry %q is not a protobuf value: %w", name, err)
		}
		converted[name] = convertedValue
	}
	return &structpb.Struct{Fields: converted}, nil
}

// ValidateCredentialIssueRequestData validates typed values against one
// credential issue descriptor. Hosts use the same validator after applying
// their generic offering policy so host and connector validation cannot drift.
// Correctly typed empty strings, lists, and maps follow config-field semantics:
// they are omissions for requiredness and cross-field constraints. Numeric zero
// and false remain present because Struct preserves their explicit value.
func ValidateCredentialIssueRequestData(schema *v2.CredentialIssueRequestSchema, data *structpb.Struct) error {
	if err := ValidateCredentialIssueRequestSchema(schema); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCredentialIssueRequestSchema, err)
	}
	values := map[string]*structpb.Value(nil)
	if data != nil {
		values = data.GetFields()
	}
	if len(values) > maxCredentialIssueRequestFields {
		return fmt.Errorf("request data must not contain more than %d fields", maxCredentialIssueRequestFields)
	}
	if proto.Size(data) > maxCredentialIssueRequestDataBytes {
		return fmt.Errorf("request data must not exceed %d bytes", maxCredentialIssueRequestDataBytes)
	}
	fields := make(map[string]*config.Field, len(schema.GetFields()))
	for _, schemaField := range schema.GetFields() {
		fields[schemaField.GetName()] = schemaField
	}
	unknownName := ""
	unknownFound := false
	for name := range values {
		if _, ok := fields[name]; !ok {
			if !unknownFound || name < unknownName {
				unknownName = name
				unknownFound = true
			}
		}
	}
	if unknownFound {
		return fmt.Errorf("request data contains unknown field %q", unknownName)
	}
	present := make(map[string]bool, len(values))
	for _, schemaField := range schema.GetFields() {
		name := schemaField.GetName()
		value, ok := values[name]
		_, isNull := value.GetKind().(*structpb.Value_NullValue)
		if !ok || value == nil || value.GetKind() == nil || isNull {
			if credentialIssueRequestFieldIsRequired(schemaField) {
				return fmt.Errorf("request data field %q is required", name)
			}
			continue
		}
		required := credentialIssueRequestFieldIsRequired(schemaField)
		empty := credentialIssueRequestValueMatchesType(schemaField, value) && credentialIssueRequestValueIsEmpty(value)
		// Presence follows the config-field empty semantics: empty strings and
		// collections are absent, while explicitly supplied numeric zero and false
		// are present. Int64Rules.is_required retains its older zero-value rule.
		present[name] = !empty
		if empty {
			if required {
				return fmt.Errorf("request data field %q is required", name)
			}
			continue
		}
		if err := validateCredentialIssueRequestValue(schemaField, value); err != nil {
			return err
		}
	}
	for _, constraint := range schema.GetConstraints() {
		if err := validateCredentialIssueConstraint(constraint, present); err != nil {
			return err
		}
	}
	return nil
}

func credentialIssueRequestValueMatchesType(schemaField *config.Field, value *structpb.Value) bool {
	switch schemaField.WhichField() {
	case config.Field_StringField_case:
		_, ok := value.GetKind().(*structpb.Value_StringValue)
		return ok
	case config.Field_IntField_case:
		_, ok := value.GetKind().(*structpb.Value_NumberValue)
		return ok
	case config.Field_BoolField_case:
		_, ok := value.GetKind().(*structpb.Value_BoolValue)
		return ok
	case config.Field_StringSliceField_case:
		_, ok := value.GetKind().(*structpb.Value_ListValue)
		return ok
	case config.Field_StringMapField_case:
		_, ok := value.GetKind().(*structpb.Value_StructValue)
		return ok
	default:
		return false
	}
}

func credentialIssueRequestFieldIsRequired(schemaField *config.Field) bool {
	if schemaField.GetIsRequired() {
		return true
	}
	switch schemaField.WhichField() {
	case config.Field_StringField_case:
		return schemaField.GetStringField().GetRules().GetIsRequired()
	case config.Field_IntField_case:
		return schemaField.GetIntField().GetRules().GetIsRequired()
	case config.Field_BoolField_case:
		return false
	case config.Field_StringSliceField_case:
		return schemaField.GetStringSliceField().GetRules().GetIsRequired()
	case config.Field_StringMapField_case:
		return schemaField.GetStringMapField().GetRules().GetIsRequired()
	default:
		return false
	}
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
		options := schemaField.GetStringField().GetOptions()
		if len(options) > 0 && !slices.ContainsFunc(options, func(option *config.StringFieldOption) bool {
			return option.GetValue() == kind.StringValue
		}) {
			return fmt.Errorf("request data field %q must match an advertised option", name)
		}
		if err := field.ValidateStringRules(schemaField.GetStringField().GetRules(), kind.StringValue, name); err != nil {
			return err
		}
	case config.Field_IntField_case:
		kind, ok := value.GetKind().(*structpb.Value_NumberValue)
		if !ok || math.IsNaN(kind.NumberValue) || math.IsInf(kind.NumberValue, 0) || math.Trunc(kind.NumberValue) != kind.NumberValue {
			return fmt.Errorf("request data field %q must be an integer", name)
		}
		if kind.NumberValue < float64(-maxSafeJSONInteger) || kind.NumberValue > float64(maxSafeJSONInteger) {
			return fmt.Errorf("request data field %q must be within the supported integer range", name)
		}
		rules := cloneIntRulesForRequest(schemaField.GetIntField().GetRules())
		if err := field.ValidateInt64Rules(rules, int64(kind.NumberValue), name); err != nil {
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
		if len(kind.ListValue.GetValues()) > maxCredentialIssueCollectionItems {
			return fmt.Errorf("request data field %q must not contain more than %d items", name, maxCredentialIssueCollectionItems)
		}
		items := make([]string, 0, len(kind.ListValue.GetValues()))
		for _, item := range kind.ListValue.GetValues() {
			stringItem, ok := item.GetKind().(*structpb.Value_StringValue)
			if !ok {
				return fmt.Errorf("request data field %q must contain only strings", name)
			}
			items = append(items, stringItem.StringValue)
		}
		rules := cloneRepeatedStringRulesForRequest(schemaField.GetStringSliceField().GetRules())
		if err := field.ValidateRepeatedStringRules(rules, items, name); err != nil {
			return err
		}
	case config.Field_StringMapField_case:
		kind, ok := value.GetKind().(*structpb.Value_StructValue)
		if !ok {
			return fmt.Errorf("request data field %q must be an object", name)
		}
		if len(kind.StructValue.GetFields()) > maxCredentialIssueCollectionItems {
			return fmt.Errorf("request data field %q must not contain more than %d entries", name, maxCredentialIssueCollectionItems)
		}
		for _, item := range kind.StructValue.GetFields() {
			if _, ok := item.GetKind().(*structpb.Value_StringValue); !ok {
				return fmt.Errorf("request data field %q must contain only string values", name)
			}
		}
		if err := field.ValidateStringMapRules(schemaField.GetStringMapField().GetRules(), kind.StructValue.AsMap(), name); err != nil {
			return err
		}
	default:
		return fmt.Errorf("request data field %q has unsupported type", name)
	}
	return nil
}

func validateCredentialIssueIntRuleBounds(rules *config.Int64Rules) error {
	values := []int64{rules.GetEq(), rules.GetLt(), rules.GetLte(), rules.GetGt(), rules.GetGte()}
	set := []bool{rules.HasEq(), rules.HasLt(), rules.HasLte(), rules.HasGt(), rules.HasGte()}
	for index, value := range values {
		if set[index] && (value < -maxSafeJSONInteger || value > maxSafeJSONInteger) {
			return fmt.Errorf("integer rule is outside the supported JSON integer range")
		}
	}
	for _, value := range append(slices.Clone(rules.GetIn()), rules.GetNotIn()...) {
		if value < -maxSafeJSONInteger || value > maxSafeJSONInteger {
			return fmt.Errorf("integer rule is outside the supported JSON integer range")
		}
	}
	return nil
}

func cloneIntRulesForRequest(rules *config.Int64Rules) *config.Int64Rules {
	if rules == nil {
		return nil
	}
	cloned := proto.Clone(rules).(*config.Int64Rules)
	cloned.SetValidateEmpty(true)
	return cloned
}

func cloneRepeatedStringRulesForRequest(rules *config.RepeatedStringRules) *config.RepeatedStringRules {
	if rules == nil {
		return nil
	}
	cloned := proto.Clone(rules).(*config.RepeatedStringRules)
	if cloned.HasItemRules() {
		cloned.GetItemRules().SetValidateEmpty(true)
	}
	return cloned
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
