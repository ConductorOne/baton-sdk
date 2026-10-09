package connectorbuilder

import (
	"fmt"
	"regexp"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/field"
	"google.golang.org/protobuf/proto"
)

const nonblankScopePattern = `[^[:space:]\x{0085}\x{00A0}\x{1680}\x{2000}-\x{200A}\x{2028}\x{2029}\x{202F}\x{205F}\x{3000}]`

// CredentialIssueScopeField resolves an explicit scope field or the legacy
// scopes/custom_scopes_allowed contract into shared field rules. It returns a
// copy; callers may inspect it without modifying advertised metadata.
func CredentialIssueScopeField(descriptor *v2.CredentialIssueOptionDescriptor) (*config.Field, error) {
	if descriptor == nil {
		return nil, fmt.Errorf("credential issue descriptor is required")
	}
	var scopeField *config.Field
	for _, inputField := range descriptor.GetInputFields() {
		if inputField == nil || inputField.GetName() != "scopes" {
			return nil, fmt.Errorf("credential issue input field must be named scopes")
		}
		if scopeField != nil {
			return nil, fmt.Errorf("duplicate credential issue input field scopes")
		}
		if inputField.WhichField() != config.Field_StringSliceField_case || inputField.GetStringSliceField() == nil {
			return nil, fmt.Errorf("credential issue scopes field must be a string slice")
		}
		scopeField = inputField
	}
	option := descriptor.GetOption()
	if option != v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY &&
		option != v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_TOKEN {
		if scopeField != nil {
			return nil, fmt.Errorf("credential issue option does not support scopes input fields")
		}
		return nil, nil
	}
	if scopeField == nil {
		rules := config.RepeatedStringRules_builder{
			Unique:    true,
			ItemRules: config.StringRules_builder{ValidateEmpty: true, Pattern: proto.String(nonblankScopePattern)}.Build(),
		}.Build()
		if !descriptor.GetCustomScopesAllowed() {
			if len(descriptor.GetScopes()) == 0 {
				rules.SetMaxItems(0)
			} else {
				rules.GetItemRules().SetIn(append([]string(nil), descriptor.GetScopes()...))
			}
		}
		return config.Field_builder{
			Name: "scopes", StringSliceField: config.StringSliceField_builder{Rules: rules}.Build(),
		}.Build(), nil
	}
	resolved := proto.Clone(scopeField).(*config.Field)
	rules := resolved.GetStringSliceField().GetRules()
	if rules == nil {
		rules = &config.RepeatedStringRules{}
		resolved.GetStringSliceField().SetRules(rules)
	}
	if resolved.GetIsRequired() {
		rules.SetIsRequired(true)
	}
	if err := validateCredentialIssueScopeRules(rules); err != nil {
		return nil, fmt.Errorf("invalid credential issue scopes field: %w", err)
	}
	return resolved, nil
}

func validateCredentialIssueScopeRules(rules *config.RepeatedStringRules) error {
	minimum := rules.GetMinItems()
	if rules.GetIsRequired() && minimum == 0 {
		minimum = 1
	}
	if rules.HasMaxItems() && minimum > rules.GetMaxItems() {
		return fmt.Errorf("minimum items exceeds maximum items")
	}
	itemRules := rules.GetItemRules()
	if itemRules.HasPattern() {
		if _, err := regexp.CompilePOSIX(itemRules.GetPattern()); err != nil {
			return fmt.Errorf("invalid item pattern: %w", err)
		}
	}
	if itemRules.HasMinLen() && itemRules.HasMaxLen() && itemRules.GetMinLen() > itemRules.GetMaxLen() {
		return fmt.Errorf("minimum item length exceeds maximum item length")
	}
	if itemRules.HasLen() && ((itemRules.HasMinLen() && itemRules.GetLen() < itemRules.GetMinLen()) ||
		(itemRules.HasMaxLen() && itemRules.GetLen() > itemRules.GetMaxLen())) {
		return fmt.Errorf("item length conflicts with minimum or maximum length")
	}
	if len(itemRules.GetIn()) != 0 {
		allowed := make(map[string]struct{}, len(itemRules.GetIn()))
		for _, value := range itemRules.GetIn() {
			if field.ValidateStringRules(itemRules, value, "scopes") == nil {
				allowed[value] = struct{}{}
			}
		}
		if minimum > 0 && (len(allowed) == 0 || (rules.GetUnique() && minimum > uint64(len(allowed)))) {
			return fmt.Errorf("minimum items exceeds available allowed scope values")
		}
	}
	return nil
}
