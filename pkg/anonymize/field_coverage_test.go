package anonymize

import (
	"fmt"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// fieldPolicy defines how a field should be handled during anonymization.
type fieldPolicy int

const (
	// fieldPolicyAnonymize indicates the field contains PII and is anonymized.
	fieldPolicyAnonymize fieldPolicy = iota
	// fieldPolicySafe indicates the field does not contain PII (enums, timestamps, bools, etc).
	fieldPolicySafe
	// fieldPolicyRecurse indicates the field is a nested message with its own anonymization.
	fieldPolicyRecurse
	// fieldPolicyClear indicates the field is cleared entirely (e.g., Profile structs).
	fieldPolicyClear
)

// resourceFieldPolicies defines the anonymization policy for Resource fields.
var resourceFieldPolicies = map[string]fieldPolicy{
	"id":                 fieldPolicyAnonymize, // ResourceId - resource part anonymized
	"parent_resource_id": fieldPolicyAnonymize, // ResourceId - resource part anonymized
	"display_name":       fieldPolicyAnonymize, // User/group/app names
	"annotations":        fieldPolicyRecurse,   // Contains traits that are anonymized
	"description":        fieldPolicyAnonymize, // May contain identifying info
	"baton_resource":     fieldPolicySafe,      // Boolean flag
	"external_id":        fieldPolicyAnonymize, // ExternalId - all parts anonymized
	"creation_source":    fieldPolicySafe,      // Enum value
}

// resourceIdFieldPolicies defines the anonymization policy for ResourceId fields.
var resourceIdFieldPolicies = map[string]fieldPolicy{
	"resource_type":  fieldPolicyAnonymize, // Anonymized consistently with ResourceType.id
	"resource":       fieldPolicyAnonymize, // May be email, username, etc.
	"baton_resource": fieldPolicySafe,      // Boolean flag indicating if this is a baton-managed resource
}

// externalIdFieldPolicies defines the anonymization policy for ExternalId fields.
var externalIdFieldPolicies = map[string]fieldPolicy{
	"id":          fieldPolicyAnonymize, // External identifier
	"link":        fieldPolicyAnonymize, // URL that may identify org
	"description": fieldPolicyAnonymize, // May contain identifying info
}

// userTraitFieldPolicies defines the anonymization policy for UserTrait fields.
var userTraitFieldPolicies = map[string]fieldPolicy{
	"emails":          fieldPolicyAnonymize, // Email addresses - primary PII
	"status":          fieldPolicyRecurse,   // Nested struct with anonymized details field
	"profile":         fieldPolicyClear,     // Arbitrary data - cleared
	"icon":            fieldPolicyClear,     // AssetRef - cleared (profile pictures are identifying)
	"account_type":    fieldPolicySafe,      // Enum value
	"login":           fieldPolicyAnonymize, // Username/login
	"login_aliases":   fieldPolicyAnonymize, // Additional logins
	"employee_ids":    fieldPolicyAnonymize, // Employee identifiers
	"created_at":      fieldPolicyAnonymize, // Timestamp - set to single anonymized timestamp
	"last_login":      fieldPolicyAnonymize, // Timestamp - set to single anonymized timestamp
	"mfa_status":      fieldPolicySafe,      // MFA status struct (no PII)
	"sso_status":      fieldPolicySafe,      // SSO status struct (no PII)
	"structured_name": fieldPolicyAnonymize, // Given/family/middle names
}

// userTraitEmailFieldPolicies defines the anonymization policy for UserTrait.Email fields.
var userTraitEmailFieldPolicies = map[string]fieldPolicy{
	"address":    fieldPolicyAnonymize, // Email address
	"is_primary": fieldPolicySafe,      // Boolean flag
}

// userTraitStructuredNameFieldPolicies defines the anonymization policy for UserTrait.StructuredName fields.
var userTraitStructuredNameFieldPolicies = map[string]fieldPolicy{
	"given_name":   fieldPolicyAnonymize, // First name
	"family_name":  fieldPolicyAnonymize, // Last name
	"middle_names": fieldPolicyAnonymize, // Middle names
	"prefix":       fieldPolicyClear,     // Title like "Mr.", "Dr." - can be identifying
	"suffix":       fieldPolicyClear,     // Suffix like "Jr.", "III" - can be identifying
}

// userTraitStatusFieldPolicies defines the anonymization policy for UserTrait.Status fields.
// NOTE: The "details" field could potentially contain PII in some implementations.
// If your connector puts PII in status details, update this policy and the anonymization code.
var userTraitStatusFieldPolicies = map[string]fieldPolicy{
	"status":  fieldPolicySafe,      // Enum value (ENABLED, DISABLED, etc.)
	"details": fieldPolicyAnonymize, // Status details - may contain identifying info
}

// userTraitMFAStatusFieldPolicies defines the anonymization policy for UserTrait.MFAStatus fields.
var userTraitMFAStatusFieldPolicies = map[string]fieldPolicy{
	"mfa_enabled": fieldPolicySafe, // Boolean flag
}

// userTraitSSOStatusFieldPolicies defines the anonymization policy for UserTrait.SSOStatus fields.
var userTraitSSOStatusFieldPolicies = map[string]fieldPolicy{
	"sso_enabled": fieldPolicySafe, // Boolean flag
}

// groupTraitFieldPolicies defines the anonymization policy for GroupTrait fields.
var groupTraitFieldPolicies = map[string]fieldPolicy{
	"icon":    fieldPolicyClear, // AssetRef - cleared (profile pictures are identifying)
	"profile": fieldPolicyClear, // Arbitrary data - cleared
}

// roleTraitFieldPolicies defines the anonymization policy for RoleTrait fields.
var roleTraitFieldPolicies = map[string]fieldPolicy{
	"profile": fieldPolicyClear, // Arbitrary data - cleared
}

// appTraitFieldPolicies defines the anonymization policy for AppTrait fields.
var appTraitFieldPolicies = map[string]fieldPolicy{
	"help_url": fieldPolicyAnonymize, // URL that may identify org
	"icon":     fieldPolicyClear,     // AssetRef - cleared (profile pictures are identifying)
	"logo":     fieldPolicyClear,     // AssetRef - cleared (logos are identifying)
	"profile":  fieldPolicyClear,     // Arbitrary data - cleared
	"flags":    fieldPolicySafe,      // Enum flags
}

// secretTraitFieldPolicies defines the anonymization policy for SecretTrait fields.
var secretTraitFieldPolicies = map[string]fieldPolicy{
	"profile":       fieldPolicyClear,     // Arbitrary data - cleared
	"created_at":    fieldPolicyClear,     // Timestamp - cleared (not used by SDK, avoids fingerprinting)
	"expires_at":    fieldPolicyClear,     // Timestamp - cleared (not used by SDK, avoids fingerprinting)
	"last_used_at":  fieldPolicyClear,     // Timestamp - cleared (not used by SDK, avoids fingerprinting)
	"created_by_id": fieldPolicyAnonymize, // ResourceId - may identify user
	"identity_id":   fieldPolicyAnonymize, // ResourceId - may identify user
}

// entitlementFieldPolicies defines the anonymization policy for Entitlement fields.
var entitlementFieldPolicies = map[string]fieldPolicy{
	"resource":     fieldPolicyRecurse,   // Embedded Resource - anonymized recursively
	"id":           fieldPolicyAnonymize, // Entitlement ID
	"display_name": fieldPolicyAnonymize, // May contain identifying info
	"description":  fieldPolicyAnonymize, // May contain identifying info
	"grantable_to": fieldPolicyRecurse,   // Contains ResourceTypes that must be anonymized
	"annotations":  fieldPolicyRecurse,   // Contains EntitlementImmutable, ExternalLink - processed
	"purpose":      fieldPolicySafe,      // Enum value
	"slug":         fieldPolicyAnonymize, // May contain identifying info
}

// grantFieldPolicies defines the anonymization policy for Grant fields.
var grantFieldPolicies = map[string]fieldPolicy{
	"entitlement": fieldPolicyRecurse,   // Embedded Entitlement - anonymized recursively
	"principal":   fieldPolicyRecurse,   // Embedded Resource - anonymized recursively
	"id":          fieldPolicyAnonymize, // Grant ID
	"sources":     fieldPolicyAnonymize, // GrantSources - keys may be identifiers
	"annotations": fieldPolicyRecurse,   // Contains GrantExpandable, GrantMetadata, etc. - processed
}

// resourceTypeFieldPolicies defines the anonymization policy for ResourceType fields.
var resourceTypeFieldPolicies = map[string]fieldPolicy{
	"id":                 fieldPolicyAnonymize, // Type ID (optionally preserved)
	"display_name":       fieldPolicyAnonymize, // Type name (optionally preserved)
	"traits":             fieldPolicySafe,      // Enum values
	"annotations":        fieldPolicyRecurse,   // Contains ChildResourceType, ExternalLink - processed
	"description":        fieldPolicyAnonymize, // May contain identifying info
	"sourced_externally": fieldPolicySafe,      // Boolean flag
}

// assetRefFieldPolicies defines the anonymization policy for AssetRef fields.
// AssetRef is used for icon/logo references in traits - these are cleared as icons/logos are identifying.
var assetRefFieldPolicies = map[string]fieldPolicy{
	"id": fieldPolicyClear, // Cleared - icon/logo references are identifying
}

// grantSourcesFieldPolicies defines the anonymization policy for GrantSources fields.
var grantSourcesFieldPolicies = map[string]fieldPolicy{
	"sources": fieldPolicyAnonymize, // Map keys may be identifiers
}

// grantSourcesGrantSourceFieldPolicies defines the anonymization policy for GrantSources.GrantSource fields.
// Note: This message currently has no fields, but coverage ensures future fields are handled.
var grantSourcesGrantSourceFieldPolicies = map[string]fieldPolicy{
	// Currently empty - no fields in GrantSources_GrantSource
}

// grantExpandableFieldPolicies defines the anonymization policy for GrantExpandable annotation fields.
var grantExpandableFieldPolicies = map[string]fieldPolicy{
	"entitlement_ids":   fieldPolicyAnonymize, // Must match anonymized entitlement IDs
	"shallow":           fieldPolicySafe,      // Boolean flag
	"resource_type_ids": fieldPolicyAnonymize, // Must match anonymized resource type IDs
}

// grantMetadataFieldPolicies defines the anonymization policy for GrantMetadata annotation fields.
var grantMetadataFieldPolicies = map[string]fieldPolicy{
	"metadata": fieldPolicyClear, // Arbitrary data - cleared
}

// grantImmutableFieldPolicies defines the anonymization policy for GrantImmutable annotation fields.
var grantImmutableFieldPolicies = map[string]fieldPolicy{
	"source_id": fieldPolicyAnonymize, // Source identifier - anonymized
	"metadata":  fieldPolicyClear,     // Arbitrary data - cleared
}

// entitlementImmutableFieldPolicies defines the anonymization policy for EntitlementImmutable annotation fields.
var entitlementImmutableFieldPolicies = map[string]fieldPolicy{
	"source_id": fieldPolicyAnonymize, // Source identifier - anonymized
	"metadata":  fieldPolicyClear,     // Arbitrary data - cleared
}

// externalLinkFieldPolicies defines the anonymization policy for ExternalLink annotation fields.
var externalLinkFieldPolicies = map[string]fieldPolicy{
	"url": fieldPolicyAnonymize, // URLs can identify orgs - anonymized
}

// childResourceTypeFieldPolicies defines the anonymization policy for ChildResourceType annotation fields.
var childResourceTypeFieldPolicies = map[string]fieldPolicy{
	"resource_type_id": fieldPolicyAnonymize, // Must match anonymized resource type IDs
}

// validateFieldCoverage checks that all fields in a message type are accounted for
// in the policy map. Returns an error if any fields are not handled.
func validateFieldCoverage(msg proto.Message, policies map[string]fieldPolicy) error {
	md := msg.ProtoReflect().Descriptor()
	fields := md.Fields()

	var unhandledFields []string

	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		fieldName := string(field.Name())

		if _, ok := policies[fieldName]; !ok {
			unhandledFields = append(unhandledFields, fieldName)
		}
	}

	if len(unhandledFields) > 0 {
		return fmt.Errorf(
			"unhandled fields in %s: [%s] - these must be explicitly added to the field policy map with appropriate anonymization handling",
			md.FullName(),
			strings.Join(unhandledFields, ", "),
		)
	}

	return nil
}

// validateNoPolicyForMissingFields checks that the policy map doesn't have entries
// for fields that don't exist in the message (catches typos and removed fields).
func validateNoPolicyForMissingFields(msg proto.Message, policies map[string]fieldPolicy) error {
	md := msg.ProtoReflect().Descriptor()
	fields := md.Fields()

	// Build a set of actual field names
	actualFields := make(map[string]bool)
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		actualFields[string(field.Name())] = true
	}

	var extraPolicies []string
	for policyField := range policies {
		if !actualFields[policyField] {
			extraPolicies = append(extraPolicies, policyField)
		}
	}

	if len(extraPolicies) > 0 {
		return fmt.Errorf(
			"policy entries for non-existent fields in %s: [%s] - these fields may have been removed or renamed",
			md.FullName(),
			strings.Join(extraPolicies, ", "),
		)
	}

	return nil
}

// TestFieldCoverage_Resource ensures all Resource fields are handled.
func TestFieldCoverage_Resource(t *testing.T) {
	err := validateFieldCoverage(&v2.Resource{}, resourceFieldPolicies)
	require.NoError(t, err, "All Resource fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.Resource{}, resourceFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_ResourceId ensures all ResourceId fields are handled.
func TestFieldCoverage_ResourceId(t *testing.T) {
	err := validateFieldCoverage(&v2.ResourceId{}, resourceIdFieldPolicies)
	require.NoError(t, err, "All ResourceId fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.ResourceId{}, resourceIdFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_ExternalId ensures all ExternalId fields are handled.
func TestFieldCoverage_ExternalId(t *testing.T) {
	err := validateFieldCoverage(&v2.ExternalId{}, externalIdFieldPolicies)
	require.NoError(t, err, "All ExternalId fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.ExternalId{}, externalIdFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_UserTrait ensures all UserTrait fields are handled.
func TestFieldCoverage_UserTrait(t *testing.T) {
	err := validateFieldCoverage(&v2.UserTrait{}, userTraitFieldPolicies)
	require.NoError(t, err, "All UserTrait fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.UserTrait{}, userTraitFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_UserTraitEmail ensures all UserTrait.Email fields are handled.
func TestFieldCoverage_UserTraitEmail(t *testing.T) {
	err := validateFieldCoverage(&v2.UserTrait_Email{}, userTraitEmailFieldPolicies)
	require.NoError(t, err, "All UserTrait.Email fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.UserTrait_Email{}, userTraitEmailFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_UserTraitStructuredName ensures all UserTrait.StructuredName fields are handled.
func TestFieldCoverage_UserTraitStructuredName(t *testing.T) {
	err := validateFieldCoverage(&v2.UserTrait_StructuredName{}, userTraitStructuredNameFieldPolicies)
	require.NoError(t, err, "All UserTrait.StructuredName fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.UserTrait_StructuredName{}, userTraitStructuredNameFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_UserTraitStatus ensures all UserTrait.Status fields are handled.
func TestFieldCoverage_UserTraitStatus(t *testing.T) {
	err := validateFieldCoverage(&v2.UserTrait_Status{}, userTraitStatusFieldPolicies)
	require.NoError(t, err, "All UserTrait.Status fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.UserTrait_Status{}, userTraitStatusFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_UserTraitMFAStatus ensures all UserTrait.MFAStatus fields are handled.
func TestFieldCoverage_UserTraitMFAStatus(t *testing.T) {
	err := validateFieldCoverage(&v2.UserTrait_MFAStatus{}, userTraitMFAStatusFieldPolicies)
	require.NoError(t, err, "All UserTrait.MFAStatus fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.UserTrait_MFAStatus{}, userTraitMFAStatusFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_UserTraitSSOStatus ensures all UserTrait.SSOStatus fields are handled.
func TestFieldCoverage_UserTraitSSOStatus(t *testing.T) {
	err := validateFieldCoverage(&v2.UserTrait_SSOStatus{}, userTraitSSOStatusFieldPolicies)
	require.NoError(t, err, "All UserTrait.SSOStatus fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.UserTrait_SSOStatus{}, userTraitSSOStatusFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_GroupTrait ensures all GroupTrait fields are handled.
func TestFieldCoverage_GroupTrait(t *testing.T) {
	err := validateFieldCoverage(&v2.GroupTrait{}, groupTraitFieldPolicies)
	require.NoError(t, err, "All GroupTrait fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.GroupTrait{}, groupTraitFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_RoleTrait ensures all RoleTrait fields are handled.
func TestFieldCoverage_RoleTrait(t *testing.T) {
	err := validateFieldCoverage(&v2.RoleTrait{}, roleTraitFieldPolicies)
	require.NoError(t, err, "All RoleTrait fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.RoleTrait{}, roleTraitFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_AppTrait ensures all AppTrait fields are handled.
func TestFieldCoverage_AppTrait(t *testing.T) {
	err := validateFieldCoverage(&v2.AppTrait{}, appTraitFieldPolicies)
	require.NoError(t, err, "All AppTrait fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.AppTrait{}, appTraitFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_SecretTrait ensures all SecretTrait fields are handled.
func TestFieldCoverage_SecretTrait(t *testing.T) {
	err := validateFieldCoverage(&v2.SecretTrait{}, secretTraitFieldPolicies)
	require.NoError(t, err, "All SecretTrait fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.SecretTrait{}, secretTraitFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_Entitlement ensures all Entitlement fields are handled.
func TestFieldCoverage_Entitlement(t *testing.T) {
	err := validateFieldCoverage(&v2.Entitlement{}, entitlementFieldPolicies)
	require.NoError(t, err, "All Entitlement fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.Entitlement{}, entitlementFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_Grant ensures all Grant fields are handled.
func TestFieldCoverage_Grant(t *testing.T) {
	err := validateFieldCoverage(&v2.Grant{}, grantFieldPolicies)
	require.NoError(t, err, "All Grant fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.Grant{}, grantFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_ResourceType ensures all ResourceType fields are handled.
func TestFieldCoverage_ResourceType(t *testing.T) {
	err := validateFieldCoverage(&v2.ResourceType{}, resourceTypeFieldPolicies)
	require.NoError(t, err, "All ResourceType fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.ResourceType{}, resourceTypeFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_AssetRef ensures all AssetRef fields are handled.
func TestFieldCoverage_AssetRef(t *testing.T) {
	err := validateFieldCoverage(&v2.AssetRef{}, assetRefFieldPolicies)
	require.NoError(t, err, "All AssetRef fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.AssetRef{}, assetRefFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_GrantSources ensures all GrantSources fields are handled.
func TestFieldCoverage_GrantSources(t *testing.T) {
	err := validateFieldCoverage(&v2.GrantSources{}, grantSourcesFieldPolicies)
	require.NoError(t, err, "All GrantSources fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.GrantSources{}, grantSourcesFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_GrantSourcesGrantSource ensures all GrantSources.GrantSource fields are handled.
func TestFieldCoverage_GrantSourcesGrantSource(t *testing.T) {
	err := validateFieldCoverage(&v2.GrantSources_GrantSource{}, grantSourcesGrantSourceFieldPolicies)
	require.NoError(t, err, "All GrantSources.GrantSource fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.GrantSources_GrantSource{}, grantSourcesGrantSourceFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_GrantExpandable ensures all GrantExpandable annotation fields are handled.
func TestFieldCoverage_GrantExpandable(t *testing.T) {
	err := validateFieldCoverage(&v2.GrantExpandable{}, grantExpandableFieldPolicies)
	require.NoError(t, err, "All GrantExpandable fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.GrantExpandable{}, grantExpandableFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_GrantMetadata ensures all GrantMetadata annotation fields are handled.
func TestFieldCoverage_GrantMetadata(t *testing.T) {
	err := validateFieldCoverage(&v2.GrantMetadata{}, grantMetadataFieldPolicies)
	require.NoError(t, err, "All GrantMetadata fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.GrantMetadata{}, grantMetadataFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_GrantImmutable ensures all GrantImmutable annotation fields are handled.
func TestFieldCoverage_GrantImmutable(t *testing.T) {
	err := validateFieldCoverage(&v2.GrantImmutable{}, grantImmutableFieldPolicies)
	require.NoError(t, err, "All GrantImmutable fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.GrantImmutable{}, grantImmutableFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_EntitlementImmutable ensures all EntitlementImmutable annotation fields are handled.
func TestFieldCoverage_EntitlementImmutable(t *testing.T) {
	err := validateFieldCoverage(&v2.EntitlementImmutable{}, entitlementImmutableFieldPolicies)
	require.NoError(t, err, "All EntitlementImmutable fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.EntitlementImmutable{}, entitlementImmutableFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_ExternalLink ensures all ExternalLink annotation fields are handled.
func TestFieldCoverage_ExternalLink(t *testing.T) {
	err := validateFieldCoverage(&v2.ExternalLink{}, externalLinkFieldPolicies)
	require.NoError(t, err, "All ExternalLink fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.ExternalLink{}, externalLinkFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_ChildResourceType ensures all ChildResourceType annotation fields are handled.
func TestFieldCoverage_ChildResourceType(t *testing.T) {
	err := validateFieldCoverage(&v2.ChildResourceType{}, childResourceTypeFieldPolicies)
	require.NoError(t, err, "All ChildResourceType fields must have an anonymization policy")

	err = validateNoPolicyForMissingFields(&v2.ChildResourceType{}, childResourceTypeFieldPolicies)
	require.NoError(t, err, "Policy should not reference non-existent fields")
}

// TestFieldCoverage_AllTypes runs all field coverage tests as a single test for CI visibility.
func TestFieldCoverage_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		msg      proto.Message
		policies map[string]fieldPolicy
	}{
		{"Resource", &v2.Resource{}, resourceFieldPolicies},
		{"ResourceId", &v2.ResourceId{}, resourceIdFieldPolicies},
		{"ExternalId", &v2.ExternalId{}, externalIdFieldPolicies},
		{"UserTrait", &v2.UserTrait{}, userTraitFieldPolicies},
		{"UserTrait_Email", &v2.UserTrait_Email{}, userTraitEmailFieldPolicies},
		{"UserTrait_StructuredName", &v2.UserTrait_StructuredName{}, userTraitStructuredNameFieldPolicies},
		{"UserTrait_Status", &v2.UserTrait_Status{}, userTraitStatusFieldPolicies},
		{"UserTrait_MFAStatus", &v2.UserTrait_MFAStatus{}, userTraitMFAStatusFieldPolicies},
		{"UserTrait_SSOStatus", &v2.UserTrait_SSOStatus{}, userTraitSSOStatusFieldPolicies},
		{"GroupTrait", &v2.GroupTrait{}, groupTraitFieldPolicies},
		{"RoleTrait", &v2.RoleTrait{}, roleTraitFieldPolicies},
		{"AppTrait", &v2.AppTrait{}, appTraitFieldPolicies},
		{"SecretTrait", &v2.SecretTrait{}, secretTraitFieldPolicies},
		{"Entitlement", &v2.Entitlement{}, entitlementFieldPolicies},
		{"Grant", &v2.Grant{}, grantFieldPolicies},
		{"ResourceType", &v2.ResourceType{}, resourceTypeFieldPolicies},
		{"AssetRef", &v2.AssetRef{}, assetRefFieldPolicies},
		{"GrantSources", &v2.GrantSources{}, grantSourcesFieldPolicies},
		{"GrantSources_GrantSource", &v2.GrantSources_GrantSource{}, grantSourcesGrantSourceFieldPolicies},
		// Annotation types
		{"GrantExpandable", &v2.GrantExpandable{}, grantExpandableFieldPolicies},
		{"GrantMetadata", &v2.GrantMetadata{}, grantMetadataFieldPolicies},
		{"GrantImmutable", &v2.GrantImmutable{}, grantImmutableFieldPolicies},
		{"EntitlementImmutable", &v2.EntitlementImmutable{}, entitlementImmutableFieldPolicies},
		{"ExternalLink", &v2.ExternalLink{}, externalLinkFieldPolicies},
		{"ChildResourceType", &v2.ChildResourceType{}, childResourceTypeFieldPolicies},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFieldCoverage(tt.msg, tt.policies)
			require.NoError(t, err, "All %s fields must have an anonymization policy", tt.name)

			err = validateNoPolicyForMissingFields(tt.msg, tt.policies)
			require.NoError(t, err, "Policy for %s should not reference non-existent fields", tt.name)
		})
	}
}

// fieldPolicyToString returns a human-readable string for a field policy.
func fieldPolicyToString(fp fieldPolicy) string {
	switch fp {
	case fieldPolicyAnonymize:
		return "ANONYMIZE"
	case fieldPolicySafe:
		return "SAFE"
	case fieldPolicyRecurse:
		return "RECURSE"
	case fieldPolicyClear:
		return "CLEAR"
	default:
		return "UNKNOWN"
	}
}

// getAnonymizedFields returns a list of fields that are marked for anonymization.
func getAnonymizedFields(policies map[string]fieldPolicy) []string {
	var fields []string
	for field, policy := range policies {
		if policy == fieldPolicyAnonymize {
			fields = append(fields, field)
		}
	}
	return fields
}

// TestFieldPolicy_UserTraitHasPIIFields verifies that UserTrait has expected PII fields marked.
func TestFieldPolicy_UserTraitHasPIIFields(t *testing.T) {
	anonymizedFields := getAnonymizedFields(userTraitFieldPolicies)

	// These fields MUST be anonymized as they contain PII
	requiredAnonymizedFields := []string{
		"emails",
		"login",
		"login_aliases",
		"employee_ids",
		"structured_name",
	}

	for _, required := range requiredAnonymizedFields {
		found := false
		for _, actual := range anonymizedFields {
			if actual == required {
				found = true
				break
			}
		}
		require.True(t, found, "UserTrait field %q must be marked for anonymization", required)
	}
}

// TestFieldPolicy_ProfileFieldsAreCleared verifies that profile fields are cleared.
func TestFieldPolicy_ProfileFieldsAreCleared(t *testing.T) {
	tests := []struct {
		name     string
		policies map[string]fieldPolicy
	}{
		{"UserTrait", userTraitFieldPolicies},
		{"GroupTrait", groupTraitFieldPolicies},
		{"RoleTrait", roleTraitFieldPolicies},
		{"AppTrait", appTraitFieldPolicies},
		{"SecretTrait", secretTraitFieldPolicies},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy, ok := tt.policies["profile"]
			require.True(t, ok, "%s should have a policy for 'profile' field", tt.name)
			require.Equal(t, fieldPolicyClear, policy, "%s profile field should be CLEARED, not %s", tt.name, fieldPolicyToString(policy))
		})
	}
}

// knownNestedMessageTypes lists all nested message types that must have field coverage.
// If a new nested message type is added to the protobufs, add it here AND create a policy map.
// This ensures new nested types can't be added without explicit anonymization handling.
var knownNestedMessageTypes = map[string]bool{
	"c1.connector.v2.UserTrait.Email":          true,
	"c1.connector.v2.UserTrait.StructuredName": true,
	"c1.connector.v2.UserTrait.Status":         true,
	"c1.connector.v2.UserTrait.MFAStatus":      true,
	"c1.connector.v2.UserTrait.SSOStatus":      true,
	"c1.connector.v2.AssetRef":                 true,
	"c1.connector.v2.ResourceId":               true,
	"c1.connector.v2.ExternalId":               true,
	"c1.connector.v2.ResourceType":             true,
	"c1.connector.v2.Resource":                 true,
	"c1.connector.v2.Entitlement":              true,
	"c1.connector.v2.GrantSources":             true,
	"c1.connector.v2.GrantSources.GrantSource": true,
	// Well-known Google types that don't need anonymization policies
	"google.protobuf.Any":       true, // Handled via annotation processing
	"google.protobuf.Struct":    true, // Cleared as "profile" fields
	"google.protobuf.Timestamp": true, // Cleared in implementation
}

// parentTypesWithNestedMessages lists parent message types that may contain nested messages.
// These are the types we anonymize that could have new nested message fields added.
var parentTypesWithNestedMessages = []proto.Message{
	&v2.Resource{},
	&v2.ResourceType{},
	&v2.Entitlement{},
	&v2.Grant{},
	&v2.UserTrait{},
	&v2.GroupTrait{},
	&v2.RoleTrait{},
	&v2.AppTrait{},
	&v2.SecretTrait{},
}

// TestNestedTypeCoverage ensures all nested message types have field coverage.
// This test will fail if a new nested message type is added to the protobufs
// without being added to knownNestedMessageTypes and having a policy map created.
func TestNestedTypeCoverage(t *testing.T) {
	var unknownTypes []string

	for _, parent := range parentTypesWithNestedMessages {
		md := parent.ProtoReflect().Descriptor()
		fields := md.Fields()

		for i := 0; i < fields.Len(); i++ {
			field := fields.Get(i)

			// Check if this field is a message type
			if field.Kind().String() == "message" {
				msgName := string(field.Message().FullName())

				if !knownNestedMessageTypes[msgName] {
					unknownTypes = append(unknownTypes,
						fmt.Sprintf("%s.%s -> %s", md.FullName(), field.Name(), msgName))
				}
			}
		}
	}

	if len(unknownTypes) > 0 {
		t.Errorf("Unknown nested message types found - add them to knownNestedMessageTypes AND create field policy maps:\n  %s",
			strings.Join(unknownTypes, "\n  "))
	}
}
