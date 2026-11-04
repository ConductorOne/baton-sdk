package actions

import (
	"errors"
	"fmt"
	"slices"
	"time"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	resource "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/protobuf/types/known/structpb"
)

// Argument names for a credential-issue action. They follow the fields of
// IssueCredentialRequest and its credential options, so a connector moving off
// the RPC keeps its names.
const (
	ArgIdentityID = "identity_id"
	ArgExpiresAt  = "expires_at"
	ArgRequestID  = "request_id"
	ArgScopes     = "scopes"
	ArgAudiences  = "audiences"
	ArgProfile    = "profile"
)

// Return field names for a credential-issue action. The connector fills these
// in its public response; CredentialIssueResource rebuilds the secret resource
// from them, because a return field cannot carry the Resource's annotations and
// so cannot carry a SecretTrait (NewResourceReturnField drops it).
const (
	ReturnSecretResourceTypeID = "secret_resource_type_id"
	ReturnSecretResourceID     = "secret_resource_id"
	ReturnIdentityID           = "identity_id"
	ReturnExpiresAt            = "expires_at"
	ReturnResourceMode         = "resource_mode"
	ReturnRequestID            = "request_id"
)

// defaultCredentialReturnName labels the issued secret when the descriptor's
// output field leaves its name unset.
const defaultCredentialReturnName = "credential"

// standardReturnFields names the return fields CredentialIssueSchema declares
// itself, so a descriptor's output field cannot claim one of them.
var standardReturnFields = []string{
	ReturnSecretResourceTypeID,
	ReturnSecretResourceID,
	ReturnIdentityID,
	ReturnExpiresAt,
	ReturnResourceMode,
	ReturnRequestID,
}

// CredentialIssueSchema builds the schema for an action that mints a
// credential. The descriptor carries the advertised shape and the declared
// output type. Fill the return fields it declares, and pass
// CredentialIssueResource the response; it rebuilds the secret resource.
//
// It leaves resource_type_id unset and is registered with RegisterWithSecrets,
// not RegisterResourceActionWithSecrets: registration is scoped by resource
// type and allows one action per non-dynamic action type within a scope, so a
// scoped schema would permit only one issuer per secret resource type. The
// descriptor's secret_resource_type_id already says what the action mints.
//
// The schema is metadata only: the SDK does not check what a handler returns or
// whether the arguments it received are well formed. C1 does both.
func CredentialIssueSchema(name string, descriptor *v2.CredentialIssueOptionDescriptor) (*v2.BatonActionSchema, error) {
	if descriptor == nil {
		return nil, errors.New("credential issue schema: descriptor is required")
	}
	output := descriptor.GetOutput()
	if !output.GetIsSecret() {
		return nil, errors.New("credential issue schema: descriptor output must be declared secret")
	}
	if output.GetStringField() == nil {
		return nil, errors.New("credential issue schema: descriptor output must be a string field")
	}
	returnName := output.GetName()
	if returnName == "" {
		returnName = defaultCredentialReturnName
	}
	if slices.Contains(standardReturnFields, returnName) {
		return nil, fmt.Errorf("credential issue schema: output field name %q collides with a standard return field", returnName)
	}
	return v2.BatonActionSchema_builder{
		Name:            name,
		ActionType:      []v2.ActionType{v2.ActionType_ACTION_TYPE_CREDENTIAL_ISSUE},
		CredentialIssue: descriptor,
		Arguments:       credentialIssueArguments(descriptor),
		ReturnTypes: []*config.Field{
			config.Field_builder{
				Name:        ReturnSecretResourceTypeID,
				Description: "Resource type ID of the minted secret resource.",
				IsRequired:  true,
				StringField: config.StringField_builder{}.Build(),
			}.Build(),
			config.Field_builder{
				Name:        ReturnSecretResourceID,
				Description: "Resource ID of the minted secret resource.",
				IsRequired:  true,
				StringField: config.StringField_builder{}.Build(),
			}.Build(),
			config.Field_builder{
				Name:            ReturnIdentityID,
				Description:     "Echo of the identity the credential was minted for.",
				IsRequired:      true,
				ResourceIdField: config.ResourceIdField_builder{}.Build(),
			}.Build(),
			config.Field_builder{
				Name:        ReturnExpiresAt,
				Description: "Absolute expiry as an RFC 3339 timestamp.",
				StringField: config.StringField_builder{}.Build(),
			}.Build(),
			config.Field_builder{
				Name:        ReturnResourceMode,
				Description: "CredentialResourceMode enum name of the minted resource.",
				IsRequired:  true,
				StringField: config.StringField_builder{}.Build(),
			}.Build(),
			config.Field_builder{
				Name:        ReturnRequestID,
				Description: "Echo of the request ID the issuance was asked for.",
				IsRequired:  true,
				StringField: config.StringField_builder{}.Build(),
			}.Build(),
			config.Field_builder{
				Name:        returnName,
				ContentType: output.GetContentType(),
				Tags:        output.GetTags(),
				IsSecret:    true,
				IsRequired:  true,
				StringField: config.StringField_builder{}.Build(),
			}.Build(),
		},
	}.Build(), nil
}

// credentialIssueArguments returns the standard arguments, plus the option
// parameters the descriptor actually offers. An argument the descriptor cannot
// honour would invite a request the connector must refuse.
func credentialIssueArguments(descriptor *v2.CredentialIssueOptionDescriptor) []*config.Field {
	arguments := []*config.Field{
		config.Field_builder{
			Name:            ArgIdentityID,
			Description:     "The identity to mint the credential for.",
			IsRequired:      true,
			ResourceIdField: config.ResourceIdField_builder{}.Build(),
		}.Build(),
		config.Field_builder{
			Name:        ArgExpiresAt,
			Description: "Absolute expiry as an RFC 3339 timestamp.",
			StringField: config.StringField_builder{}.Build(),
		}.Build(),
		config.Field_builder{
			Name:        ArgRequestID,
			IsRequired:  true,
			StringField: config.StringField_builder{}.Build(),
		}.Build(),
	}
	if len(descriptor.GetScopes()) > 0 {
		arguments = append(arguments, config.Field_builder{
			Name:             ArgScopes,
			Description:      "Scopes the connector advertises for this credential.",
			StringSliceField: config.StringSliceField_builder{}.Build(),
		}.Build())
	}
	if len(descriptor.GetAudiences()) > 0 {
		arguments = append(arguments, config.Field_builder{
			Name:             ArgAudiences,
			Description:      "Audiences the connector advertises for this credential.",
			StringSliceField: config.StringSliceField_builder{}.Build(),
		}.Build())
	}
	if len(descriptor.GetKeyProfiles()) > 0 {
		arguments = append(arguments, config.Field_builder{
			Name:        ArgProfile,
			Description: "Key generation profile the connector advertises.",
			StringField: config.StringField_builder{}.Build(),
		}.Build())
	}
	return arguments
}

// CredentialIssueResource rebuilds the minted secret resource from an action's
// public response, including the SecretTrait the return fields cannot carry. It
// also returns the declared resource mode, so a caller can pass both straight to
// connectorbuilder.ValidateIssuedCredential.
//
// The plaintext secret is the separate secret return field and is not read
// here. This does not check request_id, scopes, audiences or the key profile
// against the request: the caller (C1) owns those checks, because only it knows
// what the issuance asked for.
func CredentialIssueResource(response *structpb.Struct) (*v2.Resource, v2.CredentialResourceMode, error) {
	fields := response.GetFields()
	secretTypeID, err := requiredStringReturn(fields, ReturnSecretResourceTypeID)
	if err != nil {
		return nil, v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_UNSPECIFIED, err
	}
	secretID, err := requiredStringReturn(fields, ReturnSecretResourceID)
	if err != nil {
		return nil, v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_UNSPECIFIED, err
	}
	identityID, err := identityIDReturn(fields)
	if err != nil {
		return nil, v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_UNSPECIFIED, err
	}
	modeName, err := requiredStringReturn(fields, ReturnResourceMode)
	if err != nil {
		return nil, v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_UNSPECIFIED, err
	}
	modeValue, ok := v2.CredentialResourceMode_value[modeName]
	if !ok {
		return nil, v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_UNSPECIFIED,
			fmt.Errorf("credential issue response: %s %q is not a CredentialResourceMode", ReturnResourceMode, modeName)
	}
	mode := v2.CredentialResourceMode(modeValue)

	traitOptions := []resource.SecretTraitOption{resource.WithSecretIdentityID(identityID)}
	expiresAt, ok := fields[ReturnExpiresAt].GetStringValue(), fields[ReturnExpiresAt] != nil
	if ok && expiresAt != "" {
		parsed, err := parseTimestamp(expiresAt)
		if err != nil {
			return nil, v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_UNSPECIFIED,
				fmt.Errorf("credential issue response: %s: %w", ReturnExpiresAt, err)
		}
		traitOptions = append(traitOptions, resource.WithSecretExpiresAt(parsed))
	}

	secret, err := resource.NewSecretResource(
		secretID,
		v2.ResourceType_builder{Id: secretTypeID}.Build(),
		secretID,
		traitOptions,
	)
	if err != nil {
		return nil, v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_UNSPECIFIED,
			fmt.Errorf("credential issue response: build secret resource: %w", err)
	}
	return secret, mode, nil
}

func requiredStringReturn(fields map[string]*structpb.Value, name string) (string, error) {
	value, ok := fields[name]
	if !ok {
		return "", fmt.Errorf("credential issue response: %s is required", name)
	}
	text := value.GetStringValue()
	if text == "" {
		return "", fmt.Errorf("credential issue response: %s is required", name)
	}
	return text, nil
}

// identityIDReturn reads the ResourceIdField the schema declares for the echoed
// identity, which arrives as an object rather than a string.
func identityIDReturn(fields map[string]*structpb.Value) (*v2.ResourceId, error) {
	value, ok := fields[ReturnIdentityID]
	if !ok || value.GetStructValue() == nil {
		return nil, fmt.Errorf("credential issue response: %s is required", ReturnIdentityID)
	}
	identity := value.GetStructValue().GetFields()
	resourceTypeID := identity["resource_type_id"].GetStringValue()
	resourceID := identity["resource_id"].GetStringValue()
	if resourceTypeID == "" || resourceID == "" {
		return nil, fmt.Errorf("credential issue response: %s needs resource_type_id and resource_id", ReturnIdentityID)
	}
	return v2.ResourceId_builder{ResourceType: resourceTypeID, Resource: resourceID}.Build(), nil
}

func parseTimestamp(text string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, text)
	if err != nil {
		return time.Time{}, fmt.Errorf("expected an RFC 3339 timestamp: %w", err)
	}
	return parsed, nil
}
