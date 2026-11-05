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

// Argument names the SDK fixes on a credential-issue action. The SDK declares
// only what C1 itself interprets; everything else is the connector's to name.
const (
	ArgIdentityID = "identity_id"
	ArgExpiresAt  = "expires_at"
	ArgRequestID  = "request_id"
)

// Return field names the SDK fixes on a credential-issue action. The connector
// fills these in its public response; CredentialIssueResource rebuilds the
// secret resource from them, because a return field cannot carry the Resource's
// annotations and so cannot carry a SecretTrait (NewResourceReturnField drops
// it). C1 has the secret resource type, the identity, the mode and the request
// ID already, so they are not returned.
const (
	ReturnSecretResourceID = "secret_resource_id"
	ReturnExpiresAt        = "expires_at"
)

// defaultCredentialReturnName labels the issued secret when the descriptor's
// output field leaves its name unset.
const defaultCredentialReturnName = "credential"

// fixedArgumentNames and standardReturnFields name what the SDK declares, so a
// caller-supplied argument or the descriptor's output field cannot claim one.
var fixedArgumentNames = []string{ArgIdentityID, ArgRequestID, ArgExpiresAt}

var standardReturnFields = []string{ReturnSecretResourceID, ReturnExpiresAt}

// CredentialIssueSchema builds the schema for an action that mints a
// credential. The descriptor carries the advertised shape and the declared
// output type. Fill the return fields it declares, and pass
// CredentialIssueResource the response; it rebuilds the secret resource.
//
// The SDK fixes only the arguments C1 interprets: the identity, the request ID,
// and the expiry when the descriptor advertises an expiry capability. Provider
// inputs — scopes, audiences, a key generation profile, anything else the
// provider needs — are declared by the connector through args, for example a
// StringSliceField with item rules or a StringField with options. Admins fix or
// narrow those on the C1 offering, and requesters choose within those limits.
//
// It leaves resource_type_id unset and is registered with RegisterWithSecrets,
// not RegisterResourceActionWithSecrets: registration is scoped by resource
// type and allows one action per non-dynamic action type within a scope, so a
// scoped schema would permit only one issuer per secret resource type. The
// descriptor's secret_resource_type_id already says what the action mints.
//
// An issuer action mints for one identity resource type, which C1 needs: the
// RPC implied it through the ResourceTypeCapability carrying the options, and an
// unscoped action schema has nowhere else to say it. A connector that issues for
// several identity types registers one action per type.
//
// The schema is metadata only: the SDK does not check what a handler returns or
// whether the arguments it received are well formed. C1 does both.
func CredentialIssueSchema(name string, identityResourceTypeID string, descriptor *v2.CredentialIssueOptionDescriptor, args ...*config.Field) (*v2.BatonActionSchema, error) {
	if identityResourceTypeID == "" {
		return nil, errors.New("credential issue schema: identity resource type id is required")
	}
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
	arguments, err := credentialIssueArguments(identityResourceTypeID, descriptor, args)
	if err != nil {
		return nil, err
	}
	returnTypes := []*config.Field{
		config.Field_builder{
			Name:        ReturnSecretResourceID,
			Description: "Resource ID of the minted secret resource.",
			IsRequired:  true,
			StringField: config.StringField_builder{}.Build(),
		}.Build(),
		config.Field_builder{
			Name:        ReturnExpiresAt,
			Description: "Absolute expiry as an RFC 3339 timestamp.",
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
	}
	return v2.BatonActionSchema_builder{
		Name:            name,
		ActionType:      []v2.ActionType{v2.ActionType_ACTION_TYPE_CREDENTIAL_ISSUE},
		CredentialIssue: descriptor,
		Arguments:       arguments,
		ReturnTypes:     returnTypes,
	}.Build(), nil
}

// credentialIssueArguments returns the fixed arguments followed by the
// connector's own, refusing a provider field that would shadow one of them.
func credentialIssueArguments(identityResourceTypeID string, descriptor *v2.CredentialIssueOptionDescriptor, providerArgs []*config.Field) ([]*config.Field, error) {
	arguments := []*config.Field{
		config.Field_builder{
			Name:        ArgIdentityID,
			Description: "The identity to mint the credential for.",
			IsRequired:  true,
			ResourceIdField: config.ResourceIdField_builder{
				Rules: config.ResourceIDRules_builder{
					AllowedResourceTypeIds: []string{identityResourceTypeID},
				}.Build(),
			}.Build(),
		}.Build(),
		config.Field_builder{
			Name:        ArgRequestID,
			Description: "Correlation ID for this issuance.",
			IsRequired:  true,
			StringField: config.StringField_builder{}.Build(),
		}.Build(),
	}
	// Only a descriptor that advertises an expiry capability gets the argument:
	// an expiry the connector could not honour would be a request it must refuse.
	if descriptor.HasExpiry() {
		arguments = append(arguments, config.Field_builder{
			Name:        ArgExpiresAt,
			Description: "Absolute expiry as an RFC 3339 timestamp.",
			StringField: config.StringField_builder{}.Build(),
		}.Build())
	}
	for _, providerArg := range providerArgs {
		if providerArg == nil {
			return nil, errors.New("credential issue schema: provider argument cannot be nil")
		}
		if slices.Contains(fixedArgumentNames, providerArg.GetName()) {
			return nil, fmt.Errorf("credential issue schema: provider argument %q collides with a fixed argument", providerArg.GetName())
		}
		arguments = append(arguments, providerArg)
	}
	return arguments, nil
}

// CredentialIssueResource rebuilds the minted secret resource from an action's
// public response, including the SecretTrait the return fields cannot carry.
// The resource type comes from the descriptor and the identity from the request,
// so neither is returned. A descriptor resource mode travels with the resource;
// pass it to connectorbuilder.ValidateIssuedCredential along with this result.
//
// The plaintext secret is the separate secret return field and is not read
// here. This does not check request_id or any provider argument (scopes,
// audiences, a key profile) against the request: the caller (C1) owns those
// checks, because only it knows what the issuance asked for.
func CredentialIssueResource(response *structpb.Struct, identityID *v2.ResourceId, descriptor *v2.CredentialIssueOptionDescriptor) (*v2.Resource, error) {
	if descriptor == nil {
		return nil, errors.New("credential issue response: descriptor is required")
	}
	if identityID == nil {
		return nil, errors.New("credential issue response: identity id is required")
	}
	fields := response.GetFields()
	secretID, err := requiredStringReturn(fields, ReturnSecretResourceID)
	if err != nil {
		return nil, err
	}
	traitOptions := []resource.SecretTraitOption{resource.WithSecretIdentityID(identityID)}
	if value, ok := fields[ReturnExpiresAt]; ok && value.GetStringValue() != "" {
		parsed, err := parseTimestamp(value.GetStringValue())
		if err != nil {
			return nil, fmt.Errorf("credential issue response: %s: %w", ReturnExpiresAt, err)
		}
		traitOptions = append(traitOptions, resource.WithSecretExpiresAt(parsed))
	}

	secret, err := resource.NewSecretResource(
		secretID,
		v2.ResourceType_builder{Id: descriptor.GetSecretResourceTypeId()}.Build(),
		secretID,
		traitOptions,
	)
	if err != nil {
		return nil, fmt.Errorf("credential issue response: build secret resource: %w", err)
	}
	return secret, nil
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

func parseTimestamp(text string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, text)
	if err != nil {
		return time.Time{}, fmt.Errorf("expected an RFC 3339 timestamp: %w", err)
	}
	return parsed, nil
}
