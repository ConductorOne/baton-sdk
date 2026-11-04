package actions

import (
	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// Argument names for a credential-issue action. They follow the fields of
// IssueCredentialRequest so a connector moving off the RPC keeps its names.
const (
	ArgIdentityID = "identity_id"
	ArgExpiresAt  = "expires_at"
	ArgRequestID  = "request_id"
)

// defaultCredentialReturnName labels the issued secret when the descriptor's
// output field leaves its name unset.
const defaultCredentialReturnName = "credential"

// CredentialIssueSchema builds the schema for an action that mints a
// credential. The descriptor carries the advertised shape and the declared
// output type. Pass the result to RegisterResourceActionWithSecrets.
//
// The schema is metadata only: the SDK does not check what a handler returns or
// whether the arguments it received are well formed. C1 does both.
func CredentialIssueSchema(name string, descriptor *v2.CredentialIssueOptionDescriptor) *v2.BatonActionSchema {
	output := descriptor.GetOutput()
	returnName := output.GetName()
	if returnName == "" {
		returnName = defaultCredentialReturnName
	}
	return v2.BatonActionSchema_builder{
		Name:            name,
		ActionType:      []v2.ActionType{v2.ActionType_ACTION_TYPE_CREDENTIAL_ISSUE},
		CredentialIssue: descriptor,
		ResourceTypeId:  descriptor.GetSecretResourceTypeId(),
		Arguments: []*config.Field{
			config.Field_builder{
				Name:        ArgIdentityID,
				IsRequired:  true,
				StringField: config.StringField_builder{}.Build(),
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
		},
		ReturnTypes: []*config.Field{
			config.Field_builder{
				Name:        returnName,
				ContentType: output.GetContentType(),
				Tags:        output.GetTags(),
				IsSecret:    true,
				IsRequired:  true,
				StringField: config.StringField_builder{}.Build(),
			}.Build(),
		},
	}.Build()
}
