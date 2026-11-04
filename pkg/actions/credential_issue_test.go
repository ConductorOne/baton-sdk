package actions

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

func apiKeyOutputDescriptor() *v2.CredentialIssueOptionDescriptor {
	return v2.CredentialIssueOptionDescriptor_builder{
		Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
		SecretResourceTypeId: "organization-api-key",
		Output: config.Field_builder{
			Name:        "api_key",
			ContentType: "application/vnd.multipass.api_key_v2+json",
			Tags:        []string{"credential", "api_key", "multipass", "api_key_v2"},
			IsSecret:    true,
			StringField: config.StringField_builder{}.Build(),
		}.Build(),
	}.Build()
}

func TestCredentialIssueSchemaDescribesTheIssuance(t *testing.T) {
	descriptor := apiKeyOutputDescriptor()
	schema := mustCredentialIssueSchema(t, "issue_api_key", descriptor)

	require.Equal(t, "issue_api_key", schema.GetName())
	require.Equal(t, []v2.ActionType{v2.ActionType_ACTION_TYPE_CREDENTIAL_ISSUE}, schema.GetActionType())
	require.True(t, proto.Equal(descriptor, schema.GetCredentialIssue()), "the descriptor travels unmodified")
	require.Empty(t, schema.GetResourceTypeId(),
		"the schema is global: resource scoping allows one action per type per resource type")

	byName := argumentsByName(schema)
	require.Len(t, schema.GetArguments(), 2,
		"a descriptor with no expiry capability declares identity and request ID only")
	require.NotNil(t, byName[ArgIdentityID].GetResourceIdField(),
		"identity_id is a ResourceId, as in IssueCredentialRequest")
	require.True(t, byName[ArgIdentityID].GetIsRequired())
	require.NotNil(t, byName[ArgRequestID].GetStringField())
	require.True(t, byName[ArgRequestID].GetIsRequired())
	require.Nil(t, byName[ArgExpiresAt], "no expiry capability means no expiry argument")
	for _, argument := range schema.GetArguments() {
		require.False(t, argument.GetIsSecret())
		require.NoError(t, argument.Validate(), "argument %q", argument.GetName())
	}

	returnField := secretReturnField(schema)
	require.Equal(t, "api_key", returnField.GetName(), "the secret return field takes the output field's name")
	require.True(t, returnField.GetIsSecret())
	require.True(t, returnField.GetIsRequired())
	require.NotNil(t, returnField.GetStringField())
	require.Equal(t, "application/vnd.multipass.api_key_v2+json", returnField.GetContentType())
	require.Equal(t, []string{"credential", "api_key", "multipass", "api_key_v2"}, returnField.GetTags())
	require.NoError(t, returnField.Validate())
}

func TestCredentialIssueSchemaDeclaresOnlyTheStandardReturnFields(t *testing.T) {
	schema := mustCredentialIssueSchema(t, "issue_api_key", apiKeyOutputDescriptor())

	require.Len(t, schema.GetReturnTypes(), 3, "secret_resource_id, expires_at and the secret")
	require.NotNil(t, returnFieldByName(schema, ReturnSecretResourceID))
	require.True(t, returnFieldByName(schema, ReturnSecretResourceID).GetIsRequired())
	require.False(t, returnFieldByName(schema, ReturnSecretResourceID).GetIsSecret())
	require.NotNil(t, returnFieldByName(schema, ReturnExpiresAt))
	require.False(t, returnFieldByName(schema, ReturnExpiresAt).GetIsRequired(), "expires_at is optional")
	require.False(t, returnFieldByName(schema, ReturnExpiresAt).GetIsSecret())

	// The resource type, identity and mode come from the descriptor and the
	// request, so they are not returned.
	for _, absent := range []string{"secret_resource_type_id", "identity_id", "resource_mode", "request_id"} {
		require.Nil(t, returnFieldByName(schema, absent))
	}
}

func TestCredentialIssueSchemaAddsTheExpiryArgumentOnlyWhenOffered(t *testing.T) {
	without := apiKeyOutputDescriptor()
	require.False(t, without.HasExpiry())
	require.Nil(t, argByName(t, without, ArgExpiresAt),
		"no expiry capability means no expiry argument")

	with := apiKeyOutputDescriptor()
	with.SetExpiry(v2.IssuanceExpiryCapability_builder{
		Min: durationpb.New(time.Hour),
		Max: durationpb.New(24 * time.Hour),
	}.Build())
	expiryArg := argByName(t, with, ArgExpiresAt)
	require.NotNil(t, expiryArg, "an expiry capability brings the argument")
	require.NotNil(t, expiryArg.GetStringField())
	require.False(t, expiryArg.GetIsRequired(), "the expiry argument is optional")
}

func TestCredentialIssueSchemaAppendsProviderArguments(t *testing.T) {
	scopes := config.Field_builder{
		Name:             "scopes",
		Description:      "Provider scopes to request.",
		StringSliceField: config.StringSliceField_builder{}.Build(),
	}.Build()
	profile := config.Field_builder{
		Name: "profile",
		StringField: config.StringField_builder{
			Options: []*config.StringFieldOption{
				config.StringFieldOption_builder{}.Build(),
			},
		}.Build(),
	}.Build()

	schema := mustCredentialIssueSchema(t, "issue_api_key", apiKeyOutputDescriptor(), scopes, profile)
	byName := argumentsByName(schema)
	require.Len(t, schema.GetArguments(), 4, "two fixed plus two provider arguments")
	require.Equal(t, scopes, byName["scopes"])
	require.Equal(t, profile, byName["profile"])
	require.NotNil(t, byName["scopes"].GetStringSliceField())

	// The provider's fields follow the fixed ones, so a reader sees what the
	// platform interprets first.
	names := make([]string, 0, len(schema.GetArguments()))
	for _, argument := range schema.GetArguments() {
		names = append(names, argument.GetName())
	}
	require.Equal(t, []string{ArgIdentityID, ArgRequestID, "scopes", "profile"}, names)
}

func TestCredentialIssueSchemaRejectsUnusableInput(t *testing.T) {
	unsecret := apiKeyOutputDescriptor()
	unsecret.GetOutput().SetIsSecret(false)

	notString := apiKeyOutputDescriptor()
	notString.GetOutput().ClearStringField()
	notString.GetOutput().SetBoolField(config.BoolField_builder{}.Build())

	colliding := apiKeyOutputDescriptor()
	colliding.GetOutput().SetName(ReturnSecretResourceID)

	for _, tc := range []struct {
		name       string
		descriptor *v2.CredentialIssueOptionDescriptor
		want       string
	}{
		{"nil descriptor", nil, "descriptor is required"},
		{"output not secret", unsecret, "must be declared secret"},
		{"output not a string", notString, "must be a string field"},
		{"output name collides", colliding, "collides with a standard return field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema, err := CredentialIssueSchema("issue_api_key", tc.descriptor)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.want)
			require.Nil(t, schema)
		})
	}
}

func TestCredentialIssueSchemaRejectsAProviderArgumentCollision(t *testing.T) {
	for _, fixed := range []string{ArgIdentityID, ArgRequestID, ArgExpiresAt} {
		t.Run(fixed, func(t *testing.T) {
			providerArg := config.Field_builder{
				Name:        fixed,
				StringField: config.StringField_builder{}.Build(),
			}.Build()
			schema, err := CredentialIssueSchema("issue_api_key", apiKeyOutputDescriptor(), providerArg)
			require.Error(t, err)
			require.ErrorContains(t, err, "collides with a fixed argument")
			require.Nil(t, schema)
		})
	}
}

func TestCredentialIssueSchemasRegisterGlobally(t *testing.T) {
	ctx := context.Background()
	manager := NewActionManager(ctx)
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		response, err := structpb.NewStruct(map[string]any{"success": true})
		if err != nil {
			return nil, nil, nil, err
		}
		return response, []*v2.PlaintextData{
			v2.PlaintextData_builder{
				Name:   "api_key",
				Schema: "application/vnd.multipass.api_key_v2+json",
				Bytes:  []byte(`{"key_value":"k"}`),
			}.Build(),
		}, nil, nil
	}

	// Two issuers for the same secret resource type, which a resource-scoped
	// registration would refuse: the duplicate-action-type check compares action
	// types within one resource type.
	orgKey := mustCredentialIssueSchema(t, "issue_org_api_key", apiKeyOutputDescriptor())
	serviceKey := mustCredentialIssueSchema(t, "issue_service_account_key", apiKeyOutputDescriptor())

	require.NoError(t, manager.RegisterWithSecrets(ctx, orgKey, handler))
	require.NoError(t, manager.RegisterWithSecrets(ctx, serviceKey, handler))

	for _, schema := range []*v2.BatonActionSchema{orgKey, serviceKey} {
		registered := manager.schemas[schema.GetName()]
		require.NotNil(t, registered, "the schema registers by name")
		require.Equal(t, schema.GetName(), registered.GetName())
		require.Empty(t, registered.GetResourceTypeId(), "a global schema carries no resource scope")
		require.Equal(t, []v2.ActionType{v2.ActionType_ACTION_TYPE_CREDENTIAL_ISSUE}, registered.GetActionType())
		require.True(t, proto.Equal(schema.GetCredentialIssue(), registered.GetCredentialIssue()))
	}
}

func TestCredentialIssueResourceRebuildsTheSecret(t *testing.T) {
	identityID := v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build()
	response, err := structpb.NewStruct(map[string]any{
		ReturnSecretResourceID: "key-123",
		ReturnExpiresAt:        "2027-01-01T00:00:00Z",
	})
	require.NoError(t, err)

	secret, err := CredentialIssueResource(response, identityID, apiKeyOutputDescriptor())
	require.NoError(t, err)
	require.Equal(t, "organization-api-key", secret.GetId().GetResourceType(),
		"the resource type comes from the descriptor")
	require.Equal(t, "key-123", secret.GetId().GetResource())

	trait := &v2.SecretTrait{}
	annos := annotations.Annotations(secret.GetAnnotations())
	found, err := annos.Pick(trait)
	require.NoError(t, err)
	require.True(t, found, "the rebuilt resource carries a SecretTrait")
	require.Equal(t, "u-1", trait.GetIdentityId().GetResource(), "the identity comes from the request")
	require.NotNil(t, trait.GetExpiresAt(), "an expiry in the response reaches the trait")
}

func TestCredentialIssueResourceOmitsAnAbsentExpiry(t *testing.T) {
	identityID := v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build()
	response, err := structpb.NewStruct(map[string]any{ReturnSecretResourceID: "key-123"})
	require.NoError(t, err)

	secret, err := CredentialIssueResource(response, identityID, apiKeyOutputDescriptor())
	require.NoError(t, err)
	trait := &v2.SecretTrait{}
	annos := annotations.Annotations(secret.GetAnnotations())
	found, err := annos.Pick(trait)
	require.NoError(t, err)
	require.True(t, found)
	require.Nil(t, trait.GetExpiresAt(), "no expiry was declared, so none is invented")
}

func TestCredentialIssueResourceRejectsIncompleteInput(t *testing.T) {
	identityID := v2.ResourceId_builder{ResourceType: "user", Resource: "u-1"}.Build()
	for _, tc := range []struct {
		name     string
		response map[string]any
		identity *v2.ResourceId
		want     string
	}{
		{"missing secret_resource_id", map[string]any{}, identityID, ReturnSecretResourceID},
		{"empty secret_resource_id", map[string]any{ReturnSecretResourceID: ""}, identityID, ReturnSecretResourceID},
		{"bad expiry", map[string]any{ReturnSecretResourceID: "k", ReturnExpiresAt: "yesterday"}, identityID, ReturnExpiresAt},
		{"no identity", map[string]any{ReturnSecretResourceID: "k"}, nil, "identity id is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response, err := structpb.NewStruct(tc.response)
			require.NoError(t, err)
			_, err = CredentialIssueResource(response, tc.identity, apiKeyOutputDescriptor())
			require.Error(t, err)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// mustCredentialIssueSchema builds a schema or fails the test.
func mustCredentialIssueSchema(t *testing.T, name string, descriptor *v2.CredentialIssueOptionDescriptor, args ...*config.Field) *v2.BatonActionSchema {
	t.Helper()
	schema, err := CredentialIssueSchema(name, descriptor, args...)
	require.NoError(t, err)
	return schema
}

func argumentsByName(schema *v2.BatonActionSchema) map[string]*config.Field {
	byName := make(map[string]*config.Field, len(schema.GetArguments()))
	for _, argument := range schema.GetArguments() {
		byName[argument.GetName()] = argument
	}
	return byName
}

// secretReturnField picks the one secret return field, so a test names the
// field it means rather than its position in the list.
func secretReturnField(schema *v2.BatonActionSchema) *config.Field {
	for _, field := range schema.GetReturnTypes() {
		if field.GetIsSecret() {
			return field
		}
	}
	return nil
}

func returnFieldByName(schema *v2.BatonActionSchema, name string) *config.Field {
	for _, field := range schema.GetReturnTypes() {
		if field.GetName() == name {
			return field
		}
	}
	return nil
}

// argByName builds the schema for a descriptor and returns one argument.
func argByName(t *testing.T, descriptor *v2.CredentialIssueOptionDescriptor, name string) *config.Field {
	t.Helper()
	return argumentsByName(mustCredentialIssueSchema(t, "issue", descriptor))[name]
}
