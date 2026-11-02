package connectorbuilder

import (
	"fmt"
	"strings"
	"testing"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestCredentialIssueFieldsCompatibility(t *testing.T) {
	legacy := testCredentialIssueRequestSchema()
	d := v2.CredentialIssueOptionDescriptor_builder{RequestSchema: legacy}.Build()
	got, err := CredentialIssueFields(d)
	require.NoError(t, err)
	require.True(t, proto.Equal(legacy, got))
	d.SetArguments(legacy.GetFields())
	d.SetConstraints(legacy.GetConstraints())
	_, err = CredentialIssueFields(d)
	require.NoError(t, err)
	d.SetArguments([]*config.Field{config.Field_builder{Name: "different", StringField: config.StringField_builder{}.Build()}.Build()})
	_, err = CredentialIssueFields(d)
	require.ErrorContains(t, err, "conflicting")
}

func TestCredentialIssueOutputField(t *testing.T) {
	output := config.Field_builder{
		Name: "credential", IsSecret: true, IsRequired: true,
		StringField: config.StringField_builder{}.Build(), ContentType: "application/json",
		Tags: []string{"descriptive", "multipass-type-api_key.v2"},
	}.Build()
	d := v2.CredentialIssueOptionDescriptor_builder{ReturnTypes: []*config.Field{output}}.Build()
	got, err := CredentialIssueOutputField(d)
	require.NoError(t, err)
	require.Equal(t, "credential", got.Name)
	require.Equal(t, "application/json", got.ContentType)
	require.Equal(t, "api_key.v2", got.LogicalType)
	d.SetSecretContentType("api_key.v2")
	_, err = CredentialIssueOutputField(d)
	require.NoError(t, err)
	d.SetSecretContentType("password_v2")
	_, err = CredentialIssueOutputField(d)
	require.ErrorContains(t, err, "conflicting")
	d.ClearSecretContentType()
	for _, value := range []string{"generic", "password_v2"} {
		output.SetContentType(value)
		_, err = CredentialIssueOutputField(d)
		require.ErrorContains(t, err, "incompatible")
	}
	output.SetContentType("application/json")
	output.SetTags([]string{"multipass-type-" + strings.Repeat("a", 128)})
	_, err = CredentialIssueOutputField(d)
	require.NoError(t, err)
	output.SetTags([]string{"multipass-type-api_key_v2", "multipass-type-generic"})
	_, err = CredentialIssueOutputField(d)
	require.ErrorContains(t, err, "ambiguous")
	output.SetTags(nil)
	d.SetReturnTypes([]*config.Field{output, output})
	_, err = CredentialIssueOutputField(d)
	require.ErrorContains(t, err, "exactly one")
}

func TestCredentialIssueDirectArgumentsValidateBeforeProducer(t *testing.T) {
	issuer := &typedIssuer{scriptedCredentialIssuer: newScriptedCredentialIssuer("service_account"), contentType: "password_v2", canonical: true}
	connector, err := NewConnector(t.Context(), newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
	require.NoError(t, err)
	cfg := validJWEConfig(t)
	cfg.GetJwkPublicKeyConfig().SetAdditionalAuthenticatedData([]byte(`{"purpose":"c1/fk-credential-delivery","version":1,"content_type":"password_v2"}`))
	for _, input := range []map[string]any{{"scopes": []any{"keys:read"}, "unexpected": "private-value"}, {"scopes": []any{"keys:read"}, "global": "not-a-bool"}} {
		data, err := structpb.NewStruct(input)
		require.NoError(t, err)
		req := issueRequest(cfg)
		req.SetRequestData(data)
		_, err = connector.IssueCredential(t.Context(), req)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private-value")
		require.Zero(t, issuer.calls)
	}
}

func TestNativeLegacyFieldEightRejected(t *testing.T) {
	for _, value := range []string{"generic", "password_v2"} {
		var output config.Field
		require.NoError(t, proto.Unmarshal(protowire.AppendString(protowire.AppendTag(nil, 8, protowire.BytesType), value), &output))
		output.SetName("credential")
		output.SetIsRequired(true)
		output.SetIsSecret(true)
		output.SetStringField(config.StringField_builder{}.Build())
		descriptor := apiKeyDescriptor("secret")
		descriptor.SetReturnTypes([]*config.Field{&output})
		_, err := CredentialIssueOutputField(descriptor)
		require.ErrorContains(t, err, "incompatible")
	}
}

func ExampleCredentialIssueOutputField() {
	output := config.Field_builder{
		Name: "credential", IsRequired: true, IsSecret: true,
		StringField: config.StringField_builder{}.Build(),
		ContentType: "application/json", Tags: []string{"multipass-type-api_key_v2"},
	}.Build()
	descriptor := v2.CredentialIssueOptionDescriptor_builder{ReturnTypes: []*config.Field{output}}.Build()
	result, err := CredentialIssueOutputField(descriptor)
	if err != nil {
		panic(err)
	}
	fmt.Println(result.Name, result.ContentType, result.LogicalType)
	// Output: credential application/json api_key_v2
}

func TestCredentialIssueValidationSchemaIgnoresPresentation(t *testing.T) {
	legacy := testCredentialIssueRequestSchema()
	old := v2.CredentialIssueOptionDescriptor_builder{RequestSchema: legacy}.Build()
	fields := proto.Clone(legacy).(*v2.CredentialIssueRequestSchema)
	fields.GetFields()[0].SetContentType("text/plain")
	fields.GetFields()[0].SetTags([]string{"description"})
	current := v2.CredentialIssueOptionDescriptor_builder{Arguments: fields.GetFields(), Constraints: fields.GetConstraints()}.Build()
	a, err := CredentialIssueValidationSchema(old)
	require.NoError(t, err)
	b, err := CredentialIssueValidationSchema(current)
	require.NoError(t, err)
	require.True(t, proto.Equal(a, b))
	require.Equal(t, "text/plain", fields.GetFields()[0].GetContentType())
	fields.GetFields()[0].SetTags([]string{"multipass-type-api_key_v2"})
	_, err = CredentialIssueValidationSchema(current)
	require.Error(t, err)
	absent, err := CredentialIssueValidationSchema(v2.CredentialIssueOptionDescriptor_builder{}.Build())
	require.NoError(t, err)
	require.Nil(t, absent)
}
