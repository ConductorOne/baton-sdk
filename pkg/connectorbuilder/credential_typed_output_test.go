package connectorbuilder

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"filippo.io/hpke"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwe"
	"github.com/conductorone/baton-sdk/pkg/secretcontracts"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type typedIssuer struct {
	*scriptedCredentialIssuer
	contentType string
	invalid     bool
	input       *CredentialIssueInput
	bytes       []byte
}

func (i *typedIssuer) IssueCapabilityDetails(context.Context) (*v2.CredentialDetailsCredentialIssue, annotations.Annotations, error) {
	d := v2.CredentialIssueOptionDescriptor_builder{
		Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
		ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
		SecretResourceTypeId: "secret", SecretContentType: proto.String(i.contentType),
		Scopes: []string{"read"}, RequestSchema: testCredentialIssueRequestSchema(),
	}.Build()
	return v2.CredentialDetailsCredentialIssue_builder{
		Options:         []*v2.CredentialIssueOptionDescriptor{d},
		PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
	}.Build(), nil, nil
}
func (i *typedIssuer) Issue(ctx context.Context, input *CredentialIssueInput) (*CredentialIssueOutput, error) {
	i.input = input
	var payload secretcontracts.Payload
	var err error
	switch i.contentType {
	case "password_v2":
		password := "test-password"
		if i.invalid {
			password = ""
		}
		payload, err = (secretcontracts.PasswordV2{Password: password}).Encode()
	case "api_key_v2":
		key := "test-key"
		if i.invalid {
			key = ""
		}
		payload, err = (secretcontracts.APIKeyV2{KeyValue: key}).Encode()
	default:
		payload = secretcontracts.Payload{Bytes: []byte{0xff, 1}}
	}
	if err != nil {
		i.calls++
		return nil, err
	}
	i.bytes = payload.Bytes
	i.plaintexts = []*v2.PlaintextData{v2.PlaintextData_builder{Name: "credential", Bytes: payload.Bytes, Schema: "independent description"}.Build()}
	return i.scriptedCredentialIssuer.Issue(ctx, input)
}
func TestNativeTypedProducerAndContext(t *testing.T) {
	for _, typ := range []string{"password_v2", "api_key_v2", "generic"} {
		t.Run(typ, func(t *testing.T) {
			ctx := context.Background()
			issuer := &typedIssuer{scriptedCredentialIssuer: newScriptedCredentialIssuer("service_account"), contentType: typ}
			connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
			require.NoError(t, err)
			key, err := hpke.MLKEM768X25519().GenerateKey()
			require.NoError(t, err)
			cfg := jweConfigForPublicKey(publicJWKJSON(t, key.PublicKey().Bytes(), nil))
			cfg.GetJwkPublicKeyConfig().SetAdditionalAuthenticatedData([]byte(`{"purpose":"c1/fk-credential-delivery","version":1,"content_type":"` + typ + `"}`))
			request := issueRequest(cfg)
			request.SetCredentialOptions(v2.CredentialIssueOptions_builder{SecretResourceTypeId: "secret", ApiKey: v2.CredentialIssueOptions_ApiKey_builder{Scopes: []string{"read"}}.Build()}.Build())
			data, err := structpb.NewStruct(map[string]any{"scopes": []any{"keys:read"}, "global": false})
			require.NoError(t, err)
			request.SetRequestData(data)
			response, err := connector.IssueCredential(ctx, request)
			require.NoError(t, err)
			require.Equal(t, 1, issuer.calls)
			require.Equal(t, data, issuer.input.RequestData)
			require.Equal(t, []string{"read"}, issuer.input.CredentialOptions.GetApiKey().GetScopes())
			var env jwe.FlattenedJWE
			require.NoError(t, json.Unmarshal(response.GetEncryptedData()[0].GetEncryptedBytes(), &env))
			enc, err := base64.RawURLEncoding.DecodeString(env.EncryptedKey)
			require.NoError(t, err)
			ciphertext, err := base64.RawURLEncoding.DecodeString(env.Ciphertext)
			require.NoError(t, err)
			recipient, err := hpke.NewRecipient(enc, key, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), nil)
			require.NoError(t, err)
			opened, err := recipient.Open([]byte(env.Protected+"."+env.AAD), ciphertext)
			require.NoError(t, err)
			require.Equal(t, issuer.bytes, opened)
			require.Equal(t, "independent description", response.GetEncryptedData()[0].GetSchema())
		})
	}
}
func TestNativeTypedRejectsContextBeforeIssue(t *testing.T) {
	for _, aad := range []string{
		"", `{"purpose":"c1/fk-credential-delivery","version":1,"content_type":"generic"}`,
		`{"purpose":"c1/fk-credential-delivery","version":1,"content_type":"password_v2","content_type":"password_v2"}`,
		`{"purpose":"wrong","version":1,"content_type":"password_v2"}`,
		`{"purpose":"c1/fk-credential-delivery","version":2,"content_type":"password_v2"}`,
	} {
		t.Run(aad, func(t *testing.T) {
			issuer := &typedIssuer{scriptedCredentialIssuer: newScriptedCredentialIssuer("service_account"), contentType: "password_v2"}
			connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
			require.NoError(t, err)
			cfg := validJWEConfig(t)
			cfg.GetJwkPublicKeyConfig().SetAdditionalAuthenticatedData([]byte(aad))
			request := issueRequest(cfg)
			data, err := structpb.NewStruct(map[string]any{"scopes": []any{"keys:read"}})
			require.NoError(t, err)
			request.SetRequestData(data)
			_, err = connector.IssueCredential(context.Background(), request)
			require.Error(t, err)
			require.Zero(t, issuer.calls)
		})
	}
}
func TestNativeRequestRejectsOutputTypeMarker(t *testing.T) {
	schema := testCredentialIssueRequestSchema()
	schema.GetFields()[0].SetSecretContentType("password_v2")
	require.Error(t, ValidateCredentialIssueRequestSchema(schema))
}

func TestNativeTypedInvalidProducerFailsAfterIssue(t *testing.T) {
	issuer := &typedIssuer{scriptedCredentialIssuer: newScriptedCredentialIssuer("service_account"), contentType: "password_v2", invalid: true}
	connector, err := NewConnector(context.Background(), newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
	require.NoError(t, err)
	cfg := validJWEConfig(t)
	cfg.GetJwkPublicKeyConfig().SetAdditionalAuthenticatedData([]byte(`{"purpose":"c1/fk-credential-delivery","version":1,"content_type":"password_v2"}`))
	request := issueRequest(cfg)
	data, err := structpb.NewStruct(map[string]any{"scopes": []any{"keys:read"}})
	require.NoError(t, err)
	request.SetRequestData(data)
	response, err := connector.IssueCredential(context.Background(), request)
	require.Error(t, err)
	require.Nil(t, response)
	require.Equal(t, 1, issuer.calls)
}
func TestNativeDescriptorTypeValidation(t *testing.T) {
	for _, typ := range []string{"", "Invalid", "bad..type", string(make([]byte, 129))} {
		descriptor := apiKeyDescriptor("secret")
		descriptor.SetSecretContentType(typ)
		require.Error(t, validateCredentialIssueDescriptorShape(descriptor))
	}
	descriptor := apiKeyDescriptor("secret")
	require.NoError(t, validateCredentialIssueDescriptorShape(descriptor))
	descriptor.SetSecretContentType("generic")
	require.NoError(t, validateCredentialIssueDescriptorShape(descriptor))
}
