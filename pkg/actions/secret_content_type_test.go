package actions

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"filippo.io/hpke"
	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto/providers/jwe"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func typedSecretSchema(contentType string) *v2.BatonActionSchema {
	schema := secretActionSchema("issue_structured")
	schema.GetReturnTypes()[1].SetSecretContentType(contentType)
	return schema
}

func typedContext(contentType string) []byte {
	return []byte(fmt.Sprintf(`{"purpose":"c1/fk-credential-delivery","version":1,"content_type":%q}`, contentType))
}

func typedRecipient(t *testing.T, keyID string, aad []byte) (*v2.EncryptionConfig, hpke.PrivateKey) {
	t.Helper()
	private, err := hpke.MLKEM768X25519().GenerateKey()
	require.NoError(t, err)
	public, err := json.Marshal(map[string]string{"kty": "AKP", "alg": jwe.Algorithm,
		"pub": base64.RawURLEncoding.EncodeToString(private.PublicKey().Bytes())})
	require.NoError(t, err)
	return v2.EncryptionConfig_builder{Provider: jwe.EncryptionProvider, KeyId: keyID,
		JwkPublicKeyConfig: v2.EncryptionConfig_JWKPublicKeyConfig_builder{PubKey: public, AdditionalAuthenticatedData: aad}.Build()}.Build(), private
}

func registerTypedHandler(t *testing.T, manager *ActionManager, resource string, schema *v2.BatonActionSchema, calls *atomic.Int32) {
	t.Helper()
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		calls.Add(1)
		value, err := json.Marshal(struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}{Username: "svc-user", Password: "fixture-only"})
		if err != nil {
			return nil, nil, nil, err
		}
		return NewReturnValues(true), []*v2.PlaintextData{v2.PlaintextData_builder{Name: "token", Bytes: value}.Build()}, nil, nil
	}
	if resource == "" {
		require.NoError(t, RegisterWithSecrets(t.Context(), manager, schema, handler))
	} else {
		require.NoError(t, manager.RegisterResourceActionWithSecrets(t.Context(), resource, schema, handler))
	}
}

func TestSecretContentTypeSchemaRoundTrip(t *testing.T) {
	for _, present := range []bool{false, true} {
		schema := secretActionSchema("schema")
		if present {
			schema.GetReturnTypes()[1].SetSecretContentType("password_v2")
		}
		for _, encoding := range []string{"binary", "JSON"} {
			t.Run(fmt.Sprintf("present=%t/%s", present, encoding), func(t *testing.T) {
				var encoded []byte
				var err error
				if encoding == "binary" {
					encoded, err = proto.Marshal(schema)
				} else {
					encoded, err = protojson.Marshal(schema)
				}
				require.NoError(t, err)
				var decoded v2.BatonActionSchema
				if encoding == "binary" {
					err = proto.Unmarshal(encoded, &decoded)
				} else {
					err = protojson.Unmarshal(encoded, &decoded)
				}
				require.NoError(t, err)
				require.True(t, proto.Equal(schema, &decoded))
				require.Equal(t, present, decoded.GetReturnTypes()[1].HasSecretContentType())
			})
		}
	}
	field := typedSecretSchema("").GetReturnTypes()[1]
	require.True(t, field.HasSecretContentType())
	require.Error(t, field.ValidateAll())
}

func TestSecretContentTypeRegistrationRejectsInvalidDeclarations(t *testing.T) {
	for _, resource := range []string{"", "account"} {
		for _, tc := range []struct {
			name   string
			mutate func(*v2.BatonActionSchema)
		}{
			{"empty", func(s *v2.BatonActionSchema) { s.GetReturnTypes()[1].SetSecretContentType("") }},
			{"uppercase", func(s *v2.BatonActionSchema) { s.GetReturnTypes()[1].SetSecretContentType("Password_V2") }},
			{"whitespace", func(s *v2.BatonActionSchema) { s.GetReturnTypes()[1].SetSecretContentType(" password_v2") }},
			{"overlong", func(s *v2.BatonActionSchema) { s.GetReturnTypes()[1].SetSecretContentType(strings.Repeat("a", 129)) }},
			{"public return", func(s *v2.BatonActionSchema) { s.GetReturnTypes()[0].SetSecretContentType("password_v2") }},
			{"input", func(s *v2.BatonActionSchema) {
				s.SetArguments([]*config.Field{config.Field_builder{Name: "input", IsSecret: true, SecretContentType: proto.String("password_v2")}.Build()})
			}},
		} {
			t.Run(resource+"/"+tc.name, func(t *testing.T) {
				manager := NewActionManager(t.Context())
				schema := typedSecretSchema("password_v2")
				tc.mutate(schema)
				handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
					t.Fatal("invalid declaration must not publish a handler")
					return nil, nil, nil, nil
				}
				var err error
				if resource == "" {
					err = manager.RegisterWithSecrets(t.Context(), schema, handler)
				} else {
					err = manager.RegisterResourceActionWithSecrets(t.Context(), resource, schema, handler)
				}
				require.Error(t, err)
				require.False(t, manager.HasActions())
			})
		}
		for _, resourceOnly := range []bool{false, true} {
			t.Run(resource+fmt.Sprintf("/ordinary registration=%t", resourceOnly), func(t *testing.T) {
				manager := NewActionManager(t.Context())
				schema := secretActionSchema("public")
				schema.SetReturnTypes(schema.GetReturnTypes()[:1])
				schema.GetReturnTypes()[0].SetSecretContentType("password_v2")
				handler := func(context.Context, *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
					return nil, nil, nil
				}
				var err error
				if resourceOnly {
					err = manager.RegisterResourceAction(t.Context(), "account", schema, handler)
				} else {
					err = manager.Register(t.Context(), schema, handler)
				}
				require.Error(t, err)
				require.False(t, manager.HasActions())
			})
		}
	}
}

func TestTypedSecretOutputRefusesUnacknowledgedCallerBeforeHandler(t *testing.T) {
	for _, resource := range []string{"", "account"} {
		for _, tc := range []struct{ name, context string }{
			{"absent", ""}, {"opaque", "opaque"}, {"generic", string(typedContext("generic"))},
			{"wrong type", string(typedContext("api_key_v2"))},
			{"wrong purpose", `{"purpose":"other","version":1,"content_type":"password_v2"}`},
			{"wrong version", `{"purpose":"c1/fk-credential-delivery","version":2,"content_type":"password_v2"}`},
			{"string version", `{"purpose":"c1/fk-credential-delivery","version":"1","content_type":"password_v2"}`},
			{"duplicate type", `{"purpose":"c1/fk-credential-delivery","version":1,"content_type":"generic","content_type":"password_v2"}`},
			{"escaped duplicate", `{"purpose":"c1/fk-credential-delivery","version":1,"content_type":"password_v2","content\u005ftype":"password_v2"}`},
			{"malformed", `{"purpose":`}, {"array", `[]`}, {"trailing", string(typedContext("password_v2")) + `{}`},
		} {
			t.Run(resource+"/"+tc.name, func(t *testing.T) {
				manager := NewActionManager(t.Context())
				var calls atomic.Int32
				registerTypedHandler(t, manager, resource, typedSecretSchema("password_v2"), &calls)
				config, _ := typedRecipient(t, "recipient", []byte(tc.context))
				id, _, _, _, _, err := manager.InvokeActionWithWaitAndEncryption(t.Context(), "issue_structured", resource, nil, time.Second, []*v2.EncryptionConfig{config})
				require.Error(t, err)
				require.Empty(t, id)
				require.Zero(t, calls.Load())
				require.Empty(t, manager.actions)
			})
		}
		for _, scenario := range []string{"age only", "conflicting JWE", "JWE plus age", "multiple secrets"} {
			t.Run(resource+"/"+scenario, func(t *testing.T) {
				manager := NewActionManager(t.Context())
				var calls atomic.Int32
				schema := typedSecretSchema("password_v2")
				if scenario == "multiple secrets" {
					schema.SetReturnTypes(append(schema.GetReturnTypes(), config.Field_builder{Name: "other", IsSecret: true}.Build()))
				}
				registerTypedHandler(t, manager, resource, schema, &calls)
				matching, _ := typedRecipient(t, "matching", typedContext("password_v2"))
				configs := []*v2.EncryptionConfig{matching}
				if scenario == "age only" {
					age, _ := ageEncryptionConfig(t)
					configs = []*v2.EncryptionConfig{age}
				} else if scenario == "JWE plus age" {
					age, _ := ageEncryptionConfig(t)
					configs = append(configs, age)
				} else if scenario == "conflicting JWE" {
					conflicting, _ := typedRecipient(t, "conflicting", typedContext("api_key_v2"))
					configs = append(configs, conflicting)
				}
				id, _, _, _, _, err := manager.InvokeActionWithWaitAndEncryption(t.Context(), "issue_structured", resource, nil, time.Second, configs)
				require.Error(t, err)
				require.Empty(t, id)
				require.Zero(t, calls.Load())
				require.Empty(t, manager.actions)
			})
		}
	}
}

func TestTypedSecretOutputEncryptsExactHelperBytesWithFrozenDeclaration(t *testing.T) {
	for _, resource := range []string{"", "account"} {
		t.Run(resource, func(t *testing.T) {
			manager := NewActionManager(t.Context())
			var calls atomic.Int32
			schema := typedSecretSchema("password_v2")
			registerTypedHandler(t, manager, resource, schema, &calls)
			schema.GetReturnTypes()[1].SetSecretContentType("api_key_v2")
			config, private := typedRecipient(t, "recipient", typedContext("password_v2"))
			id, result, public, encrypted, _, err := manager.InvokeActionWithWaitAndEncryption(t.Context(), "issue_structured", resource, nil, time.Second, []*v2.EncryptionConfig{config})
			require.NoError(t, err)
			require.NotEmpty(t, id)
			require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, result)
			require.Equal(t, int32(1), calls.Load())
			require.NotContains(t, public.GetFields(), "token")
			require.Len(t, encrypted, 1)
			jweResult := encrypted[0]
			require.Equal(t, jwe.EncryptionProvider, jweResult.GetProvider())
			var envelope jwe.FlattenedJWE
			require.NoError(t, json.Unmarshal(jweResult.GetEncryptedBytes(), &envelope))
			aad, err := base64.RawURLEncoding.Strict().DecodeString(envelope.AAD)
			require.NoError(t, err)
			require.Equal(t, typedContext("password_v2"), aad)
			enc, err := base64.RawURLEncoding.Strict().DecodeString(envelope.EncryptedKey)
			require.NoError(t, err)
			ciphertext, err := base64.RawURLEncoding.Strict().DecodeString(envelope.Ciphertext)
			require.NoError(t, err)
			recipient, err := hpke.NewRecipient(enc, private, hpke.HKDFSHA256(), hpke.ChaCha20Poly1305(), nil)
			require.NoError(t, err)
			opened, err := recipient.Open([]byte(envelope.Protected+"."+envelope.AAD), ciphertext)
			require.NoError(t, err)
			require.Equal(t, []byte(`{"username":"svc-user","password":"fixture-only"}`), opened)
		})
	}
}

func TestGenericSecretOutputKeepsOpaqueContexts(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		manager := NewActionManager(t.Context())
		var calls atomic.Int32
		schema := secretActionSchema("issue_structured")
		if explicit {
			schema.GetReturnTypes()[1].SetSecretContentType("generic")
		}
		registerTypedHandler(t, manager, "", schema, &calls)
		config, _ := typedRecipient(t, "recipient", []byte("opaque legacy context"))
		_, result, _, _, _, err := manager.InvokeActionWithWaitAndEncryption(t.Context(), "issue_structured", "", nil, time.Second, []*v2.EncryptionConfig{config})
		require.NoError(t, err)
		require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, result)
		require.Equal(t, int32(1), calls.Load())
	}
}

func TestSecretOutputAdvertisedSchemaIsAnImmutableSnapshot(t *testing.T) {
	for _, resource := range []string{"", "account"} {
		for _, declared := range []*string{nil, proto.String("generic"), proto.String("password_v2")} {
			name := "absent"
			if declared != nil {
				name = *declared
			}
			t.Run(resource+"/"+name, func(t *testing.T) {
				manager := NewActionManager(t.Context())
				var calls atomic.Int32
				schema := secretActionSchema("issue_structured")
				if declared != nil {
					schema.GetReturnTypes()[1].SetSecretContentType(*declared)
				}
				registerTypedHandler(t, manager, resource, schema, &calls)
				schema.GetReturnTypes()[1].SetSecretContentType("api_key_v2")
				assertDeclaration := func(field *config.Field) {
					require.Equal(t, declared != nil, field.HasSecretContentType())
					if declared != nil {
						require.Equal(t, *declared, field.GetSecretContentType())
					}
				}
				for _, filter := range []string{"", resource} {
					listed, _, err := manager.ListActionSchemas(t.Context(), filter)
					require.NoError(t, err)
					require.Len(t, listed, 1)
					assertDeclaration(listed[0].GetReturnTypes()[1])
					listed[0].GetReturnTypes()[1].SetSecretContentType("api_key_v2")
				}
				if resource == "" {
					got, _, err := manager.GetActionSchema(t.Context(), "issue_structured")
					require.NoError(t, err)
					assertDeclaration(got.GetReturnTypes()[1])
					got.GetReturnTypes()[1].SetSecretContentType("api_key_v2")
					got, _, err = manager.GetActionSchema(t.Context(), "issue_structured")
					require.NoError(t, err)
					assertDeclaration(got.GetReturnTypes()[1])
				}
				context := []byte("opaque generic context")
				if declared != nil && *declared != "generic" {
					context = typedContext(*declared)
				}
				recipientConfig, _ := typedRecipient(t, "recipient", context)
				_, result, _, _, _, err := manager.InvokeActionWithWaitAndEncryption(t.Context(), "issue_structured", resource, nil, time.Second, []*v2.EncryptionConfig{recipientConfig})
				require.NoError(t, err)
				require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, result)
				require.Equal(t, int32(1), calls.Load())
			})
		}
	}
}
