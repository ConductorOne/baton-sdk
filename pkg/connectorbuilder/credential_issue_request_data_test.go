package connectorbuilder

import (
	"context"
	"testing"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func testCredentialIssueRequestSchema() *v2.CredentialIssueRequestSchema {
	return v2.CredentialIssueRequestSchema_builder{
		Fields: []*config.Field{
			config.Field_builder{
				Name:       "scopes",
				IsRequired: true,
				StringSliceField: config.StringSliceField_builder{Rules: config.RepeatedStringRules_builder{
					MinItems: proto.Uint64(1),
					MaxItems: proto.Uint64(2),
					Unique:   true,
					ItemRules: config.StringRules_builder{
						In: []string{"keys:read", "keys:write"},
					}.Build(),
				}.Build()}.Build(),
			}.Build(),
			config.Field_builder{Name: "region", StringField: config.StringField_builder{
				Rules: config.StringRules_builder{Pattern: proto.String("^[a-z]+-[a-z]+-[0-9]+$")}.Build(),
			}.Build()}.Build(),
			config.Field_builder{Name: "global", BoolField: &config.BoolField{}}.Build(),
			config.Field_builder{Name: "ttl_seconds", IntField: config.IntField_builder{
				Rules: config.Int64Rules_builder{Gte: proto.Int64(60), Lte: proto.Int64(3600)}.Build(),
			}.Build()}.Build(),
		},
		Constraints: []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE,
				FieldNames: []string{"region", "global"},
			}.Build(),
		},
	}.Build()
}

func TestValidateCredentialIssueRequestData(t *testing.T) {
	schema := testCredentialIssueRequestSchema()
	valid := func() *structpb.Struct {
		value, err := structpb.NewStruct(map[string]any{
			"scopes":      []any{"keys:read"},
			"region":      "us-east-1",
			"ttl_seconds": float64(300),
		})
		require.NoError(t, err)
		return value
	}

	require.NoError(t, ValidateCredentialIssueRequestData(schema, valid()))
	require.NoError(t, ValidateCredentialIssueRequestData(nil, nil), "legacy descriptors accept legacy requests")

	tests := []struct {
		name      string
		mutate    func(*structpb.Struct)
		wantError string
	}{
		{
			name: "unknown field",
			mutate: func(data *structpb.Struct) {
				data.GetFields()["provider_flag"] = structpb.NewBoolValue(true)
			},
			wantError: `unknown field "provider_flag"`,
		},
		{
			name: "missing required field",
			mutate: func(data *structpb.Struct) {
				delete(data.GetFields(), "scopes")
			},
			wantError: `field "scopes" is required`,
		},
		{
			name: "wrong collection type",
			mutate: func(data *structpb.Struct) {
				data.GetFields()["scopes"] = structpb.NewStringValue("keys:read")
			},
			wantError: `field "scopes" must be a string list`,
		},
		{
			name: "wrong collection item type",
			mutate: func(data *structpb.Struct) {
				data.GetFields()["scopes"] = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewNumberValue(1)}})
			},
			wantError: `field "scopes" must contain only strings`,
		},
		{
			name: "field rule",
			mutate: func(data *structpb.Struct) {
				data.GetFields()["scopes"] = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewStringValue("admin")}})
			},
			wantError: "value must be one of",
		},
		{
			name: "integer must be integral",
			mutate: func(data *structpb.Struct) {
				data.GetFields()["ttl_seconds"] = structpb.NewNumberValue(60.5)
			},
			wantError: `field "ttl_seconds" must be an integer`,
		},
		{
			name: "integer rule",
			mutate: func(data *structpb.Struct) {
				data.GetFields()["ttl_seconds"] = structpb.NewNumberValue(30)
			},
			wantError: "greater than or equal to 60",
		},
		{
			name: "zero integer still validates rules",
			mutate: func(data *structpb.Struct) {
				data.GetFields()["ttl_seconds"] = structpb.NewNumberValue(0)
			},
			wantError: "greater than or equal to 60",
		},
		{
			name: "string rule",
			mutate: func(data *structpb.Struct) {
				data.GetFields()["region"] = structpb.NewStringValue("USA")
			},
			wantError: "must match pattern",
		},
		{
			name: "empty string still validates rules",
			mutate: func(data *structpb.Struct) {
				data.GetFields()["region"] = structpb.NewStringValue("")
			},
			wantError: "must match pattern",
		},
		{
			name: "empty list still validates rules",
			mutate: func(data *structpb.Struct) {
				data.GetFields()["scopes"] = structpb.NewListValue(&structpb.ListValue{})
			},
			wantError: "at least 1 items",
		},
		{
			name: "constraint",
			mutate: func(data *structpb.Struct) {
				data.GetFields()["global"] = structpb.NewBoolValue(false)
			},
			wantError: "mutually exclusive",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := valid()
			tt.mutate(data)
			err := ValidateCredentialIssueRequestData(schema, data)
			require.ErrorContains(t, err, tt.wantError)
		})
	}
}

func TestValidateCredentialIssueRequestSchema(t *testing.T) {
	t.Run("rejects duplicate fields", func(t *testing.T) {
		field := config.Field_builder{Name: "scope", StringField: &config.StringField{}}.Build()
		schema := v2.CredentialIssueRequestSchema_builder{Fields: []*config.Field{field, field}}.Build()
		require.ErrorContains(t, ValidateCredentialIssueRequestSchema(schema), `duplicate request schema field "scope"`)
	})

	t.Run("rejects unsupported output fields", func(t *testing.T) {
		schema := v2.CredentialIssueRequestSchema_builder{Fields: []*config.Field{
			config.Field_builder{Name: "result", ResourceField: &config.ResourceField{}}.Build(),
		}}.Build()
		require.ErrorContains(t, ValidateCredentialIssueRequestSchema(schema), "unsupported type")
	})

	t.Run("rejects secret fields", func(t *testing.T) {
		schema := v2.CredentialIssueRequestSchema_builder{Fields: []*config.Field{
			config.Field_builder{Name: "token", IsSecret: true, StringField: &config.StringField{}}.Build(),
		}}.Build()
		require.ErrorContains(t, ValidateCredentialIssueRequestSchema(schema), `field "token" must not be secret`)
	})

	t.Run("rejects invalid rules", func(t *testing.T) {
		schema := v2.CredentialIssueRequestSchema_builder{Fields: []*config.Field{
			config.Field_builder{Name: "region", StringField: config.StringField_builder{
				Rules: config.StringRules_builder{Pattern: proto.String("[")}.Build(),
			}.Build()}.Build(),
		}}.Build()
		require.ErrorContains(t, ValidateCredentialIssueRequestSchema(schema), "invalid pattern")
	})

	t.Run("rejects constraint references to unknown fields", func(t *testing.T) {
		schema := v2.CredentialIssueRequestSchema_builder{
			Fields: []*config.Field{config.Field_builder{Name: "region", StringField: &config.StringField{}}.Build()},
			Constraints: []*config.Constraint{config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_REQUIRED_TOGETHER,
				FieldNames: []string{"region", "account"},
			}.Build()},
		}.Build()
		require.ErrorContains(t, ValidateCredentialIssueRequestSchema(schema), `unknown field "account"`)
	})
}

func TestValidateCredentialIssueRequestDataTypes(t *testing.T) {
	schema := v2.CredentialIssueRequestSchema_builder{Fields: []*config.Field{
		config.Field_builder{Name: "enabled", BoolField: &config.BoolField{}}.Build(),
		config.Field_builder{Name: "labels", StringMapField: &config.StringMapField{}}.Build(),
	}}.Build()

	valid, err := structpb.NewStruct(map[string]any{
		"enabled": true,
		"labels":  map[string]any{"environment": "production"},
	})
	require.NoError(t, err)
	require.NoError(t, ValidateCredentialIssueRequestData(schema, valid))

	valid.GetFields()["enabled"] = structpb.NewStringValue("true")
	require.ErrorContains(t, ValidateCredentialIssueRequestData(schema, valid), `field "enabled" must be a boolean`)
	valid.GetFields()["enabled"] = structpb.NewBoolValue(true)
	valid.GetFields()["labels"] = structpb.NewStringValue("production")
	require.ErrorContains(t, ValidateCredentialIssueRequestData(schema, valid), `field "labels" must be an object`)

	emptyMapSchema := v2.CredentialIssueRequestSchema_builder{Fields: []*config.Field{
		config.Field_builder{Name: "labels", StringMapField: config.StringMapField_builder{
			Rules: config.StringMapRules_builder{IsRequired: true}.Build(),
		}.Build()}.Build(),
	}}.Build()
	emptyMap, err := structpb.NewStruct(map[string]any{"labels": map[string]any{}})
	require.NoError(t, err)
	require.ErrorContains(t, ValidateCredentialIssueRequestData(emptyMapSchema, emptyMap), "zero-value")
}

func TestValidateCredentialIssueRequestDataReportsFieldsInSchemaOrder(t *testing.T) {
	schema := v2.CredentialIssueRequestSchema_builder{Fields: []*config.Field{
		config.Field_builder{Name: "first", IsRequired: true, StringField: &config.StringField{}}.Build(),
		config.Field_builder{Name: "second", IsRequired: true, StringField: &config.StringField{}}.Build(),
	}}.Build()
	for range 20 {
		err := ValidateCredentialIssueRequestData(schema, &structpb.Struct{})
		require.ErrorContains(t, err, `field "first" is required`)
	}
}

func TestValidateCredentialIssueRequestConstraints(t *testing.T) {
	fields := []*config.Field{
		config.Field_builder{Name: "a", StringField: &config.StringField{}}.Build(),
		config.Field_builder{Name: "b", StringField: &config.StringField{}}.Build(),
		config.Field_builder{Name: "c", StringField: &config.StringField{}}.Build(),
	}
	tests := []struct {
		name           string
		constraint     *config.Constraint
		values         map[string]any
		wantError      string
		acceptedValues map[string]any
	}{
		{
			name:           "required together",
			constraint:     config.Constraint_builder{Kind: config.ConstraintKind_CONSTRAINT_KIND_REQUIRED_TOGETHER, FieldNames: []string{"a", "b"}}.Build(),
			values:         map[string]any{"a": "one"},
			wantError:      "required together",
			acceptedValues: map[string]any{"a": "one", "b": "two"},
		},
		{
			name:           "at least one",
			constraint:     config.Constraint_builder{Kind: config.ConstraintKind_CONSTRAINT_KIND_AT_LEAST_ONE, FieldNames: []string{"a", "b"}}.Build(),
			values:         map[string]any{},
			wantError:      "requires at least one",
			acceptedValues: map[string]any{"b": "two"},
		},
		{
			name:           "mutually exclusive",
			constraint:     config.Constraint_builder{Kind: config.ConstraintKind_CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE, FieldNames: []string{"a", "b"}}.Build(),
			values:         map[string]any{"a": "one", "b": "two"},
			wantError:      "mutually exclusive",
			acceptedValues: map[string]any{"a": "one"},
		},
		{
			name:           "dependent on",
			constraint:     config.Constraint_builder{Kind: config.ConstraintKind_CONSTRAINT_KIND_DEPENDENT_ON, FieldNames: []string{"a"}, SecondaryFieldNames: []string{"b", "c"}}.Build(),
			values:         map[string]any{"a": "one", "b": "two"},
			wantError:      "depend on",
			acceptedValues: map[string]any{"a": "one", "b": "two", "c": "three"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := v2.CredentialIssueRequestSchema_builder{Fields: fields, Constraints: []*config.Constraint{tt.constraint}}.Build()
			data, err := structpb.NewStruct(tt.values)
			require.NoError(t, err)
			require.ErrorContains(t, ValidateCredentialIssueRequestData(schema, data), tt.wantError)
			accepted, err := structpb.NewStruct(tt.acceptedValues)
			require.NoError(t, err)
			require.NoError(t, ValidateCredentialIssueRequestData(schema, accepted))
		})
	}
}

func TestCredentialIssueTypedInputsWireRoundTrip(t *testing.T) {
	schema := testCredentialIssueRequestSchema()
	data, err := structpb.NewStruct(map[string]any{"scopes": []any{"keys:read"}})
	require.NoError(t, err)
	descriptor := v2.CredentialIssueOptionDescriptor_builder{RequestSchema: schema}.Build()
	request := v2.IssueCredentialRequest_builder{RequestData: data}.Build()

	descriptorBytes, err := proto.Marshal(descriptor)
	require.NoError(t, err)
	requestBytes, err := proto.Marshal(request)
	require.NoError(t, err)

	descriptorRoundTrip := &v2.CredentialIssueOptionDescriptor{}
	require.NoError(t, proto.Unmarshal(descriptorBytes, descriptorRoundTrip))
	requestRoundTrip := &v2.IssueCredentialRequest{}
	require.NoError(t, proto.Unmarshal(requestBytes, requestRoundTrip))
	require.True(t, proto.Equal(schema, descriptorRoundTrip.GetRequestSchema()))
	require.True(t, proto.Equal(data, requestRoundTrip.GetRequestData()))

	legacyRequest := v2.IssueCredentialRequest_builder{RequestId: "legacy"}.Build()
	require.Nil(t, legacyRequest.GetRequestData())
}

func TestIssueCredentialValidatesAndForwardsRequestData(t *testing.T) {
	ctx := context.Background()
	issuer := newTestCredentialIssuer("service_account")
	issuer.capabilityDetails = v2.CredentialDetailsCredentialIssue_builder{
		Options: []*v2.CredentialIssueOptionDescriptor{v2.CredentialIssueOptionDescriptor_builder{
			Option:               v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
			ResourceMode:         v2.CredentialResourceMode_CREDENTIAL_RESOURCE_MODE_DISCOVERABLE,
			SecretResourceTypeId: "secret",
			RequestSchema:        testCredentialIssueRequestSchema(),
		}.Build()},
		PreferredOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_API_KEY,
	}.Build()
	connector, err := NewConnector(ctx, newTestConnector([]ResourceSyncer{issuer, newTestCredentialSecretDeleter()}))
	require.NoError(t, err)
	data, err := structpb.NewStruct(map[string]any{"scopes": []any{"keys:read"}})
	require.NoError(t, err)
	request := v2.IssueCredentialRequest_builder{
		IdentityId:        v2.ResourceId_builder{ResourceType: "service_account", Resource: "sa-1"}.Build(),
		CredentialOptions: v2.CredentialIssueOptions_builder{SecretResourceTypeId: "secret", ApiKey: &v2.CredentialIssueOptions_ApiKey{}}.Build(),
		EncryptionConfigs: []*v2.EncryptionConfig{newIssueEncryptionConfig(t)},
		RequestId:         "request-data-1",
		RequestData:       data,
	}.Build()

	request.GetRequestData().GetFields()["unknown"] = structpb.NewStringValue("value")
	_, err = connector.IssueCredential(ctx, request)
	require.ErrorContains(t, err, "unknown field")
	require.Nil(t, issuer.lastInput, "validation must precede provider mutation")

	delete(request.GetRequestData().GetFields(), "unknown")
	_, err = connector.IssueCredential(ctx, request)
	require.NoError(t, err)
	require.True(t, proto.Equal(data, issuer.lastInput.RequestData))
}
