package actions

import (
	"context"
	"fmt"
	"testing"
	"time"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

var testActionSchema = &v2.BatonActionSchema{
	Name: "lock_account",
	Arguments: []*config.Field{
		{
			Name:        "dn",
			DisplayName: "DN",
			Field:       &config.Field_StringField{},
			IsRequired:  true,
		},
	},
	ReturnTypes: []*config.Field{
		{
			Name:        "success",
			DisplayName: "Success",
			Field:       &config.Field_BoolField{},
		},
	},
}

func testActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	_, ok := args.Fields["dn"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing dn")
	}

	time.Sleep(2 * time.Millisecond)

	var userStruct structpb.Struct = structpb.Struct{
		Fields: map[string]*structpb.Value{
			"success": {
				Kind: &structpb.Value_BoolValue{BoolValue: true},
			},
		},
	}
	return &userStruct, nil, nil
}

func TestActionHandler(t *testing.T) {
	ctx := context.Background()
	m := NewActionManager(ctx)
	require.NotNil(t, m)

	err := m.RegisterAction(ctx, "lock_account", testActionSchema, testActionHandler)
	require.NoError(t, err)

	schemas, _, err := m.ListActionSchemas(ctx)
	require.NoError(t, err)
	require.Len(t, schemas, 1)
	require.Equal(t, testActionSchema, schemas[0])

	schema, _, err := m.GetActionSchema(ctx, "lock_account")
	require.NoError(t, err)
	require.Equal(t, testActionSchema, schema)

	_, status, returnArgs, _, err := m.InvokeAction(ctx, "lock_account", &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"dn": {
				Kind: &structpb.Value_StringValue{StringValue: "test"},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, status)
	require.NotNil(t, returnArgs)
	success, ok := returnArgs.Fields["success"].GetKind().(*structpb.Value_BoolValue)
	require.True(t, ok)
	require.True(t, success.BoolValue)

	_, status, _, _, err = m.InvokeAction(ctx, "lock_account", &structpb.Struct{
		Fields: map[string]*structpb.Value{},
	})

	require.Error(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, status)
}
