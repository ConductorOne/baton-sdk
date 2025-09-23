package actions

import (
	"context"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestLifecycleManagementTrait(t *testing.T) {
	ctx := context.Background()
	am := NewActionManager(ctx)

	// Create a lifecycle management trait
	trait, err := CreateLifecycleManagementTrait([]v2.BatonLifeCycleManagementActionType{
		v2.BatonLifeCycleManagementActionType_BATON_LIFE_CYCLE_MANAGEMENT_ACTION_TYPE_ENABLE,
		v2.BatonLifeCycleManagementActionType_BATON_LIFE_CYCLE_MANAGEMENT_ACTION_TYPE_DISABLE,
	})
	require.NoError(t, err)

	// Create a schema with the trait
	schema := &v2.BatonActionSchema{
		Name:        "test_action",
		DisplayName: "Test Action",
		Description: "A test action with lifecycle management trait",
		Traits:      []*anypb.Any{trait},
	}

	// Register the action
	err = am.RegisterAction(ctx, "test_action", schema, func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		return &structpb.Struct{}, nil, nil
	})
	require.NoError(t, err)

	// Test that the schema has the lifecycle trait
	assert.True(t, am.schemaHasLifecycleTrait(schema))

	// Test ListActionSchemas returns all schemas
	allSchemas, _, err := am.ListActionSchemas(ctx)
	require.NoError(t, err)
	assert.Len(t, allSchemas, 1)
	assert.Equal(t, "test_action", allSchemas[0].Name)

	// Test filtering: get only schemas with lifecycle trait
	allSchemas, _, err = am.ListActionSchemas(ctx)
	require.NoError(t, err)

	var lifecycleSchemas []*v2.BatonActionSchema
	for _, s := range allSchemas {
		if am.schemaHasLifecycleTrait(s) {
			lifecycleSchemas = append(lifecycleSchemas, s)
		}
	}

	assert.Len(t, lifecycleSchemas, 1)
	assert.Equal(t, "test_action", lifecycleSchemas[0].Name)
}

func TestSchemaWithoutLifecycleTrait(t *testing.T) {
	ctx := context.Background()
	am := NewActionManager(ctx)

	// Create a schema without lifecycle trait
	schema := &v2.BatonActionSchema{
		Name:        "regular_action",
		DisplayName: "Regular Action",
		Description: "A regular action without lifecycle management trait",
		Traits:      []*anypb.Any{},
	}

	// Register the action
	err := am.RegisterAction(ctx, "regular_action", schema, func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		return &structpb.Struct{}, nil, nil
	})
	require.NoError(t, err)

	// Test that the schema does not have the lifecycle trait
	assert.False(t, am.schemaHasLifecycleTrait(schema))

	// Test ListActionSchemas returns all schemas
	allSchemas, _, err := am.ListActionSchemas(ctx)
	require.NoError(t, err)
	assert.Len(t, allSchemas, 1)
	assert.Equal(t, "regular_action", allSchemas[0].Name)

	// Test filtering: get only schemas without lifecycle trait
	var nonLifecycleSchemas []*v2.BatonActionSchema
	for _, s := range allSchemas {
		if !am.schemaHasLifecycleTrait(s) {
			nonLifecycleSchemas = append(nonLifecycleSchemas, s)
		}
	}

	assert.Len(t, nonLifecycleSchemas, 1)
	assert.Equal(t, "regular_action", nonLifecycleSchemas[0].Name)
}
