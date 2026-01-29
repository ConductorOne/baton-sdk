package actions

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

var testActionSchema = v2.BatonActionSchema_builder{
	Name: "lock_account",
	Arguments: []*config.Field{
		config.Field_builder{
			Name:        "dn",
			DisplayName: "DN",
			StringField: &config.StringField{},
			IsRequired:  true,
		}.Build(),
	},
	ReturnTypes: []*config.Field{
		config.Field_builder{
			Name:        "success",
			DisplayName: "Success",
			BoolField:   &config.BoolField{},
		}.Build(),
	},
}.Build()

func testActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	_, ok := args.Fields["dn"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing dn")
	}

	var userStruct = structpb.Struct{
		Fields: map[string]*structpb.Value{
			"success": {
				Kind: &structpb.Value_BoolValue{BoolValue: true},
			},
		},
	}
	return &userStruct, nil, nil
}

func testAsyncActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	_, ok := args.Fields["dn"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing dn")
	}

	for i := 0; i < 12; i++ {
		select {
		case <-ctx.Done():
			return nil, nil, status.Error(codes.Canceled, "context canceled")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	var userStruct = structpb.Struct{
		Fields: map[string]*structpb.Value{
			"success": {
				Kind: &structpb.Value_BoolValue{BoolValue: true},
			},
		},
	}
	return &userStruct, nil, nil
}

var testInput = &structpb.Struct{
	Fields: map[string]*structpb.Value{
		"dn": {
			Kind: &structpb.Value_StringValue{StringValue: "test"},
		},
	},
}

func testAsyncCancelActionHandler(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	_, ok := args.Fields["dn"].GetKind().(*structpb.Value_StringValue)
	if !ok {
		return nil, nil, fmt.Errorf("missing dn")
	}

	// Create a child context that we'll cancel after a short delay
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start a goroutine to cancel after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	for i := 0; i < 12; i++ {
		select {
		case <-childCtx.Done():
			return nil, nil, status.Error(codes.Canceled, "context canceled")
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	var userStruct = structpb.Struct{
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

	err := m.Register(ctx, testActionSchema, testActionHandler)
	require.NoError(t, err)

	schemas, _, err := m.ListActionSchemas(ctx, "")
	require.NoError(t, err)
	require.Len(t, schemas, 1)
	require.Equal(t, testActionSchema, schemas[0])

	schema, _, err := m.GetActionSchema(ctx, "lock_account")
	require.NoError(t, err)
	require.Equal(t, testActionSchema, schema)

	_, status, returnArgs, _, err := m.InvokeAction(ctx, "lock_account", "", testInput)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, status)
	require.NotNil(t, returnArgs)
	success, ok := returnArgs.Fields["success"].GetKind().(*structpb.Value_BoolValue)
	require.True(t, ok)
	require.True(t, success.BoolValue)

	_, status, rv, _, err := m.InvokeAction(ctx, "lock_account", "", &structpb.Struct{
		Fields: map[string]*structpb.Value{},
	})
	expectedRv := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"error": {
				Kind: &structpb.Value_StringValue{StringValue: "missing dn"},
			},
		},
	}
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, status)
	require.Equal(t, expectedRv, rv)
}

func TestAsyncActionHandler(t *testing.T) {
	ctx := context.Background()
	m := NewActionManager(ctx)
	require.NotNil(t, m)

	err := m.Register(ctx, testActionSchema, testAsyncActionHandler)
	require.NoError(t, err)

	schemas, _, err := m.ListActionSchemas(ctx, "")
	require.NoError(t, err)
	require.Len(t, schemas, 1)
	require.Equal(t, testActionSchema, schemas[0])

	schema, _, err := m.GetActionSchema(ctx, "lock_account")
	require.NoError(t, err)
	require.Equal(t, testActionSchema, schema)

	actionId, status, rv, _, err := m.InvokeAction(ctx, "lock_account", "", testInput)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, status)
	require.Nil(t, rv)

	status, name, _, _, err := m.GetActionStatus(ctx, actionId)
	require.NoError(t, err)
	require.Equal(t, "lock_account", name)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, status)

	time.Sleep(1 * time.Second)

	status, name, rv, _, err = m.GetActionStatus(ctx, actionId)
	require.NoError(t, err)
	require.Equal(t, "lock_account", name)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, status)
	require.NotNil(t, rv)
	success, ok := rv.Fields["success"].GetKind().(*structpb.Value_BoolValue)
	require.True(t, ok)
	require.True(t, success.BoolValue)
}

func TestConstraintValidation(t *testing.T) {
	t.Run("nil constraint returns no error", func(t *testing.T) {
		constraints := []*config.Constraint{nil}
		err := validateActionConstraints(constraints, &structpb.Struct{Fields: map[string]*structpb.Value{}})
		require.NoError(t, err)
	})

	t.Run("nil structpb.Value is not considered present (no panic)", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_AT_LEAST_ONE,
				FieldNames: []string{"field_a"},
			}.Build(),
		}
		args := &structpb.Struct{Fields: map[string]*structpb.Value{"field_a": nil}}
		err := validateActionConstraints(constraints, args)
		require.Error(t, err)
	})

	t.Run("RequiredTogether - both present passes", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_REQUIRED_TOGETHER,
				FieldNames: []string{"field_a", "field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"field_a": structpb.NewStringValue("value_a"),
				"field_b": structpb.NewStringValue("value_b"),
			},
		}
		err := validateActionConstraints(constraints, args)
		require.NoError(t, err)
	})

	t.Run("RequiredTogether - one missing fails", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_REQUIRED_TOGETHER,
				FieldNames: []string{"field_a", "field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"field_a": structpb.NewStringValue("value_a"),
			},
		}
		err := validateActionConstraints(constraints, args)
		require.Error(t, err)
		require.Contains(t, err.Error(), "fields required together")
	})

	t.Run("RequiredTogether - none present passes", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_REQUIRED_TOGETHER,
				FieldNames: []string{"field_a", "field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{},
		}
		err := validateActionConstraints(constraints, args)
		require.NoError(t, err)
	})

	t.Run("MutuallyExclusive - none present passes", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE,
				FieldNames: []string{"field_a", "field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{},
		}
		err := validateActionConstraints(constraints, args)
		require.NoError(t, err)
	})

	t.Run("MutuallyExclusive - one present passes", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE,
				FieldNames: []string{"field_a", "field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"field_a": structpb.NewStringValue("value_a"),
			},
		}
		err := validateActionConstraints(constraints, args)
		require.NoError(t, err)
	})

	t.Run("MutuallyExclusive - two present fails", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE,
				FieldNames: []string{"field_a", "field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"field_a": structpb.NewStringValue("value_a"),
				"field_b": structpb.NewStringValue("value_b"),
			},
		}
		err := validateActionConstraints(constraints, args)
		require.Error(t, err)
		require.Contains(t, err.Error(), "mutually exclusive")
	})

	t.Run("AtLeastOne - none present fails", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_AT_LEAST_ONE,
				FieldNames: []string{"field_a", "field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{},
		}
		err := validateActionConstraints(constraints, args)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one required")
	})

	t.Run("AtLeastOne - one present passes", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_AT_LEAST_ONE,
				FieldNames: []string{"field_a", "field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"field_a": structpb.NewStringValue("value_a"),
			},
		}
		err := validateActionConstraints(constraints, args)
		require.NoError(t, err)
	})

	t.Run("DependentOn - primary present with secondary missing fails", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:                config.ConstraintKind_CONSTRAINT_KIND_DEPENDENT_ON,
				FieldNames:          []string{"field_a"},
				SecondaryFieldNames: []string{"field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"field_a": structpb.NewStringValue("value_a"),
			},
		}
		err := validateActionConstraints(constraints, args)
		require.Error(t, err)
		require.Contains(t, err.Error(), "depend on")
	})

	t.Run("DependentOn - both present passes", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:                config.ConstraintKind_CONSTRAINT_KIND_DEPENDENT_ON,
				FieldNames:          []string{"field_a"},
				SecondaryFieldNames: []string{"field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"field_a": structpb.NewStringValue("value_a"),
				"field_b": structpb.NewStringValue("value_b"),
			},
		}
		err := validateActionConstraints(constraints, args)
		require.NoError(t, err)
	})

	t.Run("DependentOn - primary not present passes", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:                config.ConstraintKind_CONSTRAINT_KIND_DEPENDENT_ON,
				FieldNames:          []string{"field_a"},
				SecondaryFieldNames: []string{"field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{},
		}
		err := validateActionConstraints(constraints, args)
		require.NoError(t, err)
	})

	t.Run("null value is not considered present", func(t *testing.T) {
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_REQUIRED_TOGETHER,
				FieldNames: []string{"field_a", "field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"field_a": structpb.NewStringValue("value_a"),
				"field_b": structpb.NewNullValue(),
			},
		}
		err := validateActionConstraints(constraints, args)
		require.Error(t, err)
		require.Contains(t, err.Error(), "fields required together")
	})

	t.Run("nil args passes with no constraints", func(t *testing.T) {
		err := validateActionConstraints(nil, nil)
		require.NoError(t, err)
	})

	t.Run("empty constraints passes", func(t *testing.T) {
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"field_a": structpb.NewStringValue("value_a"),
			},
		}
		err := validateActionConstraints([]*config.Constraint{}, args)
		require.NoError(t, err)
	})

	t.Run("duplicate field names are deduplicated - RequiredTogether", func(t *testing.T) {
		// If field_a is listed twice and only field_a is present,
		// without deduplication this would incorrectly pass (2 present == 2 in list)
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_REQUIRED_TOGETHER,
				FieldNames: []string{"field_a", "field_a", "field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"field_a": structpb.NewStringValue("value_a"),
			},
		}
		err := validateActionConstraints(constraints, args)
		require.Error(t, err)
		require.Contains(t, err.Error(), "fields required together")
	})

	t.Run("duplicate field names are deduplicated - MutuallyExclusive", func(t *testing.T) {
		// If field_a is listed twice and only field_a is present,
		// without deduplication this would incorrectly fail (2 present > 1)
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:       config.ConstraintKind_CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE,
				FieldNames: []string{"field_a", "field_a", "field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"field_a": structpb.NewStringValue("value_a"),
			},
		}
		err := validateActionConstraints(constraints, args)
		require.NoError(t, err)
	})

	t.Run("duplicate secondary field names are deduplicated - DependentOn", func(t *testing.T) {
		// Secondary field names should also be deduplicated
		constraints := []*config.Constraint{
			config.Constraint_builder{
				Kind:                config.ConstraintKind_CONSTRAINT_KIND_DEPENDENT_ON,
				FieldNames:          []string{"field_a"},
				SecondaryFieldNames: []string{"field_b", "field_b"},
			}.Build(),
		}
		args := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"field_a": structpb.NewStringValue("value_a"),
				"field_b": structpb.NewStringValue("value_b"),
			},
		}
		err := validateActionConstraints(constraints, args)
		require.NoError(t, err)
	})
}

func TestActionHandlerGoroutineLeaks(t *testing.T) {
	// Test case 1: Normal completion should not leak goroutines
	t.Run("normal completion", func(t *testing.T) {
		ctx := context.Background()
		m := NewActionManager(ctx)
		require.NotNil(t, m)

		err := m.Register(ctx, testActionSchema, testAsyncActionHandler)
		require.NoError(t, err)

		// Get initial goroutine count
		initialCount := runtime.NumGoroutine()

		actionId, status, _, _, err := m.InvokeAction(ctx, "lock_account", "", testInput)
		require.NoError(t, err)
		require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, status)

		// Wait for completion
		time.Sleep(1 * time.Second)

		// Check final status
		status, name, _, _, err := m.GetActionStatus(ctx, actionId)
		require.NoError(t, err)
		require.Equal(t, "lock_account", name)
		require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, status)

		// Give a small grace period for goroutines to clean up
		time.Sleep(100 * time.Millisecond)

		// Verify no goroutine leaks
		finalCount := runtime.NumGoroutine()
		require.LessOrEqual(t, finalCount, initialCount+1, "goroutine leak detected after normal completion")
	})

	// Test case 2: Cancelled context should not leak goroutines
	t.Run("context cancellation", func(t *testing.T) {
		ctx := context.Background()
		m := NewActionManager(ctx)
		require.NotNil(t, m)

		err := m.Register(ctx, testActionSchema, testAsyncCancelActionHandler)
		require.NoError(t, err)

		// Get initial goroutine count
		initialCount := runtime.NumGoroutine()

		_, status, rv, _, err := m.InvokeAction(ctx, "lock_account", "", testInput)
		require.NoError(t, err)
		require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, status)

		errMsg := rv.Fields["error"].GetKind().(*structpb.Value_StringValue).StringValue
		require.Contains(t, errMsg, "context canceled")

		// Give a small grace period for goroutines to clean up
		time.Sleep(100 * time.Millisecond)

		// Verify no goroutine leaks
		finalCount := runtime.NumGoroutine()
		require.LessOrEqual(t, finalCount, initialCount+1, "goroutine leak detected after context cancellation")
	})
}

func TestRegisterDynamic(t *testing.T) {
	ctx := context.Background()
	m := NewActionManager(ctx)

	callCount := 0
	refreshFunc := func(ctx context.Context) (*v2.BatonActionSchema, error) {
		callCount++
		return v2.BatonActionSchema_builder{
			Name:        "dynamic_action",
			Description: fmt.Sprintf("v%d", callCount),
		}.Build(), nil
	}
	handler := func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		return nil, nil, nil
	}

	// RegisterDynamic calls refreshFunc for initial schema
	err := m.RegisterDynamic(ctx, refreshFunc, handler)
	require.NoError(t, err)
	require.Equal(t, 1, callCount)

	// ListActionSchemas refreshes the schema
	schemas, _, err := m.ListActionSchemas(ctx, "")
	require.NoError(t, err)
	require.Len(t, schemas, 1)
	require.Equal(t, "v2", schemas[0].GetDescription())
	require.Equal(t, 2, callCount)

	// Each ListActionSchemas call refreshes again
	schemas, _, _ = m.ListActionSchemas(ctx, "")
	require.Equal(t, "v3", schemas[0].GetDescription())

	// Resource-scoped dynamic action
	resourceCallCount := 0
	registry, _ := m.GetTypeRegistry(ctx, "test_type")
	err = registry.RegisterDynamic(ctx, func(ctx context.Context) (*v2.BatonActionSchema, error) {
		resourceCallCount++
		return v2.BatonActionSchema_builder{Name: "resource_action", Description: fmt.Sprintf("v%d", resourceCallCount)}.Build(), nil
	}, handler)
	require.NoError(t, err)

	// Listing specific resource type only refreshes that type's actions
	schemas, _, _ = m.ListActionSchemas(ctx, "test_type")
	require.Len(t, schemas, 1)
	require.Equal(t, "test_type", schemas[0].GetResourceTypeId())
}
