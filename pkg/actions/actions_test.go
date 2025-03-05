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

	var userStruct structpb.Struct = structpb.Struct{
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

	var userStruct structpb.Struct = structpb.Struct{
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

	_, status, returnArgs, _, err := m.InvokeAction(ctx, "lock_account", testInput)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, status)
	require.NotNil(t, returnArgs)
	success, ok := returnArgs.Fields["success"].GetKind().(*structpb.Value_BoolValue)
	require.True(t, ok)
	require.True(t, success.BoolValue)

	_, status, rv, _, err := m.InvokeAction(ctx, "lock_account", &structpb.Struct{
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

	err := m.RegisterAction(ctx, "lock_account", testActionSchema, testAsyncActionHandler)
	require.NoError(t, err)

	schemas, _, err := m.ListActionSchemas(ctx)
	require.NoError(t, err)
	require.Len(t, schemas, 1)
	require.Equal(t, testActionSchema, schemas[0])

	schema, _, err := m.GetActionSchema(ctx, "lock_account")
	require.NoError(t, err)
	require.Equal(t, testActionSchema, schema)

	actionId, status, rv, _, err := m.InvokeAction(ctx, "lock_account", testInput)
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

func TestActionHandlerGoroutineLeaks(t *testing.T) {
	// Test case 1: Normal completion should not leak goroutines
	t.Run("normal completion", func(t *testing.T) {
		ctx := context.Background()
		m := NewActionManager(ctx)
		require.NotNil(t, m)

		err := m.RegisterAction(ctx, "lock_account", testActionSchema, testAsyncActionHandler)
		require.NoError(t, err)

		// Get initial goroutine count
		initialCount := runtime.NumGoroutine()

		actionId, status, _, _, err := m.InvokeAction(ctx, "lock_account", testInput)
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

		err := m.RegisterAction(ctx, "lock_account", testActionSchema, testAsyncCancelActionHandler)
		require.NoError(t, err)

		// Get initial goroutine count
		initialCount := runtime.NumGoroutine()

		_, status, rv, _, err := m.InvokeAction(ctx, "lock_account", testInput)
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
