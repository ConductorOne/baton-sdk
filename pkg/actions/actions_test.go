package actions

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"testing"
	"time"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
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

// The data race this test guards against is only detectable under -race,
// which the plain CI go-test job does not enable for this package; `make
// race-check` is the out-of-band gate that runs it. The assertions at the end
// only cover the exported snapshot accessor.
func TestCleanupOldActionsDuringConcurrentStatusWrites(t *testing.T) {
	ctx := t.Context()
	m := NewActionManager(ctx)

	// The cleanup loop only visits the len(actions)-maxOldActions oldest
	// entries, so the concurrent writer must target the first-created action.
	// The sort is unstable and StartedAt values can tie, so push the target
	// strictly earlier to make its position deterministic.
	oldest := m.GetNewAction("churn")
	oldest.StartedAt = time.Now().Add(-time.Hour)
	for i := 0; i < maxOldActions; i++ {
		m.GetNewAction("churn")
	}

	stop := make(chan struct{})
	started := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		first := true
		for {
			select {
			case <-stop:
				return
			default:
				// Write the status under the lock directly: lifecycle
				// transitions are single-shot, so no public API writes the
				// status repeatedly, and the instrument needs a sustained
				// locked writer to race cleanup's read against.
				oldest.Lock()
				oldest.Status = v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING
				oldest.Unlock()
				if first {
					close(started)
					first = false
				}
			}
		}
	}()

	// Wait for the writer's first write: otherwise it may only be scheduled
	// after close(stop), never race cleanup, and leave the action PENDING.
	<-started

	// Fails under -race if cleanup reads action status without the lock.
	m.CleanupOldActions(ctx)

	close(stop)
	<-writerDone

	// The exported snapshot accessor reads the same state race-free.
	id, actionStatus, _, _ := oldest.Result()
	require.Equal(t, oldest.Id, id)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, actionStatus)
}

func TestOutstandingActionLifecycleTransitions(t *testing.T) {
	const (
		pending  = v2.BatonActionStatus_BATON_ACTION_STATUS_PENDING
		running  = v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING
		complete = v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE
		failed   = v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED
	)
	cases := []struct {
		name string
		from v2.BatonActionStatus
		to   v2.BatonActionStatus
		want v2.BatonActionStatus
	}{
		{"pending to running", pending, running, running},
		{"pending to complete", pending, complete, complete},
		{"pending to failed", pending, failed, failed},
		{"running to complete", running, complete, complete},
		{"running to failed", running, failed, failed},
		{"running to running rejected", running, running, running},
		{"complete rejects running", complete, running, complete},
		{"complete rejects failed", complete, failed, complete},
		{"failed rejects complete", failed, complete, failed},
		{"failed rejects running", failed, running, failed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			oa := NewOutstandingAction("id", "lifecycle")
			oa.Status = tc.from
			oa.SetStatus(t.Context(), tc.to)
			_, actionStatus, _, _ := oa.Result()
			require.Equal(t, tc.want, actionStatus)
		})
	}
}

func TestLateSuccessAfterCancelReplacesIt(t *testing.T) {
	ctx := t.Context()
	oa := NewOutstandingAction("id", "cancelled")
	oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING)
	oa.setCancelled(ctx, context.Canceled)

	rv, err := structpb.NewStruct(map[string]any{"success": true})
	require.NoError(t, err)
	oa.setOutcome(ctx, rv, nil, nil)

	// The cancellation was a transport event; the handler's success is the
	// action's real outcome.
	_, actionStatus, gotRv, _ := oa.Result()
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, actionStatus)
	require.True(t, gotRv.Fields["success"].GetBoolValue())
	require.Nil(t, gotRv.Fields["error"])
}

func TestLateFailureAfterCancelReplacesError(t *testing.T) {
	ctx := t.Context()
	oa := NewOutstandingAction("id", "cancelled")
	oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING)
	oa.setCancelled(ctx, context.Canceled)

	oa.setOutcome(ctx, nil, nil, fmt.Errorf("upstream rejected the request"))

	_, actionStatus, gotRv, _ := oa.Result()
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)
	require.Equal(t, "upstream rejected the request", gotRv.Fields["error"].GetStringValue())
}

func TestLateSuccessAfterRealFailureIsDropped(t *testing.T) {
	ctx := t.Context()
	oa := NewOutstandingAction("id", "panicked")
	oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING)
	oa.SetError(ctx, fmt.Errorf("panic in action handler"))

	rv, err := structpb.NewStruct(map[string]any{"success": true})
	require.NoError(t, err)
	oa.setOutcome(ctx, rv, nil, nil)

	// A real handler failure is final; only cancellation is provisional.
	_, actionStatus, gotRv, _ := oa.Result()
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)
	require.Equal(t, "panic in action handler", gotRv.Fields["error"].GetStringValue())
	require.Nil(t, gotRv.Fields["success"])
}

func TestPublishedOutcomeIsIsolatedFromHandler(t *testing.T) {
	ctx := t.Context()
	oa := NewOutstandingAction("id", "isolated")
	oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING)

	rv, err := structpb.NewStruct(map[string]any{"k": "v"})
	require.NoError(t, err)
	anno, err := anypb.New(structpb.NewStringValue("original"))
	require.NoError(t, err)
	oa.setOutcome(ctx, rv, annotations.Annotations{anno}, nil)

	// The handler owns what it returned and may keep mutating it; the
	// published outcome must not change. Annotations are deep-copied, so
	// element mutation is isolated too.
	rv.Fields["mutated"] = structpb.NewBoolValue(true)
	anno.TypeUrl = "mutated"

	_, actionStatus, gotRv, gotAnnos := oa.Result()
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, actionStatus)
	require.Nil(t, gotRv.Fields["mutated"])
	require.Equal(t, "v", gotRv.Fields["k"].GetStringValue())
	require.Len(t, gotAnnos, 1)
	require.NotEqual(t, "mutated", gotAnnos[0].TypeUrl)

	// Under -race: concurrent handler mutation against reader marshals.
	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				rv.Fields[fmt.Sprintf("m%d", i)] = structpb.NewBoolValue(true)
			}
		}
	}()
	for i := 0; i < 100; i++ {
		_, _, snapshot, _ := oa.Result()
		_, err := proto.Marshal(snapshot)
		require.NoError(t, err)
	}
	close(stop)
	<-writerDone
}

func TestCleanupRetainsProvisionallyCancelledActions(t *testing.T) {
	ctx := t.Context()
	m := NewActionManager(ctx)

	// Two terminal actions old enough for cleanup to visit: one provisional
	// cancellation whose handler may still publish, one real failure.
	provisional := m.GetNewAction("cancelled")
	provisional.StartedAt = time.Now().Add(-2 * time.Hour)
	provisional.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING)
	provisional.setCancelled(ctx, context.Canceled)

	failed := m.GetNewAction("failed")
	failed.StartedAt = time.Now().Add(-time.Hour)
	failed.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING)
	failed.SetError(ctx, fmt.Errorf("real failure"))

	for i := 0; i < maxOldActions; i++ {
		m.GetNewAction("churn")
	}

	m.CleanupOldActions(ctx)

	// The real failure is evictable; the provisional record must survive so
	// the handler's late outcome stays observable.
	_, _, _, _, err := m.GetActionStatus(ctx, failed.Id)
	require.Error(t, err)

	actionStatus, _, _, _, err := m.GetActionStatus(ctx, provisional.Id)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)

	rv, err := structpb.NewStruct(map[string]any{"success": true})
	require.NoError(t, err)
	provisional.setOutcome(ctx, rv, nil, nil)

	actionStatus, _, gotRv, _, err := m.GetActionStatus(ctx, provisional.Id)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, actionStatus)
	require.True(t, gotRv.Fields["success"].GetBoolValue())
}

func TestPanicAfterCancelIsFinal(t *testing.T) {
	ctx := t.Context()
	oa := NewOutstandingAction("id", "cancelled-then-panicked")
	oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING)
	oa.setCancelled(ctx, context.Canceled)

	// The recovery path reports a panic through SetError: a real handler
	// failure that replaces the provisional mark and becomes final.
	oa.SetError(ctx, fmt.Errorf("panic in action handler: boom"))
	require.False(t, oa.isProvisional())

	rv, err := structpb.NewStruct(map[string]any{"success": true})
	require.NoError(t, err)
	oa.setOutcome(ctx, rv, nil, nil)

	_, actionStatus, gotRv, _ := oa.Result()
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)
	require.Equal(t, "panic in action handler: boom", gotRv.Fields["error"].GetStringValue())
	require.Nil(t, gotRv.Fields["success"])
}

func TestCancelAfterCompletionIsRejected(t *testing.T) {
	ctx := t.Context()
	oa := NewOutstandingAction("id", "completed")
	oa.SetStatus(ctx, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING)

	rv, err := structpb.NewStruct(map[string]any{"success": true})
	require.NoError(t, err)
	oa.setOutcome(ctx, rv, nil, nil)

	oa.setCancelled(ctx, context.Canceled)

	// COMPLETE is truly terminal: the cancellation neither marks the action
	// provisional nor touches the published outcome.
	require.False(t, oa.isProvisional())
	_, actionStatus, gotRv, _ := oa.Result()
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, actionStatus)
	require.True(t, gotRv.Fields["success"].GetBoolValue())
	require.Nil(t, gotRv.Fields["error"])
}

func TestCancelledInvokeStatusErrorPairing(t *testing.T) {
	ctx := t.Context()
	m := NewActionManager(ctx)
	require.NoError(t, m.Register(ctx, testActionSchema, testActionHandler))

	// The handler succeeds instantly while each request is cancelled
	// concurrently, sampling the invoke select race from both sides. The
	// orderings can't be forced individually, but every interleaving must
	// satisfy the pairing contract: a cancellation error only ever
	// accompanies FAILED, and an errorless return is never FAILED (RUNNING
	// is tolerated only for a pathological scheduler stall past the inline
	// wait).
	for i := 0; i < 200; i++ {
		invokeCtx, cancel := context.WithCancel(ctx)
		go cancel()
		_, actionStatus, _, _, err := m.InvokeAction(invokeCtx, "lock_account", "", testInput)
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)
		} else {
			require.NotEqual(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)
		}
	}
}

// testBlockingActionHandler blocks until release is closed (or the handler
// context ends), so tests control exactly when the action completes.
func testBlockingActionHandler(release <-chan struct{}) ActionHandler {
	return func(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, nil, status.Error(codes.Canceled, "context canceled")
		}
		return &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"success": {Kind: &structpb.Value_BoolValue{BoolValue: true}},
			},
		}, nil, nil
	}
}

func TestInvokeActionHonorsRequestedWait(t *testing.T) {
	ctx := t.Context()
	m := NewActionManager(ctx)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	require.NoError(t, m.Register(ctx, testActionSchema, testBlockingActionHandler(release)))

	start := time.Now()
	actionId, actionStatus, _, _, err := m.InvokeActionWithWait(ctx, "lock_account", "", testInput, 2*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotEmpty(t, actionId)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, actionStatus)
	require.GreaterOrEqual(t, elapsed, 2*time.Second)
}

func TestInvokeActionCompletesWithinRequestedWait(t *testing.T) {
	ctx := t.Context()
	m := NewActionManager(ctx)

	release := make(chan struct{})
	require.NoError(t, m.Register(ctx, testActionSchema, testBlockingActionHandler(release)))

	// Complete the handler after the default one-second wait would have
	// expired but well inside the requested window.
	go func() {
		time.Sleep(1500 * time.Millisecond)
		close(release)
	}()

	_, actionStatus, rv, _, err := m.InvokeActionWithWait(ctx, "lock_account", "", testInput, 10*time.Second)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, actionStatus)
	require.NotNil(t, rv)
	require.True(t, rv.Fields["success"].GetBoolValue())
}

func TestClampInlineWait(t *testing.T) {
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"zero takes the default", 0, defaultInlineWait},
		{"negative takes the default", -time.Second, defaultInlineWait},
		{"in range passes through", 42 * time.Second, 42 * time.Second},
		{"oversized is capped", 300 * time.Hour, maxInlineWait},
		{"saturated duration is capped", time.Duration(math.MaxInt64), maxInlineWait},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, clampInlineWait(tc.in))
		})
	}
}

func TestResourceActionInlineWaitThreads(t *testing.T) {
	ctx := t.Context()
	m := NewActionManager(ctx)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	handler := func(hctx context.Context, _ *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		select {
		case <-release:
		case <-hctx.Done():
		}
		return &structpb.Struct{}, nil, nil
	}
	require.NoError(t, m.RegisterResourceAction(ctx, "repository", testActionSchema, handler))

	// The resource-scoped path duplicates the invoke select; the requested
	// wait must thread through it just like the global path.
	start := time.Now()
	_, actionStatus, _, _, err := m.InvokeActionWithWait(ctx, "lock_account", "repository", testInput, 2*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, actionStatus)
	require.GreaterOrEqual(t, elapsed, 2*time.Second)
}
