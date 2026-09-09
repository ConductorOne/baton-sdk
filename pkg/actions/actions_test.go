package actions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"testing"
	"time"

	filippoage "filippo.io/age"
	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/crypto"
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

func TestActionStatusPredicates(t *testing.T) {
	cases := []struct {
		status   v2.BatonActionStatus
		inFlight bool
		settled  bool
	}{
		{v2.BatonActionStatus_BATON_ACTION_STATUS_UNSPECIFIED, false, false},
		{v2.BatonActionStatus_BATON_ACTION_STATUS_UNKNOWN, false, false},
		{v2.BatonActionStatus_BATON_ACTION_STATUS_PENDING, true, false},
		{v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, true, false},
		{v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, false, true},
		{v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.status.String(), func(t *testing.T) {
			require.Equal(t, tc.inFlight, IsInFlight(tc.status))
			require.Equal(t, tc.settled, IsSettled(tc.status))
		})
	}
}

func secretActionSchema(name string) *v2.BatonActionSchema {
	return v2.BatonActionSchema_builder{
		Name: name,
		ReturnTypes: []*config.Field{
			config.Field_builder{
				Name:       "success",
				BoolField:  &config.BoolField{},
				IsRequired: true,
			}.Build(),
			config.Field_builder{
				Name:        "token",
				StringField: &config.StringField{},
				IsSecret:    true,
			}.Build(),
		},
	}.Build()
}

func ageEncryptionConfig(t *testing.T) (*v2.EncryptionConfig, filippoage.Identity) {
	t.Helper()
	identity, err := filippoage.GenerateHybridIdentity()
	require.NoError(t, err)
	return v2.EncryptionConfig_builder{
		AgeRecipientConfig: v2.EncryptionConfig_AgeRecipientConfig_builder{
			Recipient: identity.Recipient().String(),
		}.Build(),
	}.Build(), identity
}

func decryptAgeActionResult(t *testing.T, encrypted *v2.EncryptedData, identity filippoage.Identity) []byte {
	t.Helper()
	reader, err := filippoage.Decrypt(bytes.NewReader(encrypted.GetEncryptedBytes()), identity)
	require.NoError(t, err)
	var plaintext bytes.Buffer
	_, err = plaintext.ReadFrom(reader)
	require.NoError(t, err)
	return plaintext.Bytes()
}

func TestActionHandlerWithSecretsEncryptsInlineResultForEveryRecipient(t *testing.T) {
	ctx := t.Context()
	manager := NewActionManager(ctx)
	schema := secretActionSchema("issue_token")
	schema.SetReturnTypes(append(schema.GetReturnTypes(),
		config.Field_builder{Name: "refresh_token", StringField: &config.StringField{}, IsSecret: true}.Build()))
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		response, err := structpb.NewStruct(map[string]interface{}{"success": true})
		require.NoError(t, err)
		return response, []*v2.PlaintextData{
			v2.PlaintextData_builder{
				Name:        "token",
				Description: "issued token",
				Schema:      "text/plain",
				Bytes:       []byte("action-secret"),
			}.Build(),
			v2.PlaintextData_builder{
				Name:  "refresh_token",
				Bytes: []byte("refresh-secret"),
			}.Build(),
		}, nil, nil
	}
	require.NoError(t, RegisterWithSecrets(ctx, manager, schema, handler))

	configA, identityA := ageEncryptionConfig(t)
	configB, identityB := ageEncryptionConfig(t)
	_, actionStatus, response, encryptedData, _, err := manager.InvokeActionWithWaitAndEncryption(
		ctx,
		"issue_token",
		"",
		nil,
		time.Second,
		[]*v2.EncryptionConfig{configA, configB},
	)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, actionStatus)
	require.Equal(t, true, response.GetFields()["success"].GetBoolValue())
	require.NotContains(t, response.GetFields(), "token")
	require.Len(t, encryptedData, 4)
	require.Equal(t, "token", encryptedData[0].GetName())
	require.Equal(t, "issued token", encryptedData[0].GetDescription())
	require.Equal(t, "text/plain", encryptedData[0].GetSchema())
	require.Equal(t, []byte("action-secret"), decryptAgeActionResult(t, encryptedData[0], identityA))
	require.Equal(t, []byte("action-secret"), decryptAgeActionResult(t, encryptedData[1], identityB))
	require.Equal(t, "refresh_token", encryptedData[2].GetName())
	require.Equal(t, []byte("refresh-secret"), decryptAgeActionResult(t, encryptedData[2], identityA))
	require.Equal(t, []byte("refresh-secret"), decryptAgeActionResult(t, encryptedData[3], identityB))
}

func TestActionHandlerWithSecretsEncryptsBeforeStatusPublication(t *testing.T) {
	ctx := t.Context()
	manager := NewActionManager(ctx)
	release := make(chan struct{})
	handler := func(ctx context.Context, _ *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		}
		return &structpb.Struct{}, []*v2.PlaintextData{
			v2.PlaintextData_builder{Name: "token", Bytes: []byte("polled-secret")}.Build(),
		}, nil, nil
	}
	require.NoError(t, RegisterWithSecrets(ctx, manager, secretActionSchema("issue_token_async"), handler))
	config, identity := ageEncryptionConfig(t)

	id, actionStatus, _, encryptedData, _, err := manager.InvokeActionWithWaitAndEncryption(
		ctx,
		"issue_token_async",
		"",
		nil,
		time.Millisecond,
		[]*v2.EncryptionConfig{config},
	)
	require.NoError(t, err)
	require.True(t, IsInFlight(actionStatus))
	require.Empty(t, encryptedData)
	// Detached settlement must use the recipient snapshot captured at invoke.
	config.GetAgeRecipientConfig().SetRecipient("mutated-after-invoke")
	close(release)

	var settledEncrypted []*v2.EncryptedData
	require.Eventually(t, func() bool {
		status, _, response, gotEncrypted, _, statusErr := manager.GetActionStatusWithEncryptedData(ctx, id)
		if statusErr != nil || status != v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE {
			return false
		}
		require.NotContains(t, response.GetFields(), "token")
		settledEncrypted = gotEncrypted
		return true
	}, time.Second, time.Millisecond)
	require.Len(t, settledEncrypted, 1)
	require.Equal(t, []byte("polled-secret"), decryptAgeActionResult(t, settledEncrypted[0], identity))

	_, _, _, repeatedEncrypted, _, err := manager.GetActionStatusWithEncryptedData(ctx, id)
	require.NoError(t, err)
	require.True(t, proto.Equal(settledEncrypted[0], repeatedEncrypted[0]))
	_, resultStatus, _, resultEncrypted, _ := manager.actions[id].ResultWithEncryptedData()
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, resultStatus)
	require.True(t, proto.Equal(settledEncrypted[0], resultEncrypted[0]))
}

func TestActionHandlerWithSecretsRejectsInvalidRecipientsBeforeInvocation(t *testing.T) {
	tests := []struct {
		name    string
		configs []*v2.EncryptionConfig
	}{
		{name: "missing"},
		{name: "nil config", configs: []*v2.EncryptionConfig{nil}},
		{name: "unknown provider", configs: []*v2.EncryptionConfig{v2.EncryptionConfig_builder{Provider: "unknown"}.Build()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			manager := NewActionManager(ctx)
			invoked := false
			handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
				invoked = true
				return nil, nil, nil, nil
			}
			require.NoError(t, RegisterWithSecrets(ctx, manager, secretActionSchema("issue_token"), handler))

			_, _, _, _, _, err := manager.InvokeActionWithWaitAndEncryption(ctx, "issue_token", "", nil, time.Second, test.configs)
			require.Error(t, err)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.False(t, invoked)
		})
	}
}

func TestResourceActionHandlerWithSecretsEncryptsResult(t *testing.T) {
	ctx := t.Context()
	manager := NewActionManager(ctx)
	registry, err := manager.GetTypeRegistry(ctx, "service-account")
	require.NoError(t, err)
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		return &structpb.Struct{}, []*v2.PlaintextData{
			v2.PlaintextData_builder{Name: "token", Bytes: []byte("resource-secret")}.Build(),
		}, nil, nil
	}
	require.NoError(t, RegisterWithSecrets(ctx, registry, secretActionSchema("issue_resource_token"), handler))
	config, identity := ageEncryptionConfig(t)

	_, actionStatus, response, encryptedData, _, err := manager.InvokeActionWithWaitAndEncryption(
		ctx,
		"issue_resource_token",
		"service-account",
		nil,
		time.Second,
		[]*v2.EncryptionConfig{config},
	)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, actionStatus)
	require.NotContains(t, response.GetFields(), "token")
	require.Len(t, encryptedData, 1)
	require.Equal(t, []byte("resource-secret"), decryptAgeActionResult(t, encryptedData[0], identity))
}

func TestActionHandlerWithSecretsFailsWhenRequiredSecretIsMissingOnSuccess(t *testing.T) {
	ctx := t.Context()
	manager := NewActionManager(ctx)
	schema := secretActionSchema("missing_required_token")
	schema.GetReturnTypes()[1].SetIsRequired(true)
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		response, err := structpb.NewStruct(map[string]interface{}{"success": true})
		require.NoError(t, err)
		return response, nil, nil, nil
	}
	require.NoError(t, RegisterWithSecrets(ctx, manager, schema, handler))
	config, _ := ageEncryptionConfig(t)

	_, actionStatus, response, encryptedData, _, err := manager.InvokeActionWithWaitAndEncryption(
		ctx,
		"missing_required_token",
		"",
		nil,
		time.Second,
		[]*v2.EncryptionConfig{config},
	)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)
	require.Empty(t, encryptedData)
	require.Contains(t, response.GetFields()["error"].GetStringValue(), `required secret return type "token" is missing`)
}

func TestActionHandlerWithSecretsDoesNotRequireSecretWhenHandlerFails(t *testing.T) {
	ctx := t.Context()
	manager := NewActionManager(ctx)
	schema := secretActionSchema("failing_required_token")
	schema.GetReturnTypes()[1].SetIsRequired(true)
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		return nil, nil, nil, errors.New("provider rejected action")
	}
	require.NoError(t, RegisterWithSecrets(ctx, manager, schema, handler))
	config, _ := ageEncryptionConfig(t)

	_, actionStatus, response, encryptedData, _, err := manager.InvokeActionWithWaitAndEncryption(
		ctx,
		"failing_required_token",
		"",
		nil,
		time.Second,
		[]*v2.EncryptionConfig{config},
	)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)
	require.Empty(t, encryptedData)
	require.Equal(t, "provider rejected action", response.GetFields()["error"].GetStringValue())
}

func TestActionHandlerWithSecretsDropsPlaintextWhenHandlerFails(t *testing.T) {
	ctx := t.Context()
	manager := NewActionManager(ctx)
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		response, err := structpb.NewStruct(map[string]interface{}{"partial": "safe"})
		require.NoError(t, err)
		return response, []*v2.PlaintextData{
			v2.PlaintextData_builder{Name: "token", Bytes: []byte("failed-action-secret")}.Build(),
		}, nil, errors.New("provider rejected action")
	}
	require.NoError(t, RegisterWithSecrets(ctx, manager, secretActionSchema("failing_issue"), handler))
	config, _ := ageEncryptionConfig(t)

	_, actionStatus, response, encryptedData, _, err := manager.InvokeActionWithWaitAndEncryption(
		ctx,
		"failing_issue",
		"",
		nil,
		time.Second,
		[]*v2.EncryptionConfig{config},
	)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)
	require.Empty(t, encryptedData)
	require.Equal(t, "safe", response.GetFields()["partial"].GetStringValue())
	require.Equal(t, "provider rejected action", response.GetFields()["error"].GetStringValue())
	require.NotContains(t, response.String(), "failed-action-secret")
}

func TestActionHandlerWithSecretsDropsPlaintextWhenEncryptionFails(t *testing.T) {
	ctx := t.Context()
	manager := NewActionManager(ctx)
	manager.encryptPlaintext = func(context.Context, *crypto.EncryptionManager, *v2.PlaintextData) ([]*v2.EncryptedData, error) {
		return nil, errors.New("injected encryption failure")
	}
	plaintext := []byte("encryption-failure-secret")
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		return &structpb.Struct{}, []*v2.PlaintextData{
			v2.PlaintextData_builder{Name: "token", Bytes: plaintext}.Build(),
		}, nil, nil
	}
	require.NoError(t, RegisterWithSecrets(ctx, manager, secretActionSchema("oversized_token"), handler))
	encryptionConfig, _ := ageEncryptionConfig(t)

	_, actionStatus, response, encryptedData, _, err := manager.InvokeActionWithWaitAndEncryption(
		ctx,
		"oversized_token",
		"",
		nil,
		time.Second,
		[]*v2.EncryptionConfig{encryptionConfig},
	)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)
	require.Empty(t, encryptedData)
	require.Contains(t, response.GetFields()["error"].GetStringValue(), "encrypt action return value")
	require.NotContains(t, response.String(), string(plaintext))
}

func TestActionHandlerWithSecretsClonesEncryptedResultBeforePublication(t *testing.T) {
	ctx := t.Context()
	manager := NewActionManager(ctx)
	handlerOwned := v2.EncryptedData_builder{
		Name:           "token",
		EncryptedBytes: []byte("original-ciphertext"),
	}.Build()
	manager.encryptPlaintext = func(context.Context, *crypto.EncryptionManager, *v2.PlaintextData) ([]*v2.EncryptedData, error) {
		return []*v2.EncryptedData{handlerOwned}, nil
	}
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		return &structpb.Struct{}, []*v2.PlaintextData{
			v2.PlaintextData_builder{Name: "token", Bytes: []byte("plaintext")}.Build(),
		}, nil, nil
	}
	require.NoError(t, RegisterWithSecrets(ctx, manager, secretActionSchema("clone_ciphertext"), handler))
	config, _ := ageEncryptionConfig(t)

	id, actionStatus, _, encryptedData, _, err := manager.InvokeActionWithWaitAndEncryption(
		ctx,
		"clone_ciphertext",
		"",
		nil,
		time.Second,
		[]*v2.EncryptionConfig{config},
	)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, actionStatus)
	handlerOwned.SetEncryptedBytes([]byte("mutated-ciphertext"))
	require.Equal(t, []byte("original-ciphertext"), encryptedData[0].GetEncryptedBytes())

	_, _, _, statusEncrypted, _, err := manager.GetActionStatusWithEncryptedData(ctx, id)
	require.NoError(t, err)
	require.Equal(t, []byte("original-ciphertext"), statusEncrypted[0].GetEncryptedBytes())
}

func TestActionHandlerWithSecretsRedactsPanicValue(t *testing.T) {
	ctx := t.Context()
	manager := NewActionManager(ctx)
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		panic("panic-secret")
	}
	require.NoError(t, RegisterWithSecrets(ctx, manager, secretActionSchema("panicking_issue"), handler))
	config, _ := ageEncryptionConfig(t)

	_, actionStatus, response, encryptedData, _, err := manager.InvokeActionWithWaitAndEncryption(
		ctx,
		"panicking_issue",
		"",
		nil,
		time.Second,
		[]*v2.EncryptionConfig{config},
	)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)
	require.Empty(t, encryptedData)
	require.Equal(t, "panic in action handler", response.GetFields()["error"].GetStringValue())
	require.NotContains(t, response.String(), "panic-secret")
}

func TestActionHandlerIncludesPanicValueWhenHandlerDoesNotReturnSecrets(t *testing.T) {
	ctx := t.Context()
	manager := NewActionManager(ctx)
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		panic("lock failed")
	}
	require.NoError(t, manager.Register(ctx, testActionSchema, handler))

	_, actionStatus, response, _, err := manager.InvokeActionWithWait(ctx, "lock_account", "", testInput, time.Second)
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)
	require.Equal(t, "panic in action handler: lock failed", response.GetFields()["error"].GetStringValue())
}

func TestActionHandlerWithSecretsPublishesCiphertextAfterInvokeCancellation(t *testing.T) {
	invokeCtx, cancel := context.WithCancel(t.Context())
	manager := NewActionManager(invokeCtx)
	started := make(chan struct{})
	release := make(chan struct{})
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		close(started)
		<-release
		return &structpb.Struct{}, []*v2.PlaintextData{
			v2.PlaintextData_builder{Name: "token", Bytes: []byte("late-secret")}.Build(),
		}, nil, nil
	}
	require.NoError(t, RegisterWithSecrets(invokeCtx, manager, secretActionSchema("cancelled_issue"), handler))
	config, identity := ageEncryptionConfig(t)

	type invokeResult struct {
		id            string
		status        v2.BatonActionStatus
		encryptedData []*v2.EncryptedData
		err           error
	}
	resultCh := make(chan invokeResult, 1)
	go func() {
		id, actionStatus, _, encryptedData, _, err := manager.InvokeActionWithWaitAndEncryption(
			invokeCtx,
			"cancelled_issue",
			"",
			nil,
			time.Second,
			[]*v2.EncryptionConfig{config},
		)
		resultCh <- invokeResult{id: id, status: actionStatus, encryptedData: encryptedData, err: err}
	}()

	<-started
	cancel()
	cancelledResult := <-resultCh
	require.ErrorIs(t, cancelledResult.err, context.Canceled)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, cancelledResult.status)
	require.Empty(t, cancelledResult.encryptedData)
	close(release)

	var settledEncrypted []*v2.EncryptedData
	require.Eventually(t, func() bool {
		status, _, _, encryptedData, _, err := manager.GetActionStatusWithEncryptedData(t.Context(), cancelledResult.id)
		settledEncrypted = encryptedData
		return err == nil && status == v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE
	}, time.Second, time.Millisecond)
	require.Len(t, settledEncrypted, 1)
	require.Equal(t, []byte("late-secret"), decryptAgeActionResult(t, settledEncrypted[0], identity))
}

func TestActionHandlerWithSecretsRejectsInvalidOutputs(t *testing.T) {
	tests := []struct {
		name      string
		response  map[string]interface{}
		plaintext []*v2.PlaintextData
		errText   string
	}{
		{
			name:     "secret in public response",
			response: map[string]interface{}{"token": "plaintext-secret"},
			errText:  "must not be included in the public response",
		},
		{
			name: "undeclared plaintext",
			plaintext: []*v2.PlaintextData{
				v2.PlaintextData_builder{Name: "password", Bytes: []byte("undeclared-sensitive-value")}.Build(),
			},
			errText: "is not declared as a secret return type",
		},
		{
			name: "duplicate plaintext name",
			plaintext: []*v2.PlaintextData{
				v2.PlaintextData_builder{Name: "token", Bytes: []byte("duplicate-value-one")}.Build(),
				v2.PlaintextData_builder{Name: "token", Bytes: []byte("duplicate-value-two")}.Build(),
			},
			errText: "duplicate plaintext return value",
		},
		{
			name: "empty plaintext bytes",
			plaintext: []*v2.PlaintextData{
				v2.PlaintextData_builder{Name: "token"}.Build(),
			},
			errText: "must have a name and non-empty bytes",
		},
		{
			name:      "nil plaintext",
			plaintext: []*v2.PlaintextData{nil},
			errText:   "must have a name and non-empty bytes",
		},
		{
			name: "empty plaintext name",
			plaintext: []*v2.PlaintextData{
				v2.PlaintextData_builder{Bytes: []byte("empty-name-sensitive-value")}.Build(),
			},
			errText: "must have a name and non-empty bytes",
		},
		{
			name: "name declared non-secret",
			plaintext: []*v2.PlaintextData{
				v2.PlaintextData_builder{Name: "success", Bytes: []byte("non-secret-field-sensitive-value")}.Build(),
			},
			errText: "is not declared as a secret return type",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			manager := NewActionManager(ctx)
			handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
				response, err := structpb.NewStruct(test.response)
				require.NoError(t, err)
				return response, test.plaintext, nil, nil
			}
			require.NoError(t, RegisterWithSecrets(ctx, manager, secretActionSchema("issue_token"), handler))
			config, _ := ageEncryptionConfig(t)

			id, actionStatus, response, encryptedData, _, err := manager.InvokeActionWithWaitAndEncryption(
				ctx,
				"issue_token",
				"",
				nil,
				time.Second,
				[]*v2.EncryptionConfig{config},
			)
			require.NoError(t, err)
			require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, actionStatus)
			require.Empty(t, encryptedData)
			require.NotContains(t, response.String(), "plaintext-secret")
			for _, plaintext := range test.plaintext {
				if len(plaintext.GetBytes()) > 0 {
					require.NotContains(t, response.String(), string(plaintext.GetBytes()))
				}
			}
			require.Contains(t, response.GetFields()["error"].GetStringValue(), test.errText)

			_, _, statusResponse, statusEncrypted, _, statusErr := manager.GetActionStatusWithEncryptedData(ctx, id)
			require.NoError(t, statusErr)
			require.Empty(t, statusEncrypted)
			require.Contains(t, statusResponse.GetFields()["error"].GetStringValue(), test.errText)
		})
	}
}

type actionRegistryWithoutSecretSupport struct{}

func (actionRegistryWithoutSecretSupport) Register(context.Context, *v2.BatonActionSchema, ActionHandler) error {
	return nil
}

func (actionRegistryWithoutSecretSupport) RegisterAction(context.Context, string, *v2.BatonActionSchema, ActionHandler) error {
	return nil
}

func TestSecretActionRegistrationValidatesSchemaAndRegistry(t *testing.T) {
	ctx := t.Context()
	handler := func(context.Context, *structpb.Struct) (*structpb.Struct, []*v2.PlaintextData, annotations.Annotations, error) {
		return nil, nil, nil, nil
	}

	err := RegisterWithSecrets(ctx, actionRegistryWithoutSecretSupport{}, secretActionSchema("issue_token"), handler)
	require.ErrorContains(t, err, "does not support secret results")

	manager := NewActionManager(ctx)
	err = manager.Register(ctx, secretActionSchema("issue_token"), testActionHandler)
	require.ErrorContains(t, err, "must use RegisterWithSecrets")

	err = manager.RegisterWithSecrets(ctx, v2.BatonActionSchema_builder{Name: "no_secret"}.Build(), handler)
	require.ErrorContains(t, err, "requires at least one secret return type")

	emptyNameSchema := secretActionSchema("empty_name")
	emptyNameSchema.GetReturnTypes()[1].SetName("")
	err = manager.RegisterWithSecrets(ctx, emptyNameSchema, handler)
	require.ErrorContains(t, err, "name cannot be empty")

	duplicateNameSchema := secretActionSchema("duplicate_name")
	duplicateNameSchema.SetReturnTypes(append(duplicateNameSchema.GetReturnTypes(),
		config.Field_builder{Name: "token", StringField: &config.StringField{}, IsSecret: true}.Build()))
	err = manager.RegisterWithSecrets(ctx, duplicateNameSchema, handler)
	require.ErrorContains(t, err, "duplicate secret return type")
}
