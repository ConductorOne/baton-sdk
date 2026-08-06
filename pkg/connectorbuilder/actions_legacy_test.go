package connectorbuilder

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

type fakeThirdPartyActionManager struct {
	schema         *v2.BatonActionSchema
	invokeDeadline time.Time
	hadDeadline    bool
	invoked        chan struct{}
}

func (f *fakeThirdPartyActionManager) ListActionSchemas(_ context.Context, _ string) ([]*v2.BatonActionSchema, annotations.Annotations, error) {
	return []*v2.BatonActionSchema{f.schema}, nil, nil
}

func (f *fakeThirdPartyActionManager) GetActionSchema(_ context.Context, _ string) (*v2.BatonActionSchema, annotations.Annotations, error) {
	return f.schema, nil, nil
}

func (f *fakeThirdPartyActionManager) InvokeAction(ctx context.Context, _ string, _ string, _ *structpb.Struct) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	f.invokeDeadline, f.hadDeadline = ctx.Deadline()
	close(f.invoked)
	return "legacy-1", v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, nil, nil, nil
}

func (f *fakeThirdPartyActionManager) GetActionStatus(_ context.Context, _ string) (v2.BatonActionStatus, string, *structpb.Struct, annotations.Annotations, error) {
	return v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, "legacy", nil, nil, nil
}

// A third-party CustomActionManager must receive the detached handler context
// (one-hour deadline), not the 2s inline-wait pin reserved for the SDK's own
// deadline-aware ActionManager: it treats the deadline as an execution cap.
func TestRegisterLegacyActionThirdPartyManagerKeepsHandlerContext(t *testing.T) {
	ctx := t.Context()

	legacy := &fakeThirdPartyActionManager{
		schema:  v2.BatonActionSchema_builder{Name: "legacy_action"}.Build(),
		invoked: make(chan struct{}),
	}

	m := actions.NewActionManager(ctx)
	require.NoError(t, registerLegacyAction(ctx, m, legacy.schema, legacy))

	_, _, _, _, err := m.InvokeAction(ctx, "legacy_action", "", &structpb.Struct{})
	require.NoError(t, err)

	select {
	case <-legacy.invoked:
	case <-time.After(5 * time.Second):
		t.Fatal("legacy manager was never invoked")
	}

	require.True(t, legacy.hadDeadline)
	require.Greater(t, time.Until(legacy.invokeDeadline), 30*time.Minute)
}

// A deadline-aware inner ActionManager blocks until the action truly
// finishes: the outer action must stay RUNNING at its own inline wait and
// resolve later with the real response, never as an empty completion.
func TestRegisterLegacyActionTracksInnerManagerToCompletion(t *testing.T) {
	ctx := t.Context()

	schema := v2.BatonActionSchema_builder{Name: "inner_action"}.Build()
	rv, err := structpb.NewStruct(map[string]any{"done": true})
	require.NoError(t, err)

	inner := actions.NewActionManager(ctx)
	require.NoError(t, inner.Register(ctx, schema, func(_ context.Context, _ *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		time.Sleep(1500 * time.Millisecond)
		return rv, nil, nil
	}))

	outer := actions.NewActionManager(ctx)
	require.NoError(t, registerLegacyAction(ctx, outer, schema, inner))

	outerID, outerStatus, outerRv, _, err := outer.InvokeAction(ctx, "inner_action", "", &structpb.Struct{})
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, outerStatus)
	require.Nil(t, outerRv)

	require.Eventually(t, func() bool {
		st, _, gotRv, _, err := outer.GetActionStatus(ctx, outerID)
		return err == nil && st == v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE && gotRv != nil
	}, 5*time.Second, 100*time.Millisecond)
}

type fakeAsyncThirdPartyActionManager struct {
	schema      *v2.BatonActionSchema
	rv          *structpb.Struct
	statusCalls atomic.Int32
	gotID       atomic.Value
}

func (f *fakeAsyncThirdPartyActionManager) ListActionSchemas(_ context.Context, _ string) ([]*v2.BatonActionSchema, annotations.Annotations, error) {
	return []*v2.BatonActionSchema{f.schema}, nil, nil
}

func (f *fakeAsyncThirdPartyActionManager) GetActionSchema(_ context.Context, _ string) (*v2.BatonActionSchema, annotations.Annotations, error) {
	return f.schema, nil, nil
}

func (f *fakeAsyncThirdPartyActionManager) InvokeAction(_ context.Context, _ string, _ string, _ *structpb.Struct) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	return "legacy-async-1", v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, nil, nil, nil
}

func (f *fakeAsyncThirdPartyActionManager) GetActionStatus(_ context.Context, id string) (v2.BatonActionStatus, string, *structpb.Struct, annotations.Annotations, error) {
	f.gotID.Store(id)
	if f.statusCalls.Add(1) == 1 {
		return v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, "async", nil, nil, nil
	}
	return v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, "async", f.rv, nil, nil
}

// A third-party manager returning a non-terminal status gets polled to a
// terminal one with the action id it returned.
func TestRegisterLegacyActionPollsAsyncThirdPartyManager(t *testing.T) {
	ctx := t.Context()

	rv, err := structpb.NewStruct(map[string]any{"done": true})
	require.NoError(t, err)
	legacy := &fakeAsyncThirdPartyActionManager{
		schema: v2.BatonActionSchema_builder{Name: "async_action"}.Build(),
		rv:     rv,
	}

	outer := actions.NewActionManager(ctx)
	require.NoError(t, registerLegacyAction(ctx, outer, legacy.schema, legacy))

	outerID, _, _, _, err := outer.InvokeAction(ctx, "async_action", "", &structpb.Struct{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		st, _, gotRv, _, err := outer.GetActionStatus(ctx, outerID)
		return err == nil && st == v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE && gotRv != nil
	}, 10*time.Second, 100*time.Millisecond)

	require.Equal(t, "legacy-async-1", legacy.gotID.Load())
	require.GreaterOrEqual(t, legacy.statusCalls.Load(), int32(2))
}
