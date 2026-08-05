package connectorbuilder

import (
	"context"
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
