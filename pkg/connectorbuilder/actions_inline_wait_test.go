package connectorbuilder

import (
	"context"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// Connectors return *actions.ActionManager as their deprecated
// CustomActionManager (baton-microsoft-entra, baton-zendesk, ...), so its
// method set must keep satisfying that interface.
var _ CustomActionManager = (*actions.ActionManager)(nil)

// testBlockingGlobalActionProvider registers one action whose handler blocks
// until release is closed.
type testBlockingGlobalActionProvider struct {
	ConnectorBuilder
	release chan struct{}
}

func (t *testBlockingGlobalActionProvider) GlobalActions(ctx context.Context, registry actions.ActionRegistry) error {
	schema := v2.BatonActionSchema_builder{
		Name:        "blocking-action",
		DisplayName: "Blocking Action",
	}.Build()
	handler := func(hctx context.Context, _ *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
		select {
		case <-t.release:
		case <-hctx.Done():
		}
		return &structpb.Struct{}, nil, nil
	}
	return registry.Register(ctx, schema, handler)
}

// The inline_wait request field must reach the action manager: a blocking
// action invoked with a three-second wait returns RUNNING no earlier than
// that, while an unset field keeps the default short wait.
func TestInvokeActionThreadsInlineWaitFromRequest(t *testing.T) {
	ctx := t.Context()

	provider := &testBlockingGlobalActionProvider{
		ConnectorBuilder: newTestConnector([]ResourceSyncer{}),
		release:          make(chan struct{}),
	}
	t.Cleanup(func() { close(provider.release) })

	connector, err := NewConnector(ctx, provider)
	require.NoError(t, err)

	start := time.Now()
	resp, err := connector.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name:       "blocking-action",
		Args:       &structpb.Struct{},
		InlineWait: durationpb.New(3 * time.Second),
	}.Build())
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotEmpty(t, resp.GetId())
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, resp.GetStatus())
	require.GreaterOrEqual(t, elapsed, 3*time.Second)

	// An unset field keeps the default wait: the ceiling only has to exclude
	// the explicit three-second wait above, leaving two seconds of slack over
	// the one-second nominal.
	start = time.Now()
	resp, err = connector.InvokeAction(ctx, v2.InvokeActionRequest_builder{
		Name: "blocking-action",
		Args: &structpb.Struct{},
	}.Build())
	elapsed = time.Since(start)

	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, resp.GetStatus())
	require.GreaterOrEqual(t, elapsed, time.Second)
	require.Less(t, elapsed, 3*time.Second)
}
