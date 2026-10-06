package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestStaticMaterializationSummaryTiming(t *testing.T) {
	s := &syncer{run: newRunState(), stats: newRunStats()}
	s.stats.addStepDuration(MaterializeStaticEntitlementsOp.String(), 2*time.Second)
	fields := zapFieldsByKey(t, s.syncSummaryFields(trace.SpanFromContext(t.Context())))
	require.EqualValues(t, 2000, fields["sync_steps_total_ms"])
	require.Equal(t, map[string]int64{MaterializeStaticEntitlementsOp.String(): 2000}, fields["sync_step_durations_ms"])
}

func TestActionLogsOmitPagePayload(t *testing.T) {
	var output bytes.Buffer
	logger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&output), zap.DebugLevel))
	ctx := ctxzap.ToContext(t.Context(), logger)
	token, err := (staticMaterializationCursor{Version: 1, Template: []byte("template-only-private-marker")}).encode()
	require.NoError(t, err)
	action := Action{Op: MaterializeStaticEntitlementsOp, ResourceTypeID: "group", PageToken: token}
	state := newRunState()
	parent := state.pushAction(ctx, Action{Op: SyncStaticEntitlementsOp, PageToken: "connector-private-token"})
	children, err := state.transitionAction(ctx, parent, "", []Action{action})
	require.NoError(t, err)
	state.finishAction(ctx, children[0])
	logger.Error("action error", zap.Any("action", children[0]))
	require.NotContains(t, output.String(), "template")
	require.NotContains(t, output.String(), "connector-private-token")
	require.NotContains(t, output.String(), "page_token")
	require.Contains(t, output.String(), "group")
	encoded, err := json.Marshal(action)
	require.NoError(t, err)
	var restored Action
	require.NoError(t, json.Unmarshal(encoded, &restored))
	require.Equal(t, token, restored.PageToken)
}
