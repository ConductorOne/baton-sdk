package connectorbuilder

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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
	require.NoError(t, registerLegacyAction(ctx, m, legacy.schema, legacy, shortPollIntervals))

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
	require.NoError(t, registerLegacyAction(ctx, outer, schema, inner, shortPollIntervals))

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
	finalStatus v2.BatonActionStatus
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
	switch f.statusCalls.Add(1) {
	case 1:
		// One transient lookup failure must not fail the action.
		return v2.BatonActionStatus_BATON_ACTION_STATUS_UNKNOWN, "", nil, nil, context.DeadlineExceeded
	case 2:
		return v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING, "async", nil, nil, nil
	default:
		return f.finalStatus, "async", f.rv, nil, nil
	}
}

// A third-party manager returning a non-terminal status gets polled to a
// terminal one with the action id it returned, riding through a transient
// status-check failure.
func TestRegisterLegacyActionPollsAsyncThirdPartyManager(t *testing.T) {
	ctx := t.Context()

	rv, err := structpb.NewStruct(map[string]any{"done": true})
	require.NoError(t, err)
	legacy := &fakeAsyncThirdPartyActionManager{
		schema:      v2.BatonActionSchema_builder{Name: "async_action"}.Build(),
		rv:          rv,
		finalStatus: v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE,
	}

	outer := actions.NewActionManager(ctx)
	require.NoError(t, registerLegacyAction(ctx, outer, legacy.schema, legacy, shortPollIntervals))

	outerID, _, _, _, err := outer.InvokeAction(ctx, "async_action", "", &structpb.Struct{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		st, _, gotRv, _, err := outer.GetActionStatus(ctx, outerID)
		return err == nil && st == v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE && gotRv != nil
	}, 5*time.Second, 10*time.Millisecond)

	require.Equal(t, "legacy-async-1", legacy.gotID.Load())
	require.GreaterOrEqual(t, legacy.statusCalls.Load(), int32(3))
}

// A legacy action that polls to FAILED must mark the outer action FAILED,
// never resolve it as a success.
func TestRegisterLegacyActionPolledFailureFailsOuterAction(t *testing.T) {
	ctx := t.Context()

	legacy := &fakeAsyncThirdPartyActionManager{
		schema:      v2.BatonActionSchema_builder{Name: "failing_action"}.Build(),
		finalStatus: v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED,
	}

	outer := actions.NewActionManager(ctx)
	require.NoError(t, registerLegacyAction(ctx, outer, legacy.schema, legacy, shortPollIntervals))

	outerID, _, _, _, err := outer.InvokeAction(ctx, "failing_action", "", &structpb.Struct{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		st, _, _, _, err := outer.GetActionStatus(ctx, outerID)
		return err == nil && st == v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED
	}, 5*time.Second, 10*time.Millisecond)
}

type fakeSyncNoStatusActionManager struct {
	schema      *v2.BatonActionSchema
	rv          *structpb.Struct
	statusCalls atomic.Int32
}

func (f *fakeSyncNoStatusActionManager) ListActionSchemas(_ context.Context, _ string) ([]*v2.BatonActionSchema, annotations.Annotations, error) {
	return []*v2.BatonActionSchema{f.schema}, nil, nil
}

func (f *fakeSyncNoStatusActionManager) GetActionSchema(_ context.Context, _ string) (*v2.BatonActionSchema, annotations.Annotations, error) {
	return f.schema, nil, nil
}

func (f *fakeSyncNoStatusActionManager) InvokeAction(_ context.Context, _ string, _ string, _ *structpb.Struct) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	return "", v2.BatonActionStatus_BATON_ACTION_STATUS_UNSPECIFIED, f.rv, nil, nil
}

func (f *fakeSyncNoStatusActionManager) GetActionStatus(_ context.Context, _ string) (v2.BatonActionStatus, string, *structpb.Struct, annotations.Annotations, error) {
	f.statusCalls.Add(1)
	return v2.BatonActionStatus_BATON_ACTION_STATUS_UNSPECIFIED, "", nil, nil, nil
}

// Legacy synchronous managers were never required to populate id or status;
// a response with the zero status and no id must resolve the outer action
// immediately, never enter the polling loop.
func TestRegisterLegacyActionSyncManagerWithoutStatusResolves(t *testing.T) {
	ctx := t.Context()

	rv, err := structpb.NewStruct(map[string]any{"done": true})
	require.NoError(t, err)
	legacy := &fakeSyncNoStatusActionManager{
		schema: v2.BatonActionSchema_builder{Name: "sync_action"}.Build(),
		rv:     rv,
	}

	outer := actions.NewActionManager(ctx)
	require.NoError(t, registerLegacyAction(ctx, outer, legacy.schema, legacy, shortPollIntervals))

	_, outerStatus, outerRv, _, err := outer.InvokeAction(ctx, "sync_action", "", &structpb.Struct{})
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, outerStatus)
	require.NotNil(t, outerRv)
	require.Equal(t, int32(0), legacy.statusCalls.Load())
}

// shortPollIntervals drives the poll loop in milliseconds so outcome tables
// don't add real wall time to the suite.
var shortPollIntervals = legacyPollIntervals{initial: time.Millisecond, max: 4 * time.Millisecond}

type scriptedPollResult struct {
	status v2.BatonActionStatus
	err    error
	resp   *structpb.Struct
}

type scriptedLegacyActionManager struct {
	schema       *v2.BatonActionSchema
	invokeID     string
	invokeStatus v2.BatonActionStatus
	invokeRv     *structpb.Struct
	polls        []scriptedPollResult
	pollCalls    atomic.Int32
}

func (f *scriptedLegacyActionManager) ListActionSchemas(_ context.Context, _ string) ([]*v2.BatonActionSchema, annotations.Annotations, error) {
	return []*v2.BatonActionSchema{f.schema}, nil, nil
}

func (f *scriptedLegacyActionManager) GetActionSchema(_ context.Context, _ string) (*v2.BatonActionSchema, annotations.Annotations, error) {
	return f.schema, nil, nil
}

func (f *scriptedLegacyActionManager) InvokeAction(_ context.Context, _ string, _ string, _ *structpb.Struct) (string, v2.BatonActionStatus, *structpb.Struct, annotations.Annotations, error) {
	return f.invokeID, f.invokeStatus, f.invokeRv, nil, nil
}

func (f *scriptedLegacyActionManager) GetActionStatus(_ context.Context, _ string) (v2.BatonActionStatus, string, *structpb.Struct, annotations.Annotations, error) {
	i := int(f.pollCalls.Add(1)) - 1
	if i >= len(f.polls) {
		// Polling past the script (or a case that should never poll) fails
		// loudly instead of hanging the loop.
		return v2.BatonActionStatus_BATON_ACTION_STATUS_UNSPECIFIED, "", nil, nil, fmt.Errorf("unexpected status poll %d", i+1)
	}
	p := f.polls[i]
	if p.err != nil {
		return v2.BatonActionStatus_BATON_ACTION_STATUS_UNKNOWN, "", nil, nil, p.err
	}
	if p.resp != nil {
		return p.status, "scripted", p.resp, nil, nil
	}
	return p.status, "scripted", f.invokeRv, nil, nil
}

// Every seam and poll outcome the wrapper distinguishes: terminal statuses at
// the invoke seam resolve like terminal polls (in-band FAILED must not become
// a success), never-populated shapes keep the legacy pass-through, and the
// poll loop tolerates transient lookup errors and indeterminate statuses up
// to the shared threshold before failing closed.
func TestRegisterLegacyActionSeamAndPollOutcomes(t *testing.T) {
	const (
		unspecified = v2.BatonActionStatus_BATON_ACTION_STATUS_UNSPECIFIED
		unknown     = v2.BatonActionStatus_BATON_ACTION_STATUS_UNKNOWN
		running     = v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING
		complete    = v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE
		failed      = v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED
	)
	lookupErr := scriptedPollResult{err: context.DeadlineExceeded}

	cases := []struct {
		name         string
		invokeID     string
		invokeStatus v2.BatonActionStatus
		polls        []scriptedPollResult
		wantStatus   v2.BatonActionStatus
		wantErrIn    string
	}{
		{"in-band failure at the invoke seam fails", "id-1", failed, nil, failed, "failed"},
		{"indeterminate seam status polls to an outcome", "id-1", unknown,
			[]scriptedPollResult{{status: complete}}, complete, ""},
		{"indeterminate seam status without an id resolves as before", "", unknown, nil, complete, ""},
		{"unspecified status resolves as before", "id-1", unspecified, nil, complete, ""},
		{"in-flight without an id resolves as before", "", running, nil, complete, ""},
		{"threshold consecutive lookup errors fail closed", "id-1", running,
			[]scriptedPollResult{lookupErr, lookupErr, lookupErr}, failed, "deadline"},
		{"lookup errors under the threshold recover", "id-1", running,
			[]scriptedPollResult{lookupErr, lookupErr, {status: complete}}, complete, ""},
		{"indeterminate polls under the threshold recover", "id-1", running,
			[]scriptedPollResult{{status: unspecified}, {status: running}, {status: complete}}, complete, ""},
		{"persistent indeterminate polls fail closed", "id-1", running,
			[]scriptedPollResult{{status: unspecified}, {status: unspecified}, {status: unspecified}}, failed, "unexpected status"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			rv, err := structpb.NewStruct(map[string]any{"done": true})
			require.NoError(t, err)
			legacy := &scriptedLegacyActionManager{
				schema:       v2.BatonActionSchema_builder{Name: "scripted_action"}.Build(),
				invokeID:     tc.invokeID,
				invokeStatus: tc.invokeStatus,
				invokeRv:     rv,
				polls:        tc.polls,
			}
			outer := actions.NewActionManager(ctx)
			require.NoError(t, registerLegacyAction(ctx, outer, legacy.schema, legacy, shortPollIntervals))

			_, st, gotRv, _, err := outer.InvokeAction(ctx, "scripted_action", "", &structpb.Struct{})
			require.NoError(t, err)
			require.Equal(t, tc.wantStatus, st)
			if tc.wantErrIn != "" {
				require.Contains(t, gotRv.Fields["error"].GetStringValue(), tc.wantErrIn)
			} else {
				require.Nil(t, gotRv.Fields["error"])
			}
		})
	}
}

// The poll loop's warnings must reach the caller's logger: the detached
// handler context carries it across the goroutine boundary.
func TestLegacyPollWarningsReachCallerLogger(t *testing.T) {
	core, observed := observer.New(zap.WarnLevel)
	ctx := ctxzap.ToContext(t.Context(), zap.New(core))

	rv, err := structpb.NewStruct(map[string]any{"done": true})
	require.NoError(t, err)
	legacy := &scriptedLegacyActionManager{
		schema:       v2.BatonActionSchema_builder{Name: "warned_action"}.Build(),
		invokeID:     "id-1",
		invokeStatus: v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING,
		invokeRv:     rv,
		polls: []scriptedPollResult{
			{err: context.DeadlineExceeded},
			{status: v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE},
		},
	}
	outer := actions.NewActionManager(ctx)
	require.NoError(t, registerLegacyAction(ctx, outer, legacy.schema, legacy, shortPollIntervals))

	_, st, _, _, err := outer.InvokeAction(ctx, "warned_action", "", &structpb.Struct{})
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE, st)
	require.Eventually(t, func() bool {
		return observed.FilterMessage("legacy action status check failed, retrying").Len() == 1
	}, time.Second, 10*time.Millisecond)
}

type capturingRegistry struct {
	handler actions.ActionHandler
}

func (c *capturingRegistry) Register(_ context.Context, _ *v2.BatonActionSchema, handler actions.ActionHandler) error {
	c.handler = handler
	return nil
}

func (c *capturingRegistry) RegisterAction(_ context.Context, _ string, _ *v2.BatonActionSchema, handler actions.ActionHandler) error {
	c.handler = handler
	return nil
}

// The poll loop's context exit must surface the handler budget's cause, not
// a bare context error.
func TestLegacyPollSurfacesHandlerBudgetCause(t *testing.T) {
	legacy := &scriptedLegacyActionManager{
		schema:       v2.BatonActionSchema_builder{Name: "budgeted_action"}.Build(),
		invokeID:     "id-1",
		invokeStatus: v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING,
		polls:        slices.Repeat([]scriptedPollResult{{status: v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING}}, 64),
	}
	reg := &capturingRegistry{}
	require.NoError(t, registerLegacyAction(t.Context(), reg, legacy.schema, legacy, shortPollIntervals))

	cause := errors.New("action handler timed out")
	handlerCtx, cancel := context.WithTimeoutCause(t.Context(), 25*time.Millisecond, cause)
	defer cancel()

	_, _, err := reg.handler(handlerCtx, &structpb.Struct{})
	require.ErrorIs(t, err, cause)
	require.ErrorContains(t, err, "did not reach a terminal status")
}

// Removing the meaningful-payload guard in the poll loop must fail this
// test: an indeterminate poll's payload must not displace the invoke
// response that the fail-closed exit returns.
func TestLegacyIndeterminatePollPayloadDoesNotDisplaceResponse(t *testing.T) {
	ctx := t.Context()

	invokePayload, err := structpb.NewStruct(map[string]any{"from": "invoke"})
	require.NoError(t, err)
	pollPayload, err := structpb.NewStruct(map[string]any{"from": "poll"})
	require.NoError(t, err)

	indeterminate := scriptedPollResult{
		status: v2.BatonActionStatus_BATON_ACTION_STATUS_UNSPECIFIED,
		resp:   pollPayload,
	}
	legacy := &scriptedLegacyActionManager{
		schema:       v2.BatonActionSchema_builder{Name: "displacing_action"}.Build(),
		invokeID:     "id-1",
		invokeStatus: v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING,
		invokeRv:     invokePayload,
		polls:        []scriptedPollResult{indeterminate, indeterminate, indeterminate},
	}
	outer := actions.NewActionManager(ctx)
	require.NoError(t, registerLegacyAction(ctx, outer, legacy.schema, legacy, shortPollIntervals))

	_, st, gotRv, _, err := outer.InvokeAction(ctx, "displacing_action", "", &structpb.Struct{})
	require.NoError(t, err)
	require.Equal(t, v2.BatonActionStatus_BATON_ACTION_STATUS_FAILED, st)
	require.Equal(t, "invoke", gotRv.Fields["from"].GetStringValue())
	require.Contains(t, gotRv.Fields["error"].GetStringValue(), "unexpected status")
}

// Removing the interval fallback must fail this test: with a zero-valued
// pacing struct the first poll must still wait the defaults' initial tick
// rather than firing immediately (and then busy-looping on a zero cap).
func TestZeroPollIntervalsFallBackToDefaults(t *testing.T) {
	rv, err := structpb.NewStruct(map[string]any{"done": true})
	require.NoError(t, err)
	legacy := &scriptedLegacyActionManager{
		schema:       v2.BatonActionSchema_builder{Name: "unpaced_action"}.Build(),
		invokeID:     "id-1",
		invokeStatus: v2.BatonActionStatus_BATON_ACTION_STATUS_RUNNING,
		invokeRv:     rv,
		polls:        []scriptedPollResult{{status: v2.BatonActionStatus_BATON_ACTION_STATUS_COMPLETE}},
	}
	reg := &capturingRegistry{}
	require.NoError(t, registerLegacyAction(t.Context(), reg, legacy.schema, legacy, legacyPollIntervals{}))

	start := time.Now()
	_, _, err = reg.handler(t.Context(), &structpb.Struct{})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.EqualValues(t, 1, legacy.pollCalls.Load())
	require.GreaterOrEqual(t, elapsed, 900*time.Millisecond)
}
