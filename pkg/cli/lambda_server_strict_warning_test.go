//go:build baton_lambda_support

package cli

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	c1_lambda_grpc "github.com/conductorone/baton-sdk/pkg/lambda/grpc"
)

func strictWarningObserver(t *testing.T, r *lambdaConnectorReloader) (context.Context, *observer.ObservedLogs) {
	t.Helper()
	core, observed := observer.New(zapcore.DebugLevel)
	logger := zap.New(core, zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Message == "lambda-run: connector config version is not ready" && r.mu.TryLock() {
			r.mu.Unlock()
			t.Error("strict refusal warning emitted outside the reloader lock")
		}
		return nil
	}))
	return ctxzap.ToContext(context.Background(), logger), observed
}

func assertStrictWarning(t *testing.T, entry observer.LoggedEntry, requested, current string) {
	t.Helper()
	require.Equal(t, zapcore.WarnLevel, entry.Level)
	require.Equal(t, "lambda-run: connector config version is not ready", entry.Message)
	require.Equal(t, map[string]any{
		"requested_version": requested,
		"current_version":   current,
	}, entry.ContextMap(), "strict refusal logging must contain only the two version fields")
}

func TestLambdaStrictConfigVersionWarningRateLimit(t *testing.T) {
	for _, identityMode := range []bool{false, true} {
		name := "legacy"
		if identityMode {
			name = "identity"
		}
		t.Run(name, func(t *testing.T) {
			f := newStrictVersionFixture(t, "A")
			f.reloader.identityMode = identityMode
			f.reloader.lastRebuildAt = time.Now()
			capLoggedAt := time.Now().Add(time.Hour)
			f.reloader.lastCapLoggedAt = capLoggedAt
			ctx, observed := strictWarningObserver(t, f.reloader)
			for _, requested := range []string{"B", "C", "B", "C", "B"} {
				headers := metadata.Pairs("x-baton-connector-config-version", requested, "x-baton-connector-config-version-strict", "true", "private-header", "header-canary")
				assertStrictVersionNotReady(t, f.invokeStatusWithContext(t, ctx, headers))
			}
			require.Empty(t, f.builds, "the repeat fixture must stay inside the rebuild floor")
			require.Empty(t, f.dispatched)
			require.Equal(t, capLoggedAt, f.reloader.lastCapLoggedAt, "strict warning rate limiting must not reuse the reload-cap timestamp")
			entries := observed.FilterMessage("lambda-run: connector config version is not ready").All()
			require.Len(t, entries, 1, "alternating requested versions must not re-arm the warning")
			assertStrictWarning(t, entries[0], "B", "A")

			f.reloader.mu.Lock()
			f.reloader.lastRebuildAt = time.Time{}
			f.reloader.mu.Unlock()
			f.servedVersion = "C"
			require.Equal(t, codes.OK, f.invokeStatusWithContext(t, ctx, metadata.Pairs("x-baton-connector-config-version", "C")).Code())
			require.Equal(t, []string{"C"}, f.builds)
			require.Equal(t, []string{"C"}, f.dispatched)
			assertStrictVersionNotReady(t, f.invokeStatusWithContext(t, ctx, metadata.Pairs("x-baton-connector-config-version", "B", "x-baton-connector-config-version-strict", "true")))
			require.Len(t, observed.FilterMessage("lambda-run: connector config version is not ready").All(), 1, "a new served/requested pair must not re-arm the warning")

			f.reloader.mu.Lock()
			f.reloader.lastStrictRefusalLoggedAt = time.Now().Add(-2 * lambdaConnectorReloadMinInterval)
			f.reloader.mu.Unlock()
			assertStrictVersionNotReady(t, f.invokeStatusWithContext(t, ctx, metadata.Pairs("x-baton-connector-config-version", "D", "x-baton-connector-config-version-strict", "true")))
			entries = observed.FilterMessage("lambda-run: connector config version is not ready").All()
			require.Len(t, entries, 2, "elapsed time must re-arm the warning")
			assertStrictWarning(t, entries[1], "D", "C")
			require.Equal(t, capLoggedAt, f.reloader.lastCapLoggedAt)
		})
	}
}

func TestLambdaStrictConfigVersionWarningAbsentOnOtherPaths(t *testing.T) {
	for _, identityMode := range []bool{false, true} {
		name := "legacy"
		if identityMode {
			name = "identity"
		}
		t.Run(name, func(t *testing.T) {
			f := newStrictVersionFixture(t, "A")
			f.reloader.identityMode = identityMode
			f.reloader.lastRebuildAt = time.Now()
			ctx, observed := strictWarningObserver(t, f.reloader)
			for _, headers := range []metadata.MD{
				metadata.Pairs("x-baton-connector-config-version", "B"),
				nil,
				metadata.Pairs("x-baton-connector-config-version", "A", "x-baton-connector-config-version-strict", "true"),
			} {
				require.Equal(t, codes.OK, f.invokeStatusWithContext(t, ctx, headers).Code())
			}
			f.reloader.current.connector.(*strictVersionConnector).grantErr = status.Error(codes.FailedPrecondition, "lambda-run: connector config version is not ready")
			ordinary := f.invokeStatusWithContext(t, ctx, metadata.Pairs("x-baton-connector-config-version", "A", "x-baton-connector-config-version-strict", "true"))
			require.Equal(t, codes.FailedPrecondition, ordinary.Code())
			require.Empty(t, ordinary.Details())
			require.Empty(t, observed.All(), "non-strict, matching and vendor-status paths must not emit a strict-refusal warning")
			require.True(t, f.reloader.lastStrictRefusalLoggedAt.IsZero())
			require.Empty(t, f.builds)
		})
	}
}

func TestLambdaStrictConfigVersionWarningAtGuardExit(t *testing.T) {
	for _, identityMode := range []bool{false, true} {
		name := "legacy"
		if identityMode {
			name = "identity"
		}
		t.Run(name, func(t *testing.T) {
			f := newStrictVersionFixture(t, "A")
			f.servedVersion = "B"
			f.reloader.identityMode = identityMode
			ctx, observed := strictWarningObserver(t, f.reloader)
			checked, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			defer release()
			f.reloader.testHooks = &lambdaConnectorReloaderTestHooks{
				beforeDispatch: func(req *c1_lambda_grpc.Request) {
					if values := req.Headers().Get("x-baton-connector-config-version"); len(values) > 0 && values[0] == "A" {
						close(checked)
						<-resume
					}
				},
			}
			strictA := startStrictVersionInvokeWithContext(t, ctx, f.reloader, metadata.Pairs("x-baton-connector-config-version", "A", "x-baton-connector-config-version-strict", "true"))
			waitStrictVersionBarrier(t, checked)
			require.Equal(t, codes.OK, f.invokeStatusWithContext(t, ctx, metadata.Pairs("x-baton-connector-config-version", "B")).Code())
			release()
			assertStrictVersionNotReady(t, waitStrictVersionInvokeStatus(t, strictA))
			entries := observed.FilterMessage("lambda-run: connector config version is not ready").All()
			require.Len(t, entries, 1)
			assertStrictWarning(t, entries[0], "A", "B")
			followup := startStrictVersionInvokeWithContext(t, ctx, f.reloader, metadata.Pairs("x-baton-connector-config-version", "B", "x-baton-connector-config-version-strict", "true"))
			require.Equal(t, codes.OK, waitStrictVersionInvoke(t, followup), "guard refusal must release the reloader lock")
			require.Equal(t, []string{"B", "B"}, f.dispatched)
			require.Equal(t, []string{"B"}, f.builds)
			require.Len(t, observed.FilterMessage("lambda-run: connector config version is not ready").All(), 1)
		})
	}
}
