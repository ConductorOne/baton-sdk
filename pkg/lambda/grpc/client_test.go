package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExtractMeaningfulLogLines(t *testing.T) {
	cases := []struct {
		name   string
		raw    string
		output string
	}{
		{
			name:   "empty log",
			raw:    "",
			output: "",
		},
		{
			name:   "log with only irrelevant lines",
			raw:    "START RequestId: abc-123 Version: $LATEST\nEND RequestId: abc-123\nREPORT RequestId: abc-123 Duration: 100 ms\n",
			output: "",
		},
		{
			name:   "log with relevant and irrelevant lines",
			raw:    "START RequestId: abc-123 Version: $LATEST\nThis is a meaningful log line\nEND RequestId: abc-123\nAnother meaningful log line\nREPORT RequestId: abc-123 Duration: 100 ms\n",
			output: "This is a meaningful log line\nAnother meaningful log line",
		},
		{
			name:   "log with JSON lines filtered out",
			raw:    `{"tenant_id":"tenant-1","message":"This is a log message","connector_id":"connector-1"}` + "\n" + `{"message":"Another log message","app_id":"app-1"}`,
			output: "",
		},
		{
			name: "log with mixed JSON and non-JSON lines",
			raw: `{"level":"info","ts":1234,"msg":"Challenging auth...","tenant_id":"t1"}` + "\n" +
				`lambda-run: failed to get connector: authenticating during initialization` + "\n" +
				`account_inactive`,
			output: "lambda-run: failed to get connector: authenticating during initialization\naccount_inactive",
		},
		{
			name:   "Runtime.ExitError preserved in output",
			raw:    "START RequestId: abc-123 Version: $LATEST\nRuntime.ExitError\nEND RequestId: abc-123\n",
			output: "Runtime.ExitError",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := extractMeaningfulLogLines(c.raw)
			require.Equal(t, c.output, result, "unexpected log line extraction result")
		})
	}
}

func TestIsLambdaOOM(t *testing.T) {
	cases := []struct {
		name   string
		rawLog string
		want   bool
	}{
		{
			name:   "empty log",
			rawLog: "",
			want:   false,
		},
		{
			name:   "normal execution",
			rawLog: "START RequestId: abc-123\nEND RequestId: abc-123\nREPORT RequestId: abc-123 Duration: 100 ms Memory Size: 512 MB Max Memory Used: 128 MB\n",
			want:   false,
		},
		{
			name: "OOM via signal killed",
			rawLog: "START RequestId: abc-123\nRequestId: abc-123 Error: Runtime exited with error: signal: killed\n" +
				"Runtime.ExitError\nEND RequestId: abc-123\n" +
				"REPORT RequestId: abc-123 Duration: 5000 ms Memory Size: 512 MB Max Memory Used: 512 MB\n",
			want: true,
		},
		{
			name:   "OOM via memory match without signal killed",
			rawLog: "START RequestId: abc-123\nEND RequestId: abc-123\nREPORT RequestId: abc-123 Duration: 5000 ms Memory Size: 256 MB Max Memory Used: 256 MB\n",
			want:   true,
		},
		{
			name:   "timeout not detected as OOM",
			rawLog: "START RequestId: abc-123\nEND RequestId: abc-123\nREPORT RequestId: abc-123 Duration: 300000 ms Memory Size: 512 MB Max Memory Used: 200 MB\n",
			want:   false,
		},
		{
			name:   "signal killed without Runtime.ExitError not detected",
			rawLog: "some log line with signal: killed but no exit error marker\n",
			want:   false,
		},
		{
			name:   "Runtime.ExitError without signal killed not detected",
			rawLog: "Runtime.ExitError\n",
			want:   false,
		},
		{
			name:   "memory fields on separate lines",
			rawLog: "Memory Size: 1024 MB\nMax Memory Used: 1024 MB\n",
			want:   true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := isLambdaOOM(c.rawLog)
			require.Equal(t, c.want, got)
		})
	}
}

func TestClassifyLambdaError(t *testing.T) {
	cases := []struct {
		name          string
		functionError string
		statusCode    int32
		payload       []byte
		rawLog        string
		wantCode      codes.Code
		wantSubstring string
		wantIsGRPC    bool
	}{
		{
			name:          "timeout via payload",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{"errorMessage":"Task timed out after 300.00 seconds"}`),
			rawLog:        "START RequestId: abc-123\nEND RequestId: abc-123\n",
			wantCode:      codes.DeadlineExceeded,
			wantSubstring: "function timed out",
			wantIsGRPC:    true,
		},
		{
			name:          "timeout via context deadline exceeded in logs",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        `{"level":"error","error":"context deadline exceeded","msg":"sync failed"}`,
			wantCode:      codes.DeadlineExceeded,
			wantSubstring: "function timed out",
			wantIsGRPC:    true,
		},
		{
			name:          "OOM via signal killed",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog: "START RequestId: abc-123\nRequestId: abc-123 Error: Runtime exited with error: signal: killed\n" +
				"Runtime.ExitError\nEND RequestId: abc-123\n" +
				"REPORT RequestId: abc-123 Duration: 5000 ms Memory Size: 512 MB Max Memory Used: 512 MB\n",
			wantCode:      codes.ResourceExhausted,
			wantSubstring: "function ran out of memory",
			wantIsGRPC:    true,
		},
		{
			name:          "OOM via memory limit reached",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        "START RequestId: abc-123\nEND RequestId: abc-123\nREPORT RequestId: abc-123 Duration: 5000 ms Memory Size: 256 MB Max Memory Used: 256 MB\n",
			wantCode:      codes.ResourceExhausted,
			wantSubstring: "function ran out of memory",
			wantIsGRPC:    true,
		},
		{
			name:          "generic error with filtered logs",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        "START RequestId: abc-123\nlambda-run: failed to get connector: auth error\nEND RequestId: abc-123\n",
			wantSubstring: "lambda-run: failed to get connector: auth error",
			wantIsGRPC:    false,
		},
		{
			name:          "generic error without meaningful logs",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        "START RequestId: abc-123\nEND RequestId: abc-123\nREPORT RequestId: abc-123 Duration: 100 ms Memory Size: 512 MB Max Memory Used: 128 MB\n",
			wantSubstring: "lambda_transport: function returned error: Unhandled; status code: 200",
			wantIsGRPC:    false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := classifyLambdaError(c.functionError, c.statusCode, c.payload, c.rawLog)
			require.Error(t, err)
			require.Contains(t, err.Error(), c.wantSubstring)

			if c.wantIsGRPC {
				st, ok := status.FromError(err)
				require.True(t, ok, "expected gRPC status error")
				require.Equal(t, c.wantCode, st.Code())
			}
		})
	}
}
