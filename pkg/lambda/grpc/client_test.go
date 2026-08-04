package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// realOOMReport and realOOMPayload are a verbatim REPORT line and error payload
// captured from a real production OOM: a 128MB-ceiling function killed while at
// 126MB used, i.e. under the ceiling, so it cannot be detected by comparing
// Max Memory Used against Memory Size. realHealthyReport is the same function's
// REPORT line after its ceiling was raised to 384MB.
const (
	realOOMReport = "REPORT RequestId: 595dc20a-caa6-455c-b6cb-182bc88397ed\tDuration: 103085.64 ms\tBilled Duration: 103086 ms\t" +
		"Memory Size: 128 MB\tMax Memory Used: 126 MB\tStatus: error\tError Type: Runtime.OutOfMemory"
	realHealthyReport = "REPORT RequestId: 9c1e95f4-8857-4877-813e-7c790b9e1c73\tDuration: 41.71 ms\tBilled Duration: 42 ms\tMemory Size: 384 MB\tMax Memory Used: 232 MB"
	realOOMPayload    = `{"errorType":"Runtime.ExitError","errorMessage":"RequestId: 595dc20a-caa6-455c-b6cb-182bc88397ed Error: Runtime exited with error: signal: killed"}`
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
		name    string
		rawLog  string
		payload string
		want    bool
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
			name: "OOM via signal killed in log tail, no REPORT line",
			rawLog: "START RequestId: abc-123\nRequestId: abc-123 Error: Runtime exited with error: signal: killed\n" +
				"Runtime.ExitError\nEND RequestId: abc-123\n",
			want: true,
		},
		{
			name:   "memory at ceiling alone is not OOM without a REPORT verdict",
			rawLog: "START RequestId: abc-123\nEND RequestId: abc-123\nREPORT RequestId: abc-123 Duration: 5000 ms Memory Size: 256 MB Max Memory Used: 256 MB\n",
			want:   false,
		},
		{
			name:   "timeout not detected as OOM",
			rawLog: "START RequestId: abc-123\nEND RequestId: abc-123\nREPORT RequestId: abc-123 Duration: 300000 ms Memory Size: 512 MB Max Memory Used: 200 MB Status: timeout\n",
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
			name:   "memory fields alone on separate lines are not OOM without a REPORT verdict",
			rawLog: "Memory Size: 1024 MB\nMax Memory Used: 1024 MB\n",
			want:   false,
		},
		{
			name:   "real OOM REPORT line, under the memory ceiling (126/128 MB)",
			rawLog: "START RequestId: 595dc20a-caa6-455c-b6cb-182bc88397ed Version: $LATEST\n" + `{"level":"info","msg":"syncing resources"}` + "\n" + realOOMReport,
			want:   true,
		},
		{
			name:    "real OOM REPORT line, with disagreeing payload also present",
			rawLog:  "START RequestId: abc\n" + realOOMReport,
			payload: realOOMPayload,
			want:    true,
		},
		{
			name:   "real healthy REPORT line",
			rawLog: "START RequestId: 9c1e95f4-8857-4877-813e-7c790b9e1c73 Version: $LATEST\nEND RequestId: 9c1e95f4-8857-4877-813e-7c790b9e1c73\n" + realHealthyReport,
			want:   false,
		},
		{
			name:   "truncated log: leading JSON fragment missing its opening brace, REPORT line intact",
			rawLog: `"catalog_name":"example","duration_ms":201,"method":"GET"}` + "\n" + realOOMReport,
			want:   true,
		},
		{
			name:    "OOM via payload signal killed, no REPORT line at all",
			rawLog:  "START RequestId: abc-123\n",
			payload: realOOMPayload,
			want:    true,
		},
		{
			name:   "Error Type on REPORT line with single-space separators",
			rawLog: "REPORT RequestId: abc Memory Size: 128 MB Max Memory Used: 126 MB Status: error Error Type: Runtime.OutOfMemory",
			want:   true,
		},
		{
			name:   "Error Type on REPORT line with multi-space separators",
			rawLog: "REPORT RequestId: abc    Memory Size: 128 MB    Max Memory Used: 126 MB    Status: error    Error Type: Runtime.OutOfMemory",
			want:   true,
		},
		{
			name:   "Status/Error Type text in an app log line does not count, only the REPORT line",
			rawLog: `{"msg":"Status: error Error Type: Runtime.OutOfMemory"}` + "\n" + realHealthyReport,
			want:   false,
		},
		{
			name:   "REPORT line Error Type is a non-OOM crash, not OOM",
			rawLog: "REPORT RequestId: abc\tMemory Size: 128 MB\tMax Memory Used: 64 MB\tStatus: error\tError Type: Runtime.ExitError",
			want:   false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := isLambdaOOM(c.rawLog, c.payload)
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
			name:          "timeout via REPORT line Status field",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        "START RequestId: abc\nREPORT RequestId: abc Duration: 300000.00 ms Memory Size: 512 MB Max Memory Used: 200 MB Status: timeout",
			wantCode:      codes.DeadlineExceeded,
			wantSubstring: "function timed out",
			wantIsGRPC:    true,
		},
		{
			name:          "timeout via escaped context deadline exceeded pattern in filtered logs",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        `lambda-run: failed to sync: \"error\":\"context deadline exceeded\"`,
			wantCode:      codes.DeadlineExceeded,
			wantSubstring: "function timed out",
			wantIsGRPC:    true,
		},
		{
			name:          "unescaped context deadline exceeded in a JSON app log line is filtered out, not a timeout",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        `{"level":"error","error":"context deadline exceeded","msg":"sync failed"}` + "\n" + realHealthyReport,
			wantSubstring: "lambda_transport: function returned error:",
			wantIsGRPC:    false,
		},
		{
			name:          "real OOM REPORT line wins over an unescaped deadline string elsewhere in the log",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        `{"level":"error","error":"context deadline exceeded"}` + "\n" + realOOMReport,
			wantCode:      codes.ResourceExhausted,
			wantSubstring: "function ran out of memory",
			wantIsGRPC:    true,
		},
		{
			name:          "real OOM: REPORT line under the memory ceiling (126/128 MB), the case 859 missed",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        "START RequestId: abc\nEND RequestId: abc\n" + realOOMReport,
			wantCode:      codes.ResourceExhausted,
			wantSubstring: "function ran out of memory",
			wantIsGRPC:    true,
		},
		{
			name:          "real OOM: REPORT line plus disagreeing payload (Runtime.ExitError / signal: killed)",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(realOOMPayload),
			rawLog:        "START RequestId: abc\n" + `{"level":"info","msg":"syncing resources"}` + "\n" + realOOMReport,
			wantCode:      codes.ResourceExhausted,
			wantSubstring: "function ran out of memory",
			wantIsGRPC:    true,
		},
		{
			name:          "OOM via payload signal killed, no REPORT line",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(realOOMPayload),
			rawLog:        "START RequestId: abc-123\n",
			wantCode:      codes.ResourceExhausted,
			wantSubstring: "function ran out of memory",
			wantIsGRPC:    true,
		},
		{
			name:          "timeout payload wins over an ambiguous signal-killed log with no REPORT line",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{"errorMessage":"Task timed out after 300.00 seconds"}`),
			rawLog:        "START RequestId: abc\nRuntime.ExitError\nsignal: killed\n",
			wantCode:      codes.DeadlineExceeded,
			wantSubstring: "function timed out",
			wantIsGRPC:    true,
		},
		{
			name:          "generic error with filtered logs",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        "START RequestId: abc-123\nlambda-run: failed to get connector: auth error\nEND RequestId: abc-123\n",
			wantSubstring: "lambda_transport: function returned error:",
			wantIsGRPC:    false,
		},
		{
			name:          "generic error without meaningful logs",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        "START RequestId: abc-123\nEND RequestId: abc-123\n" + realHealthyReport,
			wantSubstring: "lambda_transport: function returned error: Unhandled; status code: 200",
			wantIsGRPC:    false,
		},
		{
			name:          "timeout via REPORT line Status field, tab-separated",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        "START RequestId: abc\nREPORT RequestId: abc\tDuration: 300000.00 ms\tMemory Size: 512 MB\tMax Memory Used: 200 MB\tStatus: timeout",
			wantCode:      codes.DeadlineExceeded,
			wantSubstring: "function timed out",
			wantIsGRPC:    true,
		},
		{
			name:          "Status: timeout text in an app log line does not count, only the REPORT line",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       []byte(`{}`),
			rawLog:        `{"msg":"Status: timeout, retrying"}` + "\n" + realHealthyReport,
			wantSubstring: "lambda_transport: function returned error:",
			wantIsGRPC:    false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := classifyLambdaError(c.functionError, c.statusCode, c.payload, c.rawLog)
			require.Error(t, err)
			require.Contains(t, err.Error(), c.wantSubstring)
			require.Contains(t, err.Error(), "lambda_transport:", "every classification must carry the lambda_transport: marker")
			require.Contains(t, err.Error(), "logSummary:", "every classification must carry the logSummary: marker")

			st, ok := status.FromError(err)
			if c.wantIsGRPC {
				require.True(t, ok, "expected gRPC status error")
				require.Equal(t, c.wantCode, st.Code())
			} else {
				require.False(t, ok, "expected a plain (non-gRPC) error")
			}
		})
	}
}
