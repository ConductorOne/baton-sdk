package grpc

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Real REPORT lines captured from a connector function that was being killed for
// exceeding a 128 MB limit, and from the same function after its limit was
// raised to 384 MB. AWS separates REPORT fields with tabs.
const (
	reportOOM = "REPORT RequestId: 595dc20a-caa6-455c-b6cb-182bc88397ed\tDuration: 103085.64 ms\t" +
		"Billed Duration: 103086 ms\tMemory Size: 128 MB\tMax Memory Used: 126 MB\tStatus: error\t" +
		"Error Type: Runtime.OutOfMemory"

	reportHealthy = "REPORT RequestId: 9c1e95f4-8857-4877-813e-7c790b9e1c73\tDuration: 41.71 ms\t" +
		"Billed Duration: 42 ms\tMemory Size: 384 MB\tMax Memory Used: 232 MB"
)

func TestParseLambdaReportLine(t *testing.T) {
	cases := []struct {
		name  string
		raw   string
		found bool
		want  lambdaReport
	}{
		{
			name:  "no report line",
			raw:   "START RequestId: abc-123 Version: $LATEST\nsome output\n",
			found: false,
		},
		{
			name:  "oom report",
			raw:   "START RequestId: 595dc20a-caa6-455c-b6cb-182bc88397ed Version: $LATEST\n" + reportOOM + "\n",
			found: true,
			want: lambdaReport{
				RequestID:        "595dc20a-caa6-455c-b6cb-182bc88397ed",
				DurationMS:       103085.64,
				BilledDurationMS: 103086,
				MemorySizeMB:     128,
				MaxMemoryUsedMB:  126,
				Status:           "error",
				ErrorType:        "Runtime.OutOfMemory",
			},
		},
		{
			name:  "healthy report has memory but no status",
			raw:   reportHealthy,
			found: true,
			want: lambdaReport{
				RequestID:        "9c1e95f4-8857-4877-813e-7c790b9e1c73",
				DurationMS:       41.71,
				BilledDurationMS: 42,
				MemorySizeMB:     384,
				MaxMemoryUsedMB:  232,
			},
		},
		{
			name: "fields separated by spaces instead of tabs",
			raw: "REPORT RequestId: abc-123  Duration: 10.50 ms  Billed Duration: 11 ms  " +
				"Memory Size: 256 MB  Max Memory Used: 250 MB  Status: timeout",
			found: true,
			want: lambdaReport{
				RequestID:        "abc-123",
				DurationMS:       10.50,
				BilledDurationMS: 11,
				MemorySizeMB:     256,
				MaxMemoryUsedMB:  250,
				Status:           "timeout",
			},
		},
		{
			name: "init duration and xray fields do not shift other values",
			raw: "REPORT RequestId: abc-123\tDuration: 20.00 ms\tBilled Duration: 20 ms\t" +
				"Memory Size: 512 MB\tMax Memory Used: 100 MB\tInit Duration: 300.12 ms\t" +
				"XRAY TraceId: 1-abc-def\tSegmentId: 0123456789abcdef\tSampled: true",
			found: true,
			want: lambdaReport{
				RequestID:        "abc-123",
				DurationMS:       20.00,
				BilledDurationMS: 20,
				MemorySizeMB:     512,
				MaxMemoryUsedMB:  100,
			},
		},
		{
			name: "last report line wins on a warm sandbox tail",
			raw: reportHealthy + "\n" +
				"START RequestId: 595dc20a-caa6-455c-b6cb-182bc88397ed Version: $LATEST\n" +
				reportOOM,
			found: true,
			want: lambdaReport{
				RequestID:        "595dc20a-caa6-455c-b6cb-182bc88397ed",
				DurationMS:       103085.64,
				BilledDurationMS: 103086,
				MemorySizeMB:     128,
				MaxMemoryUsedMB:  126,
				Status:           "error",
				ErrorType:        "Runtime.OutOfMemory",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, found := parseLambdaReportLine(c.raw)
			require.Equal(t, c.found, found, "unexpected report-line presence")
			require.Equal(t, c.want, got, "unexpected parsed report")
		})
	}
}

func TestClassifyLambdaFailure(t *testing.T) {
	cases := []struct {
		name          string
		functionError string
		statusCode    int32
		payload       string
		rawLog        string

		wantClass        string
		wantCode         codes.Code
		wantRequestID    string
		wantErrorType    string
		wantMemorySize   int
		wantMaxMemory    int
		wantDurationMS   float64
		wantLogSummary   string
		wantUtilization  int
		wantErrorMessage string
	}{
		{
			// The production shape: the platform kills the sandbox, there is no
			// application log line to surface, and the only statement of cause
			// is the REPORT line's Error Type.
			name:          "oom from report error type",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       `{"errorType":"Runtime.ExitError","errorMessage":"RequestId: 595dc20a-caa6-455c-b6cb-182bc88397ed Error: Runtime exited with error: signal: killed"}`,
			rawLog: "START RequestId: 595dc20a-caa6-455c-b6cb-182bc88397ed Version: $LATEST\n" +
				"END RequestId: 595dc20a-caa6-455c-b6cb-182bc88397ed\n" + reportOOM + "\n",

			wantClass:       FailureClassOOM,
			wantCode:        codes.ResourceExhausted,
			wantRequestID:   "595dc20a-caa6-455c-b6cb-182bc88397ed",
			wantErrorType:   "Runtime.OutOfMemory",
			wantMemorySize:  128,
			wantMaxMemory:   126,
			wantDurationMS:  103085.64,
			wantLogSummary:  "",
			wantUtilization: 98,
			wantErrorMessage: "RequestId: 595dc20a-caa6-455c-b6cb-182bc88397ed " +
				"Error: Runtime exited with error: signal: killed",
		},
		{
			// Older runtimes stamp the kill without an Error Type, so peak
			// memory reaching the ceiling on a failed invoke is the fallback.
			name:          "oom from memory at ceiling without error type",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       `{"errorType":"Runtime.ExitError","errorMessage":"signal: killed"}`,
			rawLog: "REPORT RequestId: abc-123\tDuration: 5000.00 ms\tBilled Duration: 5000 ms\t" +
				"Memory Size: 128 MB\tMax Memory Used: 128 MB\tStatus: error\n",

			wantClass:        FailureClassOOM,
			wantCode:         codes.ResourceExhausted,
			wantRequestID:    "abc-123",
			wantErrorType:    "Runtime.ExitError",
			wantMemorySize:   128,
			wantMaxMemory:    128,
			wantDurationMS:   5000,
			wantUtilization:  100,
			wantErrorMessage: "signal: killed",
		},
		{
			// Existing signal, unchanged.
			name:          "timeout from payload",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       `{"errorMessage":"2026-05-22T21:50:47.000Z 595dc20a Task timed out after 120.00 seconds"}`,
			rawLog: "START RequestId: abc-123 Version: $LATEST\n" +
				"REPORT RequestId: abc-123\tDuration: 120000.00 ms\tBilled Duration: 120000 ms\t" +
				"Memory Size: 128 MB\tMax Memory Used: 128 MB\tStatus: timeout\n",

			wantClass:      FailureClassTimeout,
			wantCode:       codes.DeadlineExceeded,
			wantRequestID:  "abc-123",
			wantMemorySize: 128,
			wantMaxMemory:  128,
			wantDurationMS: 120000,
			// Peak memory is at the ceiling here too, so this case also pins
			// the ordering: a timeout must not be reclassified as an OOM.
			wantUtilization: 100,
			wantErrorMessage: "2026-05-22T21:50:47.000Z 595dc20a " +
				"Task timed out after 120.00 seconds",
		},
		{
			// New signal: the REPORT line alone classifies the timeout.
			name:          "timeout from report status only",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       ``,
			rawLog: "REPORT RequestId: abc-123\tDuration: 60000.00 ms\tBilled Duration: 60000 ms\t" +
				"Memory Size: 512 MB\tMax Memory Used: 100 MB\tStatus: timeout\n",

			wantClass:       FailureClassTimeout,
			wantCode:        codes.DeadlineExceeded,
			wantRequestID:   "abc-123",
			wantMemorySize:  512,
			wantMaxMemory:   100,
			wantDurationMS:  60000,
			wantUtilization: 19,
		},
		{
			// Existing signal, unchanged: the connector logged its own context
			// deadline in an escaped, nested form.
			name:          "timeout from escaped context deadline in logs",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       ``,
			rawLog: `lambda-run: sync failed: {\"error\":\"context deadline exceeded\"}` + "\n" +
				reportHealthy + "\n",

			wantClass:       FailureClassTimeout,
			wantCode:        codes.DeadlineExceeded,
			wantRequestID:   "9c1e95f4-8857-4877-813e-7c790b9e1c73",
			wantMemorySize:  384,
			wantMaxMemory:   232,
			wantDurationMS:  41.71,
			wantLogSummary:  `lambda-run: sync failed: {\"error\":\"context deadline exceeded\"}`,
			wantUtilization: 60,
		},
		{
			name:          "panic is unhandled and keeps the application log lines",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       `{"errorType":"Runtime.ExitError","errorMessage":"RequestId: abc-123 Error: Runtime exited with error: exit status 2"}`,
			rawLog: "START RequestId: abc-123 Version: $LATEST\n" +
				"panic: runtime error: invalid memory address or nil pointer dereference\n" +
				"goroutine 1 [running]:\n" +
				"REPORT RequestId: abc-123\tDuration: 120.00 ms\tBilled Duration: 120 ms\t" +
				"Memory Size: 512 MB\tMax Memory Used: 90 MB\tStatus: error\tError Type: Runtime.ExitError\n",

			wantClass:      FailureClassUnhandled,
			wantCode:       codes.Unknown,
			wantRequestID:  "abc-123",
			wantErrorType:  "Runtime.ExitError",
			wantMemorySize: 512,
			wantMaxMemory:  90,
			wantDurationMS: 120,
			wantLogSummary: "panic: runtime error: invalid memory address or nil pointer dereference\n" +
				"goroutine 1 [running]:",
			wantUtilization:  17,
			wantErrorMessage: "RequestId: abc-123 Error: Runtime exited with error: exit status 2",
		},
		{
			name:          "handled function error",
			functionError: "Handled",
			statusCode:    200,
			payload:       `{"errorType":"connectorError","errorMessage":"account_inactive"}`,
			rawLog:        "lambda-run: failed to get connector: authenticating during initialization\n" + reportHealthy + "\n",

			wantClass:        FailureClassHandled,
			wantCode:         codes.Unknown,
			wantRequestID:    "9c1e95f4-8857-4877-813e-7c790b9e1c73",
			wantErrorType:    "connectorError",
			wantMemorySize:   384,
			wantMaxMemory:    232,
			wantDurationMS:   41.71,
			wantLogSummary:   "lambda-run: failed to get connector: authenticating during initialization",
			wantUtilization:  60,
			wantErrorMessage: "account_inactive",
		},
		{
			name:          "no report line leaves memory fields unknown",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       ``,
			rawLog:        "lambda-run: unexpected failure\n",

			wantClass:      FailureClassUnhandled,
			wantCode:       codes.Unknown,
			wantLogSummary: "lambda-run: unexpected failure",
		},
		{
			// A connector whose SDK predates the RPC cannot resolve the request
			// message's type URL. The method is absent, not broken, so it maps
			// to Unimplemented and the caller can skip the step.
			name:          "unresolved request type from an old connector sdk",
			functionError: "Unhandled",
			statusCode:    200,
			payload: `{"errorMessage":"proto: (line 1:88): unable to resolve ` +
				`\"type.googleapis.com/c1.connector.v2.ExampleServiceListExamplesRequest\": ` +
				`\"not found\"","errorType":"prefixError"}`,
			rawLog: "2026/05/22 21:50:47 unable to resolve type URL\n",

			wantClass:      FailureClassUnsupportedRPC,
			wantCode:       codes.Unimplemented,
			wantErrorType:  "prefixError",
			wantLogSummary: "2026/05/22 21:50:47 unable to resolve type URL",
			wantErrorMessage: `proto: (line 1:88): unable to resolve ` +
				`"type.googleapis.com/c1.connector.v2.ExampleServiceListExamplesRequest": "not found"`,
		},
		{
			// The memory fallback infers an OOM from peak memory reaching the
			// ceiling. A function that surfaced its own error value already
			// explained the failure, and a Go runtime routinely sits at its
			// ceiling without being killed, so the ceiling proves nothing here.
			name:          "function error type at memory ceiling is not an oom",
			functionError: "Unhandled",
			statusCode:    200,
			payload:       `{"errorType":"prefixError","errorMessage":"malformed request"}`,
			rawLog: "REPORT RequestId: abc-123\tDuration: 5000.00 ms\tBilled Duration: 5000 ms\t" +
				"Memory Size: 128 MB\tMax Memory Used: 128 MB\tStatus: error\n",

			wantClass:        FailureClassUnhandled,
			wantCode:         codes.Unknown,
			wantRequestID:    "abc-123",
			wantErrorType:    "prefixError",
			wantMemorySize:   128,
			wantMaxMemory:    128,
			wantDurationMS:   5000,
			wantUtilization:  100,
			wantErrorMessage: "malformed request",
		},
		{
			// An absent RPC is a statement only the function can make, so the
			// classification reads the payload's error type rather than the
			// resolved one. A REPORT line carrying its own Error Type must not
			// overwrite that and turn the capability gap back into a crash --
			// the ErrorType field still reports the platform's verdict, because
			// that is what the field means.
			name:          "report error type does not mask an unresolved request type",
			functionError: "Unhandled",
			statusCode:    200,
			payload: `{"errorMessage":"proto: (line 1:88): unable to resolve ` +
				`\"type.googleapis.com/c1.connector.v2.ExampleServiceListExamplesRequest\": ` +
				`\"not found\"","errorType":"prefixError"}`,
			rawLog: "REPORT RequestId: def-456\tDuration: 12.00 ms\tBilled Duration: 12 ms\t" +
				"Memory Size: 128 MB\tMax Memory Used: 64 MB\tStatus: error\t" +
				"Error Type: Runtime.ExitError\n",

			wantClass:      FailureClassUnsupportedRPC,
			wantCode:       codes.Unimplemented,
			wantRequestID:  "def-456",
			wantErrorType:  "Runtime.ExitError",
			wantMemorySize: 128,
			wantMaxMemory:  64,
			wantDurationMS: 12,
			wantErrorMessage: `proto: (line 1:88): unable to resolve ` +
				`"type.googleapis.com/c1.connector.v2.ExampleServiceListExamplesRequest": "not found"`,
			wantUtilization: 50,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			failure := classifyLambdaFailure(c.functionError, c.statusCode, []byte(c.payload), c.rawLog)

			require.Equal(t, c.wantClass, failure.FailureClass, "unexpected failure class")
			require.Equal(t, c.wantCode, failure.Code(), "unexpected grpc code")
			require.Equal(t, c.wantRequestID, failure.RequestID, "unexpected request id")
			require.Equal(t, c.wantErrorType, failure.ErrorType, "unexpected error type")
			require.Equal(t, c.wantErrorMessage, failure.ErrorMessage, "unexpected error message")
			require.Equal(t, c.wantMemorySize, failure.MemorySizeMB, "unexpected memory size")
			require.Equal(t, c.wantMaxMemory, failure.MaxMemoryUsedMB, "unexpected max memory used")
			require.InDelta(t, c.wantDurationMS, failure.DurationMS, 0.001, "unexpected duration")
			require.Equal(t, c.wantLogSummary, failure.LogSummary, "unexpected log summary")
			require.Equal(t, c.wantUtilization, failure.MemoryUtilizationPct(), "unexpected memory utilization")
			require.Equal(t, c.functionError, failure.FunctionError, "unexpected function error")
			require.Equal(t, c.statusCode, failure.StatusCode, "unexpected status code")

			// Every failure must stay sanitizable: callers strip the log
			// summary by keying off these two markers, so no path may omit
			// either one.
			//
			// FROZEN marker text (RFC 0009 §4.4): the hosted runner's
			// invoke-error mapping string-matches "lambda_transport:" and
			// "logSummary:" to classify infra failures. Rewording either
			// marker silently breaks that classification under an unchanged
			// consumer (the #1048 incident shape). The freeze lifts only
			// when the runner's typed failure-class routing lands.
			require.Contains(t, failure.Error(), "lambda_transport:", "error string must carry the transport prefix")
			require.Contains(t, failure.Error(), "logSummary:", "error string must carry the logSummary separator")
		})
	}
}

// TestClassifyLambdaFailureOOMIsSearchable pins the acceptance requirement that
// the OOM and timeout signals are greppable in a log aggregator.
func TestClassifyLambdaFailureOOMIsSearchable(t *testing.T) {
	oom := classifyLambdaFailure("Unhandled", 200, nil, reportOOM)
	require.Equal(t, FailureClassOOM, oom.FailureClass)
	// This real OOM peaked just below its ceiling (126 MB of 128 MB), so a
	// memory-comparison heuristic alone would have missed it. Reading the
	// REPORT line's Error Type is what makes it detectable.
	require.Less(t, oom.MaxMemoryUsedMB, oom.MemorySizeMB,
		"fixture must keep exercising the Error Type path, not the memory fallback")
	require.Equal(t, "Runtime.OutOfMemory", oom.ErrorType)
	require.Contains(t, oom.Error(), "function ran out of memory")
	require.Contains(t, oom.Error(), "used 126 MB of 128 MB")
	require.Contains(t, oom.Error(), "Runtime.OutOfMemory")
	require.Contains(t, oom.Error(), "595dc20a-caa6-455c-b6cb-182bc88397ed")

	timedOut := classifyLambdaFailure(
		"Unhandled", 200,
		[]byte(`{"errorMessage":"Task timed out after 120.00 seconds"}`),
		reportHealthy,
	)
	require.Equal(t, FailureClassTimeout, timedOut.FailureClass)
	require.Contains(t, timedOut.Error(), "function timed out")
}

// TestLambdaInvokeFailureIsRecoverable pins the two ways a caller reads the
// failure: errors.As for the structured fields, and status.Code for the retry
// decision. The DeadlineExceeded case matters most - the sync framework relies
// on that code to retry and checkpoint.
func TestLambdaInvokeFailureIsRecoverable(t *testing.T) {
	var err error = classifyLambdaFailure("Unhandled", 200, nil, reportOOM)
	wrapped := fmt.Errorf("invoking connector: %w", err)

	var failure *LambdaInvokeFailure
	require.True(t, errors.As(wrapped, &failure), "failure must be recoverable with errors.As through a wrap")
	require.Equal(t, FailureClassOOM, failure.FailureClass)
	require.Equal(t, 128, failure.MemorySizeMB)
	require.Equal(t, 126, failure.MaxMemoryUsedMB)
	require.Equal(t, 98, failure.MemoryUtilizationPct())

	require.Equal(t, codes.ResourceExhausted, status.Code(err), "oom must map to ResourceExhausted")

	timedOut := classifyLambdaFailure(
		"Unhandled", 200,
		[]byte(`{"errorMessage":"Task timed out after 120.00 seconds"}`),
		reportHealthy,
	)
	require.Equal(t, codes.DeadlineExceeded, status.Code(error(timedOut)), "timeout must stay DeadlineExceeded")

	st, ok := status.FromError(error(timedOut))
	require.True(t, ok, "failure must satisfy the grpc status interface")
	require.Equal(t, codes.DeadlineExceeded, st.Code())
	require.Contains(t, st.Message(), "lambda_transport:")
}

func TestMemoryUtilizationPct(t *testing.T) {
	cases := []struct {
		name     string
		size     int
		used     int
		expected int
	}{
		{name: "unknown when report absent", size: 0, used: 0, expected: 0},
		{name: "unknown when size missing", size: 0, used: 100, expected: 0},
		{name: "unknown when used missing", size: 128, used: 0, expected: 0},
		{name: "at ceiling", size: 128, used: 128, expected: 100},
		{name: "just under ceiling", size: 128, used: 126, expected: 98},
		{name: "headroom after raise", size: 384, used: 232, expected: 60},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			failure := &LambdaInvokeFailure{MemorySizeMB: c.size, MaxMemoryUsedMB: c.used}
			require.Equal(t, c.expected, failure.MemoryUtilizationPct())
		})
	}
}

// truncatedTail builds a tail log that AWS cut mid-line: it opens with the back
// half of a structured log record, so the fragment does not start with "{" and
// would otherwise survive the JSON filter.
// It returns the leading fragment and the whole tail.
func truncatedTail() (string, string) {
	firstLineFragment := `"catalog_name":"example","duration_ms":201,"method":"GET",` +
		`"pageToken":"","query":{"limit":"50"},"url":"/v2/persons/1/employments"}`

	var b strings.Builder
	_, _ = b.WriteString(firstLineFragment)
	_, _ = b.WriteString("\n")
	for b.Len() < lambdaLogTailTruncationThresholdBytes {
		_, _ = b.WriteString(`{"level":"debug","msg":"listing resources","duration_ms":12,"method":"GET"}` + "\n")
	}
	_, _ = b.WriteString(reportOOM + "\n")

	return firstLineFragment, b.String()
}

func TestDropTruncatedFirstLine(t *testing.T) {
	fragment, truncated := truncatedTail()
	require.GreaterOrEqual(t, len(truncated), lambdaLogTailTruncationThresholdBytes)

	t.Run("drops the partial leading line", func(t *testing.T) {
		require.NotContains(t, dropTruncatedFirstLine(truncated), fragment)
	})

	t.Run("keeps a whole leading line in a full window", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("START RequestId: abc-123 Version: $LATEST\n")
		for b.Len() < lambdaLogTailTruncationThresholdBytes {
			b.WriteString(`{"level":"debug","msg":"listing resources","duration_ms":12}` + "\n")
		}
		require.Equal(t, b.String(), dropTruncatedFirstLine(b.String()))
	})

	t.Run("keeps a plain-text leading line below the threshold", func(t *testing.T) {
		raw := "lambda-run: failed to get connector: authenticating during initialization\n" + reportOOM
		require.Less(t, len(raw), lambdaLogTailTruncationThresholdBytes)
		require.Equal(t, raw, dropTruncatedFirstLine(raw))
	})

	t.Run("keeps a timestamped leading line in a full window", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("2026-05-22T21:50:47.000Z\tabc-123\tINFO\tlisting resources\n")
		for b.Len() < lambdaLogTailTruncationThresholdBytes {
			b.WriteString(`{"level":"debug","msg":"listing resources","duration_ms":12}` + "\n")
		}
		require.Equal(t, b.String(), dropTruncatedFirstLine(b.String()))
	})

	t.Run("keeps a single-line log with no newline", func(t *testing.T) {
		raw := strings.Repeat("x", lambdaLogTailTruncationThresholdBytes+10)
		require.Equal(t, raw, dropTruncatedFirstLine(raw))
	})

	// Connector runtimes log through Go's standard logger, whose default prefix
	// is "2006/01/02 15:04:05". Recognising only RFC3339 treated every one of
	// those whole lines as a truncated fragment and dropped it.
	t.Run("keeps a Go stdlib timestamped leading line in a full window", func(t *testing.T) {
		var b strings.Builder
		b.WriteString("2026/05/22 21:50:47 lambda-run: failed to get connector\n")
		for b.Len() < lambdaLogTailTruncationThresholdBytes {
			b.WriteString(`{"level":"debug","msg":"listing resources","duration_ms":12}` + "\n")
		}
		require.Equal(t, b.String(), dropTruncatedFirstLine(b.String()))
	})
}

// TestClassifyLambdaFailureTruncationDoesNotChangeClass pins that sanitizing the
// log summary cannot change which class an invoke lands in.
//
// The truncation pre-filter drops a leading line it cannot recognise as a whole
// record. Classifying from that filtered text made the in-function timeout
// signal vanish whenever it landed on the first line of a full tail window,
// downgrading a retryable DeadlineExceeded into a terminal Unknown.
func TestClassifyLambdaFailureTruncationDoesNotChangeClass(t *testing.T) {
	const deadlineMarker = `\"error\":\"context deadline exceeded\"`

	fullTail := func(firstLine string) string {
		var b strings.Builder
		b.WriteString(firstLine)
		b.WriteString("\n")
		for b.Len() < lambdaLogTailTruncationThresholdBytes {
			b.WriteString("2026/05/22 21:50:48 still listing resources\n")
		}
		b.WriteString(reportHealthy)
		return b.String()
	}

	// A whole Go-stdlib line that the pre-filter used to misjudge as a fragment.
	t.Run("whole leading line carrying the signal", func(t *testing.T) {
		raw := fullTail(`2026/05/22 21:50:47 {"level":"error",` + deadlineMarker + `}`)
		require.GreaterOrEqual(t, len(raw), lambdaLogTailTruncationThresholdBytes)

		failure := classifyLambdaFailure("Unhandled", 200, nil, raw)
		require.Equal(t, FailureClassTimeout, failure.FailureClass)
		require.Equal(t, codes.DeadlineExceeded, failure.Code(),
			"a timeout must stay retryable for the sync framework")
	})

	// A genuine mid-record fragment that still carries the signal. The pre-filter
	// is right to keep this out of the summary and wrong to let that decision
	// reach classification, so this case is covered by reading the raw log.
	t.Run("truncated leading line carrying the signal", func(t *testing.T) {
		fragment := `msg":"page timed out",` + deadlineMarker + `}`
		raw := fullTail(fragment)
		require.GreaterOrEqual(t, len(raw), lambdaLogTailTruncationThresholdBytes)

		failure := classifyLambdaFailure("Unhandled", 200, nil, raw)
		require.Equal(t, FailureClassTimeout, failure.FailureClass)
		require.Equal(t, codes.DeadlineExceeded, failure.Code())
		require.NotContains(t, failure.LogSummary, fragment,
			"the fragment must still be kept out of the sanitizable summary")
	})
}

// TestClassifyLambdaFailureTruncatedTailDoesNotLeak is the regression test for
// the leak: before this change a truncated tail's leading fragment became the
// entire error string, with no transport prefix, so it bypassed sanitization and
// could surface connector log content in a customer-visible field.
func TestClassifyLambdaFailureTruncatedTailDoesNotLeak(t *testing.T) {
	fragment, truncated := truncatedTail()

	// Pin the mechanism: the JSON filter on its own does not stop the fragment,
	// because a line cut mid-record no longer starts with "{".
	require.Contains(t, extractMeaningfulLogLines(truncated), fragment,
		"precondition: the line filter alone lets the partial line through")

	failure := classifyLambdaFailure("Unhandled", 200, nil, truncated)

	require.NotContains(t, failure.LogSummary, fragment, "the partial line must not reach the log summary")
	require.NotContains(t, failure.Error(), fragment, "the partial line must not reach the error string")
	require.Contains(t, failure.Error(), "lambda_transport:", "the error must remain sanitizable")
	require.Contains(t, failure.Error(), "logSummary:", "the error must remain sanitizable")

	// The REPORT line still classifies the invoke, which is the whole point of
	// reading it instead of discarding it.
	require.Equal(t, FailureClassOOM, failure.FailureClass)
	require.Equal(t, 128, failure.MemorySizeMB)
	require.Equal(t, 126, failure.MaxMemoryUsedMB)
}

func TestLooksLikeTimestampPrefix(t *testing.T) {
	cases := []struct {
		line     string
		expected bool
	}{
		{line: "2026-05-22T21:50:47.000Z\tabc\tINFO\thello", expected: true},
		// Go's standard logger default prefix, which connector runtimes emit.
		{line: "2026/05/22 21:50:47 lambda-run: failed to get connector", expected: true},
		{line: "2026/05/22 21:50:47", expected: true},
		{line: "2026/05/22 21:50", expected: false},
		{line: "2026/05/22", expected: false},
		{line: "2026-05-22 21:50:47", expected: false},
		{line: "202X-05-22T21:50:47.000Z", expected: false},
		{line: "2026-05-22", expected: false},
		{line: "", expected: false},
		{line: `"catalog_name":"example"}`, expected: false},
	}

	for _, c := range cases {
		t.Run(c.line, func(t *testing.T) {
			require.Equal(t, c.expected, looksLikeTimestampPrefix(c.line))
		})
	}
}

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
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := extractMeaningfulLogLines(c.raw)
			require.Equal(t, c.output, result, "unexpected log line extraction result")
		})
	}
}
