package grpc

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Failure classes for a Lambda invoke that came back with a FunctionError set.
//
// The AWS platform reports a runtime crash as FunctionError: "Unhandled" with
// StatusCode 200 regardless of why the sandbox died, so the class has to be
// recovered from the tail log and the error payload. Callers use it to tell an
// out-of-memory kill apart from a platform timeout or an ordinary connector
// error without needing CloudWatch access.
const (
	// FailureClassOOM means the platform killed the sandbox for exceeding its
	// configured memory limit.
	FailureClassOOM = "oom"
	// FailureClassTimeout means the platform killed the sandbox for exceeding
	// its configured execution timeout, or the function's own context deadline
	// was exhausted mid-invoke.
	FailureClassTimeout = "timeout"
	// FailureClassUnhandled means the runtime crashed (panic, non-zero exit)
	// for a reason we could not narrow further.
	FailureClassUnhandled = "unhandled"
	// FailureClassHandled means the function returned an error response through
	// the runtime API rather than crashing.
	FailureClassHandled = "handled"
)

// oomErrorType is the Error Type the AWS platform stamps on the REPORT line
// when it kills a sandbox for exceeding its memory limit.
const oomErrorType = "Runtime.OutOfMemory"

// LambdaInvokeFailure is a structured description of a failed Lambda invoke.
//
// Every field is derived from data the invoke already returns: the base64 tail
// log (which always ends with the platform REPORT line), the error payload, and
// the FunctionError / StatusCode pair. Nothing here requires extra
// instrumentation or an additional API call.
//
// The memory and duration fields are zero when the REPORT line was absent from
// the tail window, so treat zero as "unknown" rather than "zero bytes used".
type LambdaInvokeFailure struct {
	// FailureClass is one of the FailureClass* constants.
	FailureClass string
	// RequestID is the AWS request ID, usable to jump straight to the matching
	// CloudWatch log stream.
	RequestID string
	// ErrorType is the runtime error type, from the REPORT line's Error Type
	// field when present, else from the error payload's errorType.
	ErrorType string
	// ErrorMessage is the error payload's errorMessage. It can carry connector
	// output, so it is treated as untrusted for customer-visible strings.
	ErrorMessage string
	// MemorySizeMB is the memory ceiling configured on the function.
	MemorySizeMB int
	// MaxMemoryUsedMB is the peak memory the platform observed for the invoke.
	MaxMemoryUsedMB int
	// DurationMS is the wall-clock duration the platform measured.
	DurationMS float64
	// BilledDurationMS is the rounded duration AWS billed.
	BilledDurationMS int
	// FunctionError is the raw AWS FunctionError value ("Unhandled"/"Handled").
	FunctionError string
	// StatusCode is the raw AWS invoke status code. It is 200 even for a
	// runtime crash, so it is recorded but never used for classification.
	StatusCode int32
	// LogSummary holds the filtered application log lines, with the same
	// semantics as before: platform bookkeeping and structured JSON lines
	// removed. It can carry connector output, so it must be logged rather than
	// returned to a customer.
	LogSummary string
}

// MemoryUtilizationPct returns peak memory as a percentage of the configured
// ceiling, or 0 when the REPORT line did not supply both numbers.
func (e *LambdaInvokeFailure) MemoryUtilizationPct() int {
	if e.MemorySizeMB <= 0 || e.MaxMemoryUsedMB <= 0 {
		return 0
	}
	return e.MaxMemoryUsedMB * 100 / e.MemorySizeMB
}

// Code returns the gRPC code this failure maps to.
//
// Timeouts stay codes.DeadlineExceeded because the sync framework relies on that
// code to retry and checkpoint. An OOM is codes.ResourceExhausted: retrying an
// OOM lands on a fresh sandbox with the same memory ceiling, so it fails again
// for the same reason every time until the connector or the Lambda memory
// configuration changes. Retrying it is not useful work, so it is treated as
// terminal rather than transient. Anything we could not classify keeps
// codes.Unknown, which is what the untyped errors this replaces already
// produced.
func (e *LambdaInvokeFailure) Code() codes.Code {
	switch e.FailureClass {
	case FailureClassTimeout:
		return codes.DeadlineExceeded
	case FailureClassOOM:
		return codes.ResourceExhausted
	default:
		return codes.Unknown
	}
}

// Error renders the failure.
//
// The string deliberately keeps both the "lambda_transport:" prefix and the
// "logSummary:" separator on every path. Downstream sanitizers key off those
// markers to strip the log summary before it can reach a customer-visible field
// such as a connector sync status last_error, and a path that omitted them used
// to slip raw log text past that check.
func (e *LambdaInvokeFailure) Error() string {
	var b strings.Builder
	_, _ = b.WriteString("lambda_transport: ")

	switch e.FailureClass {
	case FailureClassOOM:
		_, _ = b.WriteString("function ran out of memory")
		if e.MemorySizeMB > 0 && e.MaxMemoryUsedMB > 0 {
			fmt.Fprintf(&b, " (used %d MB of %d MB)", e.MaxMemoryUsedMB, e.MemorySizeMB)
		}
	case FailureClassTimeout:
		_, _ = b.WriteString("function timed out")
	default:
		_, _ = b.WriteString("function returned error")
	}

	fmt.Fprintf(&b, ": %s", e.FunctionError)
	if e.ErrorType != "" && e.ErrorType != e.FunctionError {
		fmt.Fprintf(&b, "; errorType: %s", e.ErrorType)
	}
	if e.RequestID != "" {
		fmt.Fprintf(&b, "; requestId: %s", e.RequestID)
	}
	fmt.Fprintf(&b, "; status code: %d", e.StatusCode)
	fmt.Fprintf(&b, "; logSummary: %s", e.LogSummary)

	return b.String()
}

// GRPCStatus lets status.Code and status.FromError see the mapped code while
// callers can still recover the struct with errors.As.
func (e *LambdaInvokeFailure) GRPCStatus() *status.Status {
	return status.New(e.Code(), e.Error())
}

// lambdaReport is the parsed platform REPORT line for one invoke.
//
// Memory Size and Max Memory Used are present on every invoke, successful or
// not. Status and Error Type appear only on failure.
type lambdaReport struct {
	RequestID        string
	DurationMS       float64
	BilledDurationMS int
	MemorySizeMB     int
	MaxMemoryUsedMB  int
	Status           string
	ErrorType        string
}

// reportFieldKeys are the labels AWS uses on the REPORT line. The list is used
// to locate field boundaries, so an unknown label is ignored rather than
// mis-parsed as part of the preceding value.
var reportFieldKeys = []string{
	"REPORT RequestId",
	"RequestId",
	"Version",
	"Duration",
	"Billed Duration",
	"Memory Size",
	"Max Memory Used",
	"Init Duration",
	"Restore Duration",
	"Billed Restore Duration",
	"Status",
	"Error Type",
	"XRAY TraceId",
	"SegmentId",
	"Sampled",
}

const reportLinePrefix = "REPORT RequestId:"

// parseLambdaReportFields splits a REPORT line into label/value pairs.
//
// AWS separates the fields with tabs, but the separator is not guaranteed and
// values themselves contain spaces ("103085.64 ms", "128 MB"), so splitting on
// whitespace is not safe. Instead we locate each known label and take
// everything up to the next label as its value. That works whether the fields
// are tab-separated, multi-space-separated, or single-space-separated, and it is
// order-independent.
func parseLambdaReportFields(line string) map[string]string {
	type hit struct {
		start int
		end   int // first byte after the label's colon
		key   string
	}

	var hits []hit
	for _, key := range reportFieldKeys {
		needle := key + ":"
		for offset := 0; ; {
			idx := strings.Index(line[offset:], needle)
			if idx < 0 {
				break
			}
			abs := offset + idx
			hits = append(hits, hit{start: abs, end: abs + len(needle), key: key})
			offset = abs + len(needle)
		}
	}

	// Longest label wins on overlap, so "Billed Duration:" is not also read as
	// a bare "Duration:" field.
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].start != hits[j].start {
			return hits[i].start < hits[j].start
		}
		return len(hits[i].key) > len(hits[j].key)
	})

	kept := make([]hit, 0, len(hits))
	for _, h := range hits {
		if len(kept) > 0 && h.start < kept[len(kept)-1].end {
			continue
		}
		kept = append(kept, h)
	}

	fields := make(map[string]string, len(kept))
	for i, h := range kept {
		valueEnd := len(line)
		if i+1 < len(kept) {
			valueEnd = kept[i+1].start
		}
		value := strings.TrimSpace(line[h.end:valueEnd])
		key := strings.TrimPrefix(h.key, "REPORT ")
		if _, ok := fields[key]; !ok {
			fields[key] = value
		}
	}

	return fields
}

// parseLambdaReportLine finds and parses the platform REPORT line in a tail log.
//
// The REPORT line is the last thing the platform writes for an invoke, so it is
// reliably inside the tail window even when the window cut off earlier output.
// The last occurrence is used because a warm sandbox's tail can include the
// trailing REPORT of a previous invoke.
func parseLambdaReportLine(raw string) (lambdaReport, bool) {
	line := ""
	for _, candidate := range strings.Split(raw, "\n") {
		candidate = strings.TrimSpace(candidate)
		if strings.HasPrefix(candidate, reportLinePrefix) {
			line = candidate
		}
	}
	if line == "" {
		return lambdaReport{}, false
	}

	fields := parseLambdaReportFields(line)
	report := lambdaReport{
		RequestID: fields["RequestId"],
		Status:    fields["Status"],
		ErrorType: fields["Error Type"],
	}
	report.MemorySizeMB = parseReportMB(fields["Memory Size"])
	report.MaxMemoryUsedMB = parseReportMB(fields["Max Memory Used"])
	report.BilledDurationMS = int(parseReportMS(fields["Billed Duration"]))
	report.DurationMS = parseReportMS(fields["Duration"])

	return report, true
}

// parseReportMB reads a "128 MB" style value. Unparseable input yields 0, which
// callers treat as unknown.
func parseReportMB(value string) int {
	n, err := strconv.Atoi(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(value), "MB")))
	if err != nil {
		return 0
	}
	return n
}

// parseReportMS reads a "103085.64 ms" style value. Unparseable input yields 0.
func parseReportMS(value string) float64 {
	f, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(value), "ms")), 64)
	if err != nil {
		return 0
	}
	return f
}

// lambdaErrorPayload is the JSON body the runtime API returns for a function
// error. It is dropped on the error path today, which is where the OOM error
// type would otherwise be visible.
type lambdaErrorPayload struct {
	ErrorType    string `json:"errorType"`
	ErrorMessage string `json:"errorMessage"`
}

func parseLambdaErrorPayload(payload []byte) lambdaErrorPayload {
	var parsed lambdaErrorPayload
	if len(payload) == 0 {
		return parsed
	}
	// A crashed sandbox does not always produce well-formed JSON; an
	// unparseable payload just leaves the fields empty.
	_ = json.Unmarshal(payload, &parsed)
	return parsed
}

// ignoredLogPrefixes are tail-log lines the AWS platform writes itself. They
// are bookkeeping, not diagnostic, so extractMeaningfulLogLines drops them.
var ignoredLogPrefixes = []string{
	"START RequestId:",
	"END RequestId:",
	"REPORT RequestId:",
	"INIT_REPORT",
	"RequestId:",
	"Duration:",
	"Billed Duration:",
	"Memory Size:",
	"Max Memory Used:",
}

// extractMeaningfulLogLines strips platform bookkeeping lines and structured
// JSON log lines from a tail log, keeping whatever plain-text application
// output remains. That remainder can carry connector output, so it must be
// logged rather than returned to a customer; see LogSummary.
func extractMeaningfulLogLines(raw string) string {
	lines := strings.Split(raw, "\n")
	var filtered []string

	for _, line := range lines {
		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}

		if slices.ContainsFunc(ignoredLogPrefixes, func(prefix string) bool {
			return strings.HasPrefix(line, prefix)
		}) || strings.Contains(line, "Runtime.ExitError") {
			continue
		}

		// Skip structured JSON log lines (zap logger output) - they are
		// diagnostic context, not the actual error.
		if strings.HasPrefix(line, "{") {
			continue
		}

		filtered = append(filtered, line)
	}

	return strings.Join(filtered, "\n")
}

// classifyLambdaFailure builds the structured failure for an invoke that came
// back with a FunctionError set.
func classifyLambdaFailure(functionError string, statusCode int32, payload []byte, rawLog string) *LambdaInvokeFailure {
	report, haveReport := parseLambdaReportLine(rawLog)
	errPayload := parseLambdaErrorPayload(payload)
	filteredLogs := extractMeaningfulLogLines(dropTruncatedFirstLine(rawLog))

	failure := &LambdaInvokeFailure{
		RequestID:        report.RequestID,
		ErrorType:        errPayload.ErrorType,
		ErrorMessage:     errPayload.ErrorMessage,
		MemorySizeMB:     report.MemorySizeMB,
		MaxMemoryUsedMB:  report.MaxMemoryUsedMB,
		DurationMS:       report.DurationMS,
		BilledDurationMS: report.BilledDurationMS,
		FunctionError:    functionError,
		StatusCode:       statusCode,
		LogSummary:       filteredLogs,
	}
	// The REPORT line's Error Type is the platform's own verdict, so it wins
	// over the payload's when both are present.
	if haveReport && report.ErrorType != "" {
		failure.ErrorType = report.ErrorType
	}
	failure.FailureClass = lambdaFailureClass(functionError, payload, filteredLogs, report, failure.ErrorType)

	return failure
}

// lambdaFailureClass decides which failure class an invoke falls into.
//
// Timeout is checked first: a sandbox killed on its execution timeout can also
// show peak memory at its ceiling, which would otherwise trip the OOM
// memory-comparison fallback.
func lambdaFailureClass(functionError string, payload []byte, filteredLogs string, report lambdaReport, errorType string) string {
	// Existing signal, unchanged: the platform writes this into the error
	// payload on a hard timeout kill.
	if strings.Contains(string(payload), "Task timed out after") {
		return FailureClassTimeout
	}
	// Existing signal, unchanged: the function's own context deadline was
	// exhausted and the connector logged it.
	if strings.Contains(filteredLogs, `\"error\":\"context deadline exceeded\"`) {
		return FailureClassTimeout
	}
	// New signal: newer runtimes stamp the outcome on the REPORT line.
	if strings.EqualFold(report.Status, "timeout") {
		return FailureClassTimeout
	}

	// Primary OOM signal, from the REPORT line or the error payload.
	if errorType == oomErrorType {
		return FailureClassOOM
	}
	// Fallback for runtimes that report the kill without an Error Type: a
	// failed invoke whose peak memory reached its ceiling was an OOM.
	if strings.EqualFold(report.Status, "error") &&
		report.MemorySizeMB > 0 && report.MaxMemoryUsedMB >= report.MemorySizeMB {
		return FailureClassOOM
	}

	if functionError == "Handled" {
		return FailureClassHandled
	}
	return FailureClassUnhandled
}

// lambdaLogTailTruncationThresholdBytes is the point at which a tail log may
// have been cut mid-line. AWS returns the last 4 KB of the execution log with
// LogType: Tail and cuts the window at a byte offset, not a line boundary, so a
// tail at or above roughly that size can open with a partial line.
const lambdaLogTailTruncationThresholdBytes = 4000

// dropTruncatedFirstLine removes a leading partial line from a truncated tail.
//
// The partial line is the cause of a real leak: a fragment that starts mid-way
// through a structured log record no longer begins with "{", so it survives the
// JSON filter in extractMeaningfulLogLines and can end up in an error string
// carrying whatever the connector happened to be logging.
//
// Both conditions must hold before anything is dropped. A log small enough to
// fit inside the tail window was not cut at all, and a first line that begins
// with a recognised line start is whole even in a full window. That keeps a
// genuine plain-text first line - which is the diagnostic we most want to
// preserve - out of the blast radius.
func dropTruncatedFirstLine(raw string) string {
	if len(raw) < lambdaLogTailTruncationThresholdBytes {
		return raw
	}
	first, rest, found := strings.Cut(raw, "\n")
	if !found {
		return raw
	}
	if looksLikeLogLineStart(strings.TrimSpace(first)) {
		return raw
	}
	return rest
}

// platformLogLinePrefixes are the line starts the AWS platform emits itself.
var platformLogLinePrefixes = []string{
	"START RequestId:",
	"END RequestId:",
	"REPORT RequestId:",
	"INIT_REPORT",
	"INIT_START",
	"RESTORE_REPORT",
	"RESTORE_START",
}

// looksLikeLogLineStart reports whether a line plausibly begins at a log record
// boundary rather than mid-record.
func looksLikeLogLineStart(line string) bool {
	if line == "" {
		return true
	}
	// A structured (zap/JSON) record.
	if strings.HasPrefix(line, "{") {
		return true
	}
	for _, prefix := range platformLogLinePrefixes {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	// A text-format runtime line, which the platform prefixes with an
	// RFC3339 timestamp such as "2006-01-02T15:04:05.000Z".
	return looksLikeTimestampPrefix(line)
}

// looksLikeTimestampPrefix reports whether a line opens with an RFC3339-style
// date, i.e. "NNNN-NN-NNT".
func looksLikeTimestampPrefix(line string) bool {
	const stamp = "NNNN-NN-NNT"
	if len(line) < len(stamp) {
		return false
	}
	for i := range len(stamp) {
		want := stamp[i]
		got := line[i]
		switch want {
		case 'N':
			if got < '0' || got > '9' {
				return false
			}
		default:
			if got != want {
				return false
			}
		}
	}
	return true
}
