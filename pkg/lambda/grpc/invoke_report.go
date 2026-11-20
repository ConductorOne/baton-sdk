package grpc

import (
	"encoding/base64"
	"encoding/json"

	"google.golang.org/grpc/metadata"
)

// LambdaInvokeReportMetadataKey is the response header under which the invoker
// returns the platform REPORT line of a successful Lambda invoke. Read it with
// a grpc.Header call option and LambdaInvokeReportFromMetadata.
//
// The value is set only by the invoking side of the lambda transport. A
// function cannot supply it: a header of the same name in the function's own
// response is dropped.
const LambdaInvokeReportMetadataKey = "baton-lambda-invoke-report"

// LambdaInvokeReport is the platform REPORT line of one successful invoke. It
// comes from the tail log the invoke response already carries, so it is
// available without access to the function's CloudWatch log group.
type LambdaInvokeReport struct {
	RequestID        string  `json:"request_id"`
	DurationMS       float64 `json:"duration_ms"`
	BilledDurationMS int     `json:"billed_duration_ms"`
	MemorySizeMB     int     `json:"memory_size_mb"`
	MaxMemoryUsedMB  int     `json:"max_memory_used_mb"`
	// ColdStart reports that this invoke started a new execution environment:
	// the REPORT line carried Init Duration (on-demand) or Restore Duration
	// (SnapStart). Provisioned-concurrency environments are initialized ahead
	// of the invoke and report neither.
	ColdStart bool `json:"cold_start"`
	// InitDurationMS is the platform's Init Duration. It is zero on a warm
	// invoke and on a SnapStart restore.
	InitDurationMS float64 `json:"init_duration_ms,omitempty"`
}

// LambdaInvokeReportFromMetadata returns the invoke report in md. ok is false
// when the response came from a transport other than Lambda, when the tail log
// did not contain this invoke's REPORT line, or when the value is malformed.
func LambdaInvokeReportFromMetadata(md metadata.MD) (LambdaInvokeReport, bool) {
	values := md.Get(LambdaInvokeReportMetadataKey)
	if len(values) != 1 {
		return LambdaInvokeReport{}, false
	}
	var report LambdaInvokeReport
	if err := json.Unmarshal([]byte(values[0]), &report); err != nil {
		return LambdaInvokeReport{}, false
	}
	return report, true
}

func (r LambdaInvokeReport) metadataValue() (string, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// invokeReportFromLogResult reads requestID's REPORT line from the base64 tail
// log of a successful invoke. A missing or undecodable log yields no report.
func invokeReportFromLogResult(logResult *string, requestID string) *LambdaInvokeReport {
	if logResult == nil {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(*logResult)
	if err != nil {
		return nil
	}
	report, ok := parseLambdaReportLineFor(string(decoded), requestID)
	if !ok {
		return nil
	}
	return &LambdaInvokeReport{
		RequestID:        report.RequestID,
		DurationMS:       report.DurationMS,
		BilledDurationMS: report.BilledDurationMS,
		MemorySizeMB:     report.MemorySizeMB,
		MaxMemoryUsedMB:  report.MaxMemoryUsedMB,
		ColdStart:        report.ColdStart,
		InitDurationMS:   report.InitDurationMS,
	}
}
