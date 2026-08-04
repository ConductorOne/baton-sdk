package grpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type lambdaTransport struct {
	lambdaClient *lambda.Client
	functionName string
}

func (l *lambdaTransport) RoundTrip(ctx context.Context, req *Request) (*Response, error) {
	payload, frameOnly, err := req.marshalPayload()
	if err != nil {
		return nil, fmt.Errorf("lambda_transport: failed to marshal frame: %w", err)
	}
	if frameOnly != nil {
		ctxzap.Extract(ctx).Warn(
			"lambda_transport: request has no legacy encoding, sending v2 frame only; a connector on a pre-frame SDK cannot process this call",
			zap.String("method", req.Method()),
			zap.String("function_name", l.functionName),
			zap.NamedError("legacy_encoding_error", frameOnly),
		)
	}

	input := &lambda.InvokeInput{
		LogType:      types.LogTypeTail,
		FunctionName: aws.String(l.functionName),
		Payload:      payload,
	}

	// Invoke the Lambda function.
	invokeResp, err := l.lambdaClient.Invoke(ctx, input)
	if err != nil {
		if isTransientNetworkError(err) {
			return nil, status.Errorf(codes.Unavailable, "lambda_transport: transient network error invoking function: %s", err)
		}
		return nil, fmt.Errorf("lambda_transport: failed to invoke lambda function: %w", err)
	}

	// Check if the function returned an error.
	if invokeResp.FunctionError != nil {
		logSummary := ""
		if invokeResp.LogResult != nil {
			decodedLog, err := base64.StdEncoding.DecodeString(*invokeResp.LogResult)
			if err == nil {
				logSummary = string(decodedLog)
			}
		}

		return nil, classifyLambdaError(*invokeResp.FunctionError, invokeResp.StatusCode, invokeResp.Payload, logSummary)
	}

	resp := &Response{}
	err = json.Unmarshal(invokeResp.Payload, resp)
	if err != nil {
		return nil, fmt.Errorf("lambda_transport: failed to unmarshal response: %w", err)
	}

	return resp, err
}

// NewLambdaClientTransport returns a new client transport that invokes a lambda function.
func NewLambdaClientTransport(ctx context.Context, client *lambda.Client, functionName string) (ClientTransport, error) {
	return &lambdaTransport{
		lambdaClient: client,
		functionName: functionName,
	}, nil
}

type ClientTransport interface {
	RoundTrip(context.Context, *Request) (*Response, error)
}

type clientConn struct {
	t ClientTransport
}

func (c *clientConn) Invoke(ctx context.Context, method string, args any, reply any, opts ...grpc.CallOption) error {
	req, reqOk := args.(proto.Message)
	resp, respOk := reply.(proto.Message)
	if !reqOk || !respOk {
		return status.Errorf(codes.Unknown, "args and reply must satisfy proto.Message")
	}

	md, _ := metadata.FromOutgoingContext(ctx)

	// Propagate the context deadline to the server via the grpc-timeout header,
	// mirroring grpc-go's HTTP/2 transport behavior.
	if deadline, ok := ctx.Deadline(); ok {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return status.Errorf(codes.DeadlineExceeded, "context deadline exceeded before invoking method %s", method)
		}
		md = md.Copy()
		md.Set("grpc-timeout", encodeTimeout(timeout))
	}

	treq, err := NewRequest(method, req, md)
	if err != nil {
		return status.Errorf(codes.Unknown, "failed creating request: %s", err)
	}

	tresp, err := c.t.RoundTrip(ctx, treq)
	if err != nil {
		return err
	}

	st, err := tresp.Status()
	if err != nil {
		return err
	}

	if st.Code() != codes.OK {
		return st.Err()
	}

	err = tresp.UnmarshalResponse(resp)
	if err != nil {
		return err
	}

	// TODO(morgabra): call opts here, some are probably important (e.g. PerRPCCredsCallOption, etc)
	for _, opt := range opts {
		switch o := opt.(type) {
		case grpc.HeaderCallOption:
			for k, v := range tresp.Headers() {
				o.HeaderAddr.Append(k, v...)
			}
		case grpc.TrailerCallOption:
			for k, v := range tresp.Trailers() {
				o.TrailerAddr.Append(k, v...)
			}
		}
	}

	return nil
}

func (c *clientConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Errorf(codes.Unimplemented, "streaming is not supported")
}

func NewClientConn(transport ClientTransport) grpc.ClientConnInterface {
	return &clientConn{
		t: transport,
	}
}

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
		}) {
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

var (
	lambdaReportErrorTypeRegex = regexp.MustCompile(`Error Type:[ \t]*(\S+)`)
	lambdaReportStatusRegex    = regexp.MustCompile(`Status:[ \t]*(\S+)`)
)

// lambdaReportLine extracts the REPORT line from a Lambda log tail. AWS writes it
// last, so it survives 4KB tail truncation even when earlier lines (including
// START) are cut off. It carries AWS's own verdict on how the invoke ended.
func lambdaReportLine(rawLog string) string {
	report := ""
	for _, line := range strings.Split(rawLog, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "REPORT RequestId:") {
			report = line
		}
	}
	return report
}

// isLambdaReportOOM reports whether the Lambda REPORT line's Error Type field
// indicates an out-of-memory crash. This catches crashes under the memory
// ceiling (e.g. 126MB used of a 128MB limit), which max-memory-used arithmetic
// would miss.
func isLambdaReportOOM(rawLog string) bool {
	m := lambdaReportErrorTypeRegex.FindStringSubmatch(lambdaReportLine(rawLog))
	return len(m) == 2 && strings.Contains(strings.ToLower(m[1]), "outofmemory")
}

// isLambdaReportTimeout reports whether the Lambda REPORT line's Status field
// indicates AWS's platform-level timeout kill.
func isLambdaReportTimeout(rawLog string) bool {
	m := lambdaReportStatusRegex.FindStringSubmatch(lambdaReportLine(rawLog))
	return len(m) == 2 && strings.EqualFold(m[1], "timeout")
}

// isLambdaOOM checks the Lambda REPORT line and, failing that, the invoke
// payload for signs of an out-of-memory crash. The payload's "signal: killed"
// is also what a timeout kill produces, so callers must rule out a timeout
// before relying on this signal.
func isLambdaOOM(rawLog string, payload string) bool {
	if isLambdaReportOOM(rawLog) {
		return true
	}

	if strings.Contains(payload, "Runtime.ExitError") && strings.Contains(payload, "signal: killed") {
		return true
	}

	return strings.Contains(rawLog, "Runtime.ExitError") && strings.Contains(rawLog, "signal: killed")
}

// classifyLambdaError determines the appropriate error type for a Lambda function error.
func classifyLambdaError(functionError string, statusCode int32, payload []byte, rawLog string) error {
	payloadStr := string(payload)
	filteredLogs := extractMeaningfulLogLines(rawLog)

	if isLambdaReportOOM(rawLog) {
		return status.Errorf(codes.ResourceExhausted, "lambda_transport: function ran out of memory: %s; logSummary: %s", functionError, filteredLogs)
	}
	if strings.Contains(payloadStr, "Task timed out after") {
		return status.Errorf(codes.DeadlineExceeded, "lambda_transport: function timed out: %s; logSummary: %s", functionError, filteredLogs)
	}
	if isLambdaReportTimeout(rawLog) {
		return status.Errorf(codes.DeadlineExceeded, "lambda_transport: function timed out: %s; logSummary: %s", functionError, filteredLogs)
	}
	if strings.Contains(filteredLogs, `\"error\":\"context deadline exceeded\"`) {
		return status.Errorf(codes.DeadlineExceeded, "lambda_transport: function timed out: %s; logSummary: %s", functionError, filteredLogs)
	}
	if isLambdaOOM(rawLog, payloadStr) {
		return status.Errorf(codes.ResourceExhausted, "lambda_transport: function ran out of memory: %s; logSummary: %s", functionError, filteredLogs)
	}

	return fmt.Errorf(
		"lambda_transport: function returned error: %s; status code: %d; logSummary: %s",
		functionError,
		statusCode,
		filteredLogs,
	)
}
