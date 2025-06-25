package grpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
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
	payload, err := req.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("lambda_transport: failed to marshal frame: %w", err)
	}

	input := &lambda.InvokeInput{
		LogType:      types.LogTypeTail,
		FunctionName: aws.String(l.functionName),
		Payload:      payload,
	}

	// Invoke the Lambda function.
	invokeResp, err := l.lambdaClient.Invoke(ctx, input)
	if err != nil {
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

		filteredLogs := extractMeaningfulLogLines(logSummary)

		return nil, fmt.Errorf(
			"lambda_transport: function returned error: %s; logSummary: %s",
			*invokeResp.FunctionError,
			filteredLogs,
		)
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

	// TODO(morgabra): Should we do some of this stuff? (e.g. detect ctx deadline and set grpc-timeout, etc?)
	// https://github.com/grpc/grpc-go/blob/9dc22c029c2592b5b6235d9ef6f14d62ecd6a509/internal/transport/http2_client.go#L541
	md, _ := metadata.FromOutgoingContext(ctx)

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

func extractMeaningfulLogLines(raw string) string {
	lines := strings.Split(raw, "\n")
	var filtered []string

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip noisy AWS system log lines
		if line == "" ||
			strings.HasPrefix(line, "START RequestId:") ||
			strings.HasPrefix(line, "END RequestId:") ||
			strings.HasPrefix(line, "REPORT RequestId:") ||
			strings.HasPrefix(line, "INIT_REPORT") ||
			strings.HasPrefix(line, "RequestId:") || // duplicate sometimes appears
			strings.HasPrefix(line, "Duration:") ||
			strings.HasPrefix(line, "Billed Duration:") ||
			strings.HasPrefix(line, "Memory Size:") ||
			strings.HasPrefix(line, "Max Memory Used:") ||
			strings.Contains(line, "Runtime.ExitError") {
			continue
		}

		filtered = append(filtered, line)
	}

	return strings.Join(filtered, "\n")
}
