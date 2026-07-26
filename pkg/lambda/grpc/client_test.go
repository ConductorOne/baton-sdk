package grpc

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeInvoker struct {
	resp *lambda.InvokeOutput
	err  error
}

func (f *fakeInvoker) Invoke(_ context.Context, _ *lambda.InvokeInput, _ ...func(*lambda.Options)) (*lambda.InvokeOutput, error) {
	return f.resp, f.err
}

// A function that runs but returns an error (FunctionError set) is a
// caller/function-state fault, not a server fault — RoundTrip must surface it
// as FailedPrecondition so it never shows up as a gRPC Unknown (counted 5xx).
func TestRoundTrip_FunctionErrorIsFailedPrecondition(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		logs    string
	}{
		{
			name:    "no meaningful logs",
			payload: "{}",
			logs:    "START RequestId: abc\nEND RequestId: abc\n",
		},
		{
			name:    "meaningful logs present",
			payload: "{}",
			logs:    "Unhandled: connector failed to initialize\n",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tr := &lambdaTransport{
				functionName: "fn",
				lambdaClient: &fakeInvoker{resp: &lambda.InvokeOutput{
					FunctionError: aws.String("Unhandled"),
					StatusCode:    200,
					Payload:       []byte(c.payload),
					LogResult:     aws.String(base64.StdEncoding.EncodeToString([]byte(c.logs))),
				}},
			}

			_, err := tr.RoundTrip(context.Background(), &Request{})
			require.Error(t, err)
			require.Equal(t, codes.FailedPrecondition, status.Code(err), "function-returned error must be FailedPrecondition, got: %v", err)
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
