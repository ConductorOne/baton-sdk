package transport

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"

	c1_lambda_grpc "github.com/ductone/c1-lambda/pkg/grpc"
)

type lambdaTransport struct {
	lambdaClient *lambda.Client
	functionName string
}

func (l *lambdaTransport) RoundTrip(ctx context.Context, req *c1_lambda_grpc.Request) (*c1_lambda_grpc.Response, error) {
	payload, err := req.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("lambda_transport: failed to marshal frame: %v", err)
	}

	input := &lambda.InvokeInput{
		FunctionName: aws.String(l.functionName),
		Payload:      payload,
	}

	// Invoke the Lambda function.
	invokeResp, err := l.lambdaClient.Invoke(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("lambda_transport: failed to invoke lambda function: %v", err)
	}

	// Check if the function returned an error.
	if invokeResp.FunctionError != nil {
		return nil, fmt.Errorf("lambda_transport: function returned error: %v", *invokeResp.FunctionError)
	}

	resp := &c1_lambda_grpc.Response{}
	err = json.Unmarshal(invokeResp.Payload, resp)
	if err != nil {
		return nil, fmt.Errorf("lambda_transport: failed to unmarshal response: %v", err)
	}

	return resp, err
}

// NewLambdaClientTransport returns a new client transport that invokes a lambda function.
func NewLambdaClientTransport(ctx context.Context, client *lambda.Client, functionName string) (c1_lambda_grpc.ClientTransport, error) {
	return &lambdaTransport{
		lambdaClient: client,
		functionName: functionName,
	}, nil
}
