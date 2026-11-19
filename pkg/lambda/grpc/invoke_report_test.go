package grpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
)

const (
	coldRequestID = "11111111-2222-3333-4444-555555555555"
	warmRequestID = "66666666-7777-8888-9999-000000000000"
	reportCold    = "REPORT RequestId: " + coldRequestID + "\tDuration: 1500.25 ms\tBilled Duration: 1501 ms\t" +
		"Memory Size: 1024 MB\tMax Memory Used: 210 MB\tInit Duration: 412.75 ms\t"
	reportWarm = "REPORT RequestId: " + warmRequestID + "\tDuration: 80.50 ms\tBilled Duration: 81 ms\t" +
		"Memory Size: 1024 MB\tMax Memory Used: 215 MB\t"
)

func TestParseLambdaReportLineInitDuration(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		requestID string
		found     bool
		want      lambdaReport
	}{
		{
			name:  "cold start carries init duration",
			raw:   "START RequestId: " + coldRequestID + " Version: $LATEST\nhello\nEND RequestId: " + coldRequestID + "\n" + reportCold + "\n",
			found: true,
			want: lambdaReport{
				RequestID:        coldRequestID,
				DurationMS:       1500.25,
				BilledDurationMS: 1501,
				MemorySizeMB:     1024,
				MaxMemoryUsedMB:  210,
				InitDurationMS:   412.75,
				ColdStart:        true,
			},
		},
		{
			name:  "warm invoke has no init duration",
			raw:   reportWarm + "\n",
			found: true,
			want: lambdaReport{
				RequestID:        warmRequestID,
				DurationMS:       80.5,
				BilledDurationMS: 81,
				MemorySizeMB:     1024,
				MaxMemoryUsedMB:  215,
			},
		},
		{
			name: "snapstart restore is a cold start without init duration",
			raw: "REPORT RequestId: " + warmRequestID + "\tDuration: 90.00 ms\tBilled Duration: 300 ms\t" +
				"Memory Size: 1024 MB\tMax Memory Used: 200 MB\tRestore Duration: 210.00 ms\tBilled Restore Duration: 210 ms\n",
			found: true,
			want: lambdaReport{
				RequestID:        warmRequestID,
				DurationMS:       90,
				BilledDurationMS: 300,
				MemorySizeMB:     1024,
				MaxMemoryUsedMB:  200,
				ColdStart:        true,
			},
		},
		{
			name:      "previous invoke's cold REPORT is not attributed to this request",
			raw:       reportCold + "\nSTART RequestId: " + warmRequestID + "\nsome output without a report",
			requestID: warmRequestID,
			found:     false,
		},
		{
			name:      "request id selects its own line when an older one follows in the tail",
			raw:       reportWarm + "\n" + reportCold + "\n",
			requestID: warmRequestID,
			found:     true,
			want: lambdaReport{
				RequestID:        warmRequestID,
				DurationMS:       80.5,
				BilledDurationMS: 81,
				MemorySizeMB:     1024,
				MaxMemoryUsedMB:  215,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, found := parseLambdaReportLineFor(c.raw, c.requestID)
			require.Equal(t, c.found, found)
			require.Equal(t, c.want, got)
		})
	}
}

func TestInvokeReportFromLogResult(t *testing.T) {
	require.Nil(t, invokeReportFromLogResult(nil, coldRequestID))
	require.Nil(t, invokeReportFromLogResult(aws.String("not base64!"), coldRequestID))

	logResult := base64.StdEncoding.EncodeToString([]byte(reportCold + "\n"))
	report := invokeReportFromLogResult(&logResult, coldRequestID)
	require.NotNil(t, report)
	require.Equal(t, LambdaInvokeReport{
		RequestID:        coldRequestID,
		DurationMS:       1500.25,
		BilledDurationMS: 1501,
		MemorySizeMB:     1024,
		MaxMemoryUsedMB:  210,
		ColdStart:        true,
		InitDurationMS:   412.75,
	}, *report)

	require.Nil(t, invokeReportFromLogResult(&logResult, warmRequestID))
}

type reportClientTransport struct {
	report  *LambdaInvokeReport
	headers metadata.MD
}

func (f *reportClientTransport) RoundTrip(ctx context.Context, req *Request) (*Response, error) {
	resp, err := okResponse(f.headers)
	if err != nil {
		return nil, err
	}
	resp.invokeReport = f.report
	return resp, nil
}

func okResponse(headers metadata.MD) (*Response, error) {
	respAny, err := anypb.New(&structpb.Struct{})
	if err != nil {
		return nil, err
	}
	stAny, err := anypb.New(status.New(codes.OK, "OK").Proto())
	if err != nil {
		return nil, err
	}
	hdrs, err := MarshalMetadata(headers)
	if err != nil {
		return nil, err
	}
	return &Response{
		msg: pbtransport.Response_builder{
			Resp:    respAny,
			Status:  stAny,
			Headers: hdrs,
		}.Build(),
	}, nil
}

func TestInvokeDeliversReportInHeader(t *testing.T) {
	want := LambdaInvokeReport{RequestID: coldRequestID, DurationMS: 12, ColdStart: true, InitDurationMS: 400}
	cc := NewClientConn(&reportClientTransport{
		report:  &want,
		headers: metadata.Pairs("x-other", "kept"),
	})

	var header metadata.MD
	err := cc.Invoke(context.Background(), "/test.Service/Method", &structpb.Struct{}, &structpb.Struct{}, grpc.Header(&header))
	require.NoError(t, err)
	require.Equal(t, []string{"kept"}, header.Get("x-other"))

	got, ok := LambdaInvokeReportFromMetadata(header)
	require.True(t, ok)
	require.Equal(t, want, got)
}

func TestInvokeDropsFunctionSuppliedReportHeader(t *testing.T) {
	spoofed := `{"request_id":"spoofed","cold_start":true,"init_duration_ms":1}`

	t.Run("without a transport report", func(t *testing.T) {
		cc := NewClientConn(&reportClientTransport{headers: metadata.Pairs(LambdaInvokeReportMetadataKey, spoofed)})
		var header metadata.MD
		err := cc.Invoke(context.Background(), "/test.Service/Method", &structpb.Struct{}, &structpb.Struct{}, grpc.Header(&header))
		require.NoError(t, err)
		_, ok := LambdaInvokeReportFromMetadata(header)
		require.False(t, ok)
	})

	t.Run("with a transport report", func(t *testing.T) {
		want := LambdaInvokeReport{RequestID: warmRequestID, DurationMS: 5}
		cc := NewClientConn(&reportClientTransport{report: &want, headers: metadata.Pairs(LambdaInvokeReportMetadataKey, spoofed)})
		var header metadata.MD
		err := cc.Invoke(context.Background(), "/test.Service/Method", &structpb.Struct{}, &structpb.Struct{}, grpc.Header(&header))
		require.NoError(t, err)
		got, ok := LambdaInvokeReportFromMetadata(header)
		require.True(t, ok)
		require.Equal(t, want, got)
	})
}

type fakeLambdaHTTP struct {
	payload   []byte
	logResult string
	requestID string
}

func (f *fakeLambdaHTTP) Do(req *http.Request) (*http.Response, error) {
	h := http.Header{}
	h.Set("X-Amz-Log-Result", f.logResult)
	h.Set("X-Amzn-Requestid", f.requestID)
	h.Set("X-Amz-Executed-Version", "$LATEST")
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     h,
		Body:       io.NopCloser(bytes.NewReader(f.payload)),
		Request:    req,
	}, nil
}

func TestLambdaTransportRoundTripReadsThisInvokesReport(t *testing.T) {
	resp, err := okResponse(metadata.MD{})
	require.NoError(t, err)
	payload, err := resp.MarshalJSON()
	require.NoError(t, err)

	// The tail holds a previous invoke's cold REPORT ahead of this invoke's
	// warm one; only the line for the response's request ID is used.
	tail := base64.StdEncoding.EncodeToString([]byte(reportCold + "\nSTART RequestId: " + warmRequestID + "\n" + reportWarm + "\n"))
	client := lambda.New(lambda.Options{
		Region:      "us-west-2",
		Credentials: aws.AnonymousCredentials{},
		HTTPClient:  &fakeLambdaHTTP{payload: payload, logResult: tail, requestID: warmRequestID},
	})
	transport, err := NewLambdaClientTransport(context.Background(), client, "fn")
	require.NoError(t, err)

	var header metadata.MD
	err = NewClientConn(transport).Invoke(context.Background(), "/test.Service/Method", &structpb.Struct{}, &structpb.Struct{}, grpc.Header(&header))
	require.NoError(t, err)

	got, ok := LambdaInvokeReportFromMetadata(header)
	require.True(t, ok)
	require.Equal(t, LambdaInvokeReport{
		RequestID:        warmRequestID,
		DurationMS:       80.5,
		BilledDurationMS: 81,
		MemorySizeMB:     1024,
		MaxMemoryUsedMB:  215,
	}, got)
}
