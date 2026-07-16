package grpc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	batonv1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
)

// TestNewLambdaClientTransport_WireFramesDefaultOff documents the fix for
// connectors that predate the DiscardUnknown transport unmarshal option:
// they decode transport JSON with strict protojson and fail outright on an
// unrecognized top-level field, so dual-encoding the v2 wire frame by
// default breaks them. A transport must opt in with WithWireFrames before
// it will send anything a strict-unmarshal connector can't read.
func TestNewLambdaClientTransport_WireFramesDefaultOff(t *testing.T) {
	transport, err := NewLambdaClientTransport(t.Context(), nil, "some-function")
	require.NoError(t, err)
	lt, ok := transport.(*lambdaTransport)
	require.True(t, ok)
	require.False(t, lt.wireFramesEnabled)

	transport, err = NewLambdaClientTransport(t.Context(), nil, "some-function", WithWireFrames(true))
	require.NoError(t, err)
	lt, ok = transport.(*lambdaTransport)
	require.True(t, ok)
	require.True(t, lt.wireFramesEnabled)
}

// TestRequest_MarshalLegacy_NoFrameFields verifies the default (wire frames
// disabled) payload shape is decodable by a connector that unmarshals
// transport JSON with plain, strict protojson.Unmarshal — no
// DiscardUnknown, no frame decoding — matching the earliest deployed lambda
// connectors that predate both features.
func TestRequest_MarshalLegacy_NoFrameFields(t *testing.T) {
	req := batonv1.BatonServiceHelloRequest_builder{HostId: "test-host"}.Build()
	treq, err := NewRequest("/c1.connectorapi.baton.v1.BatonService/Hello", req, metadata.MD{})
	require.NoError(t, err)

	payload, err := treq.marshalLegacy()
	require.NoError(t, err)

	var wire map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &wire))
	require.NotContains(t, wire, "v")
	require.NotContains(t, wire, "frame")
	require.Contains(t, wire, "method")

	// Strict protojson.Unmarshal — the pre-DiscardUnknown decode path —
	// must succeed since no unrecognized fields are present.
	legacyMsg := &pbtransport.Request{}
	require.NoError(t, protojson.Unmarshal(payload, legacyMsg))
	require.Equal(t, treq.Method(), legacyMsg.GetMethod())
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
