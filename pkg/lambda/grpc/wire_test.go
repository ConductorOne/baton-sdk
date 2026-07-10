package grpc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	batonv1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
)

// unresolvableAnnotation fabricates an annotation whose type is not linked
// into this test binary — the shape a connector-specific annotation (e.g.
// baton-jira's c1.connector.v2.CustomField) has inside a runtime that does
// not register it. The value bytes encode field 1 (string) = "team".
func unresolvableAnnotation() *anypb.Any {
	return &anypb.Any{
		TypeUrl: "type.googleapis.com/fake.connector.v1.CustomField",
		Value:   []byte("\n\x04team"),
	}
}

func knownAnnotation(t *testing.T) *anypb.Any {
	t.Helper()
	profile, err := structpb.NewStruct(map[string]any{"name": "test-group"})
	require.NoError(t, err)
	known, err := anypb.New(v2.GroupTrait_builder{Profile: profile}.Build())
	require.NoError(t, err)
	return known
}

// ce918Response builds the CE-918 payload shape: a ticket schema whose
// custom fields carry annotations of a type this process cannot resolve.
func ce918Response(t *testing.T) *pbtransport.Response {
	t.Helper()
	schemas := v2.TicketsServiceListTicketSchemasResponse_builder{
		List: []*v2.TicketSchema{
			v2.TicketSchema_builder{
				Id:          "DEV:10003",
				DisplayName: "Task (DEV)",
				CustomFields: map[string]*v2.TicketCustomField{
					"customfield_10001": v2.TicketCustomField_builder{
						Id:          "customfield_10001",
						DisplayName: "Team",
						Annotations: []*anypb.Any{unresolvableAnnotation()},
					}.Build(),
				},
				Annotations: []*anypb.Any{knownAnnotation(t), unresolvableAnnotation()},
			}.Build(),
		},
	}.Build()

	respAny, err := anypb.New(schemas)
	require.NoError(t, err)
	stAny, err := anypb.New(status.New(codes.OK, "OK").Proto())
	require.NoError(t, err)
	return pbtransport.Response_builder{
		Resp:   respAny,
		Status: stAny,
	}.Build()
}

func unpackSchemas(t *testing.T, f *Response) *v2.TicketsServiceListTicketSchemasResponse {
	t.Helper()
	out := &v2.TicketsServiceListTicketSchemasResponse{}
	require.NoError(t, f.UnmarshalResponse(out))
	return out
}

func TestResponse_WireFrame_PreservesUnresolvableAnnotations(t *testing.T) {
	msg := ce918Response(t)

	// The legacy encoding cannot represent this payload at all: protojson
	// aborts on the first unresolvable Any. This is the CE-918 sync crash.
	_, err := protojson.Marshal(msg)
	require.ErrorContains(t, err, "unable to resolve")

	f := &Response{msg: msg, wireV2: true}
	payload, err := f.MarshalJSON()
	require.NoError(t, err)

	var wire map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &wire))
	assert.Contains(t, wire, "frame")
	assert.NotContains(t, wire, "resp", "v2 responses are frame-only")

	rt := &Response{}
	require.NoError(t, rt.UnmarshalJSON(payload))
	assert.True(t, rt.wireV2)
	require.True(t, proto.Equal(msg, rt.msg), "frame round-trip must be lossless")

	schema := unpackSchemas(t, rt).GetList()[0]
	fieldAnns := schema.GetCustomFields()["customfield_10001"].GetAnnotations()
	require.Len(t, fieldAnns, 1)
	assert.Equal(t, unresolvableAnnotation().GetTypeUrl(), fieldAnns[0].GetTypeUrl())
	assert.Equal(t, unresolvableAnnotation().GetValue(), fieldAnns[0].GetValue(),
		"annotation wire bytes must arrive byte-identical")
	assert.Len(t, schema.GetAnnotations(), 2)
}

func TestResponse_LegacyMarshal_FailsOnUnresolvableAnnotations(t *testing.T) {
	f := &Response{msg: ce918Response(t)}

	// A response to a legacy invoker cannot silently drop data the sender
	// happens not to resolve — the sender's registry is no authority on what
	// the receiver needs. The marshal fails loudly instead (pre-frame
	// behavior).
	_, err := f.MarshalJSON()
	require.ErrorContains(t, err, "unable to resolve")
}

func TestRequest_DualEncode_SkewMatrix(t *testing.T) {
	req := batonv1.BatonServiceHelloRequest_builder{
		HostId:      "test-host",
		Annotations: []*anypb.Any{knownAnnotation(t)},
	}.Build()
	treq, err := NewRequest("/c1.connectorapi.baton.v1.BatonService/Hello", req, metadata.MD{})
	require.NoError(t, err)

	payload, frameOnly, err := treq.marshalPayload()
	require.NoError(t, err)
	assert.Nil(t, frameOnly)

	var wire map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &wire))
	assert.Contains(t, wire, "method")
	assert.Contains(t, wire, "req")
	assert.Contains(t, wire, "frame")
	assert.Contains(t, wire, "v")

	// Old connector: plain protojson with DiscardUnknown — the pre-frame
	// fast path. The unknown frame fields are discarded and the legacy view
	// carries the full request.
	legacyMsg := &pbtransport.Request{}
	require.NoError(t, protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(payload, legacyMsg))
	legacyReq := &batonv1.BatonServiceHelloRequest{}
	require.NoError(t, anypb.UnmarshalTo(legacyMsg.GetReq(), legacyReq, proto.UnmarshalOptions{}))
	require.Len(t, legacyReq.GetAnnotations(), 1)
	assert.Equal(t, knownAnnotation(t).GetTypeUrl(), legacyReq.GetAnnotations()[0].GetTypeUrl())

	// New connector: reads the frame.
	rt := &Request{}
	require.NoError(t, rt.UnmarshalJSON(payload))
	assert.True(t, rt.wireV2)
	frameReq := &batonv1.BatonServiceHelloRequest{}
	require.NoError(t, rt.UnmarshalRequest(frameReq))
	require.Len(t, frameReq.GetAnnotations(), 1)
}

func TestRequest_UnresolvableAnnotations_FrameOnly(t *testing.T) {
	req := batonv1.BatonServiceHelloRequest_builder{
		HostId:      "test-host",
		Annotations: []*anypb.Any{knownAnnotation(t), unresolvableAnnotation()},
	}.Build()
	treq, err := NewRequest("/c1.connectorapi.baton.v1.BatonService/Hello", req, metadata.MD{})
	require.NoError(t, err)

	// No legacy view exists for a payload protojson cannot represent; the
	// frame is sent alone and the reason is reported so the invoker can log
	// it. A pre-frame connector fails on this payload — as it would have
	// pre-frames, when the marshal itself failed.
	payload, frameOnly, err := treq.marshalPayload()
	require.NoError(t, err)
	require.ErrorContains(t, frameOnly, "unable to resolve")

	var wire map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &wire))
	assert.Contains(t, wire, "frame")
	assert.NotContains(t, wire, "method")
	assert.NotContains(t, wire, "req")

	// New connector: decodes the frame losslessly, method included.
	rt := &Request{}
	require.NoError(t, rt.UnmarshalJSON(payload))
	assert.True(t, rt.wireV2)
	assert.Equal(t, treq.Method(), rt.Method())
	frameReq := &batonv1.BatonServiceHelloRequest{}
	require.NoError(t, rt.UnmarshalRequest(frameReq))
	require.Len(t, frameReq.GetAnnotations(), 2)
	assert.Equal(t, unresolvableAnnotation().GetValue(), frameReq.GetAnnotations()[1].GetValue())
}

func TestServer_Handler_EchoesWireVersion(t *testing.T) {
	ctx := t.Context()
	s := NewServer(nil)

	legacyReq, err := NewRequest("/c1.connectorapi.baton.v1.BatonService/Hello", &batonv1.BatonServiceHelloRequest{}, metadata.MD{})
	require.NoError(t, err)

	// A request that arrived legacy gets a legacy response.
	resp, err := s.Handler(ctx, legacyReq)
	require.NoError(t, err)
	payload, err := resp.MarshalJSON()
	require.NoError(t, err)
	var wire map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &wire))
	assert.Contains(t, wire, "status")
	assert.NotContains(t, wire, "frame")

	// A request that arrived as a frame gets a frame back.
	dual, err := legacyReq.MarshalJSON()
	require.NoError(t, err)
	frameReq := &Request{}
	require.NoError(t, frameReq.UnmarshalJSON(dual))
	require.True(t, frameReq.wireV2)

	resp, err = s.Handler(ctx, frameReq)
	require.NoError(t, err)
	payload, err = resp.MarshalJSON()
	require.NoError(t, err)
	wire = nil
	require.NoError(t, json.Unmarshal(payload, &wire))
	assert.Contains(t, wire, "frame")
	assert.NotContains(t, wire, "status")

	rt := &Response{}
	require.NoError(t, rt.UnmarshalJSON(payload))
	st, err := rt.Status()
	require.NoError(t, err)
	assert.NotNil(t, st)
}

func TestRequest_MarshalJSON_SizeGuardFallsBackToLegacy(t *testing.T) {
	prev := maxDualEncodedPayload
	maxDualEncodedPayload = 64
	t.Cleanup(func() { maxDualEncodedPayload = prev })

	treq, err := NewRequest("/c1.connectorapi.baton.v1.BatonService/Hello", batonv1.BatonServiceHelloRequest_builder{
		HostId: "test-host",
	}.Build(), metadata.MD{})
	require.NoError(t, err)

	payload, err := treq.MarshalJSON()
	require.NoError(t, err)

	var wire map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &wire))
	assert.NotContains(t, wire, "frame")
	assert.Contains(t, wire, "method")

	// v2 peers accept the legacy fallback.
	rt := &Request{}
	require.NoError(t, rt.UnmarshalJSON(payload))
	assert.False(t, rt.wireV2)
	assert.Equal(t, treq.Method(), rt.Method())
}

func TestDecodeWireFrame_UnsupportedVersion(t *testing.T) {
	rt := &Request{}
	err := rt.UnmarshalJSON([]byte(`{"v": 3, "frame": "AA=="}`))
	require.ErrorContains(t, err, "unsupported wire frame version 3")
}
