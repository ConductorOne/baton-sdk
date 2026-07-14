package grpc

// Wire round-trip pins for the source-cache lookup continuation
// annotations (SourceCacheLookupOffer / Ask / Answers). The protocol rides
// request and response annotations through this transport, so both
// encodings must preserve them for peers that know the types:
//
//   - v2 wire frame: lossless binary proto (covered here and by the
//     composed lambda-stack e2e in pkg/sync);
//   - legacy protojson: known types survive; the recursive annotation
//     filter only prunes types missing from the registry — which is the
//     designed degradation for old peers (offer stripped → connector
//     never defers → cold sync), not a data-integrity risk.

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

func continuationRequestPayload(t *testing.T) []byte {
	t.Helper()
	annos := annotations.New(&v2.SourceCacheLookupOffer{})
	annos.Update(v2.SourceCacheLookupAnswers_builder{
		Answers: []*v2.SourceCacheLookupAnswers_Answer{
			v2.SourceCacheLookupAnswers_Answer_builder{
				RowKind:   "grants",
				ScopeHash: "groups/g1/members",
				Found:     true,
				Etag:      `W/"abc123"`,
			}.Build(),
			v2.SourceCacheLookupAnswers_Answer_builder{
				RowKind:   "grants",
				ScopeHash: "groups/g2/members",
				Found:     false,
			}.Build(),
		},
	}.Build())
	inner := v2.GrantsServiceListGrantsRequest_builder{
		Resource: v2.Resource_builder{
			Id: v2.ResourceId_builder{ResourceType: "group", Resource: "g1"}.Build(),
		}.Build(),
		Annotations: annos,
	}.Build()

	req, err := NewRequest("/c1.connector.v2.GrantsService/ListGrants", inner, metadata.MD{})
	require.NoError(t, err)
	payload, err := json.Marshal(req)
	require.NoError(t, err)
	return payload
}

func assertContinuationRequest(t *testing.T, req *Request) {
	t.Helper()
	inner := &v2.GrantsServiceListGrantsRequest{}
	require.NoError(t, req.UnmarshalRequest(inner))
	annos := annotations.Annotations(inner.GetAnnotations())
	require.True(t, annos.Contains(&v2.SourceCacheLookupOffer{}), "offer must survive the wire")
	answers := &v2.SourceCacheLookupAnswers{}
	ok, err := annos.Pick(answers)
	require.NoError(t, err)
	require.True(t, ok, "answers must survive the wire")
	require.Len(t, answers.GetAnswers(), 2)
	require.True(t, answers.GetAnswers()[0].GetFound())
	require.Equal(t, `W/"abc123"`, answers.GetAnswers()[0].GetEtag())
	require.False(t, answers.GetAnswers()[1].GetFound(), "explicit not-found must survive (distinct from absent)")
}

func TestContinuationAnnotations_RequestRoundTrip_V2Frame(t *testing.T) {
	payload := continuationRequestPayload(t)

	decoded := &Request{}
	require.NoError(t, json.Unmarshal(payload, decoded))
	require.True(t, decoded.wireV2, "dual-encoded payload must decode via the v2 frame")
	assertContinuationRequest(t, decoded)
}

func TestContinuationAnnotations_RequestRoundTrip_LegacyJSON(t *testing.T) {
	payload := continuationRequestPayload(t)

	// Strip the v2 frame fields, forcing the legacy protojson path (an
	// old-SDK worker's encoding, or the frame-dropped oversize fallback).
	var obj map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &obj))
	delete(obj, "v")
	delete(obj, "frame")
	legacy, err := json.Marshal(obj)
	require.NoError(t, err)

	decoded := &Request{}
	require.NoError(t, json.Unmarshal(legacy, decoded))
	require.False(t, decoded.wireV2)
	// Types are registered in this process: the annotation filter keeps them.
	assertContinuationRequest(t, decoded)
}

func TestContinuationAnnotations_AskResponseRoundTrip(t *testing.T) {
	inner := v2.GrantsServiceListGrantsResponse_builder{
		Annotations: annotations.New(v2.SourceCacheLookupAsk_builder{
			Queries: []*v2.SourceCacheLookupAsk_Query{
				v2.SourceCacheLookupAsk_Query_builder{RowKind: "grants", ScopeHash: "groups/g1/members"}.Build(),
			},
		}.Build()),
	}.Build()

	// Response path: frame encoding is selected when the request carried a
	// frame (wireV2), legacy protojson otherwise. Pin both.
	for _, wireV2 := range []bool{true, false} {
		anyResp, err := anypb.New(inner)
		require.NoError(t, err)
		anyStatus, err := anypb.New(status.New(codes.OK, "OK").Proto())
		require.NoError(t, err)
		resp := &Response{
			msg: pbtransport.Response_builder{
				Resp:   anyResp,
				Status: anyStatus,
			}.Build(),
			wireV2: wireV2,
		}
		payload, err := json.Marshal(resp)
		require.NoError(t, err)

		decoded := &Response{}
		require.NoError(t, json.Unmarshal(payload, decoded))
		out := &v2.GrantsServiceListGrantsResponse{}
		require.NoError(t, decoded.UnmarshalResponse(out))
		ask := &v2.SourceCacheLookupAsk{}
		annos := annotations.Annotations(out.GetAnnotations())
		ok, err := annos.Pick(ask)
		require.NoError(t, err)
		require.True(t, ok, "ask must survive the wire (wireV2=%v)", wireV2)
		require.Len(t, ask.GetQueries(), 1)
		require.Equal(t, "groups/g1/members", ask.GetQueries()[0].GetScopeHash())
	}
}
