package grpc

import (
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
)

type Request struct {
	msg *pbtransport.Request
}

func (f *Request) UnmarshalJSON(b []byte) error {
	f.msg = &pbtransport.Request{}
	return protojson.Unmarshal(b, f.msg)
}

func (f *Request) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(f.msg)
}

func (f *Request) Method() string {
	return f.msg.GetMethod()
}

func (f *Request) UnmarshalRequest(req any) error {
	reqpb, ok := req.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "error unmarshalling request: not a proto.Message")
	}
	err := anypb.UnmarshalTo(f.msg.GetReq(), reqpb, proto.UnmarshalOptions{})
	if err != nil {
		return status.Errorf(codes.Internal, "error unmarshalling request: %v", err)
	}
	return nil
}

func (f *Request) Headers() metadata.MD {
	// TODO(morgabra): Memoize this.
	return UnmarshalMetadata(f.msg.GetHeaders())
}

func NewRequest(method string, req proto.Message, headers metadata.MD) (*Request, error) {
	reqAny, err := anypb.New(req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error marshalling request: %v", err)
	}
	reqHdrs, err := MarshalMetadata(headers)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error marshalling headers: %v", err)
	}
	return &Request{
		msg: &pbtransport.Request{
			Method:  method,
			Req:     reqAny,
			Headers: reqHdrs,
		},
	}, nil
}

type Response struct {
	msg *pbtransport.Response
}

func (f *Response) UnmarshalJSON(b []byte) error {
	f.msg = &pbtransport.Response{}
	return protojson.Unmarshal(b, f.msg)
}

func (f *Response) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(f.msg)
}

func (f *Response) UnmarshalResponse(resp any) error {
	respb, ok := resp.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "error unmarshalling response: not a proto.Message")
	}
	err := anypb.UnmarshalTo(f.msg.GetResp(), respb, proto.UnmarshalOptions{})
	if err != nil {
		return status.Errorf(codes.Internal, "error unmarshalling request: %v", err)
	}
	return nil
}

func (f *Response) Status() (*status.Status, error) {
	if f.msg.GetStatus() == nil {
		return nil, status.Errorf(codes.Internal, "response status not set")
	}
	st := &spb.Status{}
	err := anypb.UnmarshalTo(f.msg.GetStatus(), st, proto.UnmarshalOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error unmarshalling status: %v", err)
	}
	return status.FromProto(st), nil
}

func (f *Response) Headers() metadata.MD {
	return UnmarshalMetadata(f.msg.GetHeaders())
}

func (f *Response) Trailers() metadata.MD {
	return UnmarshalMetadata(f.msg.GetTrailers())
}
