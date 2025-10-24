package grpc

import (
	"encoding/json"
	"errors"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type Request struct {
	msg *pbtransport.Request
}

/*
UnmarshalJSON unmarshals the JSON into a Request, discarding any unknown fields.

It also filters out any annotations that are not known to the global registry
which happens frequently for new features and would otherwise require
rolling every lambda function.

Our requests are small in size, dwarfed by the work of the connector,
so the performance impact is negligible.
*/
func (f *Request) UnmarshalJSON(b []byte) error {
	f.msg = &pbtransport.Request{}
	unmarshalOptions := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}
	err := unmarshalOptions.Unmarshal(b, f.msg)
	if err == nil {
		return nil
	}
	// There doesn't seem to be a stable interface for "unknown field" errors.
	originalErr := err

	// Parse top-level as raw and surgically filter req.annotations
	var top map[string]json.RawMessage
	if err := json.Unmarshal(b, &top); err != nil {
		return err
	}
	// Track if we actually modified the payload.
	changed := false

	if reqRaw, ok := top["req"]; ok && len(reqRaw) > 0 {
		var reqObj map[string]json.RawMessage
		if err := json.Unmarshal(reqRaw, &reqObj); err == nil {
			if annsRaw, ok := reqObj["annotations"]; ok && len(annsRaw) > 0 {
				var anns []json.RawMessage
				if err := json.Unmarshal(annsRaw, &anns); err == nil {
					wellKnownAnnotations := make([]json.RawMessage, 0, len(anns))
					for _, ann := range anns {
						var annObj map[string]json.RawMessage
						if err := json.Unmarshal(ann, &annObj); err != nil {
							continue
						}
						var typeURL string
						if t, ok := annObj["@type"]; ok {
							if err := json.Unmarshal(t, &typeURL); err == nil {
								// It would be nice to log here, but we have no context.
								if _, err := protoregistry.GlobalTypes.FindMessageByURL(typeURL); err == nil {
									wellKnownAnnotations = append(wellKnownAnnotations, ann) // keep only known types
								}
							}
						}
					}
					if len(wellKnownAnnotations) != len(anns) {
						if newAnns, err := json.Marshal(wellKnownAnnotations); err == nil {
							reqObj["annotations"] = newAnns
							if newReqRaw, err := json.Marshal(reqObj); err == nil {
								top["req"] = newReqRaw
								changed = true
							}
						}
					}
				}
			}
		}
	}

	if !changed {
		return originalErr
	}

	filteredJSON, err := json.Marshal(top)
	if err != nil {
		return errors.Join(originalErr, err)
	}

	err = unmarshalOptions.Unmarshal(filteredJSON, f.msg)
	if err != nil {
		return errors.Join(originalErr, err)
	}

	return nil
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
		msg: pbtransport.Request_builder{
			Method:  method,
			Req:     reqAny,
			Headers: reqHdrs,
		}.Build(),
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
