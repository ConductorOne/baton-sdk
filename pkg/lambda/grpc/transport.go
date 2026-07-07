package grpc

import (
	"bytes"
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

const annotationsFieldName = "annotations"

/*
unmarshalTransportJSON unmarshals transport JSON into msg. It reports whether
the payload carried a v2 wire frame (binary proto, see wireFrame), which is
decoded losslessly with no type resolution.

Legacy payloads are protojson, unmarshaled discarding any unknown fields.
When a legacy payload fails to unmarshal, it retries after filtering out any
annotations whose types are not known to the global registry. Annotation type
skew happens frequently for new features and would otherwise require rolling
every lambda function (and, in the response direction, would let an old
connector's annotations break a newer invoker, or a newer connector's
annotations break an older invoker).

The filter walks the whole document, so annotations nested in embedded rows
(resources inside grants, response-level annotation lists, etc.) are covered,
not just top-level request annotations.

The fast path is a plain protojson unmarshal; the filter only runs on
payloads that already failed, where the alternative is a hard error. Our
payloads are small relative to the work of the connector, so the performance
impact is negligible.
*/
func unmarshalTransportJSON(b []byte, msg proto.Message) (bool, error) {
	if ok, err := decodeWireFrame(b, msg); ok {
		return true, err
	}

	unmarshalOptions := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}
	// There doesn't seem to be a stable interface for "unknown field" errors,
	// so any failure falls through to the annotation filter.
	originalErr := unmarshalOptions.Unmarshal(b, msg)
	if originalErr == nil {
		return false, nil
	}

	filtered, changed := filterUnknownAnnotations(b)
	if !changed {
		return false, originalErr
	}

	if err := unmarshalOptions.Unmarshal(filtered, msg); err != nil {
		return false, errors.Join(originalErr, err)
	}

	return false, nil
}

// filterUnknownAnnotations recursively walks raw JSON and prunes entries from
// every array-valued "annotations" field whose "@type" does not resolve
// against the global type registry. It returns the (possibly rewritten)
// document and whether anything was removed. Malformed documents are returned
// unchanged so the caller surfaces the original protojson error.
func filterUnknownAnnotations(raw json.RawMessage) (json.RawMessage, bool) {
	trimmed := bytes.TrimLeft(raw, " \t\r\n")
	if len(trimmed) == 0 {
		return raw, false
	}

	switch trimmed[0] {
	case '{':
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return raw, false
		}
		changed := false
		for key, value := range obj {
			if key == annotationsFieldName {
				if newValue, c := filterAnnotationsArray(value); c {
					obj[key] = newValue
					changed = true
				}
				continue
			}
			if newValue, c := filterUnknownAnnotations(value); c {
				obj[key] = newValue
				changed = true
			}
		}
		if !changed {
			return raw, false
		}
		out, err := json.Marshal(obj)
		if err != nil {
			return raw, false
		}
		return out, true
	case '[':
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return raw, false
		}
		changed := false
		for i, elem := range arr {
			if newElem, c := filterUnknownAnnotations(elem); c {
				arr[i] = newElem
				changed = true
			}
		}
		if !changed {
			return raw, false
		}
		out, err := json.Marshal(arr)
		if err != nil {
			return raw, false
		}
		return out, true
	default:
		return raw, false
	}
}

// filterAnnotationsArray keeps only annotation entries that are objects with
// an "@type" resolvable in the global registry, matching the historical
// request-side filter semantics (malformed entries are dropped too). A value
// that is not a JSON array is returned unchanged.
//
// Because the walk is recursive, it can reach user data: protojson encodes
// google.protobuf.Struct as plain JSON, so a trait profile or metadata header
// may legitimately contain a key named "annotations" that has nothing to do
// with google.protobuf.Any. To avoid emptying such arrays, filtering only
// applies when at least one element carries a parseable "@type" — evidence
// the array is actually an Any list. Arrays with no "@type"-bearing elements
// are left untouched.
func filterAnnotationsArray(raw json.RawMessage) (json.RawMessage, bool) {
	var anns []json.RawMessage
	if err := json.Unmarshal(raw, &anns); err != nil {
		return raw, false
	}
	sawTypeURL := false
	wellKnown := make([]json.RawMessage, 0, len(anns))
	for _, ann := range anns {
		var annObj map[string]json.RawMessage
		if err := json.Unmarshal(ann, &annObj); err != nil {
			continue
		}
		t, ok := annObj["@type"]
		if !ok {
			continue
		}
		var typeURL string
		if err := json.Unmarshal(t, &typeURL); err != nil {
			continue
		}
		sawTypeURL = true
		// It would be nice to log here, but we have no context.
		if _, err := protoregistry.GlobalTypes.FindMessageByURL(typeURL); err == nil {
			wellKnown = append(wellKnown, ann) // keep only known types
		}
	}
	if !sawTypeURL {
		// Nothing in this array looks like an Any; it is probably user data
		// (for example a Struct field that happens to be named "annotations").
		return raw, false
	}
	if len(wellKnown) == len(anns) {
		return raw, false
	}
	out, err := json.Marshal(wellKnown)
	if err != nil {
		return raw, false
	}
	return out, true
}

type Request struct {
	msg *pbtransport.Request

	// wireV2 records that the request arrived as a v2 wire frame, proving
	// the invoker can read one back. The server stamps it onto the Response.
	wireV2 bool
}

// UnmarshalJSON unmarshals the JSON into a Request. v2 wire frames decode
// losslessly; legacy payloads are protojson, discarding unknown fields and
// filtering annotations with unresolvable types. See unmarshalTransportJSON.
func (f *Request) UnmarshalJSON(b []byte) error {
	f.msg = &pbtransport.Request{}
	wireV2, err := unmarshalTransportJSON(b, f.msg)
	if err != nil {
		return err
	}
	f.wireV2 = wireV2
	return nil
}

// MarshalJSON dual-encodes the request: the legacy protojson fields and the
// v2 wire frame share one JSON object, so legacy connectors keep working
// (they discard the unknown frame fields) while v2 connectors decode the
// frame and see annotations whose types this process cannot resolve. When no
// legacy view can be produced — protojson cannot represent an Any whose type
// is not linked into this process — the frame is sent alone: a legacy
// connector would have failed on that payload anyway. Oversized dual
// payloads fall back to legacy-only to stay under the Lambda invoke limit;
// v2 connectors accept those too.
func (f *Request) MarshalJSON() ([]byte, error) {
	payload, _, err := f.marshalPayload()
	return payload, err
}

// marshalPayload builds the invoke payload. The middle return reports the
// frame-only condition: when non-nil, the payload carries only the v2 frame
// and the value is the reason the legacy view could not be produced —
// callers with a context should surface it, since a legacy connector cannot
// process a frame-only payload.
func (f *Request) marshalPayload() ([]byte, error, error) {
	legacy, legacyErr := protojson.Marshal(f.msg)
	if legacyErr != nil {
		payload, err := encodeWireFrame(f.msg)
		if err != nil {
			return nil, nil, errors.Join(legacyErr, err)
		}
		return payload, legacyErr, nil
	}
	dual, err := spliceWireFrame(legacy, f.msg)
	if err != nil {
		return nil, nil, err
	}
	if len(dual) > maxDualEncodedPayload {
		return legacy, nil, nil
	}
	return dual, nil, nil
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

	// wireV2 selects the v2 wire frame encoding. The server sets it from
	// the request: a frame in the request proves the invoker reads frames.
	wireV2 bool
}

// UnmarshalJSON unmarshals the JSON into a Response. v2 wire frames decode
// losslessly with no type resolution. Legacy payloads are protojson,
// discarding unknown fields and filtering annotations with unresolvable
// types: responses carry annotations at the response level and nested inside
// rows (grants embed resources, etc.), so this protects an invoker from
// annotation types it does not know about — for example an older invoker
// receiving annotations from a connector built with a newer SDK. See
// unmarshalTransportJSON.
func (f *Response) UnmarshalJSON(b []byte) error {
	f.msg = &pbtransport.Response{}
	wireV2, err := unmarshalTransportJSON(b, f.msg)
	if err != nil {
		return err
	}
	f.wireV2 = wireV2
	return nil
}

// MarshalJSON encodes a v2 wire frame when the invoker proved it reads them
// (see wireV2), preserving annotations whose types this process cannot
// resolve. Legacy invokers get plain protojson, which fails on an Any whose
// type is not linked into this process — deliberately: the sender's registry
// is no authority on what the receiver understands or needs, so degrading
// the payload by silently dropping data is worse than failing loudly.
func (f *Response) MarshalJSON() ([]byte, error) {
	if f.wireV2 {
		return encodeWireFrame(f.msg)
	}
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
