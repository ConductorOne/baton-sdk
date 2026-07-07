package grpc

import (
	"bytes"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
)

const transportWireVersion = 2

// maxDualEncodedPayload caps dual-encoded requests below the 6MiB Lambda
// invoke payload limit. Past it the frame is dropped and the request goes
// out legacy-only, which v2 peers also accept.
var maxDualEncodedPayload = 5 << 20

/*
wireFrame is the v2 transport encoding: the binary proto bytes of a
transport Request or Response, carried base64-encoded in the JSON Lambda
payload. Binary proto copies google.protobuf.Any payloads verbatim instead
of resolving their type URLs the way protojson must, so annotation types
that are not linked into a process survive the transport intact — the fix
for connector-specific annotations (e.g. baton-jira's CustomField) being
dropped or crashing the marshal in runtimes that don't register them.

Version skew is handled without negotiation state:

  - Requests are dual-encoded: the legacy protojson fields and the frame
    share one JSON object. Legacy peers unmarshal with DiscardUnknown and
    never see the frame; v2 peers prefer it.
  - Responses carry the frame alone, but only when the request carried
    one — a frame in the request proves the invoker can read it. Legacy
    requests get legacy responses.

The field names cannot collide with the legacy encoding: protojson emits
only "method"/"req"/"headers" for Requests and
"resp"/"status"/"headers"/"trailers" for Responses.
*/
type wireFrame struct {
	V     int    `json:"v"`
	Frame []byte `json:"frame"`
}

// decodeWireFrame reports whether raw carries a v2 wire frame and, if so,
// decodes it into msg. A false return means raw is a legacy payload: either
// it isn't shaped like a frame, or it doesn't parse as JSON at all — the
// legacy path owns reporting that error.
func decodeWireFrame(raw []byte, msg proto.Message) (bool, error) {
	var wf wireFrame
	if err := json.Unmarshal(raw, &wf); err != nil || len(wf.Frame) == 0 {
		return false, nil //nolint:nilerr // not a v2 frame; the legacy path owns error reporting
	}
	if wf.V != transportWireVersion {
		return true, fmt.Errorf("transport: unsupported wire frame version %d", wf.V)
	}
	return true, proto.Unmarshal(wf.Frame, msg)
}

func encodeWireFrame(msg proto.Message) ([]byte, error) {
	frame, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return json.Marshal(wireFrame{V: transportWireVersion, Frame: frame})
}

// spliceWireFrame appends the v2 frame fields to a legacy protojson object,
// producing the dual-encoded request payload.
func spliceWireFrame(legacy []byte, msg proto.Message) ([]byte, error) {
	suffix, err := encodeWireFrame(msg)
	if err != nil {
		return nil, err
	}
	legacy = bytes.TrimSpace(legacy)
	if len(legacy) < 2 || legacy[0] != '{' || legacy[len(legacy)-1] != '}' {
		return nil, fmt.Errorf("transport: legacy payload is not a JSON object")
	}
	if len(legacy) == 2 {
		return suffix, nil
	}
	var buf bytes.Buffer
	buf.Grow(len(legacy) + len(suffix))
	buf.Write(legacy[:len(legacy)-1])
	buf.WriteByte(',')
	buf.Write(suffix[1:])
	return buf.Bytes(), nil
}
