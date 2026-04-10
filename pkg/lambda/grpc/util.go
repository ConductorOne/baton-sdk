package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
)

type timeoutUnit uint8

const (
	hour        timeoutUnit = 'H'
	minute      timeoutUnit = 'M'
	second      timeoutUnit = 'S'
	millisecond timeoutUnit = 'm'
	microsecond timeoutUnit = 'u'
	nanosecond  timeoutUnit = 'n'
)

func timeoutUnitToDuration(u timeoutUnit) (time.Duration, bool) {
	switch u {
	case hour:
		return time.Hour, true
	case minute:
		return time.Minute, true
	case second:
		return time.Second, true
	case millisecond:
		return time.Millisecond, true
	case microsecond:
		return time.Microsecond, true
	case nanosecond:
		return time.Nanosecond, true
	default:
		return 0, false
	}
}

// NOTE(morgabra): straight lift of a private function :/.
func decodeTimeout(s string) (time.Duration, error) {
	size := len(s)
	if size < 2 {
		return 0, fmt.Errorf("transport: timeout string is too short: %q", s)
	}
	if size > 9 {
		// Spec allows for 8 digits plus the unit.
		return 0, fmt.Errorf("transport: timeout string is too long: %q", s)
	}
	unit := timeoutUnit(s[size-1])
	d, ok := timeoutUnitToDuration(unit)
	if !ok {
		return 0, fmt.Errorf("transport: timeout unit is not recognized: %q", s)
	}
	t, err := strconv.ParseInt(s[:size-1], 10, 64)
	if err != nil {
		return 0, err
	}
	const maxHours = math.MaxInt64 / int64(time.Hour)
	if d == time.Hour && t > maxHours {
		// This timeout would overflow math.MaxInt64; clamp it.
		return time.Duration(math.MaxInt64), nil
	}
	return d * time.Duration(t), nil
}

func parseMethod(method string) (string, string, error) {
	if method != "" && method[0] == '/' {
		method = method[1:]
	}
	pos := strings.LastIndex(method, "/")
	if pos == -1 {
		return "", "", status.Errorf(codes.Unimplemented, "malformed method name: %q", method)
	}
	return method[:pos], method[pos+1:], nil
}

// Convert accepts a list of T and returns a list of R based on the input func.
func Convert[T any, R any](slice []T, f func(in T) R) []R {
	ret := make([]R, 0, len(slice))
	for _, t := range slice {
		ret = append(ret, f(t))
	}
	return ret
}

func MarshalMetadata(md metadata.MD) (*structpb.Struct, error) {
	x := &structpb.Struct{Fields: make(map[string]*structpb.Value, len(md))}
	for k, v := range md {
		lv, err := structpb.NewList(Convert(v, func(in string) any { return in }))
		if err != nil {
			return nil, err
		}
		x.Fields[k] = structpb.NewListValue(lv)
	}
	return x, nil
}

// UnmarshalMetadata converts a *structpb.Struct to a metadata.MD.
// Only keys with []string values are converted.
// Empty string values are ignored.
func UnmarshalMetadata(s *structpb.Struct) metadata.MD {
	md := make(metadata.MD, len(s.Fields))
	for k, v := range s.Fields {
		lv := v.GetListValue()
		if lv == nil {
			continue
		}
		values := make([]string, 0, len(lv.GetValues()))
		for _, sval := range lv.GetValues() {
			val := sval.GetStringValue()
			if val != "" {
				values = append(values, val)
			}
		}
		md.Append(k, values...)
	}
	return md
}

// isTransientNetworkError returns true for TCP-level errors that are
// transient and should be classified as codes.Unavailable rather than
// codes.Unknown so that callers (e.g. IsSyncPreservable) can treat
// them as recoverable.
func isTransientNetworkError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "no such host") ||
		(strings.Contains(msg, "unexpected EOF") && !strings.Contains(msg, "unexpected EOF on client connection"))
}

// ErrorResponse converts a given error to a status.Status and returns a *pbtransport.Response.
// status.FromError(err) must unwrap a status.Status for this to work. Non-status errors are mapped
// through Baton lambda's application error classification before falling back to codes.Unknown.
func ErrorResponse(err error) *Response {
	st, ok := status.FromError(err)
	if !ok {
		st = statusForApplicationError(err)
	}
	spb := st.Proto()
	if spb == nil {
		st = status.Newf(codes.Unknown, "unknown error: %s", err)
		spb = st.Proto()
	}
	anyst, err := anypb.New(spb)
	if err != nil {
		panic(fmt.Errorf("server: unable to serialize status: %w", err))
	}
	return &Response{
		msg: pbtransport.Response_builder{
			Resp:     nil,
			Status:   anyst,
			Headers:  nil,
			Trailers: nil,
		}.Build(),
	}
}

// statusForApplicationError mirrors the transient network handling in uhttp for
// connector SDK clients that bypass the Baton HTTP wrapper.
func statusForApplicationError(err error) *status.Status {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Newf(codes.Canceled, "canceled: %s", err)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Newf(codes.DeadlineExceeded, "deadline exceeded: %s", err)
	case errors.Is(err, io.ErrUnexpectedEOF):
		return status.Newf(codes.Unavailable, "unexpected EOF: %s", err)
	case errors.Is(err, syscall.ECONNRESET):
		return status.Newf(codes.Unavailable, "connection reset: %s", err)
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return status.Newf(codes.DeadlineExceeded, "request timeout: %s", err)
		}
		if urlErr.Temporary() {
			return status.Newf(codes.Unavailable, "temporary error: %s", err)
		}
	}

	if isTransientNetworkError(err) {
		return status.Newf(codes.Unavailable, "transient network error: %s", err)
	}

	return status.Newf(codes.Unknown, "unknown error: %s", err)
}
