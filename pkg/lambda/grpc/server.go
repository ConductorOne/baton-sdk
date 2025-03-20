package grpc

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	pbtransport "github.com/conductorone/baton-sdk/pb/c1/transport/v1"
)

var ErrIllegalSendHeader = status.Errorf(codes.Internal, "transport: SendHeader called multiple times")
var ErrIllegalHeaderWrite = status.Errorf(codes.Internal, "transport: SetHeader/SetTrailer called after headers were sent")

type TransportStream struct {
	mtx sync.Mutex

	response proto.Message
	status   *spb.Status

	methodDesc  *grpc.MethodDesc
	headersSent atomic.Bool
	headers     metadata.MD
	trailers    metadata.MD
}

func (r *TransportStream) Response() (*Response, error) {
	if r.status == nil {
		return nil, status.Errorf(codes.Internal, "error marshalling response: status not set")
	}

	headers, err := MarshalMetadata(r.headers)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error marshalling headers: %v", err)
	}
	trailers, err := MarshalMetadata(r.trailers)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error marshalling headers: %v", err)
	}

	var anyResp *anypb.Any
	if r.response != nil {
		anyResp, err = anypb.New(r.response)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "error marshalling response: %v", err)
		}
	}

	anyStatus, err := anypb.New(r.status)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error marshalling status: %v", err)
	}

	return &Response{
		msg: &pbtransport.Response{
			Resp:     anyResp,
			Status:   anyStatus,
			Headers:  headers,
			Trailers: trailers,
		},
	}, nil
}

func (r *TransportStream) Method() string {
	return r.methodDesc.MethodName
}

func (r *TransportStream) SetHeader(md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	if r.headersSent.Load() {
		return ErrIllegalHeaderWrite
	}
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.headers = metadata.Join(r.headers, md)
	return nil
}

func (r *TransportStream) SendHeader(md metadata.MD) error {
	if r.headersSent.Load() {
		return ErrIllegalSendHeader
	}
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.headersSent.Store(true)
	r.headers = metadata.Join(r.headers, md)
	return nil
}

func (r *TransportStream) SetTrailer(md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	if r.headersSent.Load() {
		return ErrIllegalHeaderWrite
	}
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.trailers = metadata.Join(r.trailers, md)
	return nil
}

func (r *TransportStream) SetResponse(resp any) error {
	if resp == nil {
		return status.Errorf(codes.Internal, "error setting response: response is nil")
	}
	resppb, ok := resp.(proto.Message)
	if !ok {
		return status.Errorf(codes.Internal, "error setting response: not a proto.Message")
	}
	r.response = resppb
	return nil
}

func (r *TransportStream) SetStatus(st *status.Status) {
	r.status = st.Proto()
}

func NewTransportStream(method *grpc.MethodDesc) *TransportStream {
	return &TransportStream{
		methodDesc: method,
		headers:    metadata.MD{},
		trailers:   metadata.MD{},
	}
}

func NewServer(unaryInterceptor grpc.UnaryServerInterceptor,
) *Server {
	return &Server{
		unaryInterceptor: unaryInterceptor,
		services:         make(map[string]*serviceInfo),
	}
}

type serviceInfo struct {
	serviceImpl any
	methods     map[string]*grpc.MethodDesc
	streams     map[string]*grpc.StreamDesc
	mdata       any
}

type Server struct {
	mu               sync.Mutex
	unaryInterceptor grpc.UnaryServerInterceptor

	services map[string]*serviceInfo
}

func MetadataForRequest(req *Request) metadata.MD {
	rv := metadata.MD{}
	for k, v := range req.Headers() {
		rv.Append(k, v...)
	}

	// https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
	// emulate grpc-go's behavior of setting these headers.
	// TODO(morgabra): what do here?
	rv.Set("content-type", "application/grpc+proto")
	rv.Set(":method", "POST")
	rv.Set(":path", req.Method())
	rv.Set(":authority", "localhost")
	rv.Set(":scheme", "http")
	return rv
}

type unknownAddr string

func (l unknownAddr) Network() string {
	return "unknown"
}

func (l unknownAddr) String() string {
	return ""
}

// TODO(morgabra): The transport impl has to give this to us I suppose.
func PeerForRequest(req *Request) *peer.Peer {
	pr := &peer.Peer{
		Addr:     unknownAddr(""),
		AuthInfo: nil, // If nil, no transport security is being used... which is technically true?
	}

	return pr
}

func TimeoutForRequest(req *Request) (time.Duration, bool, error) {
	v := req.Headers().Get("grpc-timeout")
	if len(v) > 1 {
		to, err := decodeTimeout(v[0])
		if err != nil {
			return 0, false, status.Errorf(codes.Internal, "malformed grpc-timeout: %v", err)
		}
		return to, true, nil
	}
	return 0, false, nil
}

func (s *Server) Handler(ctx context.Context, req *Request) (*Response, error) {
	serviceName, methodName, err := parseMethod(req.Method())
	if err != nil {
		return ErrorResponse(err), nil
	}
	service, ok := s.services[serviceName]
	if !ok {
		return ErrorResponse(status.Errorf(codes.Unimplemented, "unknown service %v", serviceName)), nil
	}
	method, ok := service.methods[methodName]
	if !ok {
		return ErrorResponse(status.Errorf(codes.Unimplemented, "unknown method %v for service %v", method, service)), nil
	}

	md := MetadataForRequest(req)
	p := PeerForRequest(req)
	timeout, ok, err := TimeoutForRequest(req)
	if err != nil {
		return ErrorResponse(err), nil
	}

	var timeoutDone context.CancelFunc
	if ok {
		ctx, timeoutDone = context.WithTimeout(ctx, timeout)
		defer timeoutDone()
	}

	ctx = peer.NewContext(ctx, p)
	ctx = metadata.NewIncomingContext(ctx, md)
	stream := NewTransportStream(method)
	ctx = grpc.NewContextWithServerTransportStream(ctx, stream)

	df := func(v any) error {
		err := req.UnmarshalRequest(v)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to unmarshal request: %v", err)
		}
		return nil
	}

	resp, err := method.Handler(service.serviceImpl, ctx, df, s.unaryInterceptor)
	if err != nil {
		appStatus, ok := status.FromError(err)
		if ok {
			err = appStatus.Err()
		} else {
			// Convert non-status application error to a status error with code
			// Unknown, but handle context errors specifically.
			appStatus = status.FromContextError(err)
			err = appStatus.Err()
		}
		return ErrorResponse(err), nil
	}

	err = stream.SetResponse(resp)
	if err != nil {
		return ErrorResponse(err), nil
	}

	// Everything is OK!
	// TODO(morgabra): We should maybe allow nil status == OK to save on bytes/serialization?
	stream.SetStatus(status.New(codes.OK, "OK"))
	return stream.Response()
}

// RegisterService registers a service and its implementation to the gRPC
// server. This is lifted from grpc.Server.
func (s *Server) RegisterService(sd *grpc.ServiceDesc, ss any) {
	if ss != nil {
		ht := reflect.TypeOf(sd.HandlerType).Elem()
		st := reflect.TypeOf(ss)
		if !st.Implements(ht) {
			panic(fmt.Sprintf("grpc: Server.RegisterService found the handler of type %v that does not satisfy %v", st, ht))
		}
	}
	s.register(sd, ss)
}

func (s *Server) register(sd *grpc.ServiceDesc, ss any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.services[sd.ServiceName]; ok {
		panic(fmt.Sprintf("grpc: Server.RegisterService found duplicate service registration for %q", sd.ServiceName))
	}
	info := &serviceInfo{
		serviceImpl: ss,
		methods:     make(map[string]*grpc.MethodDesc),
		streams:     make(map[string]*grpc.StreamDesc),
		mdata:       sd.Metadata,
	}
	for i := range sd.Methods {
		d := &sd.Methods[i]
		info.methods[d.MethodName] = d
	}
	for i := range sd.Streams {
		d := &sd.Streams[i]
		info.streams[d.StreamName] = d
	}
	s.services[sd.ServiceName] = info
}
