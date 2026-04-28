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
		msg: pbtransport.Response_builder{
			Resp:     anyResp,
			Status:   anyStatus,
			Headers:  headers,
			Trailers: trailers,
		}.Build(),
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
		unaryInterceptor:       unaryInterceptor,
		services:               make(map[string]*serviceInfo),
		activeServiceInstances: make(map[serviceImplementationKey]*serviceImplementationState),
	}
}

type serviceInfo struct {
	serviceImpl any
	handlerType reflect.Type
	methods     map[string]*grpc.MethodDesc
	streams     map[string]*grpc.StreamDesc
	mdata       any
}

type Server struct {
	mu                     sync.Mutex
	unaryInterceptor       grpc.UnaryServerInterceptor
	activeServiceInstances map[serviceImplementationKey]*serviceImplementationState

	services map[string]*serviceInfo
}

type serviceImplementationKey struct {
	typ reflect.Type
	ptr uintptr
}

type serviceImplementationState struct {
	active  int
	drained chan struct{}
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

	s.mu.Lock()
	service, ok := s.services[serviceName]
	if !ok {
		s.mu.Unlock()
		return ErrorResponse(status.Errorf(codes.Unimplemented, "unknown service %v", serviceName)), nil
	}
	method, ok := service.methods[methodName]
	if !ok {
		s.mu.Unlock()
		return ErrorResponse(status.Errorf(codes.Unimplemented, "unknown method %v for service %v", method, service)), nil
	}
	serviceImpl := service.serviceImpl
	releaseServiceImpl := s.acquireServiceImplementationLocked(serviceImpl)
	s.mu.Unlock()
	defer releaseServiceImpl()

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

	resp, err := method.Handler(serviceImpl, ctx, df, s.unaryInterceptor)
	if err != nil {
		appStatus, ok := status.FromError(err)
		if ok {
			err = appStatus.Err()
		} else {
			appStatus = statusForApplicationError(err)
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
		handlerType: reflect.TypeOf(sd.HandlerType).Elem(),
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

// ReplaceServiceImplementation swaps all services currently registered with oldImpl to
// newImpl and returns a channel that closes when requests using oldImpl have drained.
// It is intended for lambda connector generation reloads where the service
// descriptors stay fixed but connector-owned state must be discarded wholesale.
func (s *Server) ReplaceServiceImplementation(oldImpl any, newImpl any) (int, <-chan struct{}, error) {
	if newImpl == nil {
		return 0, closedChannel(), fmt.Errorf("grpc: replacement service implementation is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var replacements []*serviceInfo
	for serviceName, service := range s.services {
		if !sameServiceImplementation(service.serviceImpl, oldImpl) {
			continue
		}
		if service.handlerType != nil && !reflect.TypeOf(newImpl).Implements(service.handlerType) {
			return 0, closedChannel(), fmt.Errorf("grpc: replacement service implementation of type %v does not satisfy %v for service %q", reflect.TypeOf(newImpl), service.handlerType, serviceName)
		}
		replacements = append(replacements, service)
	}

	for _, service := range replacements {
		service.serviceImpl = newImpl
	}

	drained := closedChannel()
	if key, ok := serviceImplementationKeyFor(oldImpl); ok {
		if state, ok := s.activeServiceInstances[key]; ok {
			drained = state.drained
		}
	}

	return len(replacements), drained, nil
}

func sameServiceImplementation(a any, b any) bool {
	if a == nil || b == nil {
		return a == b
	}

	av := reflect.ValueOf(a)
	bv := reflect.ValueOf(b)
	if av.Kind() == reflect.Pointer && bv.Kind() == reflect.Pointer {
		return av.Pointer() == bv.Pointer()
	}
	if !av.Type().Comparable() || !bv.Type().Comparable() {
		return false
	}

	return a == b
}

func (s *Server) acquireServiceImplementationLocked(impl any) func() {
	key, ok := serviceImplementationKeyFor(impl)
	if !ok {
		return func() {}
	}

	state, ok := s.activeServiceInstances[key]
	if !ok {
		state = &serviceImplementationState{drained: make(chan struct{})}
		s.activeServiceInstances[key] = state
	}
	state.active++

	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		state := s.activeServiceInstances[key]
		if state == nil {
			return
		}
		state.active--
		if state.active == 0 {
			close(state.drained)
			delete(s.activeServiceInstances, key)
		}
	}
}

func serviceImplementationKeyFor(impl any) (serviceImplementationKey, bool) {
	if impl == nil {
		return serviceImplementationKey{}, false
	}

	v := reflect.ValueOf(impl)
	if v.Kind() != reflect.Pointer {
		return serviceImplementationKey{}, false
	}

	return serviceImplementationKey{
		typ: v.Type(),
		ptr: v.Pointer(),
	}, true
}

func closedChannel() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
