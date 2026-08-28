package chaosconnector

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strings"

	"github.com/conductorone/baton-sdk/internal/connector"
	"github.com/conductorone/baton-sdk/pkg/connectorclient"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	"github.com/conductorone/baton-sdk/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// NewDirectClient adapts a connector server to the generated client aggregate
// without serialization. The same fault runtime wraps this and gRPC clients.
func NewDirectClient(ctx context.Context, server types.ConnectorServer, run *Run) types.ConnectorClient {
	direct := &directConn{server: server}
	return &directClient{
		ConnectorClient: connectorclient.NewConnectorClient(ctx, &faultConn{
			delegate: direct,
			run:      run,
		}),
		server: server,
	}
}

// directClient adds the syncer→connector source-cache lookup delivery
// (sourcecache.SetLookup) to the direct transport by forwarding to the
// server-side connectorbuilder, mirroring what the subprocess runner's
// connectorClient does over its own channel.
type directClient struct {
	types.ConnectorClient
	server types.ConnectorServer
}

func (c *directClient) SetSourceCache(ctx context.Context, lookup sourcecache.Lookup) {
	if setter, ok := c.server.(sourcecache.SetLookup); ok {
		setter.SetSourceCache(ctx, lookup)
	}
}

// GRPCClient owns an in-memory gRPC server and its generated connector client.
type GRPCClient struct {
	types.ConnectorClient

	conn      *grpc.ClientConn
	server    *grpc.Server
	listener  *bufconn.Listener
	connector types.ConnectorServer
}

// SetSourceCache delivers the source-cache lookup to the connector-side
// builder. The lookup is an in-process interface, so it cannot ride the
// gRPC transport itself; both processes here share memory, and forwarding
// directly mirrors the delivery a runner would perform out-of-band.
func (c *GRPCClient) SetSourceCache(ctx context.Context, lookup sourcecache.Lookup) {
	if setter, ok := c.connector.(sourcecache.SetLookup); ok {
		setter.SetSourceCache(ctx, lookup)
	}
}

// NewGRPCClient starts an in-memory gRPC server. It exercises normal service
// registration and protobuf serialization without a subprocess.
func NewGRPCClient(
	ctx context.Context,
	server types.ConnectorServer,
	run *Run,
	provisioning bool,
	ticketing bool,
) (*GRPCClient, error) {
	return newGRPCClient(ctx, server, run, provisioning, ticketing, false)
}

// NewGRPCServerFaultClient applies the schedule inside a unary server
// interceptor, so injected statuses and mutated responses cross gRPC
// serialization before the SDK observes them.
func NewGRPCServerFaultClient(
	ctx context.Context,
	server types.ConnectorServer,
	run *Run,
	provisioning bool,
	ticketing bool,
) (*GRPCClient, error) {
	return newGRPCClient(ctx, server, run, provisioning, ticketing, true)
}

func newGRPCClient(
	ctx context.Context,
	server types.ConnectorServer,
	run *Run,
	provisioning bool,
	ticketing bool,
	serverFaults bool,
) (*GRPCClient, error) {
	listener := bufconn.Listen(1024 * 1024)
	var serverOpts []grpc.ServerOption
	if serverFaults {
		serverOpts = append(serverOpts, grpc.UnaryInterceptor(serverFaultInterceptor(run)))
	}
	grpcServer := grpc.NewServer(serverOpts...)
	connector.Register(ctx, grpcServer, server, &connector.RegisterOps{
		ProvisioningEnabled: provisioning,
		TicketingEnabled:    ticketing,
	})
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///chaosconnector",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		return nil, fmt.Errorf("chaosconnector: create in-memory grpc client: %w", err)
	}
	var clientConn grpc.ClientConnInterface = conn
	if !serverFaults {
		clientConn = &faultConn{delegate: conn, run: run}
	}
	return &GRPCClient{
		ConnectorClient: connectorclient.NewConnectorClient(ctx, clientConn),
		conn:            conn,
		server:          grpcServer,
		listener:        listener,
		connector:       server,
	}, nil
}

func serverFaultInterceptor(run *Run) grpc.UnaryServerInterceptor {
	faults := &faultConn{run: run}
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		run.Runtime().operationStarted()
		defer run.Runtime().operationFinished()
		op := run.Runtime().Begin(operationFromRequest(info.FullMethod, req))
		if err := faults.applyPhase(ctx, &op, PhaseBeforeCall, nil); err != nil {
			faults.recordResult(op, err)
			return nil, err
		}
		response, err := handler(ctx, req)
		if err != nil {
			faults.recordResult(op, err)
			return nil, err
		}
		if err := faults.applyPhase(ctx, &op, PhaseAfterDelegate, nil); err != nil {
			faults.recordResult(op, err)
			return nil, err
		}
		responseMessage, _ := response.(proto.Message)
		if err := faults.applyPhase(ctx, &op, PhaseBeforeResponse, responseMessage); err != nil {
			faults.recordResult(op, err)
			return nil, err
		}
		faults.recordResult(op, nil)
		return response, nil
	}
}

// Close releases the in-memory gRPC transport.
func (c *GRPCClient) Close() error {
	if c == nil {
		return nil
	}
	c.server.Stop()
	listenerErr := c.listener.Close()
	connErr := c.conn.Close()
	if listenerErr != nil {
		return listenerErr
	}
	return connErr
}

type directConn struct {
	server types.ConnectorServer
}

func (c *directConn) Invoke(
	ctx context.Context,
	method string,
	args any,
	reply any,
	_ ...grpc.CallOption,
) error {
	methodName := method[strings.LastIndex(method, "/")+1:]
	handler := reflect.ValueOf(c.server).MethodByName(methodName)
	if !handler.IsValid() {
		return status.Errorf(codes.Unimplemented, "chaosconnector: method %s is not implemented", method)
	}
	handlerType := handler.Type()
	if handlerType.NumIn() != 2 || handlerType.NumOut() != 2 {
		return status.Errorf(codes.Unimplemented, "chaosconnector: method %s is not unary", method)
	}
	values := handler.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(args)})
	if !values[1].IsNil() {
		return values[1].Interface().(error)
	}
	if values[0].IsNil() {
		return status.Errorf(codes.Internal, "chaosconnector: method %s returned a nil response", method)
	}
	response, responseOK := values[0].Interface().(proto.Message)
	target, targetOK := reply.(proto.Message)
	if !responseOK || !targetOK {
		return status.Errorf(codes.Internal, "chaosconnector: method %s returned incompatible protobuf values", method)
	}
	cloned := proto.Clone(response)
	targetValue := reflect.ValueOf(target)
	clonedValue := reflect.ValueOf(cloned)
	if targetValue.Kind() != reflect.Pointer || clonedValue.Type() != targetValue.Type() {
		return status.Errorf(codes.Internal, "chaosconnector: method %s returned incompatible response type", method)
	}
	targetValue.Elem().Set(clonedValue.Elem())
	return nil
}

func (c *directConn) NewStream(
	context.Context,
	*grpc.StreamDesc,
	string,
	...grpc.CallOption,
) (grpc.ClientStream, error) {
	return nil, status.Error(codes.Unimplemented, "chaosconnector: direct streaming is excluded")
}

type faultConn struct {
	delegate grpc.ClientConnInterface
	run      *Run
}

func (c *faultConn) Invoke(
	ctx context.Context,
	method string,
	args any,
	reply any,
	opts ...grpc.CallOption,
) error {
	c.run.Runtime().operationStarted()
	defer c.run.Runtime().operationFinished()
	op := c.run.Runtime().Begin(operationFromRequest(method, args))
	if err := c.applyPhase(ctx, &op, PhaseBeforeCall, nil); err != nil {
		c.recordResult(op, err)
		return err
	}

	delegateErr := c.delegate.Invoke(ctx, method, args, reply, opts...)
	if delegateErr != nil {
		c.recordResult(op, delegateErr)
		return delegateErr
	}
	if err := c.applyPhase(ctx, &op, PhaseAfterDelegate, nil); err != nil {
		c.recordResult(op, err)
		return err
	}
	response, _ := reply.(proto.Message)
	if err := c.applyPhase(ctx, &op, PhaseBeforeResponse, response); err != nil {
		c.recordResult(op, err)
		return err
	}
	c.recordResult(op, nil)
	return nil
}

func (c *faultConn) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	c.run.Runtime().operationStarted()
	defer c.run.Runtime().operationFinished()
	op := c.run.Runtime().Begin(operationFromRequest(method, nil))
	if err := c.applyPhase(ctx, &op, PhaseBeforeCall, nil); err != nil {
		c.recordResult(op, err)
		return nil, err
	}
	stream, err := c.delegate.NewStream(ctx, desc, method, opts...)
	c.recordResult(op, err)
	return stream, err
}

func (c *faultConn) applyPhase(
	ctx context.Context,
	op *Operation,
	phase Phase,
	response proto.Message,
) error {
	op.Phase = phase
	fired := c.run.Runtime().Match(*op)
	for _, match := range fired {
		for _, effect := range match.Effects {
			switch effect.Kind {
			case EffectSetEpoch:
				err := c.run.SetEpoch(effect.Epoch)
				c.recordStateEffect(*op, match.ID, effect, err)
				if err != nil {
					return err
				}
			case EffectMutate:
				err := c.run.Mutations().Apply(effect.Mutation, response)
				c.recordStateEffect(*op, match.ID, effect, err)
				if err != nil {
					return err
				}
			case EffectError, EffectDelay, EffectBlock, EffectCancel, EffectLoseResponse, EffectCrash:
				if err := c.run.Runtime().ApplyControlEffects(ctx, *op, []FiredRule{{
					ID:      match.ID,
					Effects: []Effect{effect},
				}}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *faultConn) recordStateEffect(op Operation, ruleID string, effect Effect, err error) {
	event := TraceEvent{
		Operation: op,
		RuleID:    ruleID,
		Effect:    effect.Kind,
		Outcome:   OutcomeInjected,
	}
	if err != nil {
		event.Outcome = OutcomeErrored
		event.Error = err.Error()
	}
	c.run.Trace().Record(event)
}

func (c *faultConn) recordResult(op Operation, err error) {
	event := TraceEvent{
		Operation: op,
		Outcome:   OutcomeReturned,
	}
	if err != nil {
		event.Outcome = OutcomeErrored
		event.Error = err.Error()
	}
	c.run.Trace().Record(event)
}

func operationFromRequest(fullMethod string, request any) Operation {
	service, method := splitMethod(fullMethod)
	op := Operation{
		Domain:  DomainConnector,
		Service: service,
		Method:  method,
	}
	message, ok := request.(proto.Message)
	if !ok || message == nil {
		return op
	}
	reflected := message.ProtoReflect()
	op.PageToken = firstStringField(reflected, "page_token", "cursor")
	op.ResourceType = firstStringField(reflected, "resource_type_id")
	op.Subject = firstStringField(reflected, "id", "name", "request_id", "event_feed_id")
	if id := findResourceID(reflected); id != nil && id.IsValid() {
		if op.ResourceType == "" {
			op.ResourceType = firstStringField(id, "resource_type")
		}
		if op.Subject == "" {
			op.Subject = firstStringField(id, "resource")
		}
	}
	return op
}

func splitMethod(fullMethod string) (string, string) {
	trimmed := strings.TrimPrefix(fullMethod, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 {
		return "", trimmed
	}
	serviceParts := strings.Split(parts[0], ".")
	return serviceParts[len(serviceParts)-1], parts[1]
}

func firstStringField(message protoreflect.Message, names ...protoreflect.Name) string {
	for _, name := range names {
		field := message.Descriptor().Fields().ByName(name)
		if field != nil && field.Kind() == protoreflect.StringKind && message.Has(field) {
			return message.Get(field).String()
		}
	}
	return ""
}

func findResourceID(message protoreflect.Message) protoreflect.Message {
	for _, name := range []protoreflect.Name{"resource_id", "identity_id", "parent_resource_id"} {
		if nested := messageField(message, name); nested != nil && nested.IsValid() {
			return nested
		}
	}
	for _, name := range []protoreflect.Name{"resource", "principal"} {
		if nested := messageField(message, name); nested != nil && nested.IsValid() {
			if id := messageField(nested, "id"); id != nil && id.IsValid() {
				return id
			}
		}
	}
	if entitlement := messageField(message, "entitlement"); entitlement != nil && entitlement.IsValid() {
		if resource := messageField(entitlement, "resource"); resource != nil && resource.IsValid() {
			return messageField(resource, "id")
		}
	}
	return nil
}

func messageField(message protoreflect.Message, name protoreflect.Name) protoreflect.Message {
	field := message.Descriptor().Fields().ByName(name)
	if field == nil || field.Kind() != protoreflect.MessageKind || !message.Has(field) {
		return nil
	}
	return message.Get(field).Message()
}
