// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             (unknown)
// source: c1/connector/v2/ticket.proto

package v2

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// TicketsServiceClient is the client API for TicketsService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type TicketsServiceClient interface {
	CreateTicket(ctx context.Context, in *TicketsServiceCreateTicketRequest, opts ...grpc.CallOption) (*TicketsServiceCreateTicketResponse, error)
	GetTicket(ctx context.Context, in *TicketsServiceGetTicketRequest, opts ...grpc.CallOption) (*TicketsServiceGetTicketResponse, error)
	GetTicketSchema(ctx context.Context, in *TicketsServiceGetTicketSchemaRequest, opts ...grpc.CallOption) (*TicketsServiceGetTicketSchemaResponse, error)
}

type ticketsServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewTicketsServiceClient(cc grpc.ClientConnInterface) TicketsServiceClient {
	return &ticketsServiceClient{cc}
}

func (c *ticketsServiceClient) CreateTicket(ctx context.Context, in *TicketsServiceCreateTicketRequest, opts ...grpc.CallOption) (*TicketsServiceCreateTicketResponse, error) {
	out := new(TicketsServiceCreateTicketResponse)
	err := c.cc.Invoke(ctx, "/c1.connector.v2.TicketsService/CreateTicket", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *ticketsServiceClient) GetTicket(ctx context.Context, in *TicketsServiceGetTicketRequest, opts ...grpc.CallOption) (*TicketsServiceGetTicketResponse, error) {
	out := new(TicketsServiceGetTicketResponse)
	err := c.cc.Invoke(ctx, "/c1.connector.v2.TicketsService/GetTicket", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *ticketsServiceClient) GetTicketSchema(ctx context.Context, in *TicketsServiceGetTicketSchemaRequest, opts ...grpc.CallOption) (*TicketsServiceGetTicketSchemaResponse, error) {
	out := new(TicketsServiceGetTicketSchemaResponse)
	err := c.cc.Invoke(ctx, "/c1.connector.v2.TicketsService/GetTicketSchema", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TicketsServiceServer is the server API for TicketsService service.
// All implementations should embed UnimplementedTicketsServiceServer
// for forward compatibility
type TicketsServiceServer interface {
	CreateTicket(context.Context, *TicketsServiceCreateTicketRequest) (*TicketsServiceCreateTicketResponse, error)
	GetTicket(context.Context, *TicketsServiceGetTicketRequest) (*TicketsServiceGetTicketResponse, error)
	GetTicketSchema(context.Context, *TicketsServiceGetTicketSchemaRequest) (*TicketsServiceGetTicketSchemaResponse, error)
}

// UnimplementedTicketsServiceServer should be embedded to have forward compatible implementations.
type UnimplementedTicketsServiceServer struct {
}

func (UnimplementedTicketsServiceServer) CreateTicket(context.Context, *TicketsServiceCreateTicketRequest) (*TicketsServiceCreateTicketResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateTicket not implemented")
}
func (UnimplementedTicketsServiceServer) GetTicket(context.Context, *TicketsServiceGetTicketRequest) (*TicketsServiceGetTicketResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetTicket not implemented")
}
func (UnimplementedTicketsServiceServer) GetTicketSchema(context.Context, *TicketsServiceGetTicketSchemaRequest) (*TicketsServiceGetTicketSchemaResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetTicketSchema not implemented")
}

// UnsafeTicketsServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to TicketsServiceServer will
// result in compilation errors.
type UnsafeTicketsServiceServer interface {
	mustEmbedUnimplementedTicketsServiceServer()
}

func RegisterTicketsServiceServer(s grpc.ServiceRegistrar, srv TicketsServiceServer) {
	s.RegisterService(&TicketsService_ServiceDesc, srv)
}

func _TicketsService_CreateTicket_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TicketsServiceCreateTicketRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TicketsServiceServer).CreateTicket(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/c1.connector.v2.TicketsService/CreateTicket",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TicketsServiceServer).CreateTicket(ctx, req.(*TicketsServiceCreateTicketRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TicketsService_GetTicket_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TicketsServiceGetTicketRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TicketsServiceServer).GetTicket(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/c1.connector.v2.TicketsService/GetTicket",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TicketsServiceServer).GetTicket(ctx, req.(*TicketsServiceGetTicketRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _TicketsService_GetTicketSchema_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TicketsServiceGetTicketSchemaRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TicketsServiceServer).GetTicketSchema(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/c1.connector.v2.TicketsService/GetTicketSchema",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TicketsServiceServer).GetTicketSchema(ctx, req.(*TicketsServiceGetTicketSchemaRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// TicketsService_ServiceDesc is the grpc.ServiceDesc for TicketsService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var TicketsService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "c1.connector.v2.TicketsService",
	HandlerType: (*TicketsServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateTicket",
			Handler:    _TicketsService_CreateTicket_Handler,
		},
		{
			MethodName: "GetTicket",
			Handler:    _TicketsService_GetTicket_Handler,
		},
		{
			MethodName: "GetTicketSchema",
			Handler:    _TicketsService_GetTicketSchema_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "c1/connector/v2/ticket.proto",
}