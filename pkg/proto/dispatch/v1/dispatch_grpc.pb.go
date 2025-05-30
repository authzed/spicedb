// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             (unknown)
// source: dispatch/v1/dispatch.proto

package dispatchv1

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

const (
	DispatchService_DispatchCheck_FullMethodName            = "/dispatch.v1.DispatchService/DispatchCheck"
	DispatchService_DispatchExpand_FullMethodName           = "/dispatch.v1.DispatchService/DispatchExpand"
	DispatchService_DispatchLookupSubjects_FullMethodName   = "/dispatch.v1.DispatchService/DispatchLookupSubjects"
	DispatchService_DispatchLookupResources2_FullMethodName = "/dispatch.v1.DispatchService/DispatchLookupResources2"
)

// DispatchServiceClient is the client API for DispatchService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DispatchServiceClient interface {
	DispatchCheck(ctx context.Context, in *DispatchCheckRequest, opts ...grpc.CallOption) (*DispatchCheckResponse, error)
	DispatchExpand(ctx context.Context, in *DispatchExpandRequest, opts ...grpc.CallOption) (*DispatchExpandResponse, error)
	DispatchLookupSubjects(ctx context.Context, in *DispatchLookupSubjectsRequest, opts ...grpc.CallOption) (DispatchService_DispatchLookupSubjectsClient, error)
	DispatchLookupResources2(ctx context.Context, in *DispatchLookupResources2Request, opts ...grpc.CallOption) (DispatchService_DispatchLookupResources2Client, error)
}

type dispatchServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewDispatchServiceClient(cc grpc.ClientConnInterface) DispatchServiceClient {
	return &dispatchServiceClient{cc}
}

func (c *dispatchServiceClient) DispatchCheck(ctx context.Context, in *DispatchCheckRequest, opts ...grpc.CallOption) (*DispatchCheckResponse, error) {
	out := new(DispatchCheckResponse)
	err := c.cc.Invoke(ctx, DispatchService_DispatchCheck_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dispatchServiceClient) DispatchExpand(ctx context.Context, in *DispatchExpandRequest, opts ...grpc.CallOption) (*DispatchExpandResponse, error) {
	out := new(DispatchExpandResponse)
	err := c.cc.Invoke(ctx, DispatchService_DispatchExpand_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dispatchServiceClient) DispatchLookupSubjects(ctx context.Context, in *DispatchLookupSubjectsRequest, opts ...grpc.CallOption) (DispatchService_DispatchLookupSubjectsClient, error) {
	stream, err := c.cc.NewStream(ctx, &DispatchService_ServiceDesc.Streams[0], DispatchService_DispatchLookupSubjects_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &dispatchServiceDispatchLookupSubjectsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type DispatchService_DispatchLookupSubjectsClient interface {
	Recv() (*DispatchLookupSubjectsResponse, error)
	grpc.ClientStream
}

type dispatchServiceDispatchLookupSubjectsClient struct {
	grpc.ClientStream
}

func (x *dispatchServiceDispatchLookupSubjectsClient) Recv() (*DispatchLookupSubjectsResponse, error) {
	m := new(DispatchLookupSubjectsResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *dispatchServiceClient) DispatchLookupResources2(ctx context.Context, in *DispatchLookupResources2Request, opts ...grpc.CallOption) (DispatchService_DispatchLookupResources2Client, error) {
	stream, err := c.cc.NewStream(ctx, &DispatchService_ServiceDesc.Streams[1], DispatchService_DispatchLookupResources2_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &dispatchServiceDispatchLookupResources2Client{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type DispatchService_DispatchLookupResources2Client interface {
	Recv() (*DispatchLookupResources2Response, error)
	grpc.ClientStream
}

type dispatchServiceDispatchLookupResources2Client struct {
	grpc.ClientStream
}

func (x *dispatchServiceDispatchLookupResources2Client) Recv() (*DispatchLookupResources2Response, error) {
	m := new(DispatchLookupResources2Response)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// DispatchServiceServer is the server API for DispatchService service.
// All implementations must embed UnimplementedDispatchServiceServer
// for forward compatibility
type DispatchServiceServer interface {
	DispatchCheck(context.Context, *DispatchCheckRequest) (*DispatchCheckResponse, error)
	DispatchExpand(context.Context, *DispatchExpandRequest) (*DispatchExpandResponse, error)
	DispatchLookupSubjects(*DispatchLookupSubjectsRequest, DispatchService_DispatchLookupSubjectsServer) error
	DispatchLookupResources2(*DispatchLookupResources2Request, DispatchService_DispatchLookupResources2Server) error
	mustEmbedUnimplementedDispatchServiceServer()
}

// UnimplementedDispatchServiceServer must be embedded to have forward compatible implementations.
type UnimplementedDispatchServiceServer struct {
}

func (UnimplementedDispatchServiceServer) DispatchCheck(context.Context, *DispatchCheckRequest) (*DispatchCheckResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DispatchCheck not implemented")
}
func (UnimplementedDispatchServiceServer) DispatchExpand(context.Context, *DispatchExpandRequest) (*DispatchExpandResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DispatchExpand not implemented")
}
func (UnimplementedDispatchServiceServer) DispatchLookupSubjects(*DispatchLookupSubjectsRequest, DispatchService_DispatchLookupSubjectsServer) error {
	return status.Errorf(codes.Unimplemented, "method DispatchLookupSubjects not implemented")
}
func (UnimplementedDispatchServiceServer) DispatchLookupResources2(*DispatchLookupResources2Request, DispatchService_DispatchLookupResources2Server) error {
	return status.Errorf(codes.Unimplemented, "method DispatchLookupResources2 not implemented")
}
func (UnimplementedDispatchServiceServer) mustEmbedUnimplementedDispatchServiceServer() {}

// UnsafeDispatchServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DispatchServiceServer will
// result in compilation errors.
type UnsafeDispatchServiceServer interface {
	mustEmbedUnimplementedDispatchServiceServer()
}

func RegisterDispatchServiceServer(s grpc.ServiceRegistrar, srv DispatchServiceServer) {
	s.RegisterService(&DispatchService_ServiceDesc, srv)
}

func _DispatchService_DispatchCheck_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DispatchCheckRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DispatchServiceServer).DispatchCheck(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DispatchService_DispatchCheck_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DispatchServiceServer).DispatchCheck(ctx, req.(*DispatchCheckRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DispatchService_DispatchExpand_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DispatchExpandRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DispatchServiceServer).DispatchExpand(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: DispatchService_DispatchExpand_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DispatchServiceServer).DispatchExpand(ctx, req.(*DispatchExpandRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DispatchService_DispatchLookupSubjects_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(DispatchLookupSubjectsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DispatchServiceServer).DispatchLookupSubjects(m, &dispatchServiceDispatchLookupSubjectsServer{stream})
}

type DispatchService_DispatchLookupSubjectsServer interface {
	Send(*DispatchLookupSubjectsResponse) error
	grpc.ServerStream
}

type dispatchServiceDispatchLookupSubjectsServer struct {
	grpc.ServerStream
}

func (x *dispatchServiceDispatchLookupSubjectsServer) Send(m *DispatchLookupSubjectsResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _DispatchService_DispatchLookupResources2_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(DispatchLookupResources2Request)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(DispatchServiceServer).DispatchLookupResources2(m, &dispatchServiceDispatchLookupResources2Server{stream})
}

type DispatchService_DispatchLookupResources2Server interface {
	Send(*DispatchLookupResources2Response) error
	grpc.ServerStream
}

type dispatchServiceDispatchLookupResources2Server struct {
	grpc.ServerStream
}

func (x *dispatchServiceDispatchLookupResources2Server) Send(m *DispatchLookupResources2Response) error {
	return x.ServerStream.SendMsg(m)
}

// DispatchService_ServiceDesc is the grpc.ServiceDesc for DispatchService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DispatchService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "dispatch.v1.DispatchService",
	HandlerType: (*DispatchServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "DispatchCheck",
			Handler:    _DispatchService_DispatchCheck_Handler,
		},
		{
			MethodName: "DispatchExpand",
			Handler:    _DispatchService_DispatchExpand_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "DispatchLookupSubjects",
			Handler:       _DispatchService_DispatchLookupSubjects_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "DispatchLookupResources2",
			Handler:       _DispatchService_DispatchLookupResources2_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "dispatch/v1/dispatch.proto",
}
