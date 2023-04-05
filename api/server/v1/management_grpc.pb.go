// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.12
// source: server/v1/management.proto

package api

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

// ManagementClient is the client API for Management service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ManagementClient interface {
	// Creates a new namespace, if it does not exist
	CreateNamespace(ctx context.Context, in *CreateNamespaceRequest, opts ...grpc.CallOption) (*CreateNamespaceResponse, error)
	// List all namespace and optionally lists specific namespace by namespaceId filter, also supports `describe` request.
	ListNamespaces(ctx context.Context, in *ListNamespacesRequest, opts ...grpc.CallOption) (*ListNamespacesResponse, error)
	// insertUserMetadata inserts the user metadata object
	InsertUserMetadata(ctx context.Context, in *InsertUserMetadataRequest, opts ...grpc.CallOption) (*InsertUserMetadataResponse, error)
	// GetUserMetadata inserts the user metadata object
	GetUserMetadata(ctx context.Context, in *GetUserMetadataRequest, opts ...grpc.CallOption) (*GetUserMetadataResponse, error)
	// updateUserMetadata updates the user metadata object
	UpdateUserMetadata(ctx context.Context, in *UpdateUserMetadataRequest, opts ...grpc.CallOption) (*UpdateUserMetadataResponse, error)
	// InsertNamespaceMetadata inserts the namespace metadata object
	InsertNamespaceMetadata(ctx context.Context, in *InsertNamespaceMetadataRequest, opts ...grpc.CallOption) (*InsertNamespaceMetadataResponse, error)
	// GetNamespaceMetadata inserts the user metadata object
	GetNamespaceMetadata(ctx context.Context, in *GetNamespaceMetadataRequest, opts ...grpc.CallOption) (*GetNamespaceMetadataResponse, error)
	// UpdateNamespaceMetadata updates the user metadata object
	UpdateNamespaceMetadata(ctx context.Context, in *UpdateNamespaceMetadataRequest, opts ...grpc.CallOption) (*UpdateNamespaceMetadataResponse, error)
}

type managementClient struct {
	cc grpc.ClientConnInterface
}

func NewManagementClient(cc grpc.ClientConnInterface) ManagementClient {
	return &managementClient{cc}
}

func (c *managementClient) CreateNamespace(ctx context.Context, in *CreateNamespaceRequest, opts ...grpc.CallOption) (*CreateNamespaceResponse, error) {
	out := new(CreateNamespaceResponse)
	err := c.cc.Invoke(ctx, "/tigrisdata.management.v1.Management/CreateNamespace", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *managementClient) ListNamespaces(ctx context.Context, in *ListNamespacesRequest, opts ...grpc.CallOption) (*ListNamespacesResponse, error) {
	out := new(ListNamespacesResponse)
	err := c.cc.Invoke(ctx, "/tigrisdata.management.v1.Management/ListNamespaces", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *managementClient) InsertUserMetadata(ctx context.Context, in *InsertUserMetadataRequest, opts ...grpc.CallOption) (*InsertUserMetadataResponse, error) {
	out := new(InsertUserMetadataResponse)
	err := c.cc.Invoke(ctx, "/tigrisdata.management.v1.Management/InsertUserMetadata", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *managementClient) GetUserMetadata(ctx context.Context, in *GetUserMetadataRequest, opts ...grpc.CallOption) (*GetUserMetadataResponse, error) {
	out := new(GetUserMetadataResponse)
	err := c.cc.Invoke(ctx, "/tigrisdata.management.v1.Management/GetUserMetadata", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *managementClient) UpdateUserMetadata(ctx context.Context, in *UpdateUserMetadataRequest, opts ...grpc.CallOption) (*UpdateUserMetadataResponse, error) {
	out := new(UpdateUserMetadataResponse)
	err := c.cc.Invoke(ctx, "/tigrisdata.management.v1.Management/UpdateUserMetadata", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *managementClient) InsertNamespaceMetadata(ctx context.Context, in *InsertNamespaceMetadataRequest, opts ...grpc.CallOption) (*InsertNamespaceMetadataResponse, error) {
	out := new(InsertNamespaceMetadataResponse)
	err := c.cc.Invoke(ctx, "/tigrisdata.management.v1.Management/InsertNamespaceMetadata", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *managementClient) GetNamespaceMetadata(ctx context.Context, in *GetNamespaceMetadataRequest, opts ...grpc.CallOption) (*GetNamespaceMetadataResponse, error) {
	out := new(GetNamespaceMetadataResponse)
	err := c.cc.Invoke(ctx, "/tigrisdata.management.v1.Management/GetNamespaceMetadata", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *managementClient) UpdateNamespaceMetadata(ctx context.Context, in *UpdateNamespaceMetadataRequest, opts ...grpc.CallOption) (*UpdateNamespaceMetadataResponse, error) {
	out := new(UpdateNamespaceMetadataResponse)
	err := c.cc.Invoke(ctx, "/tigrisdata.management.v1.Management/UpdateNamespaceMetadata", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ManagementServer is the server API for Management service.
// All implementations should embed UnimplementedManagementServer
// for forward compatibility
type ManagementServer interface {
	// Creates a new namespace, if it does not exist
	CreateNamespace(context.Context, *CreateNamespaceRequest) (*CreateNamespaceResponse, error)
	// List all namespace and optionally lists specific namespace by namespaceId filter, also supports `describe` request.
	ListNamespaces(context.Context, *ListNamespacesRequest) (*ListNamespacesResponse, error)
	// insertUserMetadata inserts the user metadata object
	InsertUserMetadata(context.Context, *InsertUserMetadataRequest) (*InsertUserMetadataResponse, error)
	// GetUserMetadata inserts the user metadata object
	GetUserMetadata(context.Context, *GetUserMetadataRequest) (*GetUserMetadataResponse, error)
	// updateUserMetadata updates the user metadata object
	UpdateUserMetadata(context.Context, *UpdateUserMetadataRequest) (*UpdateUserMetadataResponse, error)
	// InsertNamespaceMetadata inserts the namespace metadata object
	InsertNamespaceMetadata(context.Context, *InsertNamespaceMetadataRequest) (*InsertNamespaceMetadataResponse, error)
	// GetNamespaceMetadata inserts the user metadata object
	GetNamespaceMetadata(context.Context, *GetNamespaceMetadataRequest) (*GetNamespaceMetadataResponse, error)
	// UpdateNamespaceMetadata updates the user metadata object
	UpdateNamespaceMetadata(context.Context, *UpdateNamespaceMetadataRequest) (*UpdateNamespaceMetadataResponse, error)
}

// UnimplementedManagementServer should be embedded to have forward compatible implementations.
type UnimplementedManagementServer struct {
}

func (UnimplementedManagementServer) CreateNamespace(context.Context, *CreateNamespaceRequest) (*CreateNamespaceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateNamespace not implemented")
}
func (UnimplementedManagementServer) ListNamespaces(context.Context, *ListNamespacesRequest) (*ListNamespacesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListNamespaces not implemented")
}
func (UnimplementedManagementServer) InsertUserMetadata(context.Context, *InsertUserMetadataRequest) (*InsertUserMetadataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method InsertUserMetadata not implemented")
}
func (UnimplementedManagementServer) GetUserMetadata(context.Context, *GetUserMetadataRequest) (*GetUserMetadataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetUserMetadata not implemented")
}
func (UnimplementedManagementServer) UpdateUserMetadata(context.Context, *UpdateUserMetadataRequest) (*UpdateUserMetadataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateUserMetadata not implemented")
}
func (UnimplementedManagementServer) InsertNamespaceMetadata(context.Context, *InsertNamespaceMetadataRequest) (*InsertNamespaceMetadataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method InsertNamespaceMetadata not implemented")
}
func (UnimplementedManagementServer) GetNamespaceMetadata(context.Context, *GetNamespaceMetadataRequest) (*GetNamespaceMetadataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetNamespaceMetadata not implemented")
}
func (UnimplementedManagementServer) UpdateNamespaceMetadata(context.Context, *UpdateNamespaceMetadataRequest) (*UpdateNamespaceMetadataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateNamespaceMetadata not implemented")
}

// UnsafeManagementServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ManagementServer will
// result in compilation errors.
type UnsafeManagementServer interface {
	mustEmbedUnimplementedManagementServer()
}

func RegisterManagementServer(s grpc.ServiceRegistrar, srv ManagementServer) {
	s.RegisterService(&Management_ServiceDesc, srv)
}

func _Management_CreateNamespace_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateNamespaceRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagementServer).CreateNamespace(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tigrisdata.management.v1.Management/CreateNamespace",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagementServer).CreateNamespace(ctx, req.(*CreateNamespaceRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Management_ListNamespaces_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ListNamespacesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagementServer).ListNamespaces(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tigrisdata.management.v1.Management/ListNamespaces",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagementServer).ListNamespaces(ctx, req.(*ListNamespacesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Management_InsertUserMetadata_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InsertUserMetadataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagementServer).InsertUserMetadata(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tigrisdata.management.v1.Management/InsertUserMetadata",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagementServer).InsertUserMetadata(ctx, req.(*InsertUserMetadataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Management_GetUserMetadata_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetUserMetadataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagementServer).GetUserMetadata(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tigrisdata.management.v1.Management/GetUserMetadata",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagementServer).GetUserMetadata(ctx, req.(*GetUserMetadataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Management_UpdateUserMetadata_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateUserMetadataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagementServer).UpdateUserMetadata(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tigrisdata.management.v1.Management/UpdateUserMetadata",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagementServer).UpdateUserMetadata(ctx, req.(*UpdateUserMetadataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Management_InsertNamespaceMetadata_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(InsertNamespaceMetadataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagementServer).InsertNamespaceMetadata(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tigrisdata.management.v1.Management/InsertNamespaceMetadata",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagementServer).InsertNamespaceMetadata(ctx, req.(*InsertNamespaceMetadataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Management_GetNamespaceMetadata_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetNamespaceMetadataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagementServer).GetNamespaceMetadata(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tigrisdata.management.v1.Management/GetNamespaceMetadata",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagementServer).GetNamespaceMetadata(ctx, req.(*GetNamespaceMetadataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Management_UpdateNamespaceMetadata_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateNamespaceMetadataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ManagementServer).UpdateNamespaceMetadata(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/tigrisdata.management.v1.Management/UpdateNamespaceMetadata",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ManagementServer).UpdateNamespaceMetadata(ctx, req.(*UpdateNamespaceMetadataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Management_ServiceDesc is the grpc.ServiceDesc for Management service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Management_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "tigrisdata.management.v1.Management",
	HandlerType: (*ManagementServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateNamespace",
			Handler:    _Management_CreateNamespace_Handler,
		},
		{
			MethodName: "ListNamespaces",
			Handler:    _Management_ListNamespaces_Handler,
		},
		{
			MethodName: "InsertUserMetadata",
			Handler:    _Management_InsertUserMetadata_Handler,
		},
		{
			MethodName: "GetUserMetadata",
			Handler:    _Management_GetUserMetadata_Handler,
		},
		{
			MethodName: "UpdateUserMetadata",
			Handler:    _Management_UpdateUserMetadata_Handler,
		},
		{
			MethodName: "InsertNamespaceMetadata",
			Handler:    _Management_InsertNamespaceMetadata_Handler,
		},
		{
			MethodName: "GetNamespaceMetadata",
			Handler:    _Management_GetNamespaceMetadata_Handler,
		},
		{
			MethodName: "UpdateNamespaceMetadata",
			Handler:    _Management_UpdateNamespaceMetadata_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "server/v1/management.proto",
}
