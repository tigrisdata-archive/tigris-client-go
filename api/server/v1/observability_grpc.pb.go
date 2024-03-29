// Copyright 2022-2023 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v3.21.9
// source: server/v1/observability.proto

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

const (
	Observability_QueryTimeSeriesMetrics_FullMethodName = "/tigrisdata.observability.v1.Observability/QueryTimeSeriesMetrics"
	Observability_QuotaLimits_FullMethodName            = "/tigrisdata.observability.v1.Observability/QuotaLimits"
	Observability_QuotaUsage_FullMethodName             = "/tigrisdata.observability.v1.Observability/QuotaUsage"
	Observability_GetInfo_FullMethodName                = "/tigrisdata.observability.v1.Observability/GetInfo"
)

// ObservabilityClient is the client API for Observability service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ObservabilityClient interface {
	// Queries time series metrics
	QueryTimeSeriesMetrics(ctx context.Context, in *QueryTimeSeriesMetricsRequest, opts ...grpc.CallOption) (*QueryTimeSeriesMetricsResponse, error)
	// Returns current namespace quota limits
	QuotaLimits(ctx context.Context, in *QuotaLimitsRequest, opts ...grpc.CallOption) (*QuotaLimitsResponse, error)
	// Returns current namespace quota limits
	QuotaUsage(ctx context.Context, in *QuotaUsageRequest, opts ...grpc.CallOption) (*QuotaUsageResponse, error)
	// Provides the information about the server. This information includes returning the server version, etc.
	GetInfo(ctx context.Context, in *GetInfoRequest, opts ...grpc.CallOption) (*GetInfoResponse, error)
}

type observabilityClient struct {
	cc grpc.ClientConnInterface
}

func NewObservabilityClient(cc grpc.ClientConnInterface) ObservabilityClient {
	return &observabilityClient{cc}
}

func (c *observabilityClient) QueryTimeSeriesMetrics(ctx context.Context, in *QueryTimeSeriesMetricsRequest, opts ...grpc.CallOption) (*QueryTimeSeriesMetricsResponse, error) {
	out := new(QueryTimeSeriesMetricsResponse)
	err := c.cc.Invoke(ctx, Observability_QueryTimeSeriesMetrics_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *observabilityClient) QuotaLimits(ctx context.Context, in *QuotaLimitsRequest, opts ...grpc.CallOption) (*QuotaLimitsResponse, error) {
	out := new(QuotaLimitsResponse)
	err := c.cc.Invoke(ctx, Observability_QuotaLimits_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *observabilityClient) QuotaUsage(ctx context.Context, in *QuotaUsageRequest, opts ...grpc.CallOption) (*QuotaUsageResponse, error) {
	out := new(QuotaUsageResponse)
	err := c.cc.Invoke(ctx, Observability_QuotaUsage_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *observabilityClient) GetInfo(ctx context.Context, in *GetInfoRequest, opts ...grpc.CallOption) (*GetInfoResponse, error) {
	out := new(GetInfoResponse)
	err := c.cc.Invoke(ctx, Observability_GetInfo_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ObservabilityServer is the server API for Observability service.
// All implementations should embed UnimplementedObservabilityServer
// for forward compatibility
type ObservabilityServer interface {
	// Queries time series metrics
	QueryTimeSeriesMetrics(context.Context, *QueryTimeSeriesMetricsRequest) (*QueryTimeSeriesMetricsResponse, error)
	// Returns current namespace quota limits
	QuotaLimits(context.Context, *QuotaLimitsRequest) (*QuotaLimitsResponse, error)
	// Returns current namespace quota limits
	QuotaUsage(context.Context, *QuotaUsageRequest) (*QuotaUsageResponse, error)
	// Provides the information about the server. This information includes returning the server version, etc.
	GetInfo(context.Context, *GetInfoRequest) (*GetInfoResponse, error)
}

// UnimplementedObservabilityServer should be embedded to have forward compatible implementations.
type UnimplementedObservabilityServer struct {
}

func (UnimplementedObservabilityServer) QueryTimeSeriesMetrics(context.Context, *QueryTimeSeriesMetricsRequest) (*QueryTimeSeriesMetricsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method QueryTimeSeriesMetrics not implemented")
}
func (UnimplementedObservabilityServer) QuotaLimits(context.Context, *QuotaLimitsRequest) (*QuotaLimitsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method QuotaLimits not implemented")
}
func (UnimplementedObservabilityServer) QuotaUsage(context.Context, *QuotaUsageRequest) (*QuotaUsageResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method QuotaUsage not implemented")
}
func (UnimplementedObservabilityServer) GetInfo(context.Context, *GetInfoRequest) (*GetInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetInfo not implemented")
}

// UnsafeObservabilityServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ObservabilityServer will
// result in compilation errors.
type UnsafeObservabilityServer interface {
	mustEmbedUnimplementedObservabilityServer()
}

func RegisterObservabilityServer(s grpc.ServiceRegistrar, srv ObservabilityServer) {
	s.RegisterService(&Observability_ServiceDesc, srv)
}

func _Observability_QueryTimeSeriesMetrics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(QueryTimeSeriesMetricsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObservabilityServer).QueryTimeSeriesMetrics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Observability_QueryTimeSeriesMetrics_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObservabilityServer).QueryTimeSeriesMetrics(ctx, req.(*QueryTimeSeriesMetricsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Observability_QuotaLimits_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(QuotaLimitsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObservabilityServer).QuotaLimits(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Observability_QuotaLimits_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObservabilityServer).QuotaLimits(ctx, req.(*QuotaLimitsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Observability_QuotaUsage_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(QuotaUsageRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObservabilityServer).QuotaUsage(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Observability_QuotaUsage_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObservabilityServer).QuotaUsage(ctx, req.(*QuotaUsageRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Observability_GetInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObservabilityServer).GetInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: Observability_GetInfo_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObservabilityServer).GetInfo(ctx, req.(*GetInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Observability_ServiceDesc is the grpc.ServiceDesc for Observability service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Observability_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "tigrisdata.observability.v1.Observability",
	HandlerType: (*ObservabilityServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "QueryTimeSeriesMetrics",
			Handler:    _Observability_QueryTimeSeriesMetrics_Handler,
		},
		{
			MethodName: "QuotaLimits",
			Handler:    _Observability_QuotaLimits_Handler,
		},
		{
			MethodName: "QuotaUsage",
			Handler:    _Observability_QuotaUsage_Handler,
		},
		{
			MethodName: "GetInfo",
			Handler:    _Observability_GetInfo_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "server/v1/observability.proto",
}
