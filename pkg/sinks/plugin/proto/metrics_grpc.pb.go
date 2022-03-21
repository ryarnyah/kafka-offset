// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package proto

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

// KafkaPluginClient is the client API for KafkaPlugin service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type KafkaPluginClient interface {
	WriteKafkaMetrics(ctx context.Context, in *WriteKafkaMetricsRequest, opts ...grpc.CallOption) (*Empty, error)
}

type kafkaPluginClient struct {
	cc grpc.ClientConnInterface
}

func NewKafkaPluginClient(cc grpc.ClientConnInterface) KafkaPluginClient {
	return &kafkaPluginClient{cc}
}

func (c *kafkaPluginClient) WriteKafkaMetrics(ctx context.Context, in *WriteKafkaMetricsRequest, opts ...grpc.CallOption) (*Empty, error) {
	out := new(Empty)
	err := c.cc.Invoke(ctx, "/proto.KafkaPlugin/WriteKafkaMetrics", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// KafkaPluginServer is the server API for KafkaPlugin service.
// All implementations must embed UnimplementedKafkaPluginServer
// for forward compatibility
type KafkaPluginServer interface {
	WriteKafkaMetrics(context.Context, *WriteKafkaMetricsRequest) (*Empty, error)
	mustEmbedUnimplementedKafkaPluginServer()
}

// UnimplementedKafkaPluginServer must be embedded to have forward compatible implementations.
type UnimplementedKafkaPluginServer struct {
}

func (UnimplementedKafkaPluginServer) WriteKafkaMetrics(context.Context, *WriteKafkaMetricsRequest) (*Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteKafkaMetrics not implemented")
}
func (UnimplementedKafkaPluginServer) mustEmbedUnimplementedKafkaPluginServer() {}

// UnsafeKafkaPluginServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to KafkaPluginServer will
// result in compilation errors.
type UnsafeKafkaPluginServer interface {
	mustEmbedUnimplementedKafkaPluginServer()
}

func RegisterKafkaPluginServer(s grpc.ServiceRegistrar, srv KafkaPluginServer) {
	s.RegisterService(&KafkaPlugin_ServiceDesc, srv)
}

func _KafkaPlugin_WriteKafkaMetrics_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(WriteKafkaMetricsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(KafkaPluginServer).WriteKafkaMetrics(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/proto.KafkaPlugin/WriteKafkaMetrics",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(KafkaPluginServer).WriteKafkaMetrics(ctx, req.(*WriteKafkaMetricsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// KafkaPlugin_ServiceDesc is the grpc.ServiceDesc for KafkaPlugin service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var KafkaPlugin_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "proto.KafkaPlugin",
	HandlerType: (*KafkaPluginServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "WriteKafkaMetrics",
			Handler:    _KafkaPlugin_WriteKafkaMetrics_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pkg/sinks/plugin/proto/metrics.proto",
}
