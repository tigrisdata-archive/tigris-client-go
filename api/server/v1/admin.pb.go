// Copyright 2022 Tigris Data, Inc.
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

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.0
// 	protoc        v3.20.1
// source: server/v1/admin.proto

package api

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var File_server_v1_admin_proto protoreflect.FileDescriptor

var file_server_v1_admin_proto_rawDesc = []byte{
	0x0a, 0x15, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x76, 0x31, 0x2f, 0x61, 0x64, 0x6d, 0x69,
	0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x13, 0x74, 0x69, 0x67, 0x72, 0x69, 0x73, 0x64,
	0x61, 0x74, 0x61, 0x2e, 0x61, 0x64, 0x6d, 0x69, 0x6e, 0x2e, 0x76, 0x31, 0x32, 0x07, 0x0a, 0x05,
	0x41, 0x64, 0x6d, 0x69, 0x6e, 0x42, 0x41, 0x0a, 0x1d, 0x63, 0x6f, 0x6d, 0x2e, 0x74, 0x69, 0x67,
	0x72, 0x69, 0x73, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x64, 0x62, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x76,
	0x31, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x5a, 0x20, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x74, 0x69, 0x67, 0x72, 0x69, 0x73, 0x64, 0x61, 0x74, 0x61, 0x2f, 0x74, 0x69,
	0x67, 0x72, 0x69, 0x73, 0x2f, 0x61, 0x70, 0x69, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var file_server_v1_admin_proto_goTypes = []interface{}{}
var file_server_v1_admin_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_server_v1_admin_proto_init() }
func file_server_v1_admin_proto_init() {
	if File_server_v1_admin_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_server_v1_admin_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_server_v1_admin_proto_goTypes,
		DependencyIndexes: file_server_v1_admin_proto_depIdxs,
	}.Build()
	File_server_v1_admin_proto = out.File
	file_server_v1_admin_proto_rawDesc = nil
	file_server_v1_admin_proto_goTypes = nil
	file_server_v1_admin_proto_depIdxs = nil
}