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

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        v3.21.9
// source: server/v1/billing.proto

package api

import (
	_ "github.com/google/gnostic/openapiv3"
	_ "google.golang.org/genproto/googleapis/api/annotations"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ListInvoicesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// RFC 3339 timestamp (inclusive). Invoices will only be returned for billing periods that start at or after this time.
	StartingOn *timestamppb.Timestamp `protobuf:"bytes,1,opt,name=starting_on,json=startingOn,proto3,oneof" json:"starting_on,omitempty"`
	// RFC 3339 timestamp (exclusive). Invoices will only be returned for billing periods that end before this time.
	EndingBefore *timestamppb.Timestamp `protobuf:"bytes,2,opt,name=ending_before,json=endingBefore,proto3,oneof" json:"ending_before,omitempty"`
	// optionally filter by a specific invoice_id
	InvoiceId string `protobuf:"bytes,3,opt,name=invoice_id,json=invoiceId,proto3" json:"invoice_id,omitempty"`
	// maximum number of items to include in result set
	PageSize *int32 `protobuf:"varint,4,opt,name=page_size,json=pageSize,proto3,oneof" json:"page_size,omitempty"`
	// pagination token for fetching a particular result page, first page will be fetched if `null`
	NextPage *string `protobuf:"bytes,5,opt,name=next_page,json=nextPage,proto3,oneof" json:"next_page,omitempty"`
}

func (x *ListInvoicesRequest) Reset() {
	*x = ListInvoicesRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_server_v1_billing_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListInvoicesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListInvoicesRequest) ProtoMessage() {}

func (x *ListInvoicesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_server_v1_billing_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListInvoicesRequest.ProtoReflect.Descriptor instead.
func (*ListInvoicesRequest) Descriptor() ([]byte, []int) {
	return file_server_v1_billing_proto_rawDescGZIP(), []int{0}
}

func (x *ListInvoicesRequest) GetStartingOn() *timestamppb.Timestamp {
	if x != nil {
		return x.StartingOn
	}
	return nil
}

func (x *ListInvoicesRequest) GetEndingBefore() *timestamppb.Timestamp {
	if x != nil {
		return x.EndingBefore
	}
	return nil
}

func (x *ListInvoicesRequest) GetInvoiceId() string {
	if x != nil {
		return x.InvoiceId
	}
	return ""
}

func (x *ListInvoicesRequest) GetPageSize() int32 {
	if x != nil && x.PageSize != nil {
		return *x.PageSize
	}
	return 0
}

func (x *ListInvoicesRequest) GetNextPage() string {
	if x != nil && x.NextPage != nil {
		return *x.NextPage
	}
	return ""
}

type ListInvoicesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Array of invoices
	Data []*Invoice `protobuf:"bytes,1,rep,name=data,proto3" json:"data,omitempty"`
	// token for next page if it exists, else `null`
	NextPage *string `protobuf:"bytes,2,opt,name=next_page,json=nextPage,proto3,oneof" json:"next_page,omitempty"`
}

func (x *ListInvoicesResponse) Reset() {
	*x = ListInvoicesResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_server_v1_billing_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListInvoicesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListInvoicesResponse) ProtoMessage() {}

func (x *ListInvoicesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_server_v1_billing_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListInvoicesResponse.ProtoReflect.Descriptor instead.
func (*ListInvoicesResponse) Descriptor() ([]byte, []int) {
	return file_server_v1_billing_proto_rawDescGZIP(), []int{1}
}

func (x *ListInvoicesResponse) GetData() []*Invoice {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *ListInvoicesResponse) GetNextPage() string {
	if x != nil && x.NextPage != nil {
		return *x.NextPage
	}
	return ""
}

type Invoice struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// unique identifier for this invoice
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// entries that make up the invoice
	Entries []*InvoiceLineItem `protobuf:"bytes,2,rep,name=entries,proto3" json:"entries,omitempty"`
	// RFC 3339 starting time for usage period during which items were added to this invoice
	StartTime *timestamppb.Timestamp `protobuf:"bytes,3,opt,name=start_time,json=startTime,proto3" json:"start_time,omitempty"`
	// RFC 3339 ending time for usage period during which items were added to this invoice
	EndTime *timestamppb.Timestamp `protobuf:"bytes,4,opt,name=end_time,json=endTime,proto3" json:"end_time,omitempty"`
	// invoice subtotal
	Subtotal float32 `protobuf:"fixed32,5,opt,name=subtotal,proto3" json:"subtotal,omitempty"`
	// total invoice amount
	Total float32 `protobuf:"fixed32,6,opt,name=total,proto3" json:"total,omitempty"`
	// Tigris price plan name
	PlanName string `protobuf:"bytes,7,opt,name=plan_name,json=planName,proto3" json:"plan_name,omitempty"`
}

func (x *Invoice) Reset() {
	*x = Invoice{}
	if protoimpl.UnsafeEnabled {
		mi := &file_server_v1_billing_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Invoice) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Invoice) ProtoMessage() {}

func (x *Invoice) ProtoReflect() protoreflect.Message {
	mi := &file_server_v1_billing_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Invoice.ProtoReflect.Descriptor instead.
func (*Invoice) Descriptor() ([]byte, []int) {
	return file_server_v1_billing_proto_rawDescGZIP(), []int{2}
}

func (x *Invoice) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Invoice) GetEntries() []*InvoiceLineItem {
	if x != nil {
		return x.Entries
	}
	return nil
}

func (x *Invoice) GetStartTime() *timestamppb.Timestamp {
	if x != nil {
		return x.StartTime
	}
	return nil
}

func (x *Invoice) GetEndTime() *timestamppb.Timestamp {
	if x != nil {
		return x.EndTime
	}
	return nil
}

func (x *Invoice) GetSubtotal() float32 {
	if x != nil {
		return x.Subtotal
	}
	return 0
}

func (x *Invoice) GetTotal() float32 {
	if x != nil {
		return x.Total
	}
	return 0
}

func (x *Invoice) GetPlanName() string {
	if x != nil {
		return x.PlanName
	}
	return ""
}

type InvoiceLineItem struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Product name
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Quantity
	Quantity float32 `protobuf:"fixed32,2,opt,name=quantity,proto3" json:"quantity,omitempty"`
	// Total amount for the product
	Total float32 `protobuf:"fixed32,3,opt,name=total,proto3" json:"total,omitempty"`
	// Broken down charges
	Charges []*Charge `protobuf:"bytes,4,rep,name=charges,proto3" json:"charges,omitempty"`
}

func (x *InvoiceLineItem) Reset() {
	*x = InvoiceLineItem{}
	if protoimpl.UnsafeEnabled {
		mi := &file_server_v1_billing_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InvoiceLineItem) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InvoiceLineItem) ProtoMessage() {}

func (x *InvoiceLineItem) ProtoReflect() protoreflect.Message {
	mi := &file_server_v1_billing_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InvoiceLineItem.ProtoReflect.Descriptor instead.
func (*InvoiceLineItem) Descriptor() ([]byte, []int) {
	return file_server_v1_billing_proto_rawDescGZIP(), []int{3}
}

func (x *InvoiceLineItem) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *InvoiceLineItem) GetQuantity() float32 {
	if x != nil {
		return x.Quantity
	}
	return 0
}

func (x *InvoiceLineItem) GetTotal() float32 {
	if x != nil {
		return x.Total
	}
	return 0
}

func (x *InvoiceLineItem) GetCharges() []*Charge {
	if x != nil {
		return x.Charges
	}
	return nil
}

type Charge struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Charge name
	Name     string  `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Quantity float32 `protobuf:"fixed32,2,opt,name=quantity,proto3" json:"quantity,omitempty"`
	Subtotal float32 `protobuf:"fixed32,3,opt,name=subtotal,proto3" json:"subtotal,omitempty"`
	// Tiered charges, if any
	Tiers []*ChargeTier `protobuf:"bytes,4,rep,name=tiers,proto3" json:"tiers,omitempty"`
}

func (x *Charge) Reset() {
	*x = Charge{}
	if protoimpl.UnsafeEnabled {
		mi := &file_server_v1_billing_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Charge) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Charge) ProtoMessage() {}

func (x *Charge) ProtoReflect() protoreflect.Message {
	mi := &file_server_v1_billing_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Charge.ProtoReflect.Descriptor instead.
func (*Charge) Descriptor() ([]byte, []int) {
	return file_server_v1_billing_proto_rawDescGZIP(), []int{4}
}

func (x *Charge) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Charge) GetQuantity() float32 {
	if x != nil {
		return x.Quantity
	}
	return 0
}

func (x *Charge) GetSubtotal() float32 {
	if x != nil {
		return x.Subtotal
	}
	return 0
}

func (x *Charge) GetTiers() []*ChargeTier {
	if x != nil {
		return x.Tiers
	}
	return nil
}

type ChargeTier struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Starting point where this Tier is applicable.
	// Ex - A charge could be tiered as "Tier 1 (0-5)", "Tier 2 (5-10)"; starting_at will be 0, 5 etc.
	StartingAt float32 `protobuf:"fixed32,1,opt,name=starting_at,json=startingAt,proto3" json:"starting_at,omitempty"`
	Quantity   float32 `protobuf:"fixed32,2,opt,name=quantity,proto3" json:"quantity,omitempty"`
	Price      float32 `protobuf:"fixed32,3,opt,name=price,proto3" json:"price,omitempty"`
	Subtotal   float32 `protobuf:"fixed32,4,opt,name=subtotal,proto3" json:"subtotal,omitempty"`
}

func (x *ChargeTier) Reset() {
	*x = ChargeTier{}
	if protoimpl.UnsafeEnabled {
		mi := &file_server_v1_billing_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ChargeTier) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ChargeTier) ProtoMessage() {}

func (x *ChargeTier) ProtoReflect() protoreflect.Message {
	mi := &file_server_v1_billing_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ChargeTier.ProtoReflect.Descriptor instead.
func (*ChargeTier) Descriptor() ([]byte, []int) {
	return file_server_v1_billing_proto_rawDescGZIP(), []int{5}
}

func (x *ChargeTier) GetStartingAt() float32 {
	if x != nil {
		return x.StartingAt
	}
	return 0
}

func (x *ChargeTier) GetQuantity() float32 {
	if x != nil {
		return x.Quantity
	}
	return 0
}

func (x *ChargeTier) GetPrice() float32 {
	if x != nil {
		return x.Price
	}
	return 0
}

func (x *ChargeTier) GetSubtotal() float32 {
	if x != nil {
		return x.Subtotal
	}
	return 0
}

var File_server_v1_billing_proto protoreflect.FileDescriptor

var file_server_v1_billing_proto_rawDesc = []byte{
	0x0a, 0x17, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x2f, 0x76, 0x31, 0x2f, 0x62, 0x69, 0x6c, 0x6c,
	0x69, 0x6e, 0x67, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x15, 0x74, 0x69, 0x67, 0x72, 0x69,
	0x73, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x62, 0x69, 0x6c, 0x6c, 0x69, 0x6e, 0x67, 0x2e, 0x76, 0x31,
	0x1a, 0x1c, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x61, 0x6e, 0x6e,
	0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1b,
	0x6f, 0x70, 0x65, 0x6e, 0x61, 0x70, 0x69, 0x76, 0x33, 0x2f, 0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xbe, 0x02, 0x0a,
	0x13, 0x4c, 0x69, 0x73, 0x74, 0x49, 0x6e, 0x76, 0x6f, 0x69, 0x63, 0x65, 0x73, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x40, 0x0a, 0x0b, 0x73, 0x74, 0x61, 0x72, 0x74, 0x69, 0x6e, 0x67,
	0x5f, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x48, 0x00, 0x52, 0x0a, 0x73, 0x74, 0x61, 0x72, 0x74, 0x69, 0x6e,
	0x67, 0x4f, 0x6e, 0x88, 0x01, 0x01, 0x12, 0x44, 0x0a, 0x0d, 0x65, 0x6e, 0x64, 0x69, 0x6e, 0x67,
	0x5f, 0x62, 0x65, 0x66, 0x6f, 0x72, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x48, 0x01, 0x52, 0x0c, 0x65, 0x6e, 0x64,
	0x69, 0x6e, 0x67, 0x42, 0x65, 0x66, 0x6f, 0x72, 0x65, 0x88, 0x01, 0x01, 0x12, 0x1d, 0x0a, 0x0a,
	0x69, 0x6e, 0x76, 0x6f, 0x69, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x09, 0x69, 0x6e, 0x76, 0x6f, 0x69, 0x63, 0x65, 0x49, 0x64, 0x12, 0x20, 0x0a, 0x09, 0x70,
	0x61, 0x67, 0x65, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x48, 0x02,
	0x52, 0x08, 0x70, 0x61, 0x67, 0x65, 0x53, 0x69, 0x7a, 0x65, 0x88, 0x01, 0x01, 0x12, 0x20, 0x0a,
	0x09, 0x6e, 0x65, 0x78, 0x74, 0x5f, 0x70, 0x61, 0x67, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09,
	0x48, 0x03, 0x52, 0x08, 0x6e, 0x65, 0x78, 0x74, 0x50, 0x61, 0x67, 0x65, 0x88, 0x01, 0x01, 0x42,
	0x0e, 0x0a, 0x0c, 0x5f, 0x73, 0x74, 0x61, 0x72, 0x74, 0x69, 0x6e, 0x67, 0x5f, 0x6f, 0x6e, 0x42,
	0x10, 0x0a, 0x0e, 0x5f, 0x65, 0x6e, 0x64, 0x69, 0x6e, 0x67, 0x5f, 0x62, 0x65, 0x66, 0x6f, 0x72,
	0x65, 0x42, 0x0c, 0x0a, 0x0a, 0x5f, 0x70, 0x61, 0x67, 0x65, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x42,
	0x0c, 0x0a, 0x0a, 0x5f, 0x6e, 0x65, 0x78, 0x74, 0x5f, 0x70, 0x61, 0x67, 0x65, 0x22, 0x7a, 0x0a,
	0x14, 0x4c, 0x69, 0x73, 0x74, 0x49, 0x6e, 0x76, 0x6f, 0x69, 0x63, 0x65, 0x73, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x32, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x1e, 0x2e, 0x74, 0x69, 0x67, 0x72, 0x69, 0x73, 0x64, 0x61, 0x74, 0x61,
	0x2e, 0x62, 0x69, 0x6c, 0x6c, 0x69, 0x6e, 0x67, 0x2e, 0x76, 0x31, 0x2e, 0x49, 0x6e, 0x76, 0x6f,
	0x69, 0x63, 0x65, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x12, 0x20, 0x0a, 0x09, 0x6e, 0x65, 0x78,
	0x74, 0x5f, 0x70, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x08,
	0x6e, 0x65, 0x78, 0x74, 0x50, 0x61, 0x67, 0x65, 0x88, 0x01, 0x01, 0x42, 0x0c, 0x0a, 0x0a, 0x5f,
	0x6e, 0x65, 0x78, 0x74, 0x5f, 0x70, 0x61, 0x67, 0x65, 0x22, 0x9c, 0x02, 0x0a, 0x07, 0x49, 0x6e,
	0x76, 0x6f, 0x69, 0x63, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x40, 0x0a, 0x07, 0x65, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73,
	0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x26, 0x2e, 0x74, 0x69, 0x67, 0x72, 0x69, 0x73, 0x64,
	0x61, 0x74, 0x61, 0x2e, 0x62, 0x69, 0x6c, 0x6c, 0x69, 0x6e, 0x67, 0x2e, 0x76, 0x31, 0x2e, 0x49,
	0x6e, 0x76, 0x6f, 0x69, 0x63, 0x65, 0x4c, 0x69, 0x6e, 0x65, 0x49, 0x74, 0x65, 0x6d, 0x52, 0x07,
	0x65, 0x6e, 0x74, 0x72, 0x69, 0x65, 0x73, 0x12, 0x39, 0x0a, 0x0a, 0x73, 0x74, 0x61, 0x72, 0x74,
	0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f,
	0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x73, 0x74, 0x61, 0x72, 0x74, 0x54, 0x69,
	0x6d, 0x65, 0x12, 0x35, 0x0a, 0x08, 0x65, 0x6e, 0x64, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
	0x52, 0x07, 0x65, 0x6e, 0x64, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x73, 0x75, 0x62,
	0x74, 0x6f, 0x74, 0x61, 0x6c, 0x18, 0x05, 0x20, 0x01, 0x28, 0x02, 0x52, 0x08, 0x73, 0x75, 0x62,
	0x74, 0x6f, 0x74, 0x61, 0x6c, 0x12, 0x14, 0x0a, 0x05, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x18, 0x06,
	0x20, 0x01, 0x28, 0x02, 0x52, 0x05, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x12, 0x1b, 0x0a, 0x09, 0x70,
	0x6c, 0x61, 0x6e, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08,
	0x70, 0x6c, 0x61, 0x6e, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x90, 0x01, 0x0a, 0x0f, 0x49, 0x6e, 0x76,
	0x6f, 0x69, 0x63, 0x65, 0x4c, 0x69, 0x6e, 0x65, 0x49, 0x74, 0x65, 0x6d, 0x12, 0x12, 0x0a, 0x04,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65,
	0x12, 0x1a, 0x0a, 0x08, 0x71, 0x75, 0x61, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x02, 0x52, 0x08, 0x71, 0x75, 0x61, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x12, 0x14, 0x0a, 0x05,
	0x74, 0x6f, 0x74, 0x61, 0x6c, 0x18, 0x03, 0x20, 0x01, 0x28, 0x02, 0x52, 0x05, 0x74, 0x6f, 0x74,
	0x61, 0x6c, 0x12, 0x37, 0x0a, 0x07, 0x63, 0x68, 0x61, 0x72, 0x67, 0x65, 0x73, 0x18, 0x04, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x74, 0x69, 0x67, 0x72, 0x69, 0x73, 0x64, 0x61, 0x74, 0x61,
	0x2e, 0x62, 0x69, 0x6c, 0x6c, 0x69, 0x6e, 0x67, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x68, 0x61, 0x72,
	0x67, 0x65, 0x52, 0x07, 0x63, 0x68, 0x61, 0x72, 0x67, 0x65, 0x73, 0x22, 0x8d, 0x01, 0x0a, 0x06,
	0x43, 0x68, 0x61, 0x72, 0x67, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x71, 0x75,
	0x61, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x02, 0x52, 0x08, 0x71, 0x75,
	0x61, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x12, 0x1a, 0x0a, 0x08, 0x73, 0x75, 0x62, 0x74, 0x6f, 0x74,
	0x61, 0x6c, 0x18, 0x03, 0x20, 0x01, 0x28, 0x02, 0x52, 0x08, 0x73, 0x75, 0x62, 0x74, 0x6f, 0x74,
	0x61, 0x6c, 0x12, 0x37, 0x0a, 0x05, 0x74, 0x69, 0x65, 0x72, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28,
	0x0b, 0x32, 0x21, 0x2e, 0x74, 0x69, 0x67, 0x72, 0x69, 0x73, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x62,
	0x69, 0x6c, 0x6c, 0x69, 0x6e, 0x67, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x68, 0x61, 0x72, 0x67, 0x65,
	0x54, 0x69, 0x65, 0x72, 0x52, 0x05, 0x74, 0x69, 0x65, 0x72, 0x73, 0x22, 0x7b, 0x0a, 0x0a, 0x43,
	0x68, 0x61, 0x72, 0x67, 0x65, 0x54, 0x69, 0x65, 0x72, 0x12, 0x1f, 0x0a, 0x0b, 0x73, 0x74, 0x61,
	0x72, 0x74, 0x69, 0x6e, 0x67, 0x5f, 0x61, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x02, 0x52, 0x0a,
	0x73, 0x74, 0x61, 0x72, 0x74, 0x69, 0x6e, 0x67, 0x41, 0x74, 0x12, 0x1a, 0x0a, 0x08, 0x71, 0x75,
	0x61, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x02, 0x52, 0x08, 0x71, 0x75,
	0x61, 0x6e, 0x74, 0x69, 0x74, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x70, 0x72, 0x69, 0x63, 0x65, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x02, 0x52, 0x05, 0x70, 0x72, 0x69, 0x63, 0x65, 0x12, 0x1a, 0x0a, 0x08,
	0x73, 0x75, 0x62, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x18, 0x04, 0x20, 0x01, 0x28, 0x02, 0x52, 0x08,
	0x73, 0x75, 0x62, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x32, 0xe3, 0x01, 0x0a, 0x07, 0x42, 0x69, 0x6c,
	0x6c, 0x69, 0x6e, 0x67, 0x12, 0xd7, 0x01, 0x0a, 0x0c, 0x4c, 0x69, 0x73, 0x74, 0x49, 0x6e, 0x76,
	0x6f, 0x69, 0x63, 0x65, 0x73, 0x12, 0x2a, 0x2e, 0x74, 0x69, 0x67, 0x72, 0x69, 0x73, 0x64, 0x61,
	0x74, 0x61, 0x2e, 0x62, 0x69, 0x6c, 0x6c, 0x69, 0x6e, 0x67, 0x2e, 0x76, 0x31, 0x2e, 0x4c, 0x69,
	0x73, 0x74, 0x49, 0x6e, 0x76, 0x6f, 0x69, 0x63, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x1a, 0x2b, 0x2e, 0x74, 0x69, 0x67, 0x72, 0x69, 0x73, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x62,
	0x69, 0x6c, 0x6c, 0x69, 0x6e, 0x67, 0x2e, 0x76, 0x31, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x49, 0x6e,
	0x76, 0x6f, 0x69, 0x63, 0x65, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x6e,
	0xba, 0x47, 0x2a, 0x0a, 0x07, 0x42, 0x69, 0x6c, 0x6c, 0x69, 0x6e, 0x67, 0x12, 0x1f, 0x4c, 0x69,
	0x73, 0x74, 0x73, 0x20, 0x61, 0x6c, 0x6c, 0x20, 0x69, 0x6e, 0x76, 0x6f, 0x69, 0x63, 0x65, 0x73,
	0x20, 0x66, 0x6f, 0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x75, 0x73, 0x65, 0x72, 0x82, 0xd3, 0xe4,
	0x93, 0x02, 0x3b, 0x5a, 0x23, 0x12, 0x21, 0x2f, 0x76, 0x31, 0x2f, 0x62, 0x69, 0x6c, 0x6c, 0x69,
	0x6e, 0x67, 0x2f, 0x69, 0x6e, 0x76, 0x6f, 0x69, 0x63, 0x65, 0x73, 0x2f, 0x7b, 0x69, 0x6e, 0x76,
	0x6f, 0x69, 0x63, 0x65, 0x5f, 0x69, 0x64, 0x7d, 0x12, 0x14, 0x2f, 0x76, 0x31, 0x2f, 0x62, 0x69,
	0x6c, 0x6c, 0x69, 0x6e, 0x67, 0x2f, 0x69, 0x6e, 0x76, 0x6f, 0x69, 0x63, 0x65, 0x73, 0x42, 0x41,
	0x0a, 0x1d, 0x63, 0x6f, 0x6d, 0x2e, 0x74, 0x69, 0x67, 0x72, 0x69, 0x73, 0x64, 0x61, 0x74, 0x61,
	0x2e, 0x64, 0x62, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x76, 0x31, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x5a,
	0x20, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x74, 0x69, 0x67, 0x72,
	0x69, 0x73, 0x64, 0x61, 0x74, 0x61, 0x2f, 0x74, 0x69, 0x67, 0x72, 0x69, 0x73, 0x2f, 0x61, 0x70,
	0x69, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_server_v1_billing_proto_rawDescOnce sync.Once
	file_server_v1_billing_proto_rawDescData = file_server_v1_billing_proto_rawDesc
)

func file_server_v1_billing_proto_rawDescGZIP() []byte {
	file_server_v1_billing_proto_rawDescOnce.Do(func() {
		file_server_v1_billing_proto_rawDescData = protoimpl.X.CompressGZIP(file_server_v1_billing_proto_rawDescData)
	})
	return file_server_v1_billing_proto_rawDescData
}

var file_server_v1_billing_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_server_v1_billing_proto_goTypes = []interface{}{
	(*ListInvoicesRequest)(nil),   // 0: tigrisdata.billing.v1.ListInvoicesRequest
	(*ListInvoicesResponse)(nil),  // 1: tigrisdata.billing.v1.ListInvoicesResponse
	(*Invoice)(nil),               // 2: tigrisdata.billing.v1.Invoice
	(*InvoiceLineItem)(nil),       // 3: tigrisdata.billing.v1.InvoiceLineItem
	(*Charge)(nil),                // 4: tigrisdata.billing.v1.Charge
	(*ChargeTier)(nil),            // 5: tigrisdata.billing.v1.ChargeTier
	(*timestamppb.Timestamp)(nil), // 6: google.protobuf.Timestamp
}
var file_server_v1_billing_proto_depIdxs = []int32{
	6, // 0: tigrisdata.billing.v1.ListInvoicesRequest.starting_on:type_name -> google.protobuf.Timestamp
	6, // 1: tigrisdata.billing.v1.ListInvoicesRequest.ending_before:type_name -> google.protobuf.Timestamp
	2, // 2: tigrisdata.billing.v1.ListInvoicesResponse.data:type_name -> tigrisdata.billing.v1.Invoice
	3, // 3: tigrisdata.billing.v1.Invoice.entries:type_name -> tigrisdata.billing.v1.InvoiceLineItem
	6, // 4: tigrisdata.billing.v1.Invoice.start_time:type_name -> google.protobuf.Timestamp
	6, // 5: tigrisdata.billing.v1.Invoice.end_time:type_name -> google.protobuf.Timestamp
	4, // 6: tigrisdata.billing.v1.InvoiceLineItem.charges:type_name -> tigrisdata.billing.v1.Charge
	5, // 7: tigrisdata.billing.v1.Charge.tiers:type_name -> tigrisdata.billing.v1.ChargeTier
	0, // 8: tigrisdata.billing.v1.Billing.ListInvoices:input_type -> tigrisdata.billing.v1.ListInvoicesRequest
	1, // 9: tigrisdata.billing.v1.Billing.ListInvoices:output_type -> tigrisdata.billing.v1.ListInvoicesResponse
	9, // [9:10] is the sub-list for method output_type
	8, // [8:9] is the sub-list for method input_type
	8, // [8:8] is the sub-list for extension type_name
	8, // [8:8] is the sub-list for extension extendee
	0, // [0:8] is the sub-list for field type_name
}

func init() { file_server_v1_billing_proto_init() }
func file_server_v1_billing_proto_init() {
	if File_server_v1_billing_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_server_v1_billing_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListInvoicesRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_server_v1_billing_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListInvoicesResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_server_v1_billing_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Invoice); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_server_v1_billing_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InvoiceLineItem); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_server_v1_billing_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Charge); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_server_v1_billing_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ChargeTier); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_server_v1_billing_proto_msgTypes[0].OneofWrappers = []interface{}{}
	file_server_v1_billing_proto_msgTypes[1].OneofWrappers = []interface{}{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_server_v1_billing_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_server_v1_billing_proto_goTypes,
		DependencyIndexes: file_server_v1_billing_proto_depIdxs,
		MessageInfos:      file_server_v1_billing_proto_msgTypes,
	}.Build()
	File_server_v1_billing_proto = out.File
	file_server_v1_billing_proto_rawDesc = nil
	file_server_v1_billing_proto_goTypes = nil
	file_server_v1_billing_proto_depIdxs = nil
}
