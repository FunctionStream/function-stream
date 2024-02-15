// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v4.25.2
// source: fs/func/grpc_func/proto/grpc_func.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Response_Status int32

const (
	Response_OK    Response_Status = 0
	Response_ERROR Response_Status = 1
)

// Enum value maps for Response_Status.
var (
	Response_Status_name = map[int32]string{
		0: "OK",
		1: "ERROR",
	}
	Response_Status_value = map[string]int32{
		"OK":    0,
		"ERROR": 1,
	}
)

func (x Response_Status) Enum() *Response_Status {
	p := new(Response_Status)
	*p = x
	return p
}

func (x Response_Status) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Response_Status) Descriptor() protoreflect.EnumDescriptor {
	return file_fs_func_grpc_func_proto_grpc_func_proto_enumTypes[0].Descriptor()
}

func (Response_Status) Type() protoreflect.EnumType {
	return &file_fs_func_grpc_func_proto_grpc_func_proto_enumTypes[0]
}

func (x Response_Status) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Response_Status.Descriptor instead.
func (Response_Status) EnumDescriptor() ([]byte, []int) {
	return file_fs_func_grpc_func_proto_grpc_func_proto_rawDescGZIP(), []int{1, 0}
}

type FunctionStatus_Status int32

const (
	FunctionStatus_CREATING FunctionStatus_Status = 0
	FunctionStatus_RUNNING  FunctionStatus_Status = 1
	FunctionStatus_DELETING FunctionStatus_Status = 2
	FunctionStatus_DELETED  FunctionStatus_Status = 3
	FunctionStatus_FAILED   FunctionStatus_Status = 4
)

// Enum value maps for FunctionStatus_Status.
var (
	FunctionStatus_Status_name = map[int32]string{
		0: "CREATING",
		1: "RUNNING",
		2: "DELETING",
		3: "DELETED",
		4: "FAILED",
	}
	FunctionStatus_Status_value = map[string]int32{
		"CREATING": 0,
		"RUNNING":  1,
		"DELETING": 2,
		"DELETED":  3,
		"FAILED":   4,
	}
)

func (x FunctionStatus_Status) Enum() *FunctionStatus_Status {
	p := new(FunctionStatus_Status)
	*p = x
	return p
}

func (x FunctionStatus_Status) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (FunctionStatus_Status) Descriptor() protoreflect.EnumDescriptor {
	return file_fs_func_grpc_func_proto_grpc_func_proto_enumTypes[1].Descriptor()
}

func (FunctionStatus_Status) Type() protoreflect.EnumType {
	return &file_fs_func_grpc_func_proto_grpc_func_proto_enumTypes[1]
}

func (x FunctionStatus_Status) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use FunctionStatus_Status.Descriptor instead.
func (FunctionStatus_Status) EnumDescriptor() ([]byte, []int) {
	return file_fs_func_grpc_func_proto_grpc_func_proto_rawDescGZIP(), []int{5, 0}
}

// The request message for the Process method.
type Event struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Payload string `protobuf:"bytes,1,opt,name=payload,proto3" json:"payload,omitempty"`
}

func (x *Event) Reset() {
	*x = Event{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Event) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Event) ProtoMessage() {}

func (x *Event) ProtoReflect() protoreflect.Message {
	mi := &file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Event.ProtoReflect.Descriptor instead.
func (*Event) Descriptor() ([]byte, []int) {
	return file_fs_func_grpc_func_proto_grpc_func_proto_rawDescGZIP(), []int{0}
}

func (x *Event) GetPayload() string {
	if x != nil {
		return x.Payload
	}
	return ""
}

type Response struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Status  Response_Status `protobuf:"varint,1,opt,name=status,proto3,enum=fs_func.Response_Status" json:"status,omitempty"`
	Message *string         `protobuf:"bytes,2,opt,name=message,proto3,oneof" json:"message,omitempty"`
}

func (x *Response) Reset() {
	*x = Response{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Response) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Response) ProtoMessage() {}

func (x *Response) ProtoReflect() protoreflect.Message {
	mi := &file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Response.ProtoReflect.Descriptor instead.
func (*Response) Descriptor() ([]byte, []int) {
	return file_fs_func_grpc_func_proto_grpc_func_proto_rawDescGZIP(), []int{1}
}

func (x *Response) GetStatus() Response_Status {
	if x != nil {
		return x.Status
	}
	return Response_OK
}

func (x *Response) GetMessage() string {
	if x != nil && x.Message != nil {
		return *x.Message
	}
	return ""
}

// The request message for the Output method.
type OutputRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Event string `protobuf:"bytes,1,opt,name=event,proto3" json:"event,omitempty"`
}

func (x *OutputRequest) Reset() {
	*x = OutputRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OutputRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OutputRequest) ProtoMessage() {}

func (x *OutputRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OutputRequest.ProtoReflect.Descriptor instead.
func (*OutputRequest) Descriptor() ([]byte, []int) {
	return file_fs_func_grpc_func_proto_grpc_func_proto_rawDescGZIP(), []int{2}
}

func (x *OutputRequest) GetEvent() string {
	if x != nil {
		return x.Event
	}
	return ""
}

// The request message for the SetState method.
type SetStateRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value string `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *SetStateRequest) Reset() {
	*x = SetStateRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SetStateRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetStateRequest) ProtoMessage() {}

func (x *SetStateRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetStateRequest.ProtoReflect.Descriptor instead.
func (*SetStateRequest) Descriptor() ([]byte, []int) {
	return file_fs_func_grpc_func_proto_grpc_func_proto_rawDescGZIP(), []int{3}
}

func (x *SetStateRequest) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *SetStateRequest) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

type CreateFunctionRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (x *CreateFunctionRequest) Reset() {
	*x = CreateFunctionRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CreateFunctionRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateFunctionRequest) ProtoMessage() {}

func (x *CreateFunctionRequest) ProtoReflect() protoreflect.Message {
	mi := &file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateFunctionRequest.ProtoReflect.Descriptor instead.
func (*CreateFunctionRequest) Descriptor() ([]byte, []int) {
	return file_fs_func_grpc_func_proto_grpc_func_proto_rawDescGZIP(), []int{4}
}

func (x *CreateFunctionRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

type FunctionStatus struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name    string                `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Status  FunctionStatus_Status `protobuf:"varint,2,opt,name=status,proto3,enum=fs_func.FunctionStatus_Status" json:"status,omitempty"`
	Details *string               `protobuf:"bytes,3,opt,name=details,proto3,oneof" json:"details,omitempty"`
}

func (x *FunctionStatus) Reset() {
	*x = FunctionStatus{}
	if protoimpl.UnsafeEnabled {
		mi := &file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FunctionStatus) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FunctionStatus) ProtoMessage() {}

func (x *FunctionStatus) ProtoReflect() protoreflect.Message {
	mi := &file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FunctionStatus.ProtoReflect.Descriptor instead.
func (*FunctionStatus) Descriptor() ([]byte, []int) {
	return file_fs_func_grpc_func_proto_grpc_func_proto_rawDescGZIP(), []int{5}
}

func (x *FunctionStatus) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *FunctionStatus) GetStatus() FunctionStatus_Status {
	if x != nil {
		return x.Status
	}
	return FunctionStatus_CREATING
}

func (x *FunctionStatus) GetDetails() string {
	if x != nil && x.Details != nil {
		return *x.Details
	}
	return ""
}

var File_fs_func_grpc_func_proto_grpc_func_proto protoreflect.FileDescriptor

var file_fs_func_grpc_func_proto_grpc_func_proto_rawDesc = []byte{
	0x0a, 0x27, 0x66, 0x73, 0x2f, 0x66, 0x75, 0x6e, 0x63, 0x2f, 0x67, 0x72, 0x70, 0x63, 0x5f, 0x66,
	0x75, 0x6e, 0x63, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x67, 0x72, 0x70, 0x63, 0x5f, 0x66,
	0x75, 0x6e, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x07, 0x66, 0x73, 0x5f, 0x66, 0x75,
	0x6e, 0x63, 0x22, 0x21, 0x0a, 0x05, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x12, 0x18, 0x0a, 0x07, 0x70,
	0x61, 0x79, 0x6c, 0x6f, 0x61, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x70, 0x61,
	0x79, 0x6c, 0x6f, 0x61, 0x64, 0x22, 0x84, 0x01, 0x0a, 0x08, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x30, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0e, 0x32, 0x18, 0x2e, 0x66, 0x73, 0x5f, 0x66, 0x75, 0x6e, 0x63, 0x2e, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x06, 0x73, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x12, 0x1d, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x88, 0x01, 0x01, 0x22, 0x1b, 0x0a, 0x06, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x06, 0x0a,
	0x02, 0x4f, 0x4b, 0x10, 0x00, 0x12, 0x09, 0x0a, 0x05, 0x45, 0x52, 0x52, 0x4f, 0x52, 0x10, 0x01,
	0x42, 0x0a, 0x0a, 0x08, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x25, 0x0a, 0x0d,
	0x4f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a,
	0x05, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x65, 0x76,
	0x65, 0x6e, 0x74, 0x22, 0x39, 0x0a, 0x0f, 0x53, 0x65, 0x74, 0x53, 0x74, 0x61, 0x74, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x2b,
	0x0a, 0x15, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x22, 0xd3, 0x01, 0x0a, 0x0e,
	0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x12,
	0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61,
	0x6d, 0x65, 0x12, 0x36, 0x0a, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0e, 0x32, 0x1e, 0x2e, 0x66, 0x73, 0x5f, 0x66, 0x75, 0x6e, 0x63, 0x2e, 0x46, 0x75, 0x6e,
	0x63, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x2e, 0x53, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x1d, 0x0a, 0x07, 0x64, 0x65,
	0x74, 0x61, 0x69, 0x6c, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x07, 0x64,
	0x65, 0x74, 0x61, 0x69, 0x6c, 0x73, 0x88, 0x01, 0x01, 0x22, 0x4a, 0x0a, 0x06, 0x53, 0x74, 0x61,
	0x74, 0x75, 0x73, 0x12, 0x0c, 0x0a, 0x08, 0x43, 0x52, 0x45, 0x41, 0x54, 0x49, 0x4e, 0x47, 0x10,
	0x00, 0x12, 0x0b, 0x0a, 0x07, 0x52, 0x55, 0x4e, 0x4e, 0x49, 0x4e, 0x47, 0x10, 0x01, 0x12, 0x0c,
	0x0a, 0x08, 0x44, 0x45, 0x4c, 0x45, 0x54, 0x49, 0x4e, 0x47, 0x10, 0x02, 0x12, 0x0b, 0x0a, 0x07,
	0x44, 0x45, 0x4c, 0x45, 0x54, 0x45, 0x44, 0x10, 0x03, 0x12, 0x0a, 0x0a, 0x06, 0x46, 0x41, 0x49,
	0x4c, 0x45, 0x44, 0x10, 0x04, 0x42, 0x0a, 0x0a, 0x08, 0x5f, 0x64, 0x65, 0x74, 0x61, 0x69, 0x6c,
	0x73, 0x32, 0x52, 0x0a, 0x0b, 0x46, 0x53, 0x52, 0x65, 0x63, 0x6f, 0x6e, 0x63, 0x69, 0x6c, 0x65,
	0x12, 0x43, 0x0a, 0x09, 0x52, 0x65, 0x63, 0x6f, 0x6e, 0x63, 0x69, 0x6c, 0x65, 0x12, 0x17, 0x2e,
	0x66, 0x73, 0x5f, 0x66, 0x75, 0x6e, 0x63, 0x2e, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e,
	0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x1a, 0x17, 0x2e, 0x66, 0x73, 0x5f, 0x66, 0x75, 0x6e, 0x63,
	0x2e, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x22,
	0x00, 0x28, 0x01, 0x30, 0x01, 0x32, 0x76, 0x0a, 0x08, 0x46, 0x75, 0x6e, 0x63, 0x74, 0x69, 0x6f,
	0x6e, 0x12, 0x2f, 0x0a, 0x07, 0x50, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x12, 0x0e, 0x2e, 0x66,
	0x73, 0x5f, 0x66, 0x75, 0x6e, 0x63, 0x2e, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x1a, 0x0e, 0x2e, 0x66,
	0x73, 0x5f, 0x66, 0x75, 0x6e, 0x63, 0x2e, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x22, 0x00, 0x28, 0x01,
	0x30, 0x01, 0x12, 0x39, 0x0a, 0x08, 0x53, 0x65, 0x74, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x18,
	0x2e, 0x66, 0x73, 0x5f, 0x66, 0x75, 0x6e, 0x63, 0x2e, 0x53, 0x65, 0x74, 0x53, 0x74, 0x61, 0x74,
	0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x11, 0x2e, 0x66, 0x73, 0x5f, 0x66, 0x75,
	0x6e, 0x63, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x19, 0x5a,
	0x17, 0x66, 0x73, 0x2f, 0x66, 0x75, 0x6e, 0x63, 0x2f, 0x67, 0x72, 0x70, 0x63, 0x5f, 0x66, 0x75,
	0x6e, 0x63, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_fs_func_grpc_func_proto_grpc_func_proto_rawDescOnce sync.Once
	file_fs_func_grpc_func_proto_grpc_func_proto_rawDescData = file_fs_func_grpc_func_proto_grpc_func_proto_rawDesc
)

func file_fs_func_grpc_func_proto_grpc_func_proto_rawDescGZIP() []byte {
	file_fs_func_grpc_func_proto_grpc_func_proto_rawDescOnce.Do(func() {
		file_fs_func_grpc_func_proto_grpc_func_proto_rawDescData = protoimpl.X.CompressGZIP(file_fs_func_grpc_func_proto_grpc_func_proto_rawDescData)
	})
	return file_fs_func_grpc_func_proto_grpc_func_proto_rawDescData
}

var file_fs_func_grpc_func_proto_grpc_func_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_fs_func_grpc_func_proto_grpc_func_proto_goTypes = []interface{}{
	(Response_Status)(0),          // 0: fs_func.Response.Status
	(FunctionStatus_Status)(0),    // 1: fs_func.FunctionStatus.Status
	(*Event)(nil),                 // 2: fs_func.Event
	(*Response)(nil),              // 3: fs_func.Response
	(*OutputRequest)(nil),         // 4: fs_func.OutputRequest
	(*SetStateRequest)(nil),       // 5: fs_func.SetStateRequest
	(*CreateFunctionRequest)(nil), // 6: fs_func.CreateFunctionRequest
	(*FunctionStatus)(nil),        // 7: fs_func.FunctionStatus
}
var file_fs_func_grpc_func_proto_grpc_func_proto_depIdxs = []int32{
	0, // 0: fs_func.Response.status:type_name -> fs_func.Response.Status
	1, // 1: fs_func.FunctionStatus.status:type_name -> fs_func.FunctionStatus.Status
	7, // 2: fs_func.FSReconcile.Reconcile:input_type -> fs_func.FunctionStatus
	2, // 3: fs_func.Function.Process:input_type -> fs_func.Event
	5, // 4: fs_func.Function.SetState:input_type -> fs_func.SetStateRequest
	7, // 5: fs_func.FSReconcile.Reconcile:output_type -> fs_func.FunctionStatus
	2, // 6: fs_func.Function.Process:output_type -> fs_func.Event
	3, // 7: fs_func.Function.SetState:output_type -> fs_func.Response
	5, // [5:8] is the sub-list for method output_type
	2, // [2:5] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_fs_func_grpc_func_proto_grpc_func_proto_init() }
func file_fs_func_grpc_func_proto_grpc_func_proto_init() {
	if File_fs_func_grpc_func_proto_grpc_func_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Event); i {
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
		file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Response); i {
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
		file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OutputRequest); i {
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
		file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SetStateRequest); i {
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
		file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CreateFunctionRequest); i {
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
		file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FunctionStatus); i {
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
	file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[1].OneofWrappers = []interface{}{}
	file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes[5].OneofWrappers = []interface{}{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_fs_func_grpc_func_proto_grpc_func_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   2,
		},
		GoTypes:           file_fs_func_grpc_func_proto_grpc_func_proto_goTypes,
		DependencyIndexes: file_fs_func_grpc_func_proto_grpc_func_proto_depIdxs,
		EnumInfos:         file_fs_func_grpc_func_proto_grpc_func_proto_enumTypes,
		MessageInfos:      file_fs_func_grpc_func_proto_grpc_func_proto_msgTypes,
	}.Build()
	File_fs_func_grpc_func_proto_grpc_func_proto = out.File
	file_fs_func_grpc_func_proto_grpc_func_proto_rawDesc = nil
	file_fs_func_grpc_func_proto_grpc_func_proto_goTypes = nil
	file_fs_func_grpc_func_proto_grpc_func_proto_depIdxs = nil
}
