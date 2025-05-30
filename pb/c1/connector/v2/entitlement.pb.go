// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.4
// 	protoc        (unknown)
// source: c1/connector/v2/entitlement.proto

package v2

import (
	_ "github.com/envoyproxy/protoc-gen-validate/validate"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	anypb "google.golang.org/protobuf/types/known/anypb"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Entitlement_PurposeValue int32

const (
	Entitlement_PURPOSE_VALUE_UNSPECIFIED Entitlement_PurposeValue = 0
	Entitlement_PURPOSE_VALUE_ASSIGNMENT  Entitlement_PurposeValue = 1
	Entitlement_PURPOSE_VALUE_PERMISSION  Entitlement_PurposeValue = 2
)

// Enum value maps for Entitlement_PurposeValue.
var (
	Entitlement_PurposeValue_name = map[int32]string{
		0: "PURPOSE_VALUE_UNSPECIFIED",
		1: "PURPOSE_VALUE_ASSIGNMENT",
		2: "PURPOSE_VALUE_PERMISSION",
	}
	Entitlement_PurposeValue_value = map[string]int32{
		"PURPOSE_VALUE_UNSPECIFIED": 0,
		"PURPOSE_VALUE_ASSIGNMENT":  1,
		"PURPOSE_VALUE_PERMISSION":  2,
	}
)

func (x Entitlement_PurposeValue) Enum() *Entitlement_PurposeValue {
	p := new(Entitlement_PurposeValue)
	*p = x
	return p
}

func (x Entitlement_PurposeValue) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Entitlement_PurposeValue) Descriptor() protoreflect.EnumDescriptor {
	return file_c1_connector_v2_entitlement_proto_enumTypes[0].Descriptor()
}

func (Entitlement_PurposeValue) Type() protoreflect.EnumType {
	return &file_c1_connector_v2_entitlement_proto_enumTypes[0]
}

func (x Entitlement_PurposeValue) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Entitlement_PurposeValue.Descriptor instead.
func (Entitlement_PurposeValue) EnumDescriptor() ([]byte, []int) {
	return file_c1_connector_v2_entitlement_proto_rawDescGZIP(), []int{0, 0}
}

type Entitlement struct {
	state         protoimpl.MessageState   `protogen:"open.v1"`
	Resource      *Resource                `protobuf:"bytes,1,opt,name=resource,proto3" json:"resource,omitempty"`
	Id            string                   `protobuf:"bytes,2,opt,name=id,proto3" json:"id,omitempty"`
	DisplayName   string                   `protobuf:"bytes,3,opt,name=display_name,json=displayName,proto3" json:"display_name,omitempty"`
	Description   string                   `protobuf:"bytes,4,opt,name=description,proto3" json:"description,omitempty"`
	GrantableTo   []*ResourceType          `protobuf:"bytes,5,rep,name=grantable_to,json=grantableTo,proto3" json:"grantable_to,omitempty"`
	Annotations   []*anypb.Any             `protobuf:"bytes,6,rep,name=annotations,proto3" json:"annotations,omitempty"`
	Purpose       Entitlement_PurposeValue `protobuf:"varint,7,opt,name=purpose,proto3,enum=c1.connector.v2.Entitlement_PurposeValue" json:"purpose,omitempty"`
	Slug          string                   `protobuf:"bytes,8,opt,name=slug,proto3" json:"slug,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *Entitlement) Reset() {
	*x = Entitlement{}
	mi := &file_c1_connector_v2_entitlement_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *Entitlement) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Entitlement) ProtoMessage() {}

func (x *Entitlement) ProtoReflect() protoreflect.Message {
	mi := &file_c1_connector_v2_entitlement_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Entitlement.ProtoReflect.Descriptor instead.
func (*Entitlement) Descriptor() ([]byte, []int) {
	return file_c1_connector_v2_entitlement_proto_rawDescGZIP(), []int{0}
}

func (x *Entitlement) GetResource() *Resource {
	if x != nil {
		return x.Resource
	}
	return nil
}

func (x *Entitlement) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Entitlement) GetDisplayName() string {
	if x != nil {
		return x.DisplayName
	}
	return ""
}

func (x *Entitlement) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *Entitlement) GetGrantableTo() []*ResourceType {
	if x != nil {
		return x.GrantableTo
	}
	return nil
}

func (x *Entitlement) GetAnnotations() []*anypb.Any {
	if x != nil {
		return x.Annotations
	}
	return nil
}

func (x *Entitlement) GetPurpose() Entitlement_PurposeValue {
	if x != nil {
		return x.Purpose
	}
	return Entitlement_PURPOSE_VALUE_UNSPECIFIED
}

func (x *Entitlement) GetSlug() string {
	if x != nil {
		return x.Slug
	}
	return ""
}

type EntitlementsServiceListEntitlementsRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	Resource      *Resource              `protobuf:"bytes,1,opt,name=resource,proto3" json:"resource,omitempty"`
	PageSize      uint32                 `protobuf:"varint,2,opt,name=page_size,json=pageSize,proto3" json:"page_size,omitempty"`
	PageToken     string                 `protobuf:"bytes,3,opt,name=page_token,json=pageToken,proto3" json:"page_token,omitempty"`
	Annotations   []*anypb.Any           `protobuf:"bytes,4,rep,name=annotations,proto3" json:"annotations,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *EntitlementsServiceListEntitlementsRequest) Reset() {
	*x = EntitlementsServiceListEntitlementsRequest{}
	mi := &file_c1_connector_v2_entitlement_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *EntitlementsServiceListEntitlementsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EntitlementsServiceListEntitlementsRequest) ProtoMessage() {}

func (x *EntitlementsServiceListEntitlementsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_c1_connector_v2_entitlement_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EntitlementsServiceListEntitlementsRequest.ProtoReflect.Descriptor instead.
func (*EntitlementsServiceListEntitlementsRequest) Descriptor() ([]byte, []int) {
	return file_c1_connector_v2_entitlement_proto_rawDescGZIP(), []int{1}
}

func (x *EntitlementsServiceListEntitlementsRequest) GetResource() *Resource {
	if x != nil {
		return x.Resource
	}
	return nil
}

func (x *EntitlementsServiceListEntitlementsRequest) GetPageSize() uint32 {
	if x != nil {
		return x.PageSize
	}
	return 0
}

func (x *EntitlementsServiceListEntitlementsRequest) GetPageToken() string {
	if x != nil {
		return x.PageToken
	}
	return ""
}

func (x *EntitlementsServiceListEntitlementsRequest) GetAnnotations() []*anypb.Any {
	if x != nil {
		return x.Annotations
	}
	return nil
}

type EntitlementsServiceListEntitlementsResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	List          []*Entitlement         `protobuf:"bytes,1,rep,name=list,proto3" json:"list,omitempty"`
	NextPageToken string                 `protobuf:"bytes,2,opt,name=next_page_token,json=nextPageToken,proto3" json:"next_page_token,omitempty"`
	Annotations   []*anypb.Any           `protobuf:"bytes,3,rep,name=annotations,proto3" json:"annotations,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *EntitlementsServiceListEntitlementsResponse) Reset() {
	*x = EntitlementsServiceListEntitlementsResponse{}
	mi := &file_c1_connector_v2_entitlement_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *EntitlementsServiceListEntitlementsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*EntitlementsServiceListEntitlementsResponse) ProtoMessage() {}

func (x *EntitlementsServiceListEntitlementsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_c1_connector_v2_entitlement_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use EntitlementsServiceListEntitlementsResponse.ProtoReflect.Descriptor instead.
func (*EntitlementsServiceListEntitlementsResponse) Descriptor() ([]byte, []int) {
	return file_c1_connector_v2_entitlement_proto_rawDescGZIP(), []int{2}
}

func (x *EntitlementsServiceListEntitlementsResponse) GetList() []*Entitlement {
	if x != nil {
		return x.List
	}
	return nil
}

func (x *EntitlementsServiceListEntitlementsResponse) GetNextPageToken() string {
	if x != nil {
		return x.NextPageToken
	}
	return ""
}

func (x *EntitlementsServiceListEntitlementsResponse) GetAnnotations() []*anypb.Any {
	if x != nil {
		return x.Annotations
	}
	return nil
}

var File_c1_connector_v2_entitlement_proto protoreflect.FileDescriptor

var file_c1_connector_v2_entitlement_proto_rawDesc = string([]byte{
	0x0a, 0x21, 0x63, 0x31, 0x2f, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2f, 0x76,
	0x32, 0x2f, 0x65, 0x6e, 0x74, 0x69, 0x74, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x0f, 0x63, 0x31, 0x2e, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x6f,
	0x72, 0x2e, 0x76, 0x32, 0x1a, 0x1e, 0x63, 0x31, 0x2f, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74,
	0x6f, 0x72, 0x2f, 0x76, 0x32, 0x2f, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x19, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x61, 0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x17, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x65, 0x2f, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61,
	0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x95, 0x04, 0x0a, 0x0b, 0x45, 0x6e, 0x74,
	0x69, 0x74, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x12, 0x3f, 0x0a, 0x08, 0x72, 0x65, 0x73, 0x6f,
	0x75, 0x72, 0x63, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x63, 0x31, 0x2e,
	0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x76, 0x32, 0x2e, 0x52, 0x65, 0x73,
	0x6f, 0x75, 0x72, 0x63, 0x65, 0x42, 0x08, 0xfa, 0x42, 0x05, 0x8a, 0x01, 0x02, 0x10, 0x01, 0x52,
	0x08, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x12, 0x1a, 0x0a, 0x02, 0x69, 0x64, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x42, 0x0a, 0xfa, 0x42, 0x07, 0x72, 0x05, 0x20, 0x01, 0x28, 0x80,
	0x08, 0x52, 0x02, 0x69, 0x64, 0x12, 0x30, 0x0a, 0x0c, 0x64, 0x69, 0x73, 0x70, 0x6c, 0x61, 0x79,
	0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x42, 0x0d, 0xfa, 0x42, 0x0a,
	0x72, 0x08, 0x20, 0x01, 0x28, 0x80, 0x08, 0xd0, 0x01, 0x01, 0x52, 0x0b, 0x64, 0x69, 0x73, 0x70,
	0x6c, 0x61, 0x79, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x2f, 0x0a, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72,
	0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x42, 0x0d, 0xfa, 0x42,
	0x0a, 0x72, 0x08, 0x20, 0x01, 0x28, 0x80, 0x10, 0xd0, 0x01, 0x01, 0x52, 0x0b, 0x64, 0x65, 0x73,
	0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x40, 0x0a, 0x0c, 0x67, 0x72, 0x61, 0x6e,
	0x74, 0x61, 0x62, 0x6c, 0x65, 0x5f, 0x74, 0x6f, 0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1d,
	0x2e, 0x63, 0x31, 0x2e, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x2e, 0x76, 0x32,
	0x2e, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x54, 0x79, 0x70, 0x65, 0x52, 0x0b, 0x67,
	0x72, 0x61, 0x6e, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x54, 0x6f, 0x12, 0x36, 0x0a, 0x0b, 0x61, 0x6e,
	0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x06, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x0b, 0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x73, 0x12, 0x4d, 0x0a, 0x07, 0x70, 0x75, 0x72, 0x70, 0x6f, 0x73, 0x65, 0x18, 0x07, 0x20,
	0x01, 0x28, 0x0e, 0x32, 0x29, 0x2e, 0x63, 0x31, 0x2e, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74,
	0x6f, 0x72, 0x2e, 0x76, 0x32, 0x2e, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x6c, 0x65, 0x6d, 0x65, 0x6e,
	0x74, 0x2e, 0x50, 0x75, 0x72, 0x70, 0x6f, 0x73, 0x65, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x42, 0x08,
	0xfa, 0x42, 0x05, 0x82, 0x01, 0x02, 0x10, 0x01, 0x52, 0x07, 0x70, 0x75, 0x72, 0x70, 0x6f, 0x73,
	0x65, 0x12, 0x12, 0x0a, 0x04, 0x73, 0x6c, 0x75, 0x67, 0x18, 0x08, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x04, 0x73, 0x6c, 0x75, 0x67, 0x22, 0x69, 0x0a, 0x0c, 0x50, 0x75, 0x72, 0x70, 0x6f, 0x73, 0x65,
	0x56, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x1d, 0x0a, 0x19, 0x50, 0x55, 0x52, 0x50, 0x4f, 0x53, 0x45,
	0x5f, 0x56, 0x41, 0x4c, 0x55, 0x45, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49,
	0x45, 0x44, 0x10, 0x00, 0x12, 0x1c, 0x0a, 0x18, 0x50, 0x55, 0x52, 0x50, 0x4f, 0x53, 0x45, 0x5f,
	0x56, 0x41, 0x4c, 0x55, 0x45, 0x5f, 0x41, 0x53, 0x53, 0x49, 0x47, 0x4e, 0x4d, 0x45, 0x4e, 0x54,
	0x10, 0x01, 0x12, 0x1c, 0x0a, 0x18, 0x50, 0x55, 0x52, 0x50, 0x4f, 0x53, 0x45, 0x5f, 0x56, 0x41,
	0x4c, 0x55, 0x45, 0x5f, 0x50, 0x45, 0x52, 0x4d, 0x49, 0x53, 0x53, 0x49, 0x4f, 0x4e, 0x10, 0x02,
	0x22, 0xf3, 0x01, 0x0a, 0x2a, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74,
	0x73, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x4c, 0x69, 0x73, 0x74, 0x45, 0x6e, 0x74, 0x69,
	0x74, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x35, 0x0a, 0x08, 0x72, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x19, 0x2e, 0x63, 0x31, 0x2e, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x6f, 0x72,
	0x2e, 0x76, 0x32, 0x2e, 0x52, 0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x52, 0x08, 0x72, 0x65,
	0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x12, 0x27, 0x0a, 0x09, 0x70, 0x61, 0x67, 0x65, 0x5f, 0x73,
	0x69, 0x7a, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d, 0x42, 0x0a, 0xfa, 0x42, 0x07, 0x2a, 0x05,
	0x18, 0xfa, 0x01, 0x40, 0x01, 0x52, 0x08, 0x70, 0x61, 0x67, 0x65, 0x53, 0x69, 0x7a, 0x65, 0x12,
	0x2d, 0x0a, 0x0a, 0x70, 0x61, 0x67, 0x65, 0x5f, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x03, 0x20,
	0x01, 0x28, 0x09, 0x42, 0x0e, 0xfa, 0x42, 0x0b, 0x72, 0x09, 0x20, 0x01, 0x28, 0x80, 0x80, 0x40,
	0xd0, 0x01, 0x01, 0x52, 0x09, 0x70, 0x61, 0x67, 0x65, 0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x12, 0x36,
	0x0a, 0x0b, 0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x04, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x0b, 0x61, 0x6e, 0x6e, 0x6f, 0x74,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x22, 0xcf, 0x01, 0x0a, 0x2b, 0x45, 0x6e, 0x74, 0x69, 0x74,
	0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x4c, 0x69,
	0x73, 0x74, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x30, 0x0a, 0x04, 0x6c, 0x69, 0x73, 0x74, 0x18, 0x01,
	0x20, 0x03, 0x28, 0x0b, 0x32, 0x1c, 0x2e, 0x63, 0x31, 0x2e, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63,
	0x74, 0x6f, 0x72, 0x2e, 0x76, 0x32, 0x2e, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x6c, 0x65, 0x6d, 0x65,
	0x6e, 0x74, 0x52, 0x04, 0x6c, 0x69, 0x73, 0x74, 0x12, 0x36, 0x0a, 0x0f, 0x6e, 0x65, 0x78, 0x74,
	0x5f, 0x70, 0x61, 0x67, 0x65, 0x5f, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x42, 0x0e, 0xfa, 0x42, 0x0b, 0x72, 0x09, 0x20, 0x01, 0x28, 0x80, 0x80, 0x40, 0xd0, 0x01,
	0x01, 0x52, 0x0d, 0x6e, 0x65, 0x78, 0x74, 0x50, 0x61, 0x67, 0x65, 0x54, 0x6f, 0x6b, 0x65, 0x6e,
	0x12, 0x36, 0x0a, 0x0b, 0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18,
	0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x0b, 0x61, 0x6e, 0x6e,
	0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x32, 0xa5, 0x01, 0x0a, 0x13, 0x45, 0x6e, 0x74,
	0x69, 0x74, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65,
	0x12, 0x8d, 0x01, 0x0a, 0x10, 0x4c, 0x69, 0x73, 0x74, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x6c, 0x65,
	0x6d, 0x65, 0x6e, 0x74, 0x73, 0x12, 0x3b, 0x2e, 0x63, 0x31, 0x2e, 0x63, 0x6f, 0x6e, 0x6e, 0x65,
	0x63, 0x74, 0x6f, 0x72, 0x2e, 0x76, 0x32, 0x2e, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x6c, 0x65, 0x6d,
	0x65, 0x6e, 0x74, 0x73, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x4c, 0x69, 0x73, 0x74, 0x45,
	0x6e, 0x74, 0x69, 0x74, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x3c, 0x2e, 0x63, 0x31, 0x2e, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x6f,
	0x72, 0x2e, 0x76, 0x32, 0x2e, 0x45, 0x6e, 0x74, 0x69, 0x74, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74,
	0x73, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x4c, 0x69, 0x73, 0x74, 0x45, 0x6e, 0x74, 0x69,
	0x74, 0x6c, 0x65, 0x6d, 0x65, 0x6e, 0x74, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x42, 0x36, 0x5a, 0x34, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x63,
	0x6f, 0x6e, 0x64, 0x75, 0x63, 0x74, 0x6f, 0x72, 0x6f, 0x6e, 0x65, 0x2f, 0x62, 0x61, 0x74, 0x6f,
	0x6e, 0x2d, 0x73, 0x64, 0x6b, 0x2f, 0x70, 0x62, 0x2f, 0x63, 0x31, 0x2f, 0x63, 0x6f, 0x6e, 0x6e,
	0x65, 0x63, 0x74, 0x6f, 0x72, 0x2f, 0x76, 0x32, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
})

var (
	file_c1_connector_v2_entitlement_proto_rawDescOnce sync.Once
	file_c1_connector_v2_entitlement_proto_rawDescData []byte
)

func file_c1_connector_v2_entitlement_proto_rawDescGZIP() []byte {
	file_c1_connector_v2_entitlement_proto_rawDescOnce.Do(func() {
		file_c1_connector_v2_entitlement_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_c1_connector_v2_entitlement_proto_rawDesc), len(file_c1_connector_v2_entitlement_proto_rawDesc)))
	})
	return file_c1_connector_v2_entitlement_proto_rawDescData
}

var file_c1_connector_v2_entitlement_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_c1_connector_v2_entitlement_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_c1_connector_v2_entitlement_proto_goTypes = []any{
	(Entitlement_PurposeValue)(0),                       // 0: c1.connector.v2.Entitlement.PurposeValue
	(*Entitlement)(nil),                                 // 1: c1.connector.v2.Entitlement
	(*EntitlementsServiceListEntitlementsRequest)(nil),  // 2: c1.connector.v2.EntitlementsServiceListEntitlementsRequest
	(*EntitlementsServiceListEntitlementsResponse)(nil), // 3: c1.connector.v2.EntitlementsServiceListEntitlementsResponse
	(*Resource)(nil),                                    // 4: c1.connector.v2.Resource
	(*ResourceType)(nil),                                // 5: c1.connector.v2.ResourceType
	(*anypb.Any)(nil),                                   // 6: google.protobuf.Any
}
var file_c1_connector_v2_entitlement_proto_depIdxs = []int32{
	4, // 0: c1.connector.v2.Entitlement.resource:type_name -> c1.connector.v2.Resource
	5, // 1: c1.connector.v2.Entitlement.grantable_to:type_name -> c1.connector.v2.ResourceType
	6, // 2: c1.connector.v2.Entitlement.annotations:type_name -> google.protobuf.Any
	0, // 3: c1.connector.v2.Entitlement.purpose:type_name -> c1.connector.v2.Entitlement.PurposeValue
	4, // 4: c1.connector.v2.EntitlementsServiceListEntitlementsRequest.resource:type_name -> c1.connector.v2.Resource
	6, // 5: c1.connector.v2.EntitlementsServiceListEntitlementsRequest.annotations:type_name -> google.protobuf.Any
	1, // 6: c1.connector.v2.EntitlementsServiceListEntitlementsResponse.list:type_name -> c1.connector.v2.Entitlement
	6, // 7: c1.connector.v2.EntitlementsServiceListEntitlementsResponse.annotations:type_name -> google.protobuf.Any
	2, // 8: c1.connector.v2.EntitlementsService.ListEntitlements:input_type -> c1.connector.v2.EntitlementsServiceListEntitlementsRequest
	3, // 9: c1.connector.v2.EntitlementsService.ListEntitlements:output_type -> c1.connector.v2.EntitlementsServiceListEntitlementsResponse
	9, // [9:10] is the sub-list for method output_type
	8, // [8:9] is the sub-list for method input_type
	8, // [8:8] is the sub-list for extension type_name
	8, // [8:8] is the sub-list for extension extendee
	0, // [0:8] is the sub-list for field type_name
}

func init() { file_c1_connector_v2_entitlement_proto_init() }
func file_c1_connector_v2_entitlement_proto_init() {
	if File_c1_connector_v2_entitlement_proto != nil {
		return
	}
	file_c1_connector_v2_resource_proto_init()
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_c1_connector_v2_entitlement_proto_rawDesc), len(file_c1_connector_v2_entitlement_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_c1_connector_v2_entitlement_proto_goTypes,
		DependencyIndexes: file_c1_connector_v2_entitlement_proto_depIdxs,
		EnumInfos:         file_c1_connector_v2_entitlement_proto_enumTypes,
		MessageInfos:      file_c1_connector_v2_entitlement_proto_msgTypes,
	}.Build()
	File_c1_connector_v2_entitlement_proto = out.File
	file_c1_connector_v2_entitlement_proto_goTypes = nil
	file_c1_connector_v2_entitlement_proto_depIdxs = nil
}
