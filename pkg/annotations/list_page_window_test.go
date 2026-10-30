package annotations

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestAnnotations_WithListPageWindow(t *testing.T) {
	for _, listKind := range []string{"resources", "entitlements", "grants"} {
		t.Run(listKind, func(t *testing.T) {
			report := v2.ListPageWindow_builder{
				ListKind:             listKind,
				ResourceTypeId:       "group",
				ParentResourceId:     "shared-id",
				PagesReturned:        3,
				Window:               3,
				ParentResourceTypeId: "organization",
			}.Build()
			annos := New(&v2.RateLimitWaitReport{})
			require.Same(t, &annos, annos.WithListPageWindow(report))
			require.Len(t, annos, 2)
			require.Equal(t, "type.googleapis.com/c1.connector.v2.ListPageWindow", annos[1].GetTypeUrl())

			response := v2.ResourcesServiceListResourcesResponse_builder{Annotations: annos}.Build()
			data, err := proto.Marshal(response)
			require.NoError(t, err)
			decoded := &v2.ResourcesServiceListResourcesResponse{}
			require.NoError(t, proto.Unmarshal(data, decoded))
			picked, err := GetListPageWindowFromAnnotations(decoded.GetAnnotations())
			require.NoError(t, err)
			require.True(t, proto.Equal(report, picked))

			replacement := proto.Clone(report).(*v2.ListPageWindow)
			replacement.SetParentResourceTypeId("workspace")
			annos.WithListPageWindow(replacement)
			require.Len(t, annos, 2)
			require.True(t, annos.Contains(&v2.RateLimitWaitReport{}))
			picked, err = GetListPageWindowFromAnnotations(annos)
			require.NoError(t, err)
			require.True(t, proto.Equal(replacement, picked))
			require.NotEqual(t, report.GetParentResourceTypeId(), picked.GetParentResourceTypeId())
		})
	}
}

func TestAnnotations_ListPageWindowAbsent(t *testing.T) {
	for _, annos := range []Annotations{nil, {}, New(&v2.RateLimitWaitReport{})} {
		report, err := GetListPageWindowFromAnnotations(annos)
		require.NoError(t, err)
		require.Nil(t, report)
		before := len(annos)
		require.Same(t, &annos, annos.WithListPageWindow(nil))
		require.Len(t, annos, before)
	}
}

func TestAnnotations_ListPageWindowMalformed(t *testing.T) {
	annos := Annotations{&anypb.Any{
		TypeUrl: "type.googleapis.com/c1.connector.v2.ListPageWindow",
		Value:   []byte{0xff},
	}}
	report, err := GetListPageWindowFromAnnotations(annos)
	require.Error(t, err)
	require.Nil(t, report)
}

func TestListPageWindowDescriptor(t *testing.T) {
	descriptor := (&v2.ListPageWindow{}).ProtoReflect().Descriptor()
	require.Equal(t, protoreflect.FullName("c1.connector.v2.ListPageWindow"), descriptor.FullName())
	fields := []struct {
		name protoreflect.Name
		kind protoreflect.Kind
	}{
		{"list_kind", protoreflect.StringKind},
		{"resource_type_id", protoreflect.StringKind},
		{"parent_resource_id", protoreflect.StringKind},
		{"pages_returned", protoreflect.Uint32Kind},
		{"window", protoreflect.Uint32Kind},
		{"parent_resource_type_id", protoreflect.StringKind},
	}
	require.Equal(t, len(fields), descriptor.Fields().Len())
	for i, expected := range fields {
		field := descriptor.Fields().Get(i)
		require.EqualValues(t, i+1, field.Number())
		require.Equal(t, expected.name, field.Name())
		require.Equal(t, expected.kind, field.Kind())
		require.Equal(t, protoreflect.Optional, field.Cardinality())
	}
}
