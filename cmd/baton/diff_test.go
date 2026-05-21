package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// buildResource constructs a resource roughly representative of what the
// diff hot path sees: an id, display name, description, and an annotation
// wrapped in a *anypb.Any. The annotation payload exercises the path that
// previously forced the protojson round-trip.
func buildResource(b *testing.B, id, displayName string) *v2.Resource {
	b.Helper()
	anno, err := anypb.New(structpb.NewStringValue("annotation-payload-" + id))
	require.NoError(b, err)
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: "user",
			Resource:     id,
		}.Build(),
		DisplayName: displayName,
		Description: "desc for " + id,
		Annotations: []*anypb.Any{anno},
	}.Build()
}

func buildResourceForTest(t *testing.T, id, displayName string) *v2.Resource {
	t.Helper()
	anno, err := anypb.New(structpb.NewStringValue("annotation-payload-" + id))
	require.NoError(t, err)
	return v2.Resource_builder{
		Id: v2.ResourceId_builder{
			ResourceType: "user",
			Resource:     id,
		}.Build(),
		DisplayName: displayName,
		Description: "desc for " + id,
		Annotations: []*anypb.Any{anno},
	}.Build()
}

// TestCompareProtoEqual asserts that compareProto reports true for two
// semantically identical resources (including an annotation Any payload).
// This is the correctness anchor for the proto.Equal switch: if proto.Equal
// ever disagreed with the prior json-roundtrip approach on equal payloads,
// this test would fail.
func TestCompareProtoEqual(t *testing.T) {
	ctx := context.Background()

	a := buildResourceForTest(t, "u-1", "Alice")
	b := buildResourceForTest(t, "u-1", "Alice")

	equal, err := compareProto(ctx, a, b)
	require.NoError(t, err)
	require.True(t, equal, "identical resources should compare equal")
}

func TestCompareProtoNotEqual(t *testing.T) {
	ctx := context.Background()

	a := buildResourceForTest(t, "u-1", "Alice")
	b := buildResourceForTest(t, "u-1", "Alice (modified)")

	equal, err := compareProto(ctx, a, b)
	require.NoError(t, err)
	require.False(t, equal, "resources with different display names should not compare equal")
}

// BenchmarkCompareProto exercises compareProto on a representative resource
// pair. Used to quantify the proto.Equal switch (rec #2). Run with:
//
//	go test -run='^$' -bench=BenchmarkCompareProto -benchtime=2x -count=3 \
//	    github.com/conductorone/baton-sdk/cmd/baton
func BenchmarkCompareProto(b *testing.B) {
	ctx := context.Background()
	oldR := buildResource(b, "u-1", "Alice")
	newR := buildResource(b, "u-1", "Alice")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		equal, err := compareProto(ctx, oldR, newR)
		if err != nil {
			b.Fatal(err)
		}
		if !equal {
			b.Fatal("expected equal")
		}
	}
}
