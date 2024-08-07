package test

import (
	"slices"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/types/known/anypb"
)

func isRatelimitingAnnotation(annotation *anypb.Any) bool {
	var ratelimitDescription v2.RateLimitDescription
	err := annotation.UnmarshalTo(&ratelimitDescription)
	if err != nil {
		// Some other kind of annotation.
		return false
	}
	return slices.Contains(
		[]v2.RateLimitDescription_Status{
			v2.RateLimitDescription_STATUS_ERROR,
			v2.RateLimitDescription_STATUS_OVERLIMIT,
		},
		ratelimitDescription.Status,
	)
}

func IsRatelimited(actualAnnotations annotations.Annotations) bool {
	for _, annotation := range actualAnnotations {
		if isRatelimitingAnnotation(annotation) {
			return true
		}
	}
	return false
}

func AssertWasRatelimited(
	t *testing.T,
	actualAnnotations annotations.Annotations,
) {
	if !IsRatelimited(actualAnnotations) {
		t.Fatal("request was _not_ ratelimited, expected to be ratelimited")
	}
}

// AssertNoRatelimitAnnotations - the annotations that ResourceSyncers return
// can contain "informational" ratelimit annotations that let the caller know
// how much quota they have left and that their request was _not_ rejected. This
// helper just asserts that there are no "your request was ratelimited"
// annotations.
func AssertNoRatelimitAnnotations(
	t *testing.T,
	actualAnnotations annotations.Annotations,
) {
	if IsRatelimited(actualAnnotations) {
		t.Fatal("request was ratelimited, expected not to be ratelimited")
	}
}
