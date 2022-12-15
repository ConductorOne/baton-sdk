package resource

import (
	"google.golang.org/protobuf/types/known/structpb"
)

// GetProfileStringValue returns a string and true if the value is found.
func GetProfileStringValue(profile *structpb.Struct, k string) (string, bool) {
	if profile == nil {
		return "", false
	}

	v, ok := profile.Fields[k]
	if !ok {
		return "", false
	}

	s, ok := v.Kind.(*structpb.Value_StringValue)
	if !ok {
		return "", false
	}

	return s.StringValue, true
}

// GetProfileInt64Value returns an int64 and true if the value is found.
func GetProfileInt64Value(profile *structpb.Struct, k string) (int64, bool) {
	if profile == nil {
		return 0, false
	}

	v, ok := profile.Fields[k]
	if !ok {
		return 0, false
	}

	s, ok := v.Kind.(*structpb.Value_NumberValue)
	if !ok {
		return 0, false
	}

	return int64(s.NumberValue), true
}
