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

// GetProfileBoolValue returns a bool and true if the value is found.
func GetProfileBoolValue(profile *structpb.Struct, k string) (bool, bool) {
	if profile == nil {
		return false, false
	}

	v, ok := profile.Fields[k]
	if !ok {
		return false, false
	}

	s, ok := v.Kind.(*structpb.Value_BoolValue)
	if !ok {
		return false, false
	}

	return s.BoolValue, true
}

// GetProfileListValue returns a list of pointers to structpb.Value and true if the value is found.
func GetProfileListValue(profile *structpb.Struct, k string) ([]*structpb.Value, bool) {
	if profile == nil {
		return nil, false
	}

	v, ok := profile.Fields[k]
	if !ok {
		return nil, false
	}

	s, ok := v.Kind.(*structpb.Value_ListValue)
	if !ok {
		return nil, false
	}

	return s.ListValue.Values, true
}

// GetProfileStructValue returns a pointer to structpb.Struct and true if the value is found.
func GetProfileStructValue(profile *structpb.Struct, k string) (*structpb.Struct, bool) {
	if profile == nil {
		return nil, false
	}

	v, ok := profile.Fields[k]
	if !ok {
		return nil, false
	}

	s, ok := v.Kind.(*structpb.Value_StructValue)
	if !ok {
		return nil, false
	}

	return s.StructValue, true
}
