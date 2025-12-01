package actions

import (
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/types/known/structpb"
)

// GetStringArg extracts a string value from the args struct by key.
// Returns the value and true if found, empty string and false otherwise.
func GetStringArg(args *structpb.Struct, key string) (string, bool) {
	if args == nil || args.Fields == nil {
		return "", false
	}

	value, ok := args.Fields[key]
	if !ok {
		return "", false
	}

	stringValue, ok := value.GetKind().(*structpb.Value_StringValue)
	if !ok {
		return "", false
	}

	return stringValue.StringValue, true
}

// GetIntArg extracts an int64 value from the args struct by key.
// Returns the value and true if found, 0 and false otherwise.
func GetIntArg(args *structpb.Struct, key string) (int64, bool) {
	if args == nil || args.Fields == nil {
		return 0, false
	}

	value, ok := args.Fields[key]
	if !ok {
		return 0, false
	}

	numberValue, ok := value.GetKind().(*structpb.Value_NumberValue)
	if !ok {
		return 0, false
	}

	return int64(numberValue.NumberValue), true
}

// GetBoolArg extracts a bool value from the args struct by key.
// Returns the value and true if found, false and false otherwise.
func GetBoolArg(args *structpb.Struct, key string) (bool, bool) {
	if args == nil || args.Fields == nil {
		return false, false
	}

	value, ok := args.Fields[key]
	if !ok {
		return false, false
	}

	boolValue, ok := value.GetKind().(*structpb.Value_BoolValue)
	if !ok {
		return false, false
	}

	return boolValue.BoolValue, true
}

// GetResourceIDArg extracts a ResourceId from the args struct by key.
// The value is expected to be a struct with "resource_type_id" and "resource_id" fields
// (as stored by ResourceField). Returns the ResourceId and true if found, nil and false otherwise.
func GetResourceIDArg(args *structpb.Struct, key string) (*v2.ResourceId, bool) {
	if args == nil || args.Fields == nil {
		return nil, false
	}

	value, ok := args.Fields[key]
	if !ok {
		return nil, false
	}

	structValue, ok := value.GetKind().(*structpb.Value_StructValue)
	if !ok {
		return nil, false
	}

	// Try to get resource_type_id and resource_id fields
	resourceTypeID, ok := GetStringArg(structValue.StructValue, "resource_type_id")
	if !ok {
		// Also try resource_type as an alternative
		resourceTypeID, ok = GetStringArg(structValue.StructValue, "resource_type")
		if !ok {
			return nil, false
		}
	}

	resourceID, ok := GetStringArg(structValue.StructValue, "resource_id")
	if !ok {
		// Also try resource as an alternative
		resourceID, ok = GetStringArg(structValue.StructValue, "resource")
		if !ok {
			return nil, false
		}
	}

	return &v2.ResourceId{
		ResourceType: resourceTypeID,
		Resource:     resourceID,
	}, true
}

// GetStringSliceArg extracts a string slice from the args struct by key.
// Returns the slice and true if found, nil and false otherwise.
func GetStringSliceArg(args *structpb.Struct, key string) ([]string, bool) {
	if args == nil || args.Fields == nil {
		return nil, false
	}

	value, ok := args.Fields[key]
	if !ok {
		return nil, false
	}

	listValue, ok := value.GetKind().(*structpb.Value_ListValue)
	if !ok {
		return nil, false
	}

	result := make([]string, 0, len(listValue.ListValue.Values))
	for _, v := range listValue.ListValue.Values {
		stringValue, ok := v.GetKind().(*structpb.Value_StringValue)
		if !ok {
			return nil, false
		}
		result = append(result, stringValue.StringValue)
	}

	return result, true
}

// GetStructArg extracts a nested struct from the args struct by key.
// Returns the struct and true if found, nil and false otherwise.
func GetStructArg(args *structpb.Struct, key string) (*structpb.Struct, bool) {
	if args == nil || args.Fields == nil {
		return nil, false
	}

	value, ok := args.Fields[key]
	if !ok {
		return nil, false
	}

	structValue, ok := value.GetKind().(*structpb.Value_StructValue)
	if !ok {
		return nil, false
	}

	return structValue.StructValue, true
}

// RequireStringArg extracts a string value from the args struct by key.
// Returns the value or an error if not found or invalid.
func RequireStringArg(args *structpb.Struct, key string) (string, error) {
	value, ok := GetStringArg(args, key)
	if !ok {
		return "", fmt.Errorf("required argument %s is missing or invalid", key)
	}
	return value, nil
}

// RequireResourceIDArg extracts a ResourceId from the args struct by key.
// Returns the ResourceId or an error if not found or invalid.
func RequireResourceIDArg(args *structpb.Struct, key string) (*v2.ResourceId, error) {
	value, ok := GetResourceIDArg(args, key)
	if !ok {
		return nil, fmt.Errorf("required argument %s is missing or invalid", key)
	}
	return value, nil
}

