package actions

import (
	"fmt"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/encoding/protojson"
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

// RequireResourceIdListArg extracts a list of ResourceId from the args struct by key.
// Returns the list of ResourceId or an error if not found or invalid.
func RequireResourceIdListArg(args *structpb.Struct, key string) ([]*v2.ResourceId, error) {
	list, ok := GetResourceIdListArg(args, key)
	if !ok {
		return nil, fmt.Errorf("required argument %s is missing or invalid", key)
	}
	return list, nil
}

// GetResourceIdListArg extracts a list of ResourceId from the args struct by key.
// Returns the list and true if found and valid, or nil and false otherwise.
func GetResourceIdListArg(args *structpb.Struct, key string) ([]*v2.ResourceId, bool) {
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

	var resourceIDs []*v2.ResourceId
	for _, v := range listValue.ListValue.Values {
		structValue, ok := v.GetKind().(*structpb.Value_StructValue)
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
		resourceIDs = append(resourceIDs, &v2.ResourceId{
			ResourceType: resourceTypeID,
			Resource:     resourceID,
		})
	}

	return resourceIDs, true
}

// GetResourceFieldArg extracts a Resource proto message from the args struct by key.
// The Resource is expected to be stored as a JSON-serialized struct value.
// Returns the Resource and true if found and valid, or nil and false otherwise.
func GetResourceFieldArg(args *structpb.Struct, key string) (*v2.Resource, bool) {
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

	// Marshal the struct value back to JSON, then unmarshal into the proto message
	jsonBytes, err := protojson.Marshal(structValue.StructValue)
	if err != nil {
		return nil, false
	}

	basicResource := &config.Resource{}
	if err := protojson.Unmarshal(jsonBytes, basicResource); err != nil {
		return nil, false
	}

	return basicResourceToResource(basicResource), true
}

func resourceToBasicResource(resource *v2.Resource) *config.Resource {
	var resourceId *config.ResourceId
	if resource.Id != nil {
		resourceId = config.ResourceId_builder{
			ResourceTypeId: resource.Id.ResourceType,
			ResourceId:     resource.Id.Resource,
		}.Build()
	}
	var parentResourceId *config.ResourceId
	if resource.ParentResourceId != nil {
		parentResourceId = config.ResourceId_builder{
			ResourceTypeId: resource.ParentResourceId.ResourceType,
			ResourceId:     resource.ParentResourceId.Resource,
		}.Build()
	}
	return config.Resource_builder{
		ResourceId:       resourceId,
		ParentResourceId: parentResourceId,
		DisplayName:      resource.DisplayName,
		Description:      resource.Description,
		Annotations:      resource.Annotations,
	}.Build()
}

func basicResourceToResource(basicResource *config.Resource) *v2.Resource {
	var resourceId *v2.ResourceId
	if basicResource.GetResourceId() != nil {
		resourceId = &v2.ResourceId{
			ResourceType: basicResource.GetResourceId().GetResourceTypeId(),
			Resource:     basicResource.GetResourceId().GetResourceId(),
		}
	}
	var parentResourceId *v2.ResourceId
	if basicResource.GetParentResourceId() != nil {
		parentResourceId = &v2.ResourceId{
			ResourceType: basicResource.GetParentResourceId().GetResourceTypeId(),
			Resource:     basicResource.GetParentResourceId().GetResourceId(),
		}
	}
	return &v2.Resource{
		Id:               resourceId,
		ParentResourceId: parentResourceId,
		DisplayName:      basicResource.GetDisplayName(),
		Description:      basicResource.GetDescription(),
		Annotations:      basicResource.GetAnnotations(),
	}
}

// GetResourceListFieldArg extracts a list of Resource proto messages from the args struct by key.
// Each Resource is expected to be stored as a JSON-serialized struct value.
// Returns the list of Resource and true if found and valid, or nil and false otherwise.
func GetResourceListFieldArg(args *structpb.Struct, key string) ([]*v2.Resource, bool) {
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
	var resources []*v2.Resource
	for _, v := range listValue.ListValue.Values {
		structValue, ok := v.GetKind().(*structpb.Value_StructValue)
		if !ok {
			return nil, false
		}

		// Marshal the struct value back to JSON, then unmarshal into the proto message
		jsonBytes, err := protojson.Marshal(structValue.StructValue)
		if err != nil {
			return nil, false
		}

		basicResource := &config.Resource{}
		if err := protojson.Unmarshal(jsonBytes, basicResource); err != nil {
			return nil, false
		}

		resources = append(resources, basicResourceToResource(basicResource))
	}
	return resources, true
}

// SetResourceFieldArg stores a Resource proto message in the args struct by key.
// The Resource is serialized as a JSON struct value.
func SetResourceFieldArg(args *structpb.Struct, key string, resource *v2.Resource) error {
	if args == nil {
		return fmt.Errorf("args cannot be nil")
	}
	if resource == nil {
		return fmt.Errorf("resource cannot be nil")
	}

	basicResource := resourceToBasicResource(resource)

	// Marshal the proto message to JSON, then unmarshal into a struct value
	jsonBytes, err := protojson.Marshal(basicResource)
	if err != nil {
		return fmt.Errorf("failed to marshal resource: %w", err)
	}

	structValue := &structpb.Struct{}
	if err := protojson.Unmarshal(jsonBytes, structValue); err != nil {
		return fmt.Errorf("failed to unmarshal resource to struct: %w", err)
	}

	if args.Fields == nil {
		args.Fields = make(map[string]*structpb.Value)
	}
	args.Fields[key] = structpb.NewStructValue(structValue)
	return nil
}

// ReturnField represents a key-value pair for action return values.
type ReturnField struct {
	Key   string
	Value *structpb.Value
}

// NewReturnField creates a new return field with the given key and value.
func NewReturnField(key string, value *structpb.Value) ReturnField {
	return ReturnField{Key: key, Value: value}
}

// NewStringReturnField creates a return field with a string value.
func NewStringReturnField(key string, value string) ReturnField {
	return ReturnField{Key: key, Value: structpb.NewStringValue(value)}
}

// NewBoolReturnField creates a return field with a bool value.
func NewBoolReturnField(key string, value bool) ReturnField {
	return ReturnField{Key: key, Value: structpb.NewBoolValue(value)}
}

// NewNumberReturnField creates a return field with a number value.
func NewNumberReturnField(key string, value float64) ReturnField {
	return ReturnField{Key: key, Value: structpb.NewNumberValue(value)}
}

// NewResourceReturnField creates a return field with a Resource proto value.
func NewResourceReturnField(key string, resource *v2.Resource) (ReturnField, error) {
	if resource == nil {
		return ReturnField{}, fmt.Errorf("resource cannot be nil")
	}
	basicResource := resourceToBasicResource(resource)
	jsonBytes, err := protojson.Marshal(basicResource)
	if err != nil {
		return ReturnField{}, fmt.Errorf("failed to marshal resource: %w", err)
	}

	structValue := &structpb.Struct{}
	if err := protojson.Unmarshal(jsonBytes, structValue); err != nil {
		return ReturnField{}, fmt.Errorf("failed to unmarshal resource to struct: %w", err)
	}

	return ReturnField{Key: key, Value: structpb.NewStructValue(structValue)}, nil
}

// NewResourceIdReturnField creates a return field with a ResourceId proto value.
func NewResourceIdReturnField(key string, resourceId *v2.ResourceId) (ReturnField, error) {
	if resourceId == nil {
		return ReturnField{}, fmt.Errorf("resource ID cannot be nil")
	}
	basicResourceId := config.ResourceId_builder{
		ResourceTypeId: resourceId.ResourceType,
		ResourceId:     resourceId.Resource,
	}.Build()
	jsonBytes, err := protojson.Marshal(basicResourceId)
	if err != nil {
		return ReturnField{}, fmt.Errorf("failed to marshal resource id: %w", err)
	}

	structValue := &structpb.Struct{}
	if err := protojson.Unmarshal(jsonBytes, structValue); err != nil {
		return ReturnField{}, fmt.Errorf("failed to unmarshal resource id to struct: %w", err)
	}

	return ReturnField{Key: key, Value: structpb.NewStructValue(structValue)}, nil
}

// NewStringListReturnField creates a return field with a list of string values.
func NewStringListReturnField(key string, values []string) ReturnField {
	listValues := make([]*structpb.Value, len(values))
	for i, v := range values {
		listValues[i] = structpb.NewStringValue(v)
	}
	return ReturnField{Key: key, Value: structpb.NewListValue(&structpb.ListValue{Values: listValues})}
}

// NewNumberListReturnField creates a return field with a list of number values.
func NewNumberListReturnField(key string, values []float64) ReturnField {
	listValues := make([]*structpb.Value, len(values))
	for i, v := range values {
		listValues[i] = structpb.NewNumberValue(v)
	}
	return ReturnField{Key: key, Value: structpb.NewListValue(&structpb.ListValue{Values: listValues})}
}

// NewResourceListReturnField creates a return field with a list of Resource proto values.
func NewResourceListReturnField(key string, resources []*v2.Resource) (ReturnField, error) {
	listValues := make([]*structpb.Value, len(resources))
	for i, resource := range resources {
		if resource == nil {
			return ReturnField{}, fmt.Errorf("resource at index %d cannot be nil", i)
		}
		basicResource := resourceToBasicResource(resource)
		jsonBytes, err := protojson.Marshal(basicResource)
		if err != nil {
			return ReturnField{}, fmt.Errorf("failed to marshal resource: %w", err)
		}

		structValue := &structpb.Struct{}
		if err := protojson.Unmarshal(jsonBytes, structValue); err != nil {
			return ReturnField{}, fmt.Errorf("failed to unmarshal resource to struct: %w", err)
		}

		listValues[i] = structpb.NewStructValue(structValue)
	}
	return ReturnField{Key: key, Value: structpb.NewListValue(&structpb.ListValue{Values: listValues})}, nil
}

// NewResourceIdListReturnField creates a return field with a list of ResourceId proto values.
func NewResourceIdListReturnField(key string, resourceIDs []*v2.ResourceId) (ReturnField, error) {
	listValues := make([]*structpb.Value, len(resourceIDs))
	for i, resourceId := range resourceIDs {
		if resourceId == nil {
			return ReturnField{}, fmt.Errorf("resource id at index %d cannot be nil", i)
		}
		basicResourceId := config.ResourceId_builder{
			ResourceTypeId: resourceId.ResourceType,
			ResourceId:     resourceId.Resource,
		}.Build()
		jsonBytes, err := protojson.Marshal(basicResourceId)
		if err != nil {
			return ReturnField{}, fmt.Errorf("failed to marshal resource id: %w", err)
		}

		structValue := &structpb.Struct{}
		if err := protojson.Unmarshal(jsonBytes, structValue); err != nil {
			return ReturnField{}, fmt.Errorf("failed to unmarshal resource id to struct: %w", err)
		}

		listValues[i] = structpb.NewStructValue(structValue)
	}
	return ReturnField{Key: key, Value: structpb.NewListValue(&structpb.ListValue{Values: listValues})}, nil
}

// NewListReturnField creates a return field with a list of arbitrary values.
func NewListReturnField(key string, values []*structpb.Value) ReturnField {
	return ReturnField{Key: key, Value: structpb.NewListValue(&structpb.ListValue{Values: values})}
}

// NewReturnValues creates a return struct with the specified success status and fields.
// This helps users avoid having to remember the correct structure for return values.
func NewReturnValues(success bool, fields ...ReturnField) *structpb.Struct {
	rv := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"success": structpb.NewBoolValue(success),
		},
	}

	for _, field := range fields {
		rv.Fields[field.Key] = field.Value
	}

	return rv
}

// entitlementToBasicEntitlement converts a v2.Entitlement to a config.Entitlement.
func entitlementToBasicEntitlement(entitlement *v2.Entitlement) *config.Entitlement {
	var grantableToResourceTypeIDs []string
	for _, rt := range entitlement.GetGrantableTo() {
		grantableToResourceTypeIDs = append(grantableToResourceTypeIDs, rt.GetId())
	}

	var resourceId, resourceTypeId string
	if entitlement.GetResource() != nil && entitlement.GetResource().GetId() != nil {
		resourceId = entitlement.GetResource().GetId().GetResource()
		resourceTypeId = entitlement.GetResource().GetId().GetResourceType()
	}

	return config.Entitlement_builder{
		Id:                         entitlement.GetId(),
		DisplayName:                entitlement.GetDisplayName(),
		Description:                entitlement.GetDescription(),
		Slug:                       entitlement.GetSlug(),
		Purpose:                    entitlement.GetPurpose().String(),
		GrantableToResourceTypeIds: grantableToResourceTypeIDs,
		ResourceId:                 resourceId,
		ResourceTypeId:             resourceTypeId,
	}.Build()
}

// basicEntitlementToEntitlement converts a config.Entitlement to a v2.Entitlement.
func basicEntitlementToEntitlement(basicEntitlement *config.Entitlement) *v2.Entitlement {
	var grantableTo []*v2.ResourceType
	for _, rtId := range basicEntitlement.GetGrantableToResourceTypeIds() {
		grantableTo = append(grantableTo, &v2.ResourceType{Id: rtId})
	}

	var resource *v2.Resource
	if basicEntitlement.GetResourceId() != "" && basicEntitlement.GetResourceTypeId() != "" {
		resource = &v2.Resource{
			Id: &v2.ResourceId{
				Resource:     basicEntitlement.GetResourceId(),
				ResourceType: basicEntitlement.GetResourceTypeId(),
			},
		}
	}

	// Parse purpose from string
	purposeValue := v2.Entitlement_PURPOSE_VALUE_UNSPECIFIED
	switch basicEntitlement.GetPurpose() {
	case "PURPOSE_VALUE_ASSIGNMENT":
		purposeValue = v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT
	case "PURPOSE_VALUE_PERMISSION":
		purposeValue = v2.Entitlement_PURPOSE_VALUE_PERMISSION
	case "PURPOSE_VALUE_OWNERSHIP":
		purposeValue = v2.Entitlement_PURPOSE_VALUE_OWNERSHIP
	}

	return &v2.Entitlement{
		Id:          basicEntitlement.GetId(),
		DisplayName: basicEntitlement.GetDisplayName(),
		Description: basicEntitlement.GetDescription(),
		Slug:        basicEntitlement.GetSlug(),
		Purpose:     purposeValue,
		GrantableTo: grantableTo,
		Resource:    resource,
	}
}

// grantToBasicGrant converts a v2.Grant to a config.Grant.
func grantToBasicGrant(grant *v2.Grant) *config.Grant {
	var entitlementRef *config.EntitlementRef
	if grant.GetEntitlement() != nil {
		entitlementRef = config.EntitlementRef_builder{
			Id: grant.GetEntitlement().GetId(),
		}.Build()
	}

	var principal *config.Resource
	if grant.GetPrincipal() != nil {
		principal = resourceToBasicResource(grant.GetPrincipal())
	}

	return config.Grant_builder{
		Id:          grant.GetId(),
		Entitlement: entitlementRef,
		Principal:   principal,
	}.Build()
}

// basicGrantToGrant converts a config.Grant to a v2.Grant.
func basicGrantToGrant(basicGrant *config.Grant) *v2.Grant {
	var entitlement *v2.Entitlement
	if basicGrant.GetEntitlement() != nil {
		entitlement = &v2.Entitlement{
			Id: basicGrant.GetEntitlement().GetId(),
		}
	}

	var principal *v2.Resource
	if basicGrant.GetPrincipal() != nil {
		principal = basicResourceToResource(basicGrant.GetPrincipal())
	}

	return &v2.Grant{
		Id:          basicGrant.GetId(),
		Entitlement: entitlement,
		Principal:   principal,
	}
}

// GetEntitlementListFieldArg extracts a list of Entitlement proto messages from the args struct by key.
// Each Entitlement is expected to be stored as a JSON-serialized struct value.
// Returns the list of Entitlement and true if found and valid, or nil and false otherwise.
func GetEntitlementListFieldArg(args *structpb.Struct, key string) ([]*v2.Entitlement, bool) {
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
	var entitlements []*v2.Entitlement
	for _, v := range listValue.ListValue.Values {
		structValue, ok := v.GetKind().(*structpb.Value_StructValue)
		if !ok {
			return nil, false
		}

		// Marshal the struct value back to JSON, then unmarshal into the proto message
		jsonBytes, err := protojson.Marshal(structValue.StructValue)
		if err != nil {
			return nil, false
		}

		basicEntitlement := &config.Entitlement{}
		if err := protojson.Unmarshal(jsonBytes, basicEntitlement); err != nil {
			return nil, false
		}

		entitlements = append(entitlements, basicEntitlementToEntitlement(basicEntitlement))
	}
	return entitlements, true
}

// GetGrantListFieldArg extracts a list of Grant proto messages from the args struct by key.
// Each Grant is expected to be stored as a JSON-serialized struct value.
// Returns the list of Grant and true if found and valid, or nil and false otherwise.
func GetGrantListFieldArg(args *structpb.Struct, key string) ([]*v2.Grant, bool) {
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
	var grants []*v2.Grant
	for _, v := range listValue.ListValue.Values {
		structValue, ok := v.GetKind().(*structpb.Value_StructValue)
		if !ok {
			return nil, false
		}

		// Marshal the struct value back to JSON, then unmarshal into the proto message
		jsonBytes, err := protojson.Marshal(structValue.StructValue)
		if err != nil {
			return nil, false
		}

		basicGrant := &config.Grant{}
		if err := protojson.Unmarshal(jsonBytes, basicGrant); err != nil {
			return nil, false
		}

		grants = append(grants, basicGrantToGrant(basicGrant))
	}
	return grants, true
}

// NewEntitlementListReturnField creates a return field with a list of Entitlement proto values.
func NewEntitlementListReturnField(key string, entitlements []*v2.Entitlement) (ReturnField, error) {
	listValues := make([]*structpb.Value, len(entitlements))
	for i, entitlement := range entitlements {
		if entitlement == nil {
			return ReturnField{}, fmt.Errorf("entitlement at index %d cannot be nil", i)
		}
		basicEntitlement := entitlementToBasicEntitlement(entitlement)
		jsonBytes, err := protojson.Marshal(basicEntitlement)
		if err != nil {
			return ReturnField{}, fmt.Errorf("failed to marshal entitlement: %w", err)
		}

		structValue := &structpb.Struct{}
		if err := protojson.Unmarshal(jsonBytes, structValue); err != nil {
			return ReturnField{}, fmt.Errorf("failed to unmarshal entitlement to struct: %w", err)
		}

		listValues[i] = structpb.NewStructValue(structValue)
	}
	return ReturnField{Key: key, Value: structpb.NewListValue(&structpb.ListValue{Values: listValues})}, nil
}

// NewGrantListReturnField creates a return field with a list of Grant proto values.
func NewGrantListReturnField(key string, grants []*v2.Grant) (ReturnField, error) {
	listValues := make([]*structpb.Value, len(grants))
	for i, grant := range grants {
		if grant == nil {
			return ReturnField{}, fmt.Errorf("grant at index %d cannot be nil", i)
		}
		basicGrant := grantToBasicGrant(grant)
		jsonBytes, err := protojson.Marshal(basicGrant)
		if err != nil {
			return ReturnField{}, fmt.Errorf("failed to marshal grant: %w", err)
		}

		structValue := &structpb.Struct{}
		if err := protojson.Unmarshal(jsonBytes, structValue); err != nil {
			return ReturnField{}, fmt.Errorf("failed to unmarshal grant to struct: %w", err)
		}

		listValues[i] = structpb.NewStructValue(structValue)
	}
	return ReturnField{Key: key, Value: structpb.NewListValue(&structpb.ListValue{Values: listValues})}, nil
}
