package field

import (
	"fmt"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	v1_conf "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var RelationshipToConstraintKind = map[Relationship]v1_conf.ConstraintKind{
	RequiredTogether:  v1_conf.ConstraintKind_CONSTRAINT_KIND_REQUIRED_TOGETHER,
	MutuallyExclusive: v1_conf.ConstraintKind_CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE,
	AtLeastOne:        v1_conf.ConstraintKind_CONSTRAINT_KIND_AT_LEAST_ONE,
	Dependents:        v1_conf.ConstraintKind_CONSTRAINT_KIND_DEPENDENT_ON,
}

func (c *Configuration) MarshalJSON() ([]byte, error) {
	conf, err := c.marshal()
	if err != nil {
		return nil, err
	}

	return protojson.Marshal(conf)
}

func (c *Configuration) Marshal() ([]byte, error) {
	conf, err := c.marshal()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(conf)
}

func (c Configuration) marshal() (*v1_conf.Configuration, error) {
	var err error

	confBuilder := v1_conf.Configuration_builder{
		DisplayName:               c.DisplayName,
		HelpUrl:                   c.HelpUrl,
		IconUrl:                   c.IconUrl,
		CatalogId:                 c.CatalogId,
		IsDirectory:               c.IsDirectory,
		SupportsExternalResources: c.SupportsExternalResources,
		RequiresExternalConnector: c.RequiresExternalConnector,
	}

	// Fields
	confBuilder.Fields, confBuilder.Constraints, err = mapFieldsAndConstraints(c.Fields, c.Constraints)
	if err != nil {
		return nil, fmt.Errorf("failed to convert fields and constraints to v1: %w", err)
	}

	fieldGroups := make([]*v1_conf.FieldGroup, 0, len(c.FieldGroups))
	for _, group := range c.FieldGroups {
		fieldGroups = append(fieldGroups, fieldGroupToV1(group))
	}
	confBuilder.FieldGroups = fieldGroups

	return confBuilder.Build(), nil
}

func fieldGroupToV1(fg SchemaFieldGroup) *v1_conf.FieldGroup {
	fieldGroupV1 := &v1_conf.FieldGroup{
		Name:        fg.Name,
		DisplayName: fg.DisplayName,
		HelpText:    fg.HelpText,
	}

	fieldGroupV1.Fields = make([]string, 0, len(fg.Fields))
	for _, f := range fg.Fields {
		fieldGroupV1.Fields = append(fieldGroupV1.Fields, f.FieldName)
	}

	return fieldGroupV1
}

func mapFieldsAndConstraints(fields []SchemaField, constraints []SchemaFieldRelationship) ([]*v1_conf.Field, []*v1_conf.Constraint, error) {
	resultFields := make([]*v1_conf.Field, 0, len(fields))
	resultConstraints := make([]*v1_conf.Constraint, 0, len(constraints))

	ignore := make(map[string]struct{})
	for _, f := range fields {
		if f.ExportTarget != ExportTargetGUI && f.ExportTarget != ExportTargetOps {
			ignore[f.FieldName] = struct{}{}
			continue
		}

		fieldv1, err := schemaFieldToV1(f)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to convert field '%s' to v1: %w", f.FieldName, err)
		}

		resultFields = append(resultFields, fieldv1)
	}

	for _, rel := range constraints {
		constraint, err := constraintToV1(rel, ignore)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to convert constraint to v1: %w", err)
		}

		if constraint == nil {
			continue
		}

		resultConstraints = append(resultConstraints, constraint)
	}

	return resultFields, resultConstraints, nil
}

func constraintToV1(rel SchemaFieldRelationship, ignore map[string]struct{}) (*v1_conf.Constraint, error) {
	constraintBuilder := v1_conf.Constraint_builder{}

	constraintForIgnoredField := false
	for _, f := range rel.Fields {
		if _, ok := ignore[f.FieldName]; ok {
			constraintForIgnoredField = true
			break
		}
		constraintBuilder.FieldNames = append(constraintBuilder.FieldNames, f.FieldName)
	}
	if constraintForIgnoredField {
		return nil, nil
	}

	for _, f := range rel.ExpectedFields {
		if _, ok := ignore[f.FieldName]; ok {
			constraintForIgnoredField = true
			break
		}
		constraintBuilder.SecondaryFieldNames = append(constraintBuilder.SecondaryFieldNames, f.FieldName)
	}

	if constraintForIgnoredField {
		return nil, nil
	}

	kind, ok := RelationshipToConstraintKind[rel.Kind]
	if !ok {
		return nil, fmt.Errorf("invalid constraint kind: %d", rel.Kind)
	}
	constraintBuilder.Kind = kind

	return constraintBuilder.Build(), nil
}

func schemaFieldToV1(f SchemaField) (*v1_conf.Field, error) {
	fieldBuilder := v1_conf.Field_builder{
		Name:        f.FieldName,
		DisplayName: f.ConnectorConfig.DisplayName,
		Description: f.Description,
		Placeholder: f.ConnectorConfig.Placeholder,
		IsRequired:  f.Required,
		IsOps:       f.ExportTarget == ExportTargetOps,
		IsSecret:    f.Secret,
	}

	switch f.Variant {
	case IntVariant:
		intFieldBuilder := v1_conf.IntField_builder{Rules: f.Rules.i}
		d, err := GetDefaultValue[int](f)
		if err != nil {
			return nil, err
		}
		if d != nil {
			intFieldBuilder.DefaultValue = int64(*d)
		}

		fieldBuilder.IntField = intFieldBuilder.Build()

	case BoolVariant:
		boolFieldBuilder := v1_conf.BoolField_builder{Rules: f.Rules.b}
		d, err := GetDefaultValue[bool](f)
		if err != nil {
			return nil, err
		}
		if d != nil {
			boolFieldBuilder.DefaultValue = *d
		}
		fieldBuilder.BoolField = boolFieldBuilder.Build()
	case StringSliceVariant:
		stringSliceFieldBuilder := v1_conf.StringSliceField_builder{Rules: f.Rules.ss}
		d, err := GetDefaultValue[[]string](f)
		if err != nil {
			return nil, err
		}
		if d != nil {
			stringSliceFieldBuilder.DefaultValue = *d
		}
		fieldBuilder.StringSliceField = stringSliceFieldBuilder.Build()
	case StringMapVariant:
		stringMapFieldBuilder := v1_conf.StringMapField_builder{Rules: f.Rules.sm}
		d, err := GetDefaultValue[map[string]any](f)
		if err != nil {
			return nil, err
		}
		if d != nil {
			// Convert map[string]any to map[string]*anypb.Any
			anyMap := make(map[string]*anypb.Any)
			for k, v := range *d {
				// Convert the value to a structpb.Value
				value, err := structpb.NewValue(v)
				if err != nil {
					return nil, fmt.Errorf("failed to convert map value to structpb.Value: %w", err)
				}
				anyValue, err := anypb.New(value)
				if err != nil {
					return nil, fmt.Errorf("failed to convert structpb.Value to Any: %w", err)
				}
				anyMap[k] = anyValue
			}
			stringMapFieldBuilder.DefaultValue = anyMap
		}
		fieldBuilder.StringMapField = stringMapFieldBuilder.Build()
	case StringVariant:
		stringFieldBuilder := v1_conf.StringField_builder{Rules: f.Rules.s}
		d, err := GetDefaultValue[string](f)
		if err != nil {
			return nil, err
		}
		if d != nil {
			stringFieldBuilder.DefaultValue = *d
		}

		switch f.ConnectorConfig.FieldType {
		case Text:
			stringFieldBuilder.Type = v1_conf.StringFieldType_STRING_FIELD_TYPE_TEXT_UNSPECIFIED
		case Randomize:
			stringFieldBuilder.Type = v1_conf.StringFieldType_STRING_FIELD_TYPE_RANDOM
		case OAuth2:
			stringFieldBuilder.Type = v1_conf.StringFieldType_STRING_FIELD_TYPE_OAUTH2
		case ConnectorDerivedOptions:
			stringFieldBuilder.Type = v1_conf.StringFieldType_STRING_FIELD_TYPE_CONNECTOR_DERIVED_OPTIONS
		case FileUpload:
			stringFieldBuilder.Type = v1_conf.StringFieldType_STRING_FIELD_TYPE_FILE_UPLOAD
			stringFieldBuilder.AllowedExtensions = f.ConnectorConfig.BonusStrings
		default:
			return nil, fmt.Errorf("invalid field type: '%s'", f.ConnectorConfig.FieldType)
		}

		fieldBuilder.StringField = stringFieldBuilder.Build()
	default:
		return nil, fmt.Errorf("invalid variant: '%s'", f.Variant)
	}

	return fieldBuilder.Build(), nil
}
