package field

import (
	"fmt"

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
	conf := &v1_conf.Configuration{}

	ignore := make(map[string]struct{})
	for _, f := range c.Fields {
		if f.ExportTarget != ExportTargetGUI && f.ExportTarget != ExportTargetOps {
			ignore[f.FieldName] = struct{}{}
			continue
		}
		field := v1_conf.Field{
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
			intField := &v1_conf.IntField{Rules: f.Rules.i}
			d, err := GetDefaultValue[int](f)
			if err != nil {
				return nil, err
			}
			if d != nil {
				intField.DefaultValue = int64(*d)
			}

			field.Field = &v1_conf.Field_IntField{IntField: intField}

		case BoolVariant:
			boolField := &v1_conf.BoolField{Rules: f.Rules.b}
			d, err := GetDefaultValue[bool](f)
			if err != nil {
				return nil, err
			}
			if d != nil {
				boolField.DefaultValue = *d
			}
			field.Field = &v1_conf.Field_BoolField{BoolField: boolField}
		case StringSliceVariant:
			stringSliceField := &v1_conf.StringSliceField{Rules: f.Rules.ss}
			d, err := GetDefaultValue[[]string](f)
			if err != nil {
				return nil, err
			}
			if d != nil {
				stringSliceField.DefaultValue = *d
			}
			field.Field = &v1_conf.Field_StringSliceField{StringSliceField: stringSliceField}
		case StringVariant:
			stringField := &v1_conf.StringField{Rules: f.Rules.s}
			d, err := GetDefaultValue[string](f)
			if err != nil {
				return nil, err
			}
			if d != nil {
				stringField.DefaultValue = *d
			}

			switch f.ConnectorConfig.FieldType {
			case Text:
				stringField.Type = v1_conf.StringFieldType_STRING_FIELD_TYPE_TEXT_UNSPECIFIED
			case Randomize:
				stringField.Type = v1_conf.StringFieldType_STRING_FIELD_TYPE_RANDOM
			case OAuth2:
				stringField.Type = v1_conf.StringFieldType_STRING_FIELD_TYPE_OAUTH2
			case ConnectorDerivedOptions:
				stringField.Type = v1_conf.StringFieldType_STRING_FIELD_TYPE_CONNECTOR_DERIVED_OPTIONS
			case FileUpload:
				stringField.Type = v1_conf.StringFieldType_STRING_FIELD_TYPE_FILE_UPLOAD
				stringField.AllowedExtensions = f.ConnectorConfig.BonusStrings
			default:
				return nil, fmt.Errorf("invalid field type: '%s'", f.ConnectorConfig.FieldType)
			}

			field.Field = &v1_conf.Field_StringField{StringField: stringField}
		}
		conf.Fields = append(conf.Fields, &field)
	}

	for _, rel := range c.Constraints {
		constraint := v1_conf.Constraint{}

		contraintForIgnoredField := false
		for _, f := range rel.Fields {
			if _, ok := ignore[f.FieldName]; ok {
				contraintForIgnoredField = true
				break
			}
			constraint.FieldNames = append(constraint.FieldNames, f.FieldName)
		}
		if contraintForIgnoredField {
			continue
		}

		for _, f := range rel.ExpectedFields {
			if _, ok := ignore[f.FieldName]; ok {
				contraintForIgnoredField = true
				break
			}
			constraint.SecondaryFieldNames = append(constraint.SecondaryFieldNames, f.FieldName)
		}

		if contraintForIgnoredField {
			continue
		}

		kind, ok := RelationshipToConstraintKind[rel.Kind]
		if !ok {
			return nil, fmt.Errorf("invalid constraint kind: %d", rel.Kind)
		}
		constraint.Kind = kind

		conf.Constraints = append(conf.Constraints, &constraint)
	}
	return conf, nil
}
