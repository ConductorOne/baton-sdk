package field

type SchemaFieldGroup struct {
	Name        string
	DisplayName string
	HelpText    string
	Fields      []SchemaField
	Default     bool
}

func WithFieldGroups(fieldGroups []SchemaFieldGroup) configOption {
	return func(c Configuration) Configuration {
		c.FieldGroups = fieldGroups

		return c
	}
}

func (i *SchemaFieldGroup) FieldMap() map[string]SchemaField {
	fieldMap := make(map[string]SchemaField, len(i.Fields))
	for _, f := range i.Fields {
		fieldMap[f.FieldName] = f
	}

	return fieldMap
}
