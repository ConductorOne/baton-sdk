package field

type FieldGroup struct {
	Name        string
	DisplayName string
	HelpText    string
	Fields      []SchemaField
	Constraints []SchemaFieldRelationship
}

func WithFieldGroups(fieldGroups []FieldGroup) configOption {
	return func(c Configuration) Configuration {
		c.FieldGroups = fieldGroups

		return c
	}
}
