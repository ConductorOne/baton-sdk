package field

type SchemaFieldGroup struct {
	Name        string
	DisplayName string
	HelpText    string
	Fields      []SchemaField
}

func WithFieldGroups(fieldGroups []SchemaFieldGroup) configOption {
	return func(c Configuration) Configuration {
		c.FieldGroups = fieldGroups

		return c
	}
}
