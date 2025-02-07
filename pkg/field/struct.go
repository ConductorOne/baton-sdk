package field

type Configuration struct {
	Fields      []SchemaField
	Constraints []SchemaFieldRelationship
}

func NewConfiguration(fields []SchemaField, constraints ...SchemaFieldRelationship) Configuration {
	return Configuration{
		Fields:      fields,
		Constraints: constraints,
	}
}
