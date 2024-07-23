package field

type Configuration struct {
	Fields      []SchemaField
	Constraints []SchemaFieldRelationshipI
}

func NewConfiguration(fields []SchemaField, constraints ...SchemaFieldRelationshipI) Configuration {
	return Configuration{
		Fields:      fields,
		Constraints: constraints,
	}
}
