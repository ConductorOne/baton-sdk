package field

type Configuration struct {
	Fields      []SchemaField             `json:"Fields"`
	Constraints []SchemaFieldRelationship `json:"Constraints"`
}

func NewConfiguration(fields []SchemaField, constraints ...SchemaFieldRelationship) Configuration {
	return Configuration{
		Fields:      fields,
		Constraints: constraints,
	}
}
