package field

type Configuration struct {
	Fields      []SchemaField
	Constraints []SchemaFieldRelationship
	SupportsExternalResources bool
	RequiresExternalConnector bool
}

func NewConfiguration(fields []SchemaField, supportsExternalResources bool, requiresExternalConnector bool, constraints ...SchemaFieldRelationship) Configuration {
	return Configuration{
		Fields:      fields,
		Constraints: constraints,
		SupportsExternalResources: supportsExternalResources,
		RequiresExternalConnector: requiresExternalConnector,
	}
}
