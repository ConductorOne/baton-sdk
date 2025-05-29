package field

type Configuration struct {
	Fields                    []SchemaField
	Constraints               []SchemaFieldRelationship
	SupportsExternalResources bool
	RequiresExternalConnector bool
}

func NewConfiguration(fields []SchemaField, constraints ...SchemaFieldRelationship) Configuration {
	return Configuration{
		Fields:      fields,
		Constraints: constraints,
	}
}

// Sets the SupportsExternalResources field.
func (c Configuration) SetSupportsExternalResources(val bool) Configuration {
	c.SupportsExternalResources = val
	return c
}

// Sets the RequiresExternalConnector field.
func (c Configuration) SetRequiresExternalConnector(val bool) Configuration {
	c.RequiresExternalConnector = val
	return c
}
