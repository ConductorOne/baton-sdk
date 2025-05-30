package field

type Configuration struct {
	Fields                    []SchemaField
	Constraints               []SchemaFieldRelationship
	SupportsExternalResources bool
	RequiresExternalConnector bool
}

type configOption func(Configuration) Configuration

func WithSupportsExternalResources(value bool) configOption {
	return func(c Configuration) Configuration {
		c.SupportsExternalResources = value

		return c
	}
}

func WithRequiresExternalConnector(value bool) configOption {
	return func(c Configuration) Configuration {
		c.RequiresExternalConnector = value

		return c
	}
}

func WithConstraints(constraints ...SchemaFieldRelationship) configOption {
	return func(c Configuration) Configuration {
		c.Constraints = constraints

		return c
	}
}

func NewConfiguration(fields []SchemaField, opts ...configOption) Configuration {
	configuration := Configuration{
		Fields: fields,
	}

	for _, opt := range opts {
		configuration = opt(configuration)
	}

	return configuration
}
