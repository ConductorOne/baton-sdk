package field

type Configuration struct {
	Fields                    []SchemaField
	Constraints               []SchemaFieldRelationship
	DisplayName               string
	HelpUrl                   string
	IconUrl                   string
	CatalogId                 string
	IsDirectory               bool
	SupportsExternalResources bool
	RequiresExternalConnector bool
}

type configOption func(Configuration) Configuration

func WithConnectorDisplayName(value string) configOption {
	return func(c Configuration) Configuration {
		c.DisplayName = value

		return c
	}
}

func WithHelpUrl(value string) configOption {
	return func(c Configuration) Configuration {
		c.HelpUrl = value

		return c
	}
}

func WithIconUrl(value string) configOption {
	return func(c Configuration) Configuration {
		c.IconUrl = value

		return c
	}
}

func WithCatalogId(value string) configOption {
	return func(c Configuration) Configuration {
		c.CatalogId = value

		return c
	}
}

func WithIsDirectory(value bool) configOption {
	return func(c Configuration) Configuration {
		c.IsDirectory = value

		return c
	}
}

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
