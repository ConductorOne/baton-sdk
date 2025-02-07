package field

type fieldOption func(SchemaField) SchemaField

func WithRequired(required bool) fieldOption {
	return func(o SchemaField) SchemaField {
		o.Required = required
		return o
	}
}

func WithDescription(description string) fieldOption {
	return func(o SchemaField) SchemaField {
		o.Description = description

		return o
	}
}

func WithDefaultValue(value any) fieldOption {
	return func(o SchemaField) SchemaField {
		o.DefaultValue = value

		return o
	}
}

func WithHidden(hidden bool) fieldOption {
	return func(o SchemaField) SchemaField {
		o.CLIConfig.Hidden = hidden

		return o
	}
}

func WithShortHand(sh string) fieldOption {
	return func(o SchemaField) SchemaField {
		o.CLIConfig.ShortHand = sh

		return o
	}
}

func WithPersistent(value bool) fieldOption {
	return func(o SchemaField) SchemaField {
		o.CLIConfig.Persistent = value

		return o
	}
}
