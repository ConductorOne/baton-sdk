package configschema

type fieldOption func(ConfigField) ConfigField

func WithRequired(required bool) fieldOption {
	return func(o ConfigField) ConfigField {
		o.Required = required
		return o
	}
}

func WithDescription(description string) fieldOption {
	return func(o ConfigField) ConfigField {
		o.Description = description

		return o
	}
}

func WithDefaultValue(value any) fieldOption {
	return func(o ConfigField) ConfigField {
		o.DefaultValue = value

		return o
	}
}

func WithHidden(hidden bool) fieldOption {
	return func(o ConfigField) ConfigField {
		o.Hidden = hidden

		return o
	}
}

func WithShortHand(sh string) fieldOption {
	return func(o ConfigField) ConfigField {
		o.CLIShortHand = sh

		return o
	}
}
