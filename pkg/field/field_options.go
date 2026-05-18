package field

import (
	"fmt"

	v1_conf "github.com/conductorone/baton-sdk/pb/c1/config/v1"
)

type fieldOption func(SchemaField) SchemaField

// WithRequired sets whether the field is required or not.
// If a required field is not set, the connector will error on startup.
// In the GUI, empty required fields will fail form validation.
func WithRequired(required bool) fieldOption {
	return func(o SchemaField) SchemaField {
		o.Required = required

		switch o.Variant {
		case BoolVariant:
			if required {
				panic(fmt.Sprintf("required cannot be set on bool field: %s", o.FieldName))
			}
		case IntVariant:
			if o.Rules.i == nil {
				o.Rules.i = &v1_conf.Int64Rules{}
			}
			o.Rules.i.SetIsRequired(required)
		case StringVariant:
			if o.Rules.s == nil {
				o.Rules.s = &v1_conf.StringRules{}
			}
			o.Rules.s.SetIsRequired(required)
		case StringSliceVariant:
			if o.Rules.ss == nil {
				o.Rules.ss = &v1_conf.RepeatedStringRules{}
			}
			o.Rules.ss.SetIsRequired(required)
		case StringMapVariant:
			if o.Rules.sm == nil {
				o.Rules.sm = &v1_conf.StringMapRules{}
			}
			o.Rules.sm.SetIsRequired(required)
		default:
			panic(fmt.Sprintf("field %s has unsupported type %s", o.FieldName, o.Variant))
		}
		return o
	}
}

// WithDescription sets the description for the field.
// The description is shown in the GUI config and CLI help.
func WithDescription(description string) fieldOption {
	return func(o SchemaField) SchemaField {
		o.Description = description

		return o
	}
}

// WithDisplayName sets the display name for the field.
// The display name is only shown in the GUI, and should be a human-readable name such as "Otel Collector Endpoint".
func WithDisplayName(displayName string) fieldOption {
	return func(o SchemaField) SchemaField {
		o.ConnectorConfig.DisplayName = displayName
		return o
	}
}

// WithDefaultValue sets the default value for the field.
func WithDefaultValue(value any) fieldOption {
	return func(o SchemaField) SchemaField {
		o.DefaultValue = value

		return o
	}
}

func WithDefaultValueFunc(f func() any) fieldOption {
	return func(o SchemaField) SchemaField {
		o.DefaultValue = f()
		return o
	}
}

// WithHidden sets whether the field is hidden or not.
// Hidden fields will not be shown in the GUI config or CLI help.
func WithHidden(hidden bool) fieldOption {
	return func(o SchemaField) SchemaField {
		o.SyncerConfig.Hidden = hidden
		return o
	}
}

// ExportTarget specifies a runtime/configuration surface a field may be exported to.
// The string values are part of the public SDK interface and must remain stable.
type ExportTarget string

const (
	ExportTargetNone       ExportTarget = "none"
	ExportTargetGUI        ExportTarget = "gui"
	ExportTargetOps        ExportTarget = "ops"
	ExportTargetSelfHosted ExportTarget = "self_hosted"
	ExportTargetCLI        ExportTarget = "cli"

	// ExportTargetCLIOnly is kept as a compatibility alias for existing callers.
	ExportTargetCLIOnly = ExportTargetCLI
)

type ExportTargets uint32

const (
	exportTargetsNone ExportTargets = 0
	exportTargetsGUI  ExportTargets = 1 << iota
	exportTargetsOps
	exportTargetsSelfHosted
	exportTargetsCLI
)

func exportTargetBit(target ExportTarget) ExportTargets {
	switch target {
	case ExportTargetGUI:
		return exportTargetsGUI
	case ExportTargetOps:
		return exportTargetsOps
	case ExportTargetSelfHosted:
		return exportTargetsSelfHosted
	case ExportTargetCLI:
		return exportTargetsCLI
	default:
		return exportTargetsNone
	}
}

func combineExportTargets(targets ...ExportTarget) ExportTargets {
	var combined ExportTargets
	for _, target := range targets {
		combined |= exportTargetBit(target)
	}
	return combined
}

func (e ExportTarget) ExportsTo(target ExportTarget) bool {
	if target == ExportTargetNone {
		return e == ExportTargetNone
	}
	return exportTargetBit(e)&exportTargetBit(target) != 0
}

func (e ExportTargets) exportsTo(target ExportTarget) bool {
	if target == ExportTargetNone {
		return e == exportTargetsNone
	}
	return e&exportTargetBit(target) != 0
}

// WithExportTargets sets the export targets for the field. See ExportTarget for more details.
func WithExportTargets(targets ...ExportTarget) fieldOption {
	combined := combineExportTargets(targets...)
	return func(o SchemaField) SchemaField {
		if o.ExportTarget != ExportTargetGUI && len(targets) == 1 && targets[0] != o.ExportTarget {
			panic(fmt.Sprintf("target %s would be overwritten by %s for %s", o.ExportTarget, targets[0], o.FieldName))
		}
		if len(targets) == 1 {
			o.ExportTarget = targets[0]
			o.ExportTargets = exportTargetsNone
			return o
		}
		o.ExportTargets = combined
		if len(targets) > 0 {
			o.ExportTarget = targets[0]
		}
		return o
	}
}

// WithExportTarget sets a single export target for the field.
func WithExportTarget(target ExportTarget) fieldOption {
	return WithExportTargets(target)
}

func WithRequiredInGUI(value bool) fieldOption {
	return func(o SchemaField) SchemaField {
		o.ConnectorConfig.Required = value
		o.ExportTarget = ExportTargetGUI
		return o
	}
}

func WithShortHand(sh string) fieldOption {
	return func(o SchemaField) SchemaField {
		o.SyncerConfig.ShortHand = sh

		return o
	}
}

func WithPersistent(value bool) fieldOption {
	return func(o SchemaField) SchemaField {
		o.SyncerConfig.Persistent = value

		return o
	}
}

// WithIsSecret sets the field to be secret, causing the values to be obscured in the GUI.
// This is meant for fields that contain sensitive information, such as passwords or API keys.
func WithIsSecret(value bool) fieldOption {
	return func(o SchemaField) SchemaField {
		o.Secret = value

		return o
	}
}

// WithPlaceholder sets the placeholder value for the field.
// The placeholder is only shown in the GUI, and should be an example value such as "my-password" or "my-api-key".
func WithPlaceholder(value string) fieldOption {
	return func(o SchemaField) SchemaField {
		o.ConnectorConfig.Placeholder = value

		return o
	}
}

type intRuleMaker func(r *IntRuler)

func WithInt(f intRuleMaker) fieldOption {
	return func(o SchemaField) SchemaField {
		rules := o.Rules.i
		if rules == nil {
			rules = &v1_conf.Int64Rules{}
		}
		o.Rules.i = rules
		f(NewIntBuilder(rules))
		return o
	}
}

type stringRuleMaker func(r *StringRuler)

func WithString(f stringRuleMaker) fieldOption {
	return func(o SchemaField) SchemaField {
		rules := o.Rules.s
		if rules == nil {
			rules = &v1_conf.StringRules{}
		}
		o.Rules.s = rules
		f(NewStringBuilder(rules))
		return o
	}
}

type boolRuleMaker func(r *BoolRuler)

func WithBool(f boolRuleMaker) fieldOption {
	return func(o SchemaField) SchemaField {
		rules := o.Rules.b
		if rules == nil {
			rules = &v1_conf.BoolRules{}
		}
		o.Rules.b = rules
		f(NewBoolBuilder(rules))
		return o
	}
}

type stringSliceRuleMaker func(r *StringSliceRuler)

func WithStringSlice(f stringSliceRuleMaker) fieldOption {
	return func(o SchemaField) SchemaField {
		rules := o.Rules.ss
		if rules == nil {
			rules = &v1_conf.RepeatedStringRules{}
		}
		o.Rules.ss = rules
		f(NewRepeatedStringBuilder(rules))
		return o
	}
}

type stringMapRuleMaker func(r *StringMapRuler)

func WithStringMap(f stringMapRuleMaker) fieldOption {
	return func(o SchemaField) SchemaField {
		rules := o.Rules.sm
		if rules == nil {
			rules = &v1_conf.StringMapRules{}
		}
		o.Rules.sm = rules
		f(NewStringMapBuilder(rules))
		return o
	}
}

func WithStructFieldName(name string) fieldOption {
	return func(o SchemaField) SchemaField {
		o.StructFieldName = name
		return o
	}
}

type StringMapRuler struct {
	rules *v1_conf.StringMapRules
}

func NewStringMapBuilder(rules *v1_conf.StringMapRules) *StringMapRuler {
	return &StringMapRuler{rules: rules}
}

func (r *StringMapRuler) WithRequired(required bool) *StringMapRuler {
	r.rules.SetIsRequired(required)
	return r
}

func (r *StringMapRuler) WithValidateEmpty(validateEmpty bool) *StringMapRuler {
	r.rules.SetValidateEmpty(validateEmpty)
	return r
}
