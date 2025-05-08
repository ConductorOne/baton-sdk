package field

import (
	"fmt"

	v1_conf "github.com/conductorone/baton-sdk/pb/c1/config/v1"
)

type fieldOption func(SchemaField) SchemaField

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
			o.Rules.i.IsRequired = required
		case StringVariant:
			if o.Rules.s == nil {
				o.Rules.s = &v1_conf.StringRules{}
			}
			o.Rules.s.IsRequired = required
		case StringSliceVariant:
			if o.Rules.ss == nil {
				o.Rules.ss = &v1_conf.RepeatedStringRules{}
			}
			o.Rules.ss.IsRequired = required
		case StringMapVariant:
			if o.Rules.sm == nil {
				o.Rules.sm = &v1_conf.StringMapRules{}
			}
			o.Rules.sm.IsRequired = required
		default:
			panic(fmt.Sprintf("field %s has unsupported type %s", o.FieldName, o.Variant))
		}
		return o
	}
}

func WithDescription(description string) fieldOption {
	return func(o SchemaField) SchemaField {
		o.Description = description

		return o
	}
}

func WithDisplayName(displayName string) fieldOption {
	return func(o SchemaField) SchemaField {
		o.ConnectorConfig.DisplayName = displayName
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
		o.SyncerConfig.Hidden = hidden
		return o
	}
}

/*
ExportTarget specifies who sets the flag.

	CLI: dev ops invoking the connector, as for service mode, eg flag that specifies a file path on disk
	GUI: C1 tenant admin on the Web.  Inclusive of CLI.  eg service user name
	Ops: C1 support dashbaord. Inclusive of CLI. eg log level
	None: Only currently usable for the default fields.  Those flags are consumed by the Syncer, not the Connector.

	ExportTarget is used by both dynamic conf generation and the conf schema exporting logic
*/
type ExportTarget string

const (
	ExportTargetNone    ExportTarget = "none"
	ExportTargetGUI     ExportTarget = "gui"
	ExportTargetOps     ExportTarget = "ops"
	ExportTargetCLIOnly ExportTarget = "cli"
)

func WithExportTarget(target ExportTarget) fieldOption {
	return func(o SchemaField) SchemaField {
		if o.ExportTarget != ExportTargetGUI && target != o.ExportTarget {
			panic(fmt.Sprintf("target %s would be overwritten by %s for %s", o.ExportTarget, target, o.FieldName))
		}
		o.ExportTarget = target
		return o
	}
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

func WithIsSecret(value bool) fieldOption {
	return func(o SchemaField) SchemaField {
		o.Secret = value

		return o
	}
}

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
	r.rules.IsRequired = required
	return r
}

func (r *StringMapRuler) WithValidateEmpty(validateEmpty bool) *StringMapRuler {
	r.rules.ValidateEmpty = validateEmpty
	return r
}
