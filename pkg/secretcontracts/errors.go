package secretcontracts

import "fmt"

// Layer names the check that rejected a payload.
type Layer string

const (
	LayerUnknownType   Layer = "unknown_type"
	LayerDocument      Layer = "document"
	LayerSchema        Layer = "schema"
	LayerSemantic      Layer = "semantic"
	LayerLimit         Layer = "limit"
	LayerNormalization Layer = "normalization"
	LayerInput         Layer = "input"
)

// Error carries a layer, a rule and a field name, never a value.
type Error struct {
	Layer Layer
	Rule  string
	Field string
}

func (e *Error) Error() string {
	if e.Field == "" {
		return fmt.Sprintf("secret payload %s: %s", e.Layer, e.Rule)
	}
	return fmt.Sprintf("secret payload %s: %s at %s", e.Layer, e.Rule, e.Field)
}

func newError(layer Layer, rule, field string) *Error {
	return &Error{Layer: layer, Rule: rule, Field: declaredFieldName(field)}
}

// declaredFieldName returns field only when some bundled type declares it.
// A key a payload or caller supplied can be secret text that merely looks like
// a name, so it is never reflected.
func declaredFieldName(field string) string {
	b, err := bundle()
	if err != nil {
		return ""
	}
	for _, c := range b.contracts {
		if _, declared := c.properties[field]; declared {
			return field
		}
		if c.TextCodec != nil && c.TextCodec.Field == field {
			return field
		}
	}
	return ""
}
