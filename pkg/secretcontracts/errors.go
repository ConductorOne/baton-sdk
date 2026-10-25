package secretcontracts

import (
	"fmt"
	"regexp"
)

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

var fieldNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

func newError(layer Layer, rule, field string) *Error {
	if !fieldNamePattern.MatchString(field) {
		field = ""
	}
	return &Error{Layer: layer, Rule: rule, Field: field}
}
