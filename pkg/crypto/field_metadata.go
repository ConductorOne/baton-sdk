package crypto

import (
	"fmt"
	"regexp"
	"strings"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
)

const multipassTypePrefix = "multipass-type-"

var fieldTagPattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
var fieldMIMEPattern = regexp.MustCompile(`^[^/]+/[^/]+$`)

// FieldOutput describes one field's encoding and logical delivery type.
type FieldOutput struct {
	Name        string
	ContentType string
	LogicalType string
	HasType     bool
}

// ResolveFieldOutput validates metadata and resolves the reserved type tag.
func ResolveFieldOutput(field *config.Field) (FieldOutput, error) {
	if field == nil {
		return FieldOutput{}, fmt.Errorf("field is nil")
	}
	result := FieldOutput{Name: field.GetName(), ContentType: field.GetContentType(), LogicalType: "generic"}
	if result.ContentType != "" {
		if len(result.ContentType) > 128 || !fieldMIMEPattern.MatchString(result.ContentType) {
			return FieldOutput{}, fmt.Errorf("incompatible field content type: expected MIME type")
		}
		switch field.WhichField() {
		case config.Field_StringField_case, config.Field_StringSliceField_case, config.Field_StringMapField_case:
		default:
			return FieldOutput{}, fmt.Errorf("field content type requires a string field")
		}
	}
	if len(field.GetTags()) > 16 {
		return FieldOutput{}, fmt.Errorf("field has too many tags")
	}
	seen := make(map[string]bool, len(field.GetTags()))
	for _, tag := range field.GetTags() {
		if len(tag) > 143 || !fieldTagPattern.MatchString(tag) || seen[tag] {
			return FieldOutput{}, fmt.Errorf("invalid or duplicate field tag")
		}
		seen[tag] = true
		if strings.HasPrefix(tag, multipassTypePrefix) {
			value := strings.TrimPrefix(tag, multipassTypePrefix)
			if result.HasType || !ValidSecretContentType(value) {
				return FieldOutput{}, fmt.Errorf("invalid or ambiguous logical type tag")
			}
			result.LogicalType, result.HasType = value, true
		}
	}
	return result, nil
}
