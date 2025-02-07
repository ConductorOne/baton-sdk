package field

import "encoding/json"

type Relationship int

const (
	Invalid Relationship = iota
	RequiredTogether
	MutuallyExclusive
	AtLeastOne
	Dependents
)

type SchemaFieldRelationship struct {
	Kind           Relationship
	Fields         []SchemaField
	ExpectedFields []SchemaField // Not really expected, just another field bag
}

func listFieldNames(fields []SchemaField) []string {
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.FieldName
	}
	return names
}

type SchemaFieldRelationshipSchema struct {
	Kind            string   `json:"Kind"`
	Fields          []string `json:"Fields"`
	DependentFields []string `json:"DependentFields,omitempty"`
}

func (r SchemaFieldRelationship) MarshalJSON() ([]byte, error) {
	kind := map[Relationship]string{
		Invalid:           "INVALID",
		RequiredTogether:  "REQUIRED_TOGETHER",
		MutuallyExclusive: "MUTUALLY_EXCLUSIVE",
		AtLeastOne:        "AT_LEAST_ONE",
		Dependents:        "DEPENDENT_ON",
	}[r.Kind]

	df := listFieldNames(r.ExpectedFields)
	if len(df) == 0 {
		df = nil
	}
	return json.Marshal(SchemaFieldRelationshipSchema{
		Kind:            kind,
		Fields:          listFieldNames(r.Fields),
		DependentFields: df,
	})
}

func countFieldNames(fields ...SchemaField) int {
	seen := map[string]bool{}
	for _, field := range fields {
		seen[field.FieldName] = true
	}
	return len(seen)
}

// FieldsRequiredTogether - the provided fields are valid if and only if every
// provided field is present.
func FieldsRequiredTogether(fields ...SchemaField) SchemaFieldRelationship {
	count := countFieldNames(fields...)
	if len(fields) > count || count <= 1 {
		return SchemaFieldRelationship{Kind: Invalid}
	}
	return SchemaFieldRelationship{
		Kind:   RequiredTogether,
		Fields: fields,
	}
}

// FieldsMutuallyExclusive - the provided fields are valid if and only if at
// most one field is present.
func FieldsMutuallyExclusive(fields ...SchemaField) SchemaFieldRelationship {
	seen := map[string]bool{}
	for _, field := range fields {
		if field.Required {
			return SchemaFieldRelationship{Kind: Invalid}
		}
		seen[field.FieldName] = true
	}
	if len(fields) > len(seen) || len(seen) <= 1 {
		return SchemaFieldRelationship{Kind: Invalid}
	}
	return SchemaFieldRelationship{
		Kind:   MutuallyExclusive,
		Fields: fields,
	}
}

// FieldsAtLeastOneUsed - the provided fields are valid if and only if at least
// one field is present.
func FieldsAtLeastOneUsed(fields ...SchemaField) SchemaFieldRelationship {
	count := countFieldNames(fields...)
	if len(fields) > count || count <= 1 {
		return SchemaFieldRelationship{Kind: Invalid}
	}
	return SchemaFieldRelationship{
		Kind:   AtLeastOne,
		Fields: fields,
	}
}

// FieldsDependentOn - the provided fields are valid if and only if every field
// in `required` are also present.
func FieldsDependentOn(fields []SchemaField, required []SchemaField) SchemaFieldRelationship {
	seen0 := map[string]bool{}
	for _, field := range fields {
		seen0[field.FieldName] = true
	}
	if len(fields) > len(seen0) ||
		len(seen0) == 0 {
		return SchemaFieldRelationship{Kind: Invalid}
	}

	seen1 := map[string]bool{}
	for _, field := range required {
		if _, ok := seen0[field.FieldName]; ok {
			return SchemaFieldRelationship{Kind: Invalid}
		}
		seen1[field.FieldName] = true
	}

	if len(required) > len(seen1) ||
		len(seen1) == 0 {
		return SchemaFieldRelationship{Kind: Invalid}
	}

	return SchemaFieldRelationship{
		Kind:           Dependents,
		Fields:         fields,
		ExpectedFields: required,
	}
}
