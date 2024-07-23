package field

type Relationship int

const (
	RequiredTogether Relationship = iota + 1
	MutuallyExclusive
	AtLeastOne
)

type SchemaFieldRelationshipI interface {
	ValidateConstraint(fieldsPresent map[string]int) error
}

type DependentSchemaFieldRelationship struct {
	RequiredFields  []SchemaField
	DependentFields []SchemaField
}

type SchemaFieldRelationship struct {
	Kind   Relationship
	Fields []SchemaField
}

func (s DependentSchemaFieldRelationship) ValidateConstraint(fieldsPresent map[string]int) error {
	var dependentPresentCount int
	var requiredPresentCount int
	dependentPresent := make([]string, 0)
	requiredNotPresent := make([]string, 0)

	for _, f := range s.DependentFields {
		dependentPresentCount += fieldsPresent[f.FieldName]
		if fieldsPresent[f.FieldName] > 0 {
			dependentPresent = append(dependentPresent, f.FieldName)
		}
	}

	for _, f := range s.RequiredFields {
		requiredPresentCount += fieldsPresent[f.FieldName]
		if fieldsPresent[f.FieldName] == 0 {
			requiredNotPresent = append(requiredNotPresent, f.FieldName)
		}
	}

	if dependentPresentCount >= 1 && requiredPresentCount != len(s.RequiredFields) {
		return makeDependentFieldsError(dependentPresent, requiredNotPresent)
	}

	return nil
}

func (s SchemaFieldRelationship) ValidateConstraint(fieldsPresent map[string]int) error {
	var present int
	for _, f := range s.Fields {
		present += fieldsPresent[f.FieldName]
	}
	if present > 1 && s.Kind == MutuallyExclusive {
		return makeMutuallyExclusiveError(fieldsPresent, s)
	}
	if present > 0 && present < len(s.Fields) && s.Kind == RequiredTogether {
		return makeNeededTogetherError(fieldsPresent, s)
	}
	if present == 0 && s.Kind == AtLeastOne {
		return makeAtLeastOneError(fieldsPresent, s)
	}

	return nil
}

func FieldsRequiredTogether(fields ...SchemaField) SchemaFieldRelationship {
	return SchemaFieldRelationship{
		Kind:   RequiredTogether,
		Fields: fields,
	}
}

func FieldsMutuallyExclusive(fields ...SchemaField) SchemaFieldRelationship {
	return SchemaFieldRelationship{
		Kind:   MutuallyExclusive,
		Fields: fields,
	}
}

func FieldsAtLeastOneUsed(fields ...SchemaField) SchemaFieldRelationship {
	return SchemaFieldRelationship{
		Kind:   AtLeastOne,
		Fields: fields,
	}
}

func FieldsDependentOn(requiredFields []SchemaField, dependentFields []SchemaField) DependentSchemaFieldRelationship {
	return DependentSchemaFieldRelationship{
		RequiredFields:  requiredFields,
		DependentFields: dependentFields,
	}
}
