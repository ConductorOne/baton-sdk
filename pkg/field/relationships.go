package field

type Relationship int

const (
	RequiredTogether Relationship = iota + 1
	MutuallyExclusive
	AtLeastOne
	Dependents
)

type SchemaFieldRelationship struct {
	Kind           Relationship
	Fields         []SchemaField
	ExpectedFields []SchemaField
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

func FieldsDependentOn(dependent []SchemaField, expected []SchemaField) SchemaFieldRelationship {
	return SchemaFieldRelationship{
		Kind:           Dependents,
		Fields:         dependent,
		ExpectedFields: expected,
	}
}
